"""
Base spider for e-commerce sites with three listing pagination modes.

  URL    : every listing page has its own URL (?page=N, ?start=N, /page/N)
           or a "next" link. One warm browser walks the pages.
  SCROLL : one listing URL; products are appended while you scroll.
           Handles IntersectionObserver sentinels, scroll-event loaders,
           inner scroll containers, virtualised grids and "hybrid" pages
           that show a Load-more button after a few scrolls.
  CLICK  : one listing URL; a "Load more" button (CSS **or XPath**)
           appends the next batch (e.g. 32 products, click, 32 more...).

Subclass contract
-----------------
Required (all modes):
    name, BASE_URL, START_PATH, PAGINATION_MODE,
    LISTING_READY_SELECTOR, PRODUCT_READY_SELECTOR,
    parse_listing_page(resp) -> Iterable[str]      @staticmethod
    extract_product(resp, url) -> dict | None      @staticmethod

Per mode:
    URL    : LISTING_URL_TEMPLATE using {page}, {page_idx} or {start}
             -- or LISTING_URL_BUILDER, or LISTING_NEXT_PAGE_XPATH
    SCROLL : LISTING_ITEM_SELECTOR
    CLICK  : LISTING_ITEM_SELECTOR, LOAD_MORE_BUTTON_SELECTOR

Every *_SELECTOR accepts CSS or XPath ("//button[...]", "(//a)[1]",
"xpath=..."). LISTING_LINK_SELECTOR is evaluated *inside* each item, so
an XPath there must be relative (".//a[@href]").

Template placeholders (URL mode)
    {base_url} {start_path}
    {page_idx}  0, 1, 2 ...
    {page}      FIRST_PAGE_NUMBER + page_idx       (1, 2, 3 ...)
    {start}     START_OFFSET + page_idx * PAGE_STEP (0, 24, 48 ...)

How SCROLL / CLICK collect URLs
-------------------------------
Product links are harvested from the live DOM *during* pagination and
streamed to the parent every round, so virtualised lists (which delete
cards that scrolled out of view) still yield every product, and a crash
half-way keeps everything collected so far. The canonical
parse_listing_page() is still run on the final DOM and both sets are
merged.

PICKLE SAFETY
-------------
cfg dicts passed to child processes contain only primitives, tuples,
plain dicts and module-level / @staticmethod callables. Always read
callables with `type(self).<name>`. page_action partials are built
inside the child.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
import multiprocessing
import queue as queue_mod
import random
import signal
import threading
import time
import traceback
from enum import Enum
from multiprocessing.connection import wait as mp_wait
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional, Set
from urllib.parse import urldefrag, urljoin, urlsplit, urlunsplit

import scrapy
from scrapling.fetchers import StealthySession


# ============================================================
# Pagination modes
# ============================================================
class PaginationMode(str, Enum):
    URL = "url"
    SCROLL = "scroll"
    CLICK = "click"
    # aliases so older spiders keep working
    INFINITY_SCROLL = "scroll"
    LOAD_MORE = "click"


# Bumped whenever spiders rely on something new in this file. waffir_store.py
# checks it so an out-of-date copy fails with a clear message.
BASE_SPIDER_VERSION = 5

END_OF_LIST_STATUSES = {404, 410}
PERMANENT_PRODUCT_STATUSES = {404, 410}
THROTTLE_STATUSES = {403, 429, 503}

# Third-party scripts that commonly re-process every product card on each
# scroll (impression tracking, heatmaps, recommendation widgets). Plain text:
# a request is blocked when its URL contains one of these.
COMMON_TRACKER_DOMAINS = (
    "googletagmanager.com", "google-analytics.com", "doubleclick.net",
    "connect.facebook.net", "facebook.com/tr", "analytics.tiktok.com",
    "sc-static.net", "snapchat.com", "hotjar.com", "clarity.ms",
    "criteo.com", "criteo.net", "bat.bing.com", "segment.io", "cdn.segment.com",
    "useinsider.com", "insider-cdn", "klaviyo.com", "braze.com",
    "appsflyer.com", "branch.io", "newrelic.com", "nr-data.net",
    "datadoghq", "sentry.io", "fullstory.com", "mouseflow.com",
    "optimizely.com", "dynamicyield.com", "yotpo.com",
)

# Old name, kept so an older spider file that still imports it doesn't stop
# Scrapy from loading (Scrapy imports every spider before running any).
COMMON_TRACKER_PATTERNS = COMMON_TRACKER_DOMAINS

_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_LOWER = "abcdefghijklmnopqrstuvwxyz"


def _xpath_literal(value: str) -> str:
    """Quote a string for use inside an XPath expression."""
    if "'" not in value:
        return f"'{value}'"
    if '"' not in value:
        return f'"{value}"'
    parts = value.split("'")
    return "concat(" + ", \"'\", ".join(f"'{p}'" for p in parts) + ")"


def xpath_text_with_number(*words: str) -> str:
    """XPath to the first element whose own text contains one of `words`
    (case-insensitive) and whose text contains a digit - e.g. the element
    showing "Showing 1-30 of 686" or "2744 products"."""
    tests = " or ".join(
        f"contains(translate(., '{_UPPER}', '{_LOWER}'), "
        f"{_xpath_literal(w.lower())})" for w in words if w
    )
    return (f"(//*[text()[{tests}]]"
            f"[translate(., '0123456789', '') != .])[1]")


_LC = ("translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
       "'abcdefghijklmnopqrstuvwxyz')")
GENERIC_LOAD_MORE_XPATH = (
    f"//button[not(@disabled)][contains({_LC},'load more') or "
    f"contains({_LC},'show more') or contains({_LC},'view more')]"
)


# ============================================================
# Small helpers (module level => picklable)
# ============================================================

def _is_xpath(sel: str) -> bool:
    s = (sel or "").strip()
    return s.startswith(("xpath=", "/", "(", "./", "../"))


def _pw_selector(sel: str) -> str:
    """Make any CSS/XPath string usable with page.locator()."""
    s = (sel or "").strip()
    if s.startswith(("xpath=", "css=", "text=", "id=")):
        return s
    return "xpath=" + s if _is_xpath(s) else s


def _short_err(e: BaseException, limit: int = 300) -> str:
    msg = f"{type(e).__name__}: {e}"
    return msg if len(msg) <= limit else msg[:limit] + "..."


def _sender(conn) -> Callable[[dict], None]:
    def send(msg: dict) -> None:
        try:
            conn.send(msg)
        except Exception:
            pass
    return send


def _filter_session_kwargs(kwargs: dict, log=None) -> dict:
    """Drop kwargs this Scrapling version does not support."""
    try:
        params = inspect.signature(StealthySession.__init__).parameters
    except (TypeError, ValueError):
        return kwargs
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return kwargs
    kept = {k: v for k, v in kwargs.items() if k in params}
    dropped = sorted(set(kwargs) - set(kept))
    if dropped and log:
        log(f"StealthySession does not accept {dropped}; ignoring them")
    return kept


def _to_number(raw: str) -> Optional[float]:
    """'2,744' 2744 | '1,299.50' 1299.5 | '299,00' 299.0 | '1.299,00' 1299.0"""
    raw = raw.strip(",.")
    if not raw:
        return None
    if "," in raw and "." in raw:                 # the LAST one is the decimal
        dec, thou = (",", ".") if raw.rfind(",") > raw.rfind(".") else (".", ",")
        raw = raw.replace(thou, "").replace(dec, ".")
    elif "," in raw:
        head, _, tail = raw.rpartition(",")
        # one comma followed by 1-2 digits = decimal comma ("299,00")
        raw = (head.replace(",", "") + "." + tail) if (raw.count(",") == 1
                                                       and len(tail) in (1, 2)) \
            else raw.replace(",", "")
    elif raw.count(".") > 1:                      # "1.299.000" = thousands dots
        raw = raw.replace(".", "")
    try:
        return float(raw)
    except ValueError:
        return None


def _numbers_in(text) -> List[float]:
    """All numbers in a text. Character scan - digits plus , and . inside."""
    out: List[float] = []
    token = ""
    for ch in str(text or "") + " ":
        if ch.isdigit() and ch.isascii():
            token += ch
        elif ch in ",." and token:
            token += ch
        else:
            if token:
                value = _to_number(token)
                if value is not None:
                    out.append(value)
            token = ""
    return out


def _as_tuple(value) -> tuple:
    if not value:
        return ()
    return (value,) if isinstance(value, str) else tuple(value)


def _url_allowed(url: str, contains, excludes=()) -> bool:
    """Every `contains` text must be in the URL, no `excludes` text may be."""
    return (all(c in url for c in _as_tuple(contains))
            and not any(x in url for x in _as_tuple(excludes)))


def _normalize_url(cfg: dict, raw) -> Optional[str]:
    if not raw:
        return None
    u = urljoin(cfg["base_url"], str(raw).strip())
    if not u.startswith(("http://", "https://")):
        return None
    u, _ = urldefrag(u)
    if cfg["strip_query"]:
        p = urlsplit(u)
        u = urlunsplit((p.scheme, p.netloc, p.path, "", ""))
    if not _url_allowed(u, cfg["url_contains"], cfg["url_excludes"]):
        return None
    hook = cfg.get("normalize_url")
    if hook is not None:
        u = hook(u)
    return u or None


def _clean_urls(cfg: dict, urls: Optional[Iterable]) -> Set[str]:
    out: Set[str] = set()
    for raw in urls or ():
        u = _normalize_url(cfg, raw)
        if u:
            out.add(u)
    return out


def _past_deadline(cfg: dict) -> bool:
    """True once the listing time budget (LISTING_TIME_BUDGET_S) is used up."""
    deadline = cfg.get("listing_deadline") or 0
    return bool(deadline) and time.time() >= deadline


def _build_listing_url(cfg: dict, page_idx: int) -> str:
    # A specific listing (e.g. a category): page 1 is the URL itself, later
    # pages set LISTING_PAGE_PARAM on it ("page={page}" -> ?page=2 ...).
    listing = cfg.get("listing_url")
    if listing and cfg["mode"] == "url":
        param = cfg.get("page_param")
        if page_idx == 0 or not param:
            return listing
        return _set_query_param(listing, param.format(
            page=page_idx + cfg["first_page_number"], page_idx=page_idx,
            start=cfg["start_offset"] + page_idx * cfg["page_step"]))
    builder = cfg.get("listing_url_builder")
    if builder is not None:
        return builder(cfg, page_idx)
    return cfg["listing_url_template"].format(
        base_url=cfg["base_url"],
        start_path=cfg["start_path"],
        page_idx=page_idx,
        page=page_idx + cfg["first_page_number"],
        start=cfg["start_offset"] + page_idx * cfg["page_step"],
    )


def _backoff(attempt: int, status, base: float, cap: float) -> float:
    if status is None or status in THROTTLE_STATUSES:
        wait = min(cap, base * (2 ** (attempt - 1)))   # exponential
    else:
        wait = min(cap, base * attempt / 2)            # gentle linear
    return wait + random.uniform(0, base / 2)


def _fetch_with_retry(session, url, attempts, log, label, base_wait, cap_wait):
    """-> (resp | None, last_status, last_error). No sleep after last try."""
    status, err = None, None
    for attempt in range(1, attempts + 1):
        try:
            resp = session.fetch(url)
            status = getattr(resp, "status", None)
            if status == 200:
                return resp, status, None
            err = f"HTTP {status}"
            if status in END_OF_LIST_STATUSES:
                return None, status, err
        except Exception as e:
            status, err = None, _short_err(e)
        if attempt < attempts:
            wait = _backoff(attempt, status, base_wait, cap_wait)
            log(f"{label} attempt {attempt}/{attempts} failed ({err}); "
                f"retrying in {wait:.1f}s")
            time.sleep(wait)
    return None, status, err


def _extract_next(resp, current_url: str, xpath: str) -> Optional[str]:
    try:
        values = resp.xpath(xpath).getall()
    except Exception:
        return None
    for v in values:
        v = str(v).strip()
        if v and not v.startswith(("#", "javascript:")):
            return urljoin(current_url, v)
    return None


def _ignore_sigint() -> None:
    """Ctrl+C on Windows hits every process in the console. Children ignore
    it so the parent can shut them down cleanly instead of half-dying."""
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except Exception:
        pass


def _missing_fields(product, required) -> List[str]:
    if not isinstance(product, dict):
        return ["<extract_product returned no dict>"]
    return [f for f in required if not product.get(f)]


# ---------- JSON-LD / price helpers used by subclasses ----------

def _iter_ld_nodes(data):
    if isinstance(data, list):
        for x in data:
            yield from _iter_ld_nodes(x)
    elif isinstance(data, dict):
        yield data
        if "@graph" in data:
            yield from _iter_ld_nodes(data["@graph"])


def _jsonld_product(resp) -> dict:
    try:
        blobs = resp.xpath("//script[@type='application/ld+json']/text()").getall()
    except Exception:
        return {}
    for raw in blobs:
        try:
            data = json.loads(str(raw).strip())
        except Exception:
            continue
        for node in _iter_ld_nodes(data):
            t = node.get("@type")
            types = t if isinstance(t, list) else [t]
            if not ({"Product", "ProductGroup"} & set(types)):
                continue
            offers = node.get("offers") or {}
            if not offers and node.get("hasVariant"):
                variants = node["hasVariant"]
                first = variants[0] if isinstance(variants, list) and variants else {}
                offers = first.get("offers") or {} if isinstance(first, dict) else {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            image = node.get("image")
            if isinstance(image, list):
                image = image[0] if image else None
            if isinstance(image, dict):
                image = image.get("url")
            brand = node.get("brand")
            if isinstance(brand, dict):
                brand = brand.get("name")
            price = offers.get("price", offers.get("lowPrice"))
            return {
                "name": node.get("name"),
                "price": None if price is None else str(price),
                "currency": offers.get("priceCurrency"),
                "availability": offers.get("availability"),
                "image": image,
                "brand": brand,
                "sku": node.get("sku"),
            }
    return {}


def _parse_price(value) -> Optional[float]:
    nums = _numbers_in(value)
    return nums[0] if nums else None


# ============================================================
# In-browser JS (runs inside the child via page.evaluate)
# ============================================================

_JS_LIB = r"""
const __all = (sel, root) => {
  if (!sel) return [];
  root = root || document;
  let s = String(sel).trim();
  let xp = null;
  if (s.startsWith('css=')) { s = s.slice(4); }
  else if (s.startsWith('xpath=')) { xp = s.slice(6); }
  else if (s[0] === '/' || s[0] === '(' || s.startsWith('./') || s.startsWith('../')) { xp = s; }
  if (xp === null) {
    try { return Array.from(root.querySelectorAll(s)); } catch (e) { return []; }
  }
  const out = [];
  try {
    const r = document.evaluate(xp, root, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
    for (let i = 0; i < r.snapshotLength; i++) out.push(r.snapshotItem(i));
  } catch (e) {}
  return out;
};
"""

# Returns item count + hrefs of cards from index `from` onwards. The DOM is
# NOT mutated (attribute writes can trigger the site's MutationObservers on
# every card). Python dedups; a periodic full scan (from=0) catches
# virtualised lists that recycle nodes. `lighten` injects ONE <style> that
# puts content-visibility:auto on cards, so the browser stops laying out /
# painting the hundreds of cards that are off-screen.
_HARVEST_JS = "(args) => {" + _JS_LIB + r"""
  const itemSel = args[0], linkSel = args[1], from = args[2] || 0, lighten = args[3];
  if (lighten && !document.getElementById('__wsh_style')) {
    const s = String(itemSel).trim();
    const isX = s.startsWith('xpath=') || s[0] === '/' || s[0] === '(';
    if (!isX) {
      const st = document.createElement('style');
      st.id = '__wsh_style';
      st.textContent = (s.startsWith('css=') ? s.slice(4) : s) +
        '{content-visibility:auto;contain-intrinsic-size:auto 420px;}';
      (document.head || document.documentElement).appendChild(st);
    }
  }
  const items = __all(itemSel);
  const hrefs = [];
  for (let i = Math.min(from, items.length); i < items.length; i++) {
    const el = items[i];
    let a = null;
    if (linkSel) a = __all(linkSel, el)[0] || null;
    if (!a) {
      if (el.tagName === 'A' && el.getAttribute('href')) a = el;
      else a = el.querySelector('a[href]') || (el.closest ? el.closest('a[href]') : null);
    }
    if (!a) continue;
    const href = a.href || a.getAttribute('href');
    if (href) hrefs.push(href);
  }
  const loading = args[4] ? __all(args[4]).length : 0;
  return {count: items.length, hrefs: hrefs, loading: loading};
}"""

# One scroll step. Strategies rotate when the list stalls:
#  into_view : bring sentinel / last card into the viewport (fires
#              IntersectionObserver - jumping past it never does)
#  bottom    : absolute bottom of window + inner container
#  bounce    : back up ~3 viewports, then wheel down again
_SCROLL_JS = "(args) => {" + _JS_LIB + r"""
  const itemSel = args[0], sentinelSel = args[1], containerSel = args[2], strategy = args[3];
  const items = __all(itemSel);
  const last = items.length ? items[items.length - 1] : null;
  const sentinel = sentinelSel ? (__all(sentinelSel)[0] || null) : null;
  let box = containerSel ? (__all(containerSel)[0] || null) : null;
  if (!box && last) {
    let p = last.parentElement;
    while (p && p !== document.body && p !== document.documentElement) {
      const oy = getComputedStyle(p).overflowY;
      if ((oy === 'auto' || oy === 'scroll') && p.scrollHeight > p.clientHeight + 20) { box = p; break; }
      p = p.parentElement;
    }
  }
  const docH = Math.max(document.body ? document.body.scrollHeight : 0,
                        document.documentElement.scrollHeight);
  const vh = window.innerHeight || 800;
  if (strategy === 'into_view') {
    const target = sentinel || last;
    if (target) target.scrollIntoView({block: 'end', inline: 'nearest'});
    else window.scrollTo(0, docH);
  } else if (strategy === 'bottom') {
    if (box) box.scrollTop = box.scrollHeight;
    window.scrollTo(0, docH);
  } else {
    if (box) box.scrollTop = Math.max(0, box.scrollHeight - box.clientHeight * 3);
    window.scrollTo(0, Math.max(0, docH - vh * 3));
  }
  return {count: items.length, sentinel: !!sentinel};
}"""

_COUNT_JS = "(sel) => {" + _JS_LIB + "return __all(sel).length; }"
_TEXT_JS = ("(sel) => {" + _JS_LIB +
            "const el = __all(sel)[0]; "
            "return el ? (el.innerText || el.textContent || '') : ''; }")


# ============================================================
# In-browser pagination driver (SCROLL + CLICK)
# ============================================================

class _Harvester:
    FULL_SCAN_EVERY = 5     # every Nth poll rescans all cards
    OVERLAP = 64            # incremental scans start this far back

    def __init__(self, page, cfg: dict, send):
        self.page, self.cfg, self.send = page, cfg, send
        self.urls: Set[str] = set()
        self.count = 0
        self.loading = 0
        self.last_eval_ms = 0.0
        self.eval_ms_sum = 0.0
        self.evals = 0
        self._polls = 0
        self._pending: List[str] = []
        self._batch = 0

    def poll(self, full: bool = False) -> int:
        self._polls += 1
        full = full or self._polls % self.FULL_SCAN_EVERY == 0
        start_idx = 0 if full else max(0, self.count - self.OVERLAP)
        t0 = time.monotonic()
        try:
            res = self.page.evaluate(_HARVEST_JS, [
                self.cfg["item_selector"], self.cfg["link_selector"],
                start_idx, self.cfg["lighten_dom"], self.cfg["loading_selector"],
            ])
        except Exception:
            # usually "execution context destroyed" after a navigation
            try:
                self.page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass
            return self.count
        finally:
            self.last_eval_ms = (time.monotonic() - t0) * 1000
            self.eval_ms_sum += self.last_eval_ms
            self.evals += 1
        self.count = int(res.get("count") or 0)
        self.loading = int(res.get("loading") or 0)
        if self.cfg["harvest"]:
            for href in res.get("hrefs") or ():
                u = _normalize_url(self.cfg, href)
                if u and u not in self.urls:
                    self.urls.add(u)
                    self._pending.append(u)
        return self.count

    def avg_eval_ms(self) -> float:
        return self.eval_ms_sum / self.evals if self.evals else 0.0

    def flush(self) -> None:
        if self._pending:
            self.send({"type": "page", "start": self._batch, "urls": self._pending})
            self._batch += 1
            self._pending = []


def _js_count(page, sel: str) -> int:
    if not sel:
        return 0
    try:
        return int(page.evaluate(_COUNT_JS, sel) or 0)
    except Exception:
        return 0


def _read_total(page, cfg: dict) -> Optional[int]:
    """Product total shown by the site ("Showing 1-30 of 686"): the largest
    number in the text of LISTING_TOTAL_COUNT_SELECTOR / _TEXT's element."""
    sel = cfg["total_count_selector"]
    if not sel:
        return None
    try:
        txt = page.evaluate(_TEXT_JS, sel) or ""
    except Exception:
        return None
    return _total_from_text(txt)


def _ping_ms(page) -> float:
    """How long the page takes to answer a no-op. Large = page main thread
    is busy (site JS), not our code and not the network."""
    t0 = time.monotonic()
    try:
        page.evaluate("() => 1")
    except Exception:
        pass
    return (time.monotonic() - t0) * 1000


def _set_query_param(url: str, param: str) -> str:
    """Set/replace one `key=value` in a URL's query string."""
    key, _, value = param.partition("=")
    p = urlsplit(url)
    pairs = [kv for kv in p.query.split("&") if kv and kv.split("=", 1)[0] != key]
    pairs.append(f"{key}={value}")
    return urlunsplit((p.scheme, p.netloc, p.path, "&".join(pairs), p.fragment))


def _segment_url(cfg: dict, abs_pos: int) -> str:
    step = max(1, cfg["page_step"])
    start = (abs_pos // step) * step
    idx = start // step
    if cfg.get("segment_param"):
        # e.g. "pi={page}" appended to whatever listing is being crawled
        # (start URL or a category) - works for every category automatically
        param = cfg["segment_param"].format(
            start=cfg["start_offset"] + start, page_idx=idx,
            page=idx + cfg["first_page_number"])
        return _set_query_param(cfg.get("listing_url") or
                                _build_listing_url(cfg, 0), param)
    return cfg["segment_url_template"].format(
        base_url=cfg["base_url"], start_path=cfg["start_path"],
        start=cfg["start_offset"] + start, page_idx=idx,
        page=idx + cfg["first_page_number"],
    )


def _goto_listing(page, url: str, cfg: dict, log,
                  timeout_ms: Optional[int] = None) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded",
                  timeout=timeout_ms or cfg["nav_timeout_ms"])
    except Exception as e:
        log(f"navigation to {url} failed ({_short_err(e, 150)})")
        return False
    try:
        page.wait_for_selector(_pw_selector(cfg["item_selector"]), state="attached",
                               timeout=cfg["initial_wait_ms"])
        return True
    except Exception:
        log(f"no product cards at {url}")
        return False


_XPATH_VALUES_JS = r"""
(xp) => {
  const out = [];
  try {
    const r = document.evaluate(xp, document, null,
                                XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
    for (let i = 0; i < r.snapshotLength; i++) {
      const n = r.snapshotItem(i);
      if (n.nodeType === 2) out.push(n.value);                 // @href
      else if (n.href) out.push(n.href);                       // <a>
      else if (n.getAttribute && n.getAttribute('href')) out.push(n.getAttribute('href'));
    }
  } catch (e) {}
  return out;
}
"""


def _discover_categories(page, cfg: dict, send, log) -> None:
    """Collect category listing URLs from the start page (LISTING_CATEGORY_XPATH)."""
    try:
        raw = page.evaluate(_XPATH_VALUES_JS, cfg["category_xpath"]) or []
    except Exception as e:
        log(f"category discovery failed ({_short_err(e, 120)})")
        return
    here = page.url
    host = urlsplit(cfg["base_url"]).netloc
    must = cfg["category_url_contains"]
    found, rejected = [], 0
    for v in raw:
        u, _ = urldefrag(urljoin(here, str(v).strip()))
        if not u.startswith("http") or urlsplit(u).netloc != host or u == here:
            continue
        if not _url_allowed(u, must):
            rejected += 1
            continue
        if u not in found:
            found.append(u)
    log(f"categories: {len(found)} kept, {rejected} rejected by "
        f"LISTING_CATEGORY_URL_CONTAINS, {len(raw)} links matched the XPath")
    if found:
        send({"type": "categories", "urls": found})


def _install_blocking(page, cfg: dict, log) -> None:
    """Abort tracker / analytics requests (they often re-process every card
    on each scroll), then reload so already-loaded scripts are gone too."""
    domains = tuple(d.lower() for d in cfg["block_domains"])
    if not domains:
        return
    try:
        page.context.route(lambda url: any(d in url.lower() for d in domains),
                           lambda route: route.abort())
    except Exception as e:
        log(f"could not install request blocking ({_short_err(e, 150)})")
        return
    try:
        page.reload(wait_until="domcontentloaded", timeout=cfg["nav_timeout_ms"])
        page.wait_for_selector(_pw_selector(cfg["item_selector"]), state="attached",
                               timeout=cfg["initial_wait_ms"])
        log(f"blocking {len(domains)} tracker domains; page reloaded")
    except Exception as e:
        log(f"reload after blocking failed ({_short_err(e, 150)})")


def _center_mouse(page) -> None:
    """Wheel events hit the element under the pointer - put it mid-page."""
    try:
        size = page.evaluate("() => [window.innerWidth, window.innerHeight]")
        page.mouse.move(size[0] / 2, size[1] / 2)
    except Exception:
        pass


def _dismiss_popups(page, selectors) -> None:
    for sel in selectors or ():
        try:
            loc = page.locator(_pw_selector(sel)).first
            if loc.is_visible():
                loc.click(timeout=1500)
        except Exception:
            pass


def _scroll_step(page, cfg: dict, strategy: str) -> dict:
    try:
        info = page.evaluate(_SCROLL_JS, [
            cfg["item_selector"], cfg["end_sentinel_selector"],
            cfg["container_selector"], strategy,
        ]) or {}
    except Exception:
        info = {}
    try:  # real wheel events: some sites only listen to these
        if strategy == "bounce":
            page.wait_for_timeout(250)
            for _ in range(3):
                page.mouse.wheel(0, 700)
                page.wait_for_timeout(120)
        else:
            page.mouse.wheel(0, 600)
    except Exception:
        pass
    return info


def _click_load_more(page, selector: str, wait_ms: int) -> str:
    """-> 'clicked' | 'gone' | 'failed'."""
    if not selector:
        return "gone"
    loc = page.locator(_pw_selector(selector)).first
    try:
        loc.wait_for(state="visible", timeout=wait_ms)
    except Exception:
        return "gone"
    # a button that is disabled while loading gets ~3s to re-enable
    for _ in range(10):
        try:
            if not loc.is_disabled(timeout=2000):
                break
        except Exception:
            break
        page.wait_for_timeout(300)
    else:
        return "gone"
    try:
        loc.scroll_into_view_if_needed(timeout=3000)
    except Exception:
        pass
    attempts = (
        lambda: loc.click(timeout=5000),
        lambda: loc.click(timeout=3000, force=True),   # overlay in the way
        lambda: loc.evaluate("e => e.click()", timeout=5000),  # last resort
    )
    for attempt in attempts:
        try:
            attempt()
            return "clicked"
        except Exception:
            continue
    return "failed"


def _wait_for_growth(h: _Harvester, prev_count: int, prev_urls: int,
                     timeout_ms: int, poll_ms: int) -> bool:
    """Poll until new cards/URLs appear. While the site's loading indicator
    is visible the deadline is extended (slow API != end of list). The poll
    interval backs off when the page is slow to answer, so we don't pile
    work onto an already busy main thread."""
    deadline = time.monotonic() + timeout_ms / 1000.0
    hard_deadline = deadline + 3 * timeout_ms / 1000.0
    while True:
        h.poll()
        if len(h.urls) > prev_urls or h.count > prev_count:
            return True
        if _past_deadline(h.cfg):
            return False
        now = time.monotonic()
        if h.loading and now < hard_deadline:
            deadline = max(deadline, now + 1.0)
        if now >= deadline:
            return False
        h.page.wait_for_timeout(int(max(poll_ms, min(5000, 2 * h.last_eval_ms))))


_UNFINISHED_REASONS = {"time_budget", "segment_blocked", "browser_closed",
                       "driver_error", "max_rounds", "items_never_appeared"}


class _StopPagination(Exception):
    pass


COMPLETE_REASONS = {"end_sentinel_gone", "reached_expected_total", "button_gone"}
PROBABLE_REASONS = {"no_growth_confirmed", "segment_past_end", "segment_no_new"}
INCOMPLETE_REASONS = {"time_budget", "max_rounds", "driver_error",
                      "items_never_appeared", "browser_closed",
                      "segment_blocked"}


def _paginate_listing(page, cfg: dict, send, stats: dict):
    """Scrapling page_action for SCROLL and CLICK. Always returns page.

    With SCROLL_SEGMENT_URL_TEMPLATE set, the list is crawled in segments:
    scroll until the segment is full / the page gets slow / it stalls, then
    reload at the absolute position reached and continue on a fresh page.
    """
    mode = cfg["mode"]

    def log(m):
        send({"type": "log", "msg": f"[{mode}] {m}"})

    h = _Harvester(page, cfg, send)
    started = time.monotonic()
    reason = "driver_error"
    total = None
    rnd = 0
    seg = {"enabled": bool(cfg["segment_url_template"] or cfg.get("segment_param")),
           "verified": False,
           "start": 0, "n": 1, "max_pos": 0}
    # URLs survive Scrapling's internal retries, so totals stay cumulative
    h.urls = stats.setdefault("urls_seen", set())

    def switch_segment(why: str, pos: Optional[int] = None) -> str:
        """-> 'ok' | 'past_end' | 'no_new' | 'unsupported' | 'blocked'."""
        abs_pos = seg["start"] + h.count if pos is None else pos
        url = _segment_url(cfg, abs_pos)
        before = len(h.urls)
        seg["max_pos"] = max(seg["max_pos"], abs_pos)
        log(f"segment {seg['n'] + 1} ({why}): position {abs_pos} -> {url}")
        if not seg["verified"]:
            # probe: short timeout; a failure means "this site doesn't do
            # page params" - never keep working on a half-loaded page
            loaded = _goto_listing(page, url, cfg, log,
                                   timeout_ms=cfg["probe_timeout_ms"])
            if not loaded:
                seg["enabled"] = False
                seg["start"] = 0
                back = cfg.get("listing_url") or _build_listing_url(cfg, 0)
                log("segment URL did not load -> segments disabled; "
                    f"returning to {back}")
                if _goto_listing(page, back, cfg, log):
                    _center_mouse(page)
                    _dismiss_popups(page, cfg["popup_selectors"])
                    h.count = 0
                    h.poll(full=True)
                    h.flush()
                else:
                    log("could not reload the listing after a failed probe")
                return "unsupported"
        else:
            loaded = _goto_listing(page, url, cfg, log)
        # An empty page BEFORE the site's own total is a throttle/block, not
        # the end of the list: wait and try the same position again.
        if not loaded and total and abs_pos < total:
            for wait_s in cfg["segment_retry_waits"]:
                log(f"position {abs_pos} < total {total}: looks throttled, "
                    f"retrying in {wait_s}s")
                page.wait_for_timeout(int(wait_s * 1000))
                loaded = _goto_listing(page, url, cfg, log)
                if loaded:
                    break
            if not loaded:
                return "blocked"
        if not loaded:
            return "past_end"
        _center_mouse(page)
        _dismiss_popups(page, cfg["popup_selectors"])
        h.count = 0
        h.poll(full=True)
        h.flush()
        new = len(h.urls) - before
        step = max(1, cfg["page_step"])
        if new == 0:
            if not seg["verified"]:
                # site ignores the offset param: we are on page 1 again
                seg["enabled"] = False
                seg["start"] = 0
                log("segment URL returned no new products -> the site ignores "
                    "it; segments disabled, continuing with plain scrolling")
                return "unsupported"
            return "no_new"
        seg["verified"] = True
        seg["start"] = (abs_pos // step) * step
        seg["n"] += 1
        log(f"segment {seg['n']} loaded: +{new} new, total urls={len(h.urls)}")
        return "ok"

    # Scrapling retries a fetch internally (e.g. when it can't build the
    # response after our navigations) and calls page_action AGAIN. Without
    # this guard the whole listing restarts from position 0.
    stats["entries"] = stats.get("entries", 0) + 1
    if stats.get("done"):
        log("page_action called again by a Scrapling retry; pagination "
            "already finished - skipping")
        return page
    resume_pos = stats.get("resume_pos") if stats.get("seg_verified") else None
    if stats["entries"] > cfg["max_resumes"] + 1:
        log("too many resumes; stopping")
        return page

    try:
        try:
            page.wait_for_selector(_pw_selector(cfg["item_selector"]),
                                   state="attached",
                                   timeout=cfg["initial_wait_ms"])
        except Exception as e:
            reason = "items_never_appeared"
            log(f"items never appeared ({type(e).__name__}); "
                f"check LISTING_ITEM_SELECTOR / blocking")
            return page

        _install_blocking(page, cfg, log)
        _dismiss_popups(page, cfg["popup_selectors"])
        _center_mouse(page)
        h.poll(full=True)
        h.flush()
        total = _read_total(page, cfg)
        if cfg.get("auto_page_step") and h.count and not stats.get("page_step"):
            # first load shows exactly one page -> that is the page size
            stats["page_step"] = h.count
        if stats.get("page_step"):
            cfg["page_step"] = stats["page_step"]
        log(f"initial: cards={h.count} urls={len(h.urls)} expected_total={total} "
            f"segments={'on' if seg['enabled'] else 'off'} "
            f"page_step={cfg['page_step']}")
        if cfg.get("discover_categories") and stats["entries"] == 1:
            try:                                    # never let discovery
                _discover_categories(page, cfg, send, log)   # kill the listing
            except Exception as e:
                log(f"category discovery failed ({_short_err(e, 120)})")

        if seg["enabled"] and resume_pos:
            # continue where the previous attempt stopped
            seg["verified"] = True
            log(f"resuming at position {resume_pos} (attempt {stats['entries']})")
            if switch_segment("resume", pos=resume_pos) != "ok":
                reason = "segment_blocked"
                raise _StopPagination()
        elif seg["enabled"]:
            # Probe the segment URL right away (one cheap navigation) so we
            # never re-scroll hundreds of cards to find out it's unsupported.
            switch_segment("probe")

        sentinel = cfg["end_sentinel_selector"]
        button = cfg["button_selector"]
        stall_button = button or (
            GENERIC_LOAD_MORE_XPATH
            if (mode == "scroll" and cfg["generic_button"]) else ""
        )
        strategies = ("into_view", "bottom", "bounce")
        sentinel_seen = _js_count(page, sentinel) > 0
        stall = 0
        confirmed = False
        reason = "max_rounds"

        while rnd < cfg["max_rounds"]:
            rnd += 1
            if time.monotonic() - started > cfg["max_seconds"] or _past_deadline(cfg):
                reason = "time_budget"
                break
            if total and len(h.urls) >= total:
                reason = "reached_expected_total"
                break

            ping = _ping_ms(page)

            # proactive segment switch: keep the page small and fast
            if seg["enabled"] and stall == 0:
                why = None
                if h.count >= cfg["segment_max_items"]:
                    why = f"segment full ({h.count} cards)"
                elif ping > cfg["slow_ping_ms"]:
                    why = f"page slow (ping {ping:.0f}ms)"
                if why:
                    res = switch_segment(why)
                    if res in ("past_end", "no_new", "blocked"):
                        reason = f"segment_{res}"
                        break
                    sentinel_seen = _js_count(page, sentinel) > 0
                    continue

            t_round = time.monotonic()
            prev_count, prev_urls = h.count, len(h.urls)

            if mode == "click":
                _scroll_step(page, cfg, "into_view")       # reveal the button
                res = _click_load_more(page, button, cfg["button_wait_ms"])
                if res == "gone":
                    h.poll(full=True)
                    if seg["enabled"]:
                        sres = switch_segment("button gone")
                        if sres == "ok":
                            continue
                    reason = "button_gone"
                    break
            else:
                info = _scroll_step(page, cfg, strategies[stall % len(strategies)])
                sentinel_seen = sentinel_seen or bool(info.get("sentinel"))
                if stall >= 1 and stall_button:              # hybrid pages
                    _click_load_more(page, stall_button, 1500)
            t_action = time.monotonic() - t_round

            grew = _wait_for_growth(h, prev_count, prev_urls,
                                    cfg["growth_timeout_ms"], cfg["poll_ms"])
            h.flush()
            t_total = time.monotonic() - t_round

            if grew:
                stall = 0
                confirmed = False
                seg["max_pos"] = max(seg["max_pos"], seg["start"] + h.count)
            else:
                stall += 1
                # Segments proven to work: a stall is far cheaper to fix with a
                # fresh page at the current position than with 4 retries +
                # a confirmation pass (~50s each time on Ounass).
                if seg["enabled"] and seg["verified"] \
                        and stall >= cfg["segment_on_stall_rounds"]:
                    res = switch_segment(f"stalled {stall} round(s)")
                    if res == "ok":
                        stall, confirmed = 0, False
                        sentinel_seen = _js_count(page, sentinel) > 0
                        continue
                    if res != "unsupported":
                        reason = f"segment_{res}"
                        break
                if sentinel and not sentinel_seen:
                    sentinel_seen = _js_count(page, sentinel) > 0
                if sentinel and sentinel_seen and _js_count(page, sentinel) == 0 \
                        and not seg["enabled"]:
                    reason = "end_sentinel_gone"
                    break
                if stall >= cfg["stable_rounds"]:
                    if not confirmed:
                        confirmed = True
                        log(f"no growth for {stall} rounds; confirmation pass "
                            f"(bottom + {3 * cfg['growth_timeout_ms'] // 1000}s wait)")
                        _dismiss_popups(page, cfg["popup_selectors"])
                        _scroll_step(page, cfg, "bottom")
                        if stall_button:
                            _click_load_more(page, stall_button, 3000)
                        if _wait_for_growth(h, h.count, len(h.urls),
                                            3 * cfg["growth_timeout_ms"],
                                            cfg["poll_ms"]):
                            log("confirmation pass found more products; continuing")
                            stall = 0
                            h.flush()
                            continue
                    # this page is exhausted - can a fresh segment continue?
                    if seg["enabled"]:
                        res = switch_segment("page stopped loading")
                        if res == "ok":
                            stall, confirmed = 0, False
                            sentinel_seen = _js_count(page, sentinel) > 0
                            continue
                        if res != "unsupported":
                            reason = f"segment_{res}"
                            break
                    reason = "no_growth_confirmed"
                    break
                if stall == 1:
                    _dismiss_popups(page, cfg["popup_selectors"])

            if total is None and rnd % 5 == 0:
                total = _read_total(page, cfg)
            slow = t_total > cfg["slow_round_s"]
            if not grew or slow or rnd % cfg["log_every"] == 0:
                log(f"round {rnd}: seg={seg['n']} cards={h.count} "
                    f"pos={seg['start'] + h.count} urls={len(h.urls)}"
                    f"{'/' + str(total) if total else ''} grew={grew} "
                    f"stall={stall} | round={t_total:.1f}s action={t_action:.1f}s "
                    f"ping={ping:.0f}ms eval={h.last_eval_ms:.0f}ms "
                    f"loading={h.loading}" + ("  <-- SLOW" if slow else ""))

            page.wait_for_timeout(random.randint(cfg["pause_min_ms"],
                                                 cfg["pause_max_ms"]))
    except _StopPagination:
        pass
    except Exception as e:
        if "has been closed" in str(e) or "Connection closed" in str(e):
            reason = "browser_closed"
            log("browser closed (shutdown?) - keeping URLs collected so far")
        else:
            reason = "driver_error"
            log(f"driver error:\n{traceback.format_exc()}")
    finally:
        try:
            h.poll(full=True)
        except Exception:
            pass
        h.flush()
        elapsed = time.monotonic() - started
        if mode == "click" and reason != "button_gone":
            try:
                still = page.locator(_pw_selector(cfg["button_selector"])).first
                stats["button_visible_at_end"] = bool(still.is_visible(timeout=2000))
            except Exception:
                pass
        if reason == "segment_blocked" or reason == "segment_past_end" and \
                total and seg["start"] + h.count < total:
            reason = "segment_blocked"
        stats["done"] = reason != "segment_blocked"
        if seg["verified"]:
            stats["seg_verified"] = True
            reached = max(seg["max_pos"], seg["start"] + h.count)
            stats["resume_pos"] = max(stats.get("resume_pos") or 0, reached)
        stats.update(harvested=max(len(h.urls), stats.get("harvested") or 0),
                     reason=reason, expected_total=total,
                     cards=h.count, rounds=rnd, segments=seg["n"],
                     segments_supported=seg["verified"],
                     seconds=round(elapsed, 1),
                     avg_eval_ms=round(h.avg_eval_ms(), 1))
        log(f"stopped ({reason}): urls={len(h.urls)}"
            f"{'/' + str(total) if total else ''} segments={seg['n']} "
            f"rounds={rnd} in {elapsed:.0f}s")
    return page   # older Scrapling versions require the page back


# ============================================================
# Listing workers (child processes)
# ============================================================

class _HttpListingSession:
    """Plain HTTP (no browser) with browser-like TLS + headers, via
    Scrapling's Fetcher. For listings that are server-rendered: ~10x faster
    than a browser and no JS to wait for."""

    def __init__(self, proxy: str = "", timeout_s: int = 30):
        self.proxy, self.timeout = proxy, timeout_s
        self._cm = self._s = None

    def __enter__(self):
        try:
            from scrapling.fetchers import FetcherSession
            self._cm = FetcherSession()
            self._s = self._cm.__enter__()
        except Exception:
            from scrapling.fetchers import Fetcher
            self._cm, self._s = None, Fetcher
        return self

    def __exit__(self, *exc):
        if self._cm is not None:
            try:
                self._cm.__exit__(*exc)
            except Exception:
                pass

    def fetch(self, url: str):
        kw = {"timeout": self.timeout, "stealthy_headers": True}
        if self.proxy:
            kw["proxy"] = self.proxy
        try:
            return self._s.get(url, **kw)
        except TypeError:                 # older Scrapling signatures
            return self._s.get(url)


def _total_from_text(txt: str) -> Optional[int]:
    nums = [n for n in _numbers_in(txt) if n == int(n)]
    return int(max(nums)) if nums else None


def _total_from_response(resp, cfg: dict) -> Optional[int]:
    sel = cfg["total_count_selector"]
    if not sel:
        return None
    try:
        if _is_xpath(sel):
            xp = sel[6:] if sel.startswith("xpath=") else sel
            parts = resp.xpath(f"({xp})[1]//text()").getall()
        else:
            parts = resp.css(f"{sel} ::text").getall()
        return _total_from_text(" ".join(str(p) for p in parts))
    except Exception:
        return None


def _listing_worker_url(conn, cfg: dict, urls: Optional[list] = None):
    """URL mode. `urls` None -> the start listing; a list -> each listing
    (e.g. categories) in turn with one session. Each listing resumes from
    its bookmark (cfg["cursors"]) and reports a new one after every page."""
    _ignore_sigint()
    send = _sender(conn)

    def log(m):
        send({"type": "log", "msg": m})

    st = {"session": None, "using_http": cfg["listing_fetcher"] == "http",
          "browser_cm": None}
    kwargs = _filter_session_kwargs(dict(cfg["session_kwargs"]), log)

    def to_browser(why: str):
        log(why)
        st["browser_cm"] = StealthySession(**kwargs)
        st["session"] = st["browser_cm"].__enter__()
        st["using_http"] = False

    def fetch(url, label, probing):
        return _fetch_with_retry(st["session"], url,
                                 1 if probing else cfg["listing_attempts"],
                                 log, label, base_wait=10, cap_wait=90)

    def crawl(key: str, single: bool) -> str:
        """One listing -> stop reason. `key` = the listing URL (bookmark key)."""
        cur = (cfg.get("cursors") or {}).get(key) or {}
        start_idx = int(cur.get("page_idx") or 0)
        follow_next = bool(cfg["next_page_xpath"])
        next_url = cur.get("next_url") if follow_next else None
        if start_idx:
            log(f"resuming {key} at page {start_idx + 1}")
        seen: Set[str] = set()
        visited: Set[str] = set()
        empty_streak = no_new_streak = 0
        total: Optional[int] = None
        first = True

        for page_idx in range(start_idx, cfg["max_pages"]):
            if _past_deadline(cfg):
                return "time_budget"
            if total and len(seen) >= total:
                return "reached_expected_total"
            if follow_next and page_idx > 0:
                if not next_url:
                    return "no_next_link"
                url = next_url
            else:
                url = _build_listing_url(cfg, page_idx)
            if url in visited:
                return "url_repeated"
            visited.add(url)

            probing = st["using_http"] and first
            resp, status, err = fetch(url, f"page_idx={page_idx}", probing)
            if resp is None and probing:
                to_browser(f"plain HTTP refused ({err}); switching the listing "
                           f"to the browser - set LISTING_FETCHER = 'browser' "
                           f"for this site to skip this check")
                resp, status, err = fetch(url, f"page_idx={page_idx}", False)
            if status in END_OF_LIST_STATUSES and not first:
                return f"http_{status}_past_end"
            if resp is None:
                send({"type": "page_failed", "start": page_idx, "url": url,
                      "error": err or "unknown"})
                if follow_next:
                    return "next_chain_broken"
                continue
            try:
                page_urls = _clean_urls(cfg, cfg["parse_listing_page"](resp))
            except Exception as e:
                send({"type": "page_failed", "start": page_idx, "url": url,
                      "error": f"parser failed: {_short_err(e)}"})
                continue
            if probing and not page_urls:
                to_browser("plain HTTP page has no product links (JavaScript "
                           "listing); switching the listing to the browser")
                resp, status, err = fetch(url, f"page_idx={page_idx}", False)
                if resp is None:
                    send({"type": "page_failed", "start": page_idx, "url": url,
                          "error": err or "unknown"})
                    continue
                page_urls = _clean_urls(cfg, cfg["parse_listing_page"](resp))

            if follow_next:
                next_url = _extract_next(resp, url, cfg["next_page_xpath"])
            if first:
                first = False
                total = _total_from_response(resp, cfg)
                log(f"expected_total={total}")
                if single:
                    send({"type": "listing_report",
                          "report": {"url": key, "expected_total": total}})
            new = page_urls - seen
            seen |= page_urls
            send({"type": "page", "start": page_idx, "urls": list(page_urls)})
            send({"type": "cursor", "url": key, "cursor": {
                "page_idx": page_idx + 1, "next_url": next_url,
                "complete": False}})

            if not page_urls:
                empty_streak += 1
                if empty_streak >= cfg["empty_tail_threshold"]:
                    return "empty_tail"
            else:
                empty_streak = 0
                if not new:
                    no_new_streak += 1
                    if no_new_streak >= cfg["no_new_threshold"]:
                        return "no_new_urls"
                else:
                    no_new_streak = 0
            time.sleep(random.uniform(cfg["listing_min_delay"],
                                      cfg["listing_max_delay"]))
        return "max_pages"

    try:
        if st["using_http"]:
            log("listing via plain HTTP (no browser)")
            session_cm = _HttpListingSession(cfg.get("proxy", ""),
                                             cfg["listing_http_timeout_s"])
        else:
            session_cm = StealthySession(**kwargs)
        with session_cm as session:
            st["session"] = session
            targets = list(urls) if urls else [None]
            reason = "no_targets"
            for n, target in enumerate(targets, 1):
                if _past_deadline(cfg):
                    reason = "time_budget"
                    log(f"listing time budget used up - {len(targets) - n + 1} "
                        f"listing(s) left for the next run")
                    break
                cfg["listing_url"] = target or ""
                key = target or (cfg["base_url"] + cfg["start_path"])
                if urls:
                    log(f"=== listing {n}/{len(targets)}: {key}")
                reason = crawl(key, single=not urls)
                complete = reason not in ("time_budget", "next_chain_broken",
                                          "max_pages")
                if complete:
                    send({"type": "cursor", "url": key,
                          "cursor": {"page_idx": 0, "next_url": None,
                                     "complete": True}})
                if urls:
                    send({"type": "listing_report",
                          "report": {"url": key, "reason": reason}})
            send({"type": "done",
                  "reason": reason if not urls or reason == "time_budget"
                  else "listing_urls_complete"})
    except Exception:
        send({"type": "fatal", "error": traceback.format_exc()})
    finally:
        try:
            if st["browser_cm"] is not None:
                st["browser_cm"].__exit__(None, None, None)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _categories_in(resp, cfg: dict, page_url: str) -> List[str]:
    xp = cfg["category_xpath"].strip()
    if "@href" not in xp:
        xp = f"({xp})/@href"
    host = urlsplit(cfg["base_url"]).netloc.lower().removeprefix("www.")
    found: List[str] = []
    try:
        values = resp.xpath(xp).getall()
    except Exception:
        values = []
    for v in values:
        u, _ = urldefrag(urljoin(page_url, str(v).strip()))
        if not u.startswith("http"):
            continue
        if urlsplit(u).netloc.lower().removeprefix("www.") != host:
            continue                                   # other sites, socials ...
        if u.rstrip("/") == page_url.rstrip("/") or u in found:
            continue
        if not _url_allowed(u, cfg["category_url_contains"]):
            continue
        found.append(u)
    return found


def _category_discovery_worker(conn, cfg: dict):
    """Open the start URL once and collect the category links (category_xpath).
    Plain HTTP first; the browser if that finds nothing (JavaScript menus).
    Also reports whether the start page itself lists products."""
    _ignore_sigint()
    send = _sender(conn)

    def log(m):
        send({"type": "log", "msg": m})

    start = cfg["base_url"] + cfg["start_path"]
    found: List[str] = []
    on_start = 0
    try:
        try:
            with _HttpListingSession(cfg.get("proxy", ""),
                                     cfg["listing_http_timeout_s"]) as http:
                r = http.fetch(start)
                if getattr(r, "status", None) == 200:
                    found = _categories_in(r, cfg, start)
                    on_start = len(_clean_urls(cfg, cfg["parse_listing_page"](r)))
        except Exception as e:
            log(f"category discovery over plain HTTP failed ({_short_err(e, 120)})")
        if not found:
            log("no category links in the plain HTML; opening the start page "
                "in the browser")
            kwargs = dict(cfg["session_kwargs"])
            cat_el = cfg["category_xpath"].strip()
            if cat_el.endswith("/@href"):
                cat_el = cat_el[: -len("/@href")]
            kwargs.update(wait_selector=_pw_selector(cat_el),
                          wait_selector_state="attached")
            kwargs = _filter_session_kwargs(kwargs, log)
            with StealthySession(**kwargs) as browser:
                resp, status, err = _fetch_with_retry(browser, start, 2, log,
                                                      "categories", 5, 20)
                if resp is not None:
                    found = _categories_in(resp, cfg, start)
                    on_start = max(on_start, len(_clean_urls(
                        cfg, cfg["parse_listing_page"](resp))))
        found = found[: cfg["max_categories"]]
        log(f"categories: {len(found)} found on the start page; "
            f"{on_start} product links on the start page itself")
        send({"type": "categories", "urls": found, "start_products": on_start})
        send({"type": "done", "reason": "categories_discovered"})
    except Exception:
        send({"type": "fatal", "error": traceback.format_exc()})
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _listing_worker_url_list(conn, cfg: dict, items: list):
    """Re-fetch specific failed URL-mode pages with ONE warm session."""
    _ignore_sigint()
    send = _sender(conn)

    def log(m):
        send({"type": "log", "msg": m})

    try:
        kwargs = _filter_session_kwargs(dict(cfg["session_kwargs"]), log)
        with StealthySession(**kwargs) as session:
            for idx, url in items:
                resp, status, err = _fetch_with_retry(
                    session, url, cfg["retry_attempts"], log,
                    f"[retry] idx={idx}", base_wait=10, cap_wait=60,
                )
                if resp is None:
                    send({"type": "page_failed", "start": idx, "url": url,
                          "error": err or "unknown"})
                    continue
                try:
                    urls = _clean_urls(cfg, cfg["parse_listing_page"](resp))
                    send({"type": "page", "start": idx, "urls": list(urls)})
                except Exception as e:
                    send({"type": "page_failed", "start": idx, "url": url,
                          "error": f"parser failed: {_short_err(e)}"})
                time.sleep(random.uniform(cfg["listing_min_delay"],
                                          cfg["listing_max_delay"]))
            send({"type": "done", "reason": "retry_complete"})
    except Exception:
        send({"type": "fatal", "error": traceback.format_exc()})
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _listing_worker_single_url(conn, cfg: dict, urls: Optional[list] = None):
    """SCROLL / CLICK: paginate in-page, stream URLs as found.

    `urls` None  -> crawl the start URL (and discover categories on it).
    `urls` list  -> crawl each URL (e.g. categories) with ONE warm browser.
    """
    _ignore_sigint()
    send = _sender(conn)

    def log(m):
        send({"type": "log", "msg": m})

    try:
        targets = list(urls) if urls else [_build_listing_url(cfg, 0)]
        stats: dict = {}
        kwargs = dict(cfg["session_kwargs"])
        # cfg is shared by reference with the partial, so per-URL values
        # (listing_url, discover_categories) set below are seen by the driver
        kwargs["page_action"] = functools.partial(
            _paginate_listing, cfg=cfg, send=send, stats=stats
        )
        kwargs = _filter_session_kwargs(kwargs, log)

        budget_hit = False
        with StealthySession(**kwargs) as session:
            for n, url in enumerate(targets, 1):
                if _past_deadline(cfg):
                    budget_hit = True
                    log(f"listing time budget used up - {len(targets) - n + 1} "
                        f"listing(s) left for the next run")
                    break
                cfg["listing_url"] = url
                cfg["discover_categories"] = (urls is None and bool(cfg["category_xpath"])
                                              and cfg.get("category_mode") != "all")
                if len(targets) > 1:
                    log(f"=== listing {n}/{len(targets)}: {url}")
                stats.clear()
                cur = (cfg.get("cursors") or {}).get(url) or {}
                if cur.get("resume_pos") and cur.get("seg_verified"):
                    # continue where the previous run stopped (needs page segments)
                    stats.update(resume_pos=int(cur["resume_pos"]), seg_verified=True,
                                 done=False, harvested=0)
                    log(f"resuming {url} at position {cur['resume_pos']} "
                        f"(previous run stopped there)")
                _crawl_one_listing(session, url, cfg, stats, send, log)
                why = stats.get("reason", "")
                budget_hit = budget_hit or why == "time_budget"
                send({"type": "cursor", "url": url, "cursor": {
                    "resume_pos": stats.get("resume_pos") or 0,
                    "seg_verified": bool(stats.get("seg_verified")),
                    "complete": why not in _UNFINISHED_REASONS}})
                if n < len(targets) and not _past_deadline(cfg):
                    time.sleep(random.uniform(cfg["listing_min_delay"],
                                              cfg["listing_max_delay"]))
        if budget_hit:
            reason = "time_budget"
        elif urls:
            reason = "listing_urls_complete"
        else:
            reason = stats.get("reason", "pagination_complete")
        send({"type": "done", "reason": reason})
    except Exception:
        send({"type": "fatal", "error": traceback.format_exc()})
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _crawl_one_listing(session, url, cfg, stats, send, log) -> None:
    last_err, status = None, None
    for attempt in range(1, cfg["listing_attempts"] + 1):
        resuming = bool(stats.get("resume_pos")) and not stats.get("done", True)
        log(f"fetching {url} (mode={cfg['mode']}, "
            f"attempt {attempt}/{cfg['listing_attempts']}"
            + (f", resuming at {stats['resume_pos']})" if resuming else ")"))
        if not resuming:
            stats.clear()
            stats["harvested"] = 0
        else:
            stats["entries"] = 0        # fresh resume budget per attempt
        resp, status, parsed = None, None, 0
        try:
            resp = session.fetch(url)
            status = getattr(resp, "status", None)
        except Exception as e:
            last_err = _short_err(e)

        if resp is not None and status == 200:
            try:
                found = _clean_urls(cfg, cfg["parse_listing_page"](resp))
                parsed = len(found)
                if found:
                    send({"type": "page", "start": -1, "urls": list(found)})
            except Exception as e:
                last_err = f"parser failed: {_short_err(e)}"
        elif resp is not None:
            last_err = f"HTTP {status}"

        blocked = stats.get("harvested") and not stats.get("done", True)
        if blocked and attempt < cfg["listing_attempts"] and not _past_deadline(cfg):
            wait = cfg["resume_cooldown_s"]
            log(f"blocked at position {stats.get('resume_pos')}; "
                f"cooling down {wait}s, then resuming")
            time.sleep(wait)
            continue

        if stats.get("harvested") or parsed:
            log(f"harvested={stats.get('harvested')} final_dom={parsed}")
            report = {k: v for k, v in stats.items() if k != "urls_seen"}
            report.update(final_dom_urls=parsed, attempt=attempt, url=url)
            send({"type": "listing_report", "report": report})
            return
        if status in END_OF_LIST_STATUSES or _past_deadline(cfg):
            break
        if attempt < cfg["listing_attempts"]:
            wait = _backoff(attempt, status, 10, 90)
            log(f"no URLs ({last_err}); retrying in {wait:.1f}s")
            time.sleep(wait)

    send({"type": "page_failed", "start": 0, "url": url,
          "error": last_err or "no product URLs after pagination"})


# ============================================================
# Product worker (N of these run in parallel)
# ============================================================

def _product_worker(worker_id: int, tasks, conn, cfg: dict):
    _ignore_sigint()
    send = _sender(conn)

    def log(m):
        send({"type": "log", "msg": m})

    try:
        kwargs = _filter_session_kwargs(dict(cfg["session_kwargs_product"]), log)
        with StealthySession(**kwargs) as session:
            send({"type": "ready"})
            attempts = cfg["product_attempts"]
            while True:
                try:
                    url = tasks.get(timeout=5)
                except queue_mod.Empty:
                    break
                if url is None:
                    break

                product, err = None, None
                for attempt in range(1, attempts + 1):
                    send({"type": "heartbeat", "url": url, "attempt": attempt})
                    status = None
                    try:
                        resp = session.fetch(url)
                        status = getattr(resp, "status", None)
                        if status == 200:
                            cand = cfg["extract_product"](resp, url)
                            missing = _missing_fields(cand, cfg["required_fields"])
                            if not missing:
                                product = cand
                                break
                            err = f"missing fields {missing}"
                            if attempt >= 2:
                                break
                        else:
                            err = f"HTTP {status}"
                            if status in PERMANENT_PRODUCT_STATUSES:
                                break
                    except Exception as e:
                        err = _short_err(e)
                    if attempt < attempts:
                        time.sleep(_backoff(attempt, status, 2.0, 30.0))

                if product is not None:
                    product.setdefault("product_url", url)
                    send({"type": "product", "url": url, "product": product})
                else:
                    send({"type": "product_failed", "url": url,
                          "error": err or "unknown"})

                time.sleep(random.uniform(cfg["product_min_delay"],
                                          cfg["product_max_delay"]))
    except Exception:
        send({"type": "fatal", "error": traceback.format_exc()})
    finally:
        send({"type": "exit"})
        try:
            conn.close()
        except Exception:
            pass


# ============================================================
# Base spider
# ============================================================

class BaseInfinityScrollSpider(scrapy.Spider):

    # ---- required ----
    PAGINATION_MODE: PaginationMode = PaginationMode.URL
    BASE_URL: str = ""
    START_PATH: str = ""
    LISTING_READY_SELECTOR: str = ""
    PRODUCT_READY_SELECTOR: str = ""

    # ---- SCROLL / CLICK ----
    LISTING_ITEM_SELECTOR: str = ""            # one product card
    LISTING_LINK_SELECTOR: str = ""            # link inside a card (relative)
    LOAD_MORE_BUTTON_SELECTOR: str = ""        # CSS or XPath (required for CLICK)
    LISTING_END_SENTINEL_SELECTOR: str = ""    # disappears when list is complete
    LISTING_SCROLL_CONTAINER_SELECTOR: str = ""  # auto-detected if empty
    LISTING_TOTAL_COUNT_SELECTOR: str = ""     # e.g. "312 products" -> stop at 312
    POPUP_CLOSE_SELECTORS: tuple = ()          # cookie / newsletter close buttons
    LISTING_HARVEST_DURING_SCROLL: bool = True
    LISTING_LIGHTEN_DOM: bool = True           # content-visibility on off-screen cards
    LISTING_LOADING_SELECTOR: str = ""         # spinner shown while a batch loads
    SCROLL_STALL_CLICK_GENERIC: bool = False   # try a generic Load-more on stall

    PAGINATION_MAX_ROUNDS: int = 2000
    PAGINATION_STABLE_ROUNDS: int = 4
    PAGINATION_GROWTH_TIMEOUT_MS: int = 8000
    PAGINATION_POLL_MS: int = 300
    PAGINATION_PAUSE_MIN_MS: int = 250
    PAGINATION_PAUSE_MAX_MS: int = 700
    PAGINATION_INITIAL_WAIT_MS: int = 30000
    PAGINATION_BUTTON_WAIT_MS: int = 10000
    PAGINATION_MAX_SECONDS: int = 7200         # hitting this => verdict INCOMPLETE
    PAGINATION_LOG_EVERY: int = 5
    PAGINATION_SLOW_ROUND_S: float = 20.0      # rounds slower than this are logged

    # ---- segmented scrolling (SCROLL / CLICK) ----
    # Reload the listing at the reached position and continue on a fresh
    # page. Fixes sites that stop feeding after N loads (Ounass) and pages
    # that get slower with every card (Eyewa). Placeholders as URL mode:
    # {start} {page} {page_idx}; offsets use PAGE_STEP / FIRST_PAGE_NUMBER.
    SCROLL_SEGMENT_URL_TEMPLATE: str = ""
    # Alternative to the template: a query param set on the CURRENT listing
    # URL (start page or category), e.g. "pi={page}" or "page={page}".
    SCROLL_SEGMENT_PARAM: str = ""
    SCROLL_AUTO_PAGE_STEP: bool = False        # page size = cards on first load
    SCROLL_PROBE_TIMEOUT_MS: int = 25000       # max wait for the probe page
    SCROLL_SEGMENT_MAX_ITEMS: int = 480        # cards per page before reloading
    SCROLL_SLOW_PING_MS: float = 1500.0        # reload early if page this slow
    SCROLL_SEGMENT_ON_STALL_ROUNDS: int = 1    # stalls before reloading at position
    SCROLL_SEGMENT_RETRY_WAITS: tuple = (20, 60, 120)  # backoff on empty segment
    SCROLL_MAX_RESUMES: int = 2                # resume after a block on retry
    SCROLL_RESUME_COOLDOWN_S: int = 180        # pause before resuming a blocked run
    # Total shown by the site, for the completeness check. Either the word(s)
    # next to the number, e.g. "results" / ("Showing",) / "منتجات", or an
    # XPath/CSS to the element (LISTING_TOTAL_COUNT_SELECTOR, below).
    LISTING_TOTAL_COUNT_TEXT = ()
    LISTING_BLOCK_DOMAINS: tuple = ()          # requests containing these: blocked

    # ---- category fan-out (SCROLL / CLICK) ----
    LISTING_CATEGORY_XPATH: str = ""           # links to sub-listings on start page
    LISTING_CATEGORY_URL_CONTAINS = ()         # text a category URL must contain
    # "all"      : read category links from the start URL first, then crawl
    #              every category (+ the start URL only if it lists products)
    # "fallback" : crawl categories only if the start listing comes up short
    # "always"   : crawl the start listing, then all categories
    # "off"      : ignore LISTING_CATEGORY_XPATH
    LISTING_CATEGORY_MODE: str = "fallback"
    LISTING_MAX_CATEGORIES: int = 200

    # ---- URL mode ----
    LISTING_FETCHER: str = "browser"           # "http" for server-rendered listings
    LISTING_HTTP_TIMEOUT_S: int = 30
    LISTING_URL_TEMPLATE: str = "{base_url}{start_path}"
    LISTING_URL_BUILDER = None                 # module-level fn(cfg, idx) -> str
    LISTING_NEXT_PAGE_XPATH: str = ""          # follow rel=next instead of template
    PAGE_STEP: int = 24
    START_OFFSET: int = 0
    FIRST_PAGE_NUMBER: int = 1
    MAX_PAGES: int = 200
    EMPTY_TAIL_THRESHOLD: int = 2
    NO_NEW_URLS_THRESHOLD: int = 2

    # ---- URL cleaning ----
    LISTING_URL_CONTAINS = ()                  # text(s) a product URL must contain
    LISTING_URL_EXCLUDES = ()                  # text(s) it must NOT contain
    STRIP_QUERY_FROM_PRODUCT_URLS: bool = False

    # ---- browser ----
    HEADLESS: bool = True
    PROXY: str = ""
    LISTING_BLOCK_IMAGES: bool = True
    LISTING_DISABLE_RESOURCES: bool = False    # keep CSS: layout drives lazy-load
    PRODUCT_DISABLE_RESOURCES: bool = True
    LISTING_WAIT_MS: int = 800                 # URL mode, after ready selector
    PRODUCT_WAIT_MS: int = 300
    LISTING_SESSION_EXTRA: dict = {}
    PRODUCT_SESSION_EXTRA: dict = {}

    # ---- rate limits ----
    LISTING_MIN_DELAY: float = 1.5
    LISTING_MAX_DELAY: float = 3.0
    PRODUCT_MIN_DELAY: float = 0.3
    PRODUCT_MAX_DELAY: float = 0.9

    # ---- retries / timeouts ----
    LISTING_ATTEMPTS: int = 4
    RETRY_FAILED_LISTING_PASSES: int = 3
    LISTING_PAGE_TIMEOUT_MS: int = 60000
    LISTING_RPC_TIMEOUT: int = 600             # silence allowed (heartbeats exist)

    # Max time for collecting product URLs in one run (0 = no limit). When
    # it runs out the spider scrapes what it has; the next run continues
    # each listing from where this one stopped (see *_listing_state.json).
    LISTING_TIME_BUDGET_S: int = 0
    # URL mode, for listings without a "next" link (e.g. categories):
    # the page parameter set on the listing URL, e.g. "page={page}".
    LISTING_PAGE_PARAM: str = ""
    PRODUCT_ATTEMPTS: int = 3
    PRODUCT_PAGE_TIMEOUT_MS: int = 30000
    PRODUCT_RPC_TIMEOUT: int = 150             # per attempt, not per product

    # ---- product phase ----
    SCRAPE_PRODUCTS: bool = True
    # "browser" | "http" (plain Scrapy requests) | "auto" (test plain HTTP on
    # the first products; use it if it works, browsers otherwise)
    PRODUCT_FETCHER: str = "browser"
    PRODUCT_HTTP_PROBE_COUNT: int = 3
    PRODUCT_HTTP_CONCURRENCY: int = 4
    PRODUCT_HTTP_ABORT_STREAK: int = 8         # refusals in a row -> browsers
    PRODUCT_WORKERS: int = 2                   # parallel browsers
    PRODUCT_MAX_RESPAWNS: int = 3
    PRODUCT_WORKER_STAGGER_S: float = 2.0
    PRODUCT_REQUIRED_FIELDS: tuple = ("product_name",)

    # ---- files ----
    OUTPUT_DIR: Optional[Path] = None
    URLS_FILENAME: str = "product_urls.json"
    PRODUCTS_FILENAME: str = "products.json"
    SAVE_EVERY: int = 50
    # Products are written ONCE, one JSON object per line, to <name>.jsonl.
    # True also writes the classic indented <name>.json list.
    PRODUCTS_JSON_ARRAY: bool = False
    # True: keep earlier runs' products and skip their URLs, carry over URLs
    # collected but not yet scraped, continue listings where they stopped.
    # False: start from scratch (deletes the earlier .jsonl and bookmarks).
    RESUME: bool = True

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 8,              # only used by PRODUCT_FETCHER="http"
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.25,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 3.0,
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    # ============================================================
    # Lifecycle
    # ============================================================
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = PaginationMode(self.PAGINATION_MODE)
        self.all_product_urls: Set[str] = set()
        self.products: list = []
        self.failed_products: int = 0
        self.failed_product_urls: list = []
        self.failed_pages: list = []
        self._done_urls: Set[str] = set()
        self._lock = threading.Lock()
        self.listing_report: dict = {}
        self.category_urls: list = []
        self.listing_state: dict = {"targets": {}}  # bookmarks, kept between runs
        self.carried_urls: Set[str] = set()         # earlier runs, not yet scraped
        self._start_products = 0
        self._listing_deadline = 0.0
        self._products_this_run = 0
        self._last_done_reason = None
        self._stop = threading.Event()          # set on shutdown / Ctrl+C

        out = Path(self.OUTPUT_DIR) if self.OUTPUT_DIR else Path.cwd()
        self.urls_file = out / self.URLS_FILENAME
        self.products_file = out / self.PRODUCTS_FILENAME
        self.products_jsonl = self.products_file.with_suffix(".jsonl")
        self.failed_file = self.products_file.with_name(
            self.products_file.stem + "_failed.json"
        )
        self.report_file = self.products_file.with_name(
            self.products_file.stem + "_listing_report.json"
        )
        self.state_file = self.products_file.with_name(
            self.products_file.stem + "_listing_state.json"
        )
        self._validate_config()

    def _validate_config(self):
        name = type(self).__name__
        for attr in ("BASE_URL", "START_PATH",
                     "LISTING_READY_SELECTOR", "PRODUCT_READY_SELECTOR"):
            if not getattr(self, attr, None):
                raise ValueError(f"{name} must define {attr}")

        if self.mode in (PaginationMode.SCROLL, PaginationMode.CLICK):
            if not self.LISTING_ITEM_SELECTOR:
                raise ValueError(f"{name}: LISTING_ITEM_SELECTOR is required "
                                 f"for {self.mode.value} mode")
        renamed = {
            "LISTING_TOTAL_COUNT_REGEX": "LISTING_TOTAL_COUNT_TEXT (the word next "
                                         "to the number, e.g. 'results')",
            "LISTING_URL_PATTERN": "LISTING_URL_CONTAINS (plain text)",
            "LISTING_CATEGORY_URL_PATTERN": "LISTING_CATEGORY_URL_CONTAINS",
            "LISTING_BLOCK_URL_PATTERNS": "LISTING_BLOCK_DOMAINS "
                                          "(use COMMON_TRACKER_DOMAINS)",
        }
        for old, new in renamed.items():
            if getattr(type(self), old, None):
                raise ValueError(f"{name}: {old} was removed (no more regex) - "
                                 f"use {new}")
        if self.LISTING_CATEGORY_MODE not in ("all", "fallback", "always", "off"):
            raise ValueError(f"{name}: LISTING_CATEGORY_MODE must be "
                             f"'all', 'fallback', 'always' or 'off'")
        if self.SCROLL_SEGMENT_PARAM and "=" not in self.SCROLL_SEGMENT_PARAM:
            raise ValueError(f"{name}: SCROLL_SEGMENT_PARAM looks like "
                             f"'pi={{page}}'")
        if self.SCROLL_SEGMENT_URL_TEMPLATE and not any(
                p in self.SCROLL_SEGMENT_URL_TEMPLATE
                for p in ("{page}", "{page_idx}", "{start}")):
            raise ValueError(f"{name}: SCROLL_SEGMENT_URL_TEMPLATE needs {{start}}, "
                             f"{{page}} or {{page_idx}}")
        if self.mode is PaginationMode.CLICK and not self.LOAD_MORE_BUTTON_SELECTOR:
            raise ValueError(f"{name}: LOAD_MORE_BUTTON_SELECTOR is required "
                             f"for click mode")
        if self.mode is PaginationMode.URL and not (
            type(self).LISTING_URL_BUILDER or self.LISTING_NEXT_PAGE_XPATH
            or any(p in self.LISTING_URL_TEMPLATE
                   for p in ("{page}", "{page_idx}", "{start}"))
        ):
            raise ValueError(
                f"{name}: URL mode needs LISTING_URL_TEMPLATE with {{page}}, "
                f"{{page_idx}} or {{start}}, or LISTING_URL_BUILDER, "
                f"or LISTING_NEXT_PAGE_XPATH"
            )
        if self.LISTING_FETCHER not in ("browser", "http"):
            raise ValueError(f"{name}: LISTING_FETCHER must be 'browser' or 'http'")
        if self.LISTING_FETCHER == "http" and self.mode is not PaginationMode.URL:
            raise ValueError(f"{name}: LISTING_FETCHER='http' only works in URL "
                             f"mode (scroll/click need a browser)")
        if self.PRODUCT_FETCHER not in ("browser", "http", "auto"):
            raise ValueError(f"{name}: PRODUCT_FETCHER must be 'browser', 'http' "
                             f"or 'auto'")

    # ============================================================
    # Persistence
    # ============================================================
    def _prepare_outputs(self):
        self.products_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.RESUME:
            for f in (self.products_jsonl, self.state_file, self.urls_file):
                if f.exists():
                    f.unlink()
            self.logger.info("Fresh run: earlier products and bookmarks removed")
            return
        if self.products_jsonl.exists():
            with self.products_jsonl.open(encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        p = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if self.PRODUCTS_JSON_ARRAY:
                        self.products.append(p)
                    if p.get("product_url"):
                        self._done_urls.add(p["product_url"])
        if self.urls_file.exists():
            try:
                earlier = json.loads(self.urls_file.read_text(encoding="utf-8"))
                self.carried_urls = {u for u in earlier if u not in self._done_urls}
            except Exception:
                self.carried_urls = set()
        if self.state_file.exists():
            try:
                self.listing_state = json.loads(self.state_file.read_text(encoding="utf-8"))
                self.listing_state.setdefault("targets", {})
            except Exception:
                self.listing_state = {"targets": {}}
        unfinished = sum(1 for c in self.listing_state["targets"].values()
                         if not c.get("complete"))
        self.logger.info(
            f"Resume: {len(self._done_urls)} products already scraped (skipped), "
            f"{len(self.carried_urls)} URLs from earlier runs still to scrape, "
            f"{unfinished} listing(s) continue where they stopped")

    def save_urls(self):
        self.urls_file.parent.mkdir(parents=True, exist_ok=True)
        with self.urls_file.open("w", encoding="utf-8") as f:
            json.dump(sorted(self.all_product_urls | self.carried_urls), f,
                      indent=4, ensure_ascii=False)

    def save_state(self):
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with self.state_file.open("w", encoding="utf-8") as f:
                json.dump(self.listing_state, f, indent=2, ensure_ascii=False)
        except Exception:
            self.logger.exception("could not save listing bookmarks")

    def save_products(self):
        if not self.PRODUCTS_JSON_ARRAY:
            return                           # products live in the .jsonl only
        self.products_file.parent.mkdir(parents=True, exist_ok=True)
        with self.products_file.open("w", encoding="utf-8") as f:
            json.dump(self.products, f, indent=4, ensure_ascii=False)

    def _record_product(self, product: dict):
        with self._lock:
            self._products_this_run += 1
            self._done_urls.add(product.get("product_url"))
            with self.products_jsonl.open("a", encoding="utf-8") as f:
                f.write(json.dumps(product, ensure_ascii=False) + "\n")
            if self.PRODUCTS_JSON_ARRAY:
                self.products.append(product)
                if len(self.products) % self.SAVE_EVERY == 0:
                    self.save_products()
        self.logger.info(f"Saved: {product.get('product_name')} | "
                         f"{product.get('current_price')}")
        try:
            self.on_product(product)
        except Exception:
            self.logger.exception("on_product hook failed")

    def _record_failure(self, url: str, error: str):
        with self._lock:
            self.failed_products += 1
            self.failed_product_urls.append({"url": url, "error": error})
        self.logger.error(f"Product failed: {url} | {error}")
        try:
            self.on_product_failed(url, error)
        except Exception:
            self.logger.exception("on_product_failed hook failed")

    # ============================================================
    # Session kwargs
    # ============================================================
    def get_kwargs_listing(self) -> dict:
        in_page = self.mode is not PaginationMode.URL
        kw = dict(
            headless=self.HEADLESS,
            network_idle=False,
            # SCROLL/CLICK: the driver waits for cards itself. A wait_selector
            # here runs AFTER pagination, on whatever page the driver ended on
            # (e.g. an empty offset page) -> 60s timeout + Scrapling retry.
            wait_selector=None if in_page else self.LISTING_READY_SELECTOR,
            wait_selector_state="attached",
            wait=300 if in_page else self.LISTING_WAIT_MS,
            timeout=self.LISTING_PAGE_TIMEOUT_MS,
            block_images=self.LISTING_BLOCK_IMAGES,
            disable_resources=self.LISTING_DISABLE_RESOURCES,
        )
        if self.PROXY:
            kw["proxy"] = self.PROXY
        kw.update(self.LISTING_SESSION_EXTRA)
        return kw

    def get_kwargs_product(self) -> dict:
        kw = dict(
            headless=self.HEADLESS,
            network_idle=False,
            wait_selector=self.PRODUCT_READY_SELECTOR,
            wait_selector_state="attached",
            wait=self.PRODUCT_WAIT_MS,
            timeout=self.PRODUCT_PAGE_TIMEOUT_MS,
            disable_resources=self.PRODUCT_DISABLE_RESOURCES,
        )
        if self.PROXY:
            kw["proxy"] = self.PROXY
        kw.update(self.PRODUCT_SESSION_EXTRA)
        return kw

    # ============================================================
    # Parser hooks. Must return PICKLABLE callables (module-level functions,
    # @staticmethods, or functools.partial of those) because they are sent
    # to worker processes. Override to build parsers from data, e.g. XPaths
    # loaded from an API (see store_spider.py).
    # ============================================================
    def _listing_parser(self):
        return type(self).parse_listing_page

    def _product_extractor(self):
        return type(self).extract_product

    def _url_normalizer(self):
        return type(self).normalize_url

    # ============================================================
    # Pickle-safe cfg builders
    # ============================================================
    def _listing_cfg(self) -> dict:
        cls = type(self)
        return dict(
            mode=self.mode.value,
            listing_fetcher=self.LISTING_FETCHER,
            listing_http_timeout_s=self.LISTING_HTTP_TIMEOUT_S,
            proxy=self.PROXY,
            base_url=self.BASE_URL,
            start_path=self.START_PATH,
            page_step=self.PAGE_STEP,
            start_offset=self.START_OFFSET,
            first_page_number=self.FIRST_PAGE_NUMBER,
            listing_url_template=self.LISTING_URL_TEMPLATE,
            listing_url_builder=cls.LISTING_URL_BUILDER,
            next_page_xpath=self.LISTING_NEXT_PAGE_XPATH,
            parse_listing_page=self._listing_parser(),
            normalize_url=self._url_normalizer(),
            strip_query=self.STRIP_QUERY_FROM_PRODUCT_URLS,
            url_contains=_as_tuple(self.LISTING_URL_CONTAINS),
            url_excludes=_as_tuple(self.LISTING_URL_EXCLUDES),
            max_pages=self.MAX_PAGES,
            listing_attempts=self.LISTING_ATTEMPTS,
            retry_attempts=self.RETRY_FAILED_LISTING_PASSES,
            listing_min_delay=self.LISTING_MIN_DELAY,
            listing_max_delay=self.LISTING_MAX_DELAY,
            empty_tail_threshold=self.EMPTY_TAIL_THRESHOLD,
            no_new_threshold=self.NO_NEW_URLS_THRESHOLD,
            session_kwargs=self.get_kwargs_listing(),
            item_selector=self.LISTING_ITEM_SELECTOR,
            link_selector=self.LISTING_LINK_SELECTOR,
            harvest=self.LISTING_HARVEST_DURING_SCROLL,
            lighten_dom=self.LISTING_LIGHTEN_DOM,
            loading_selector=self.LISTING_LOADING_SELECTOR,
            slow_round_s=self.PAGINATION_SLOW_ROUND_S,
            segment_url_template=self.SCROLL_SEGMENT_URL_TEMPLATE,
            segment_param=self.SCROLL_SEGMENT_PARAM,
            auto_page_step=self.SCROLL_AUTO_PAGE_STEP,
            probe_timeout_ms=self.SCROLL_PROBE_TIMEOUT_MS,
            category_xpath=self.LISTING_CATEGORY_XPATH,
            category_url_contains=_as_tuple(self.LISTING_CATEGORY_URL_CONTAINS),
            listing_url="",
            discover_categories=False,
            category_mode=self.LISTING_CATEGORY_MODE,
            max_categories=self.LISTING_MAX_CATEGORIES,
            listing_deadline=self._listing_deadline,
            page_param=self.LISTING_PAGE_PARAM,
            # bookmarks of listings NOT finished last time (finished ones
            # start from the top again, to catch new products)
            cursors={u: c for u, c in self.listing_state["targets"].items()
                     if not c.get("complete")},
            segment_max_items=self.SCROLL_SEGMENT_MAX_ITEMS,
            slow_ping_ms=self.SCROLL_SLOW_PING_MS,
            segment_on_stall_rounds=self.SCROLL_SEGMENT_ON_STALL_ROUNDS,
            segment_retry_waits=tuple(self.SCROLL_SEGMENT_RETRY_WAITS),
            max_resumes=self.SCROLL_MAX_RESUMES,
            resume_cooldown_s=self.SCROLL_RESUME_COOLDOWN_S,
            block_domains=_as_tuple(self.LISTING_BLOCK_DOMAINS),
            nav_timeout_ms=self.LISTING_PAGE_TIMEOUT_MS,
            button_selector=self.LOAD_MORE_BUTTON_SELECTOR,
            generic_button=self.SCROLL_STALL_CLICK_GENERIC,
            end_sentinel_selector=self.LISTING_END_SENTINEL_SELECTOR,
            container_selector=self.LISTING_SCROLL_CONTAINER_SELECTOR,
            total_count_selector=(
                self.LISTING_TOTAL_COUNT_SELECTOR
                or (xpath_text_with_number(*_as_tuple(self.LISTING_TOTAL_COUNT_TEXT))
                    if self.LISTING_TOTAL_COUNT_TEXT else "")),
            popup_selectors=tuple(self.POPUP_CLOSE_SELECTORS),
            max_rounds=self.PAGINATION_MAX_ROUNDS,
            stable_rounds=self.PAGINATION_STABLE_ROUNDS,
            growth_timeout_ms=self.PAGINATION_GROWTH_TIMEOUT_MS,
            poll_ms=self.PAGINATION_POLL_MS,
            pause_min_ms=self.PAGINATION_PAUSE_MIN_MS,
            pause_max_ms=self.PAGINATION_PAUSE_MAX_MS,
            initial_wait_ms=self.PAGINATION_INITIAL_WAIT_MS,
            button_wait_ms=self.PAGINATION_BUTTON_WAIT_MS,
            max_seconds=self.PAGINATION_MAX_SECONDS,
            log_every=self.PAGINATION_LOG_EVERY,
        )

    def _product_cfg(self) -> dict:
        return dict(
            extract_product=self._product_extractor(),
            required_fields=tuple(self.PRODUCT_REQUIRED_FIELDS),
            product_attempts=self.PRODUCT_ATTEMPTS,
            product_min_delay=self.PRODUCT_MIN_DELAY,
            product_max_delay=self.PRODUCT_MAX_DELAY,
            session_kwargs_product=self.get_kwargs_product(),
        )

    # ============================================================
    # Listing phase
    # ============================================================
    def _handle_listing_msg(self, msg: dict):
        """-> None to keep reading, True/False to finish."""
        mtype = msg.get("type")
        if mtype == "log":
            self.logger.info(f"[listing] {msg['msg']}")
        elif mtype == "page":
            urls = set(msg["urls"])
            new = urls - self.all_product_urls
            self.all_product_urls.update(urls)
            if new:
                self.save_urls()
            self.logger.info(f"Listing batch {msg['start']}: +{len(new)} new, "
                             f"total={len(self.all_product_urls)}")
            try:
                self.on_listing_page(msg["start"], urls, new)
            except Exception:
                self.logger.exception("on_listing_page hook failed")
        elif mtype == "page_failed":
            self.failed_pages.append({"idx": msg["start"], "url": msg.get("url"),
                                      "error": msg["error"]})
            self.logger.error(f"[listing] failed idx={msg['start']} ({msg['error']})")
        elif mtype == "listing_report":
            rep = msg["report"]
            if not self.listing_report.get("url"):
                self.listing_report.update(rep)          # the start URL run
            else:
                self.listing_report.setdefault("category_runs", []).append(
                    {k: rep.get(k) for k in ("url", "reason", "harvested",
                                             "expected_total", "seconds")})
        elif mtype == "categories":
            self.category_urls.extend(u for u in msg["urls"]
                                      if u not in self.category_urls)
            self._start_products = max(self._start_products,
                                       int(msg.get("start_products") or 0))
            self.logger.info(f"Discovered {len(msg['urls'])} category URLs")
        elif mtype == "cursor":
            cur = dict(msg["cursor"])
            cur["updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
            self.listing_state["targets"][msg["url"]] = cur
        elif mtype == "done":
            self._last_done_reason = msg.get("reason")
            self.listing_report.setdefault("reason", msg.get("reason"))
            self.logger.info(f"Listing phase done: {msg.get('reason')}")
            return True
        elif mtype == "fatal":
            self.logger.error(f"Listing worker crashed:\n{msg['error']}")
            return False
        return None

    def _run_listing_process(self, target, *extra) -> bool:
        ctx = multiprocessing.get_context("spawn")
        parent_conn, child_conn = ctx.Pipe(duplex=False)
        proc = ctx.Process(target=target,
                           args=(child_conn, self._listing_cfg(), *extra))
        proc.start()
        child_conn.close()
        last = time.monotonic()
        try:
            while True:
                if self._stop.is_set():
                    self.logger.warning("Shutdown requested; stopping listing worker")
                    return False
                if parent_conn.poll(5):
                    try:
                        msg = parent_conn.recv()
                    except EOFError:
                        self.logger.error("Listing worker exited without 'done'")
                        return False
                    last = time.monotonic()
                    result = self._handle_listing_msg(msg)
                    if result is not None:
                        return result
                elif time.monotonic() - last > self.LISTING_RPC_TIMEOUT:
                    self.logger.error(f"Listing worker silent for "
                                      f"{self.LISTING_RPC_TIMEOUT}s; killing")
                    return False
        finally:
            try:
                parent_conn.close()
            except Exception:
                pass
            proc.join(timeout=1 if self._stop.is_set() else 15)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=5)
                if proc.is_alive():
                    proc.kill()
                    proc.join()

    def _crawl_listings(self) -> bool:
        if self.LISTING_TIME_BUDGET_S:
            self._listing_deadline = time.time() + self.LISTING_TIME_BUDGET_S
            self.logger.info(f"Listing time budget: "
                             f"{self.LISTING_TIME_BUDGET_S // 60} min")
        try:
            if self.LISTING_CATEGORY_MODE == "all" and self.LISTING_CATEGORY_XPATH:
                return self._crawl_category_first()
            return self._crawl_start_listing()
        finally:
            self.save_state()

    def _ordered_targets(self, urls: List[str]) -> List[str]:
        """Unfinished listings first (continue them), then the ones not
        visited for the longest time - so a time budget rotates fairly."""
        t = self.listing_state["targets"]

        def key(u):
            c = t.get(u)
            if c is None:
                return (1, "")                          # never crawled
            if not c.get("complete"):
                return (0, c.get("updated", ""))        # unfinished
            return (2, c.get("updated", ""))            # oldest finished first
        return sorted(urls, key=key)

    def _crawl_category_first(self) -> bool:
        start = self.BASE_URL + self.START_PATH
        self.logger.info("Categories first: reading category links from the "
                         "start URL")
        self._run_listing_process(_category_discovery_worker)
        self.listing_report.pop("reason", None)       # discovery isn't the listing
        cats = [u for u in self.category_urls if u.rstrip("/") != start.rstrip("/")]
        targets = list(cats)
        if self._start_products or not cats:
            targets.insert(0, start)   # start page lists products itself (or no cats)
        targets = self._ordered_targets(targets)
        self.listing_report.update(url=start, categories=len(cats),
                                   start_products=self._start_products)
        if not cats:
            self.logger.warning("No category links found with LISTING_CATEGORY_XPATH"
                                " - crawling the start URL only")
        self.logger.info(f"Listings to crawl: {len(targets)} "
                         f"({len(cats)} categories"
                         + (", + start URL" if start in targets else "") + ")")
        worker = (_listing_worker_url if self.mode is PaginationMode.URL
                  else _listing_worker_single_url)
        self._last_done_reason = None
        ok = self._run_listing_process(worker, targets)
        self.listing_report["reason"] = ("time_budget"
                                         if self._last_done_reason == "time_budget"
                                         or _past_deadline({"listing_deadline":
                                                            self._listing_deadline})
                                         else "categories_done")
        return ok

    def _crawl_start_listing(self) -> bool:
        if self.mode is PaginationMode.URL:
            return self._run_listing_process(_listing_worker_url)

        ok = self._run_listing_process(_listing_worker_single_url)
        if self._stop.is_set():
            return ok

        # ---- category fan-out ----
        mode = self.LISTING_CATEGORY_MODE
        cats = self.category_urls[: self.LISTING_MAX_CATEGORIES]
        if mode == "off" or not self.LISTING_CATEGORY_XPATH:
            return ok
        if not cats:
            self.logger.info("No category URLs to crawl")
            return ok
        total = self.listing_report.get("expected_total")
        have = len(self.all_product_urls)
        if mode == "fallback" and total and have >= total:
            self.logger.info(f"Start listing complete ({have}/{total}); "
                             f"skipping {len(cats)} categories")
            return ok
        why = ("LISTING_CATEGORY_MODE='always'" if mode == "always"
               else f"start listing short ({have}/{total or '?'})")
        self.logger.info(f"Crawling {len(cats)} categories to fill gaps: {why}")
        before = have
        ok_cats = self._run_listing_process(_listing_worker_single_url, cats)
        self.logger.info(f"Categories added "
                         f"{len(self.all_product_urls) - before} new URLs")
        self.listing_report["categories_crawled"] = len(cats)
        return ok or ok_cats

    def _retry_failed_listing_pages(self):
        if self.mode is not PaginationMode.URL or not self.failed_pages:
            return
        items = [(f["idx"], f["url"]) for f in self.failed_pages if f.get("url")]
        if not items:
            return
        self.logger.info(f"Retrying {len(items)} failed listing pages")
        self.failed_pages = []
        self._run_listing_process(_listing_worker_url_list, items)
        if self.failed_pages:
            self.logger.error(f"Still failing after retry: "
                              f"{[f['idx'] for f in self.failed_pages]}")

    # ============================================================
    # Product phase (browser, N parallel workers)
    # ============================================================
    def _run_product_phase(self, urls: List[str], on_item: Callable[[dict], None]):
        ctx = multiprocessing.get_context("spawn")
        tasks = ctx.Queue()
        for u in urls:
            tasks.put(u)
        cfg = self._product_cfg()
        total = len(urls)
        workers: dict = {}
        next_id = 0
        respawns = 0
        handled = 0

        def spawn():
            nonlocal next_id
            wid = next_id
            next_id += 1
            recv, send = ctx.Pipe(duplex=False)
            proc = ctx.Process(target=_product_worker, args=(wid, tasks, send, cfg))
            proc.start()
            send.close()
            workers[wid] = {"id": wid, "proc": proc, "conn": recv,
                            "last": time.monotonic(), "url": None, "alive": True}

        def retire(w, reason: str):
            nonlocal handled
            if not w["alive"]:
                return
            w["alive"] = False
            if w["url"]:
                self._record_failure(w["url"], reason)
                handled += 1
                w["url"] = None
            proc = w["proc"]
            proc.join(timeout=10)
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=5)
            try:
                w["conn"].close()
            except Exception:
                pass

        n = max(1, min(self.PRODUCT_WORKERS, total))
        self.logger.info(f"Starting {n} product worker(s) for {total} URLs")
        for i in range(n):
            spawn()
            if i < n - 1:
                time.sleep(self.PRODUCT_WORKER_STAGGER_S)

        try:
            while True:
                if self._stop.is_set():
                    self.logger.warning("Shutdown requested; stopping product workers")
                    break
                live = [w for w in workers.values() if w["alive"]]
                if not live:
                    if handled < total and respawns < self.PRODUCT_MAX_RESPAWNS:
                        respawns += 1
                        self.logger.warning(f"All workers gone with "
                                            f"{total - handled} URLs left; respawning")
                        spawn()
                        continue
                    break

                by_conn = {id(w["conn"]): w for w in live}
                ready = mp_wait([w["conn"] for w in live], timeout=5)
                now = time.monotonic()

                for conn in ready:
                    w = by_conn[id(conn)]
                    try:
                        msg = conn.recv()
                    except (EOFError, OSError):
                        retire(w, "worker process died")
                        continue
                    w["last"] = now
                    mtype = msg.get("type")
                    if mtype == "heartbeat":
                        w["url"] = msg.get("url")
                    elif mtype == "product":
                        w["url"] = None
                        handled += 1
                        try:
                            on_item(msg["product"])
                        except Exception:
                            self.logger.exception("on_item failed")
                    elif mtype == "product_failed":
                        w["url"] = None
                        handled += 1
                        self._record_failure(msg["url"], msg["error"])
                    elif mtype == "log":
                        self.logger.info(f"[product w{w['id']}] {msg['msg']}")
                    elif mtype == "fatal":
                        self.logger.error(f"Product worker w{w['id']} crashed:\n"
                                          f"{msg['error']}")
                    elif mtype == "exit":
                        retire(w, "worker exited mid-product")

                for w in live:
                    if w["alive"] and now - w["last"] > self.PRODUCT_RPC_TIMEOUT:
                        self.logger.error(f"Product worker w{w['id']} hung on "
                                          f"{w['url']}; killing")
                        retire(w, "worker hung (killed)")
                        if respawns < self.PRODUCT_MAX_RESPAWNS:
                            respawns += 1
                            spawn()
        finally:
            for w in workers.values():
                if w["alive"]:
                    w["alive"] = False
                    if w["proc"].is_alive():
                        w["proc"].kill()
                        w["proc"].join(timeout=5)
                    try:
                        w["conn"].close()
                    except Exception:
                        pass
            try:
                tasks.cancel_join_thread()
                tasks.close()
            except Exception:
                pass

    # ---- product phase (plain HTTP via Scrapling, thread pool) ----
    def _http_fetch_product(self, session, url: str):
        """-> (product | None, error, status)."""
        extract = self._product_extractor()
        r = session.fetch(url)
        status = getattr(r, "status", None)
        if status != 200:
            return None, f"HTTP {status}", status
        product = extract(r, url)
        missing = _missing_fields(product, self.PRODUCT_REQUIRED_FIELDS)
        if missing:
            return None, f"missing fields {missing}", status
        product.setdefault("product_url", url)
        return product, None, status

    def _probe_http_products(self, urls: List[str]) -> bool:
        """Try the first few products over plain HTTP. True -> all good."""
        sample = urls[: self.PRODUCT_HTTP_PROBE_COUNT]
        ok = 0
        try:
            with _HttpListingSession(self.PROXY, 30) as sess:
                for u in sample:
                    try:
                        product, err, _ = self._http_fetch_product(sess, u)
                    except Exception as e:
                        product, err = None, _short_err(e)
                    if product:
                        ok += 1
                    else:
                        self.logger.info(f"[auto] HTTP probe failed for {u}: {err}")
        except Exception as e:
            self.logger.info(f"[auto] HTTP probe unavailable: {_short_err(e)}")
        use = bool(sample) and ok == len(sample)
        self.logger.info(
            f"[auto] product pages over plain HTTP: {ok}/{len(sample)} OK -> "
            + ("using HTTP (fast, no browser)" if use else "using browsers"))
        return use

    def _run_product_phase_http(self, urls: List[str],
                                on_item: Callable[[dict], None]) -> List[str]:
        """Fetch products with N threads over plain HTTP. Returns the URLs
        left undone if the site starts refusing HTTP (403 streak)."""
        from concurrent.futures import ThreadPoolExecutor
        local = threading.local()
        opened: list = []
        lock = threading.Lock()
        state = {"refused_streak": 0, "abort": False}
        done: Set[str] = set()
        refused: List[tuple] = []        # decided at the end: browser or failure

        def session():
            if not hasattr(local, "s"):
                cm = _HttpListingSession(self.PROXY, 30)
                local.s = cm.__enter__()
                with lock:
                    opened.append(cm)
            return local.s

        def work(url: str):
            if self._stop.is_set() or state["abort"]:
                return
            err, status = "unknown", None
            for attempt in range(1, self.PRODUCT_ATTEMPTS + 1):
                try:
                    product, err, status = self._http_fetch_product(session(), url)
                except Exception as e:
                    product, err, status = None, _short_err(e), None
                if product:
                    with lock:
                        state["refused_streak"] = 0
                        done.add(url)
                    on_item(product)
                    break
                if status in THROTTLE_STATUSES:
                    with lock:
                        state["refused_streak"] += 1
                        if state["refused_streak"] >= self.PRODUCT_HTTP_ABORT_STREAK:
                            state["abort"] = True
                    if state["abort"]:
                        return                       # left for the browser
                if status in PERMANENT_PRODUCT_STATUSES:
                    break
                if attempt < self.PRODUCT_ATTEMPTS:
                    time.sleep(_backoff(attempt, status, 2.0, 30.0))
            if url not in done:
                if status in THROTTLE_STATUSES:
                    with lock:
                        refused.append((url, err))   # maybe the browser gets it
                    return
                with lock:
                    done.add(url)
                self._record_failure(url, err or "unknown")
            time.sleep(random.uniform(self.PRODUCT_MIN_DELAY, self.PRODUCT_MAX_DELAY))

        self.logger.info(f"HTTP product phase: {len(urls)} URLs, "
                         f"{self.PRODUCT_HTTP_CONCURRENCY} threads")
        try:
            with ThreadPoolExecutor(self.PRODUCT_HTTP_CONCURRENCY) as ex:
                list(ex.map(work, urls))
        finally:
            for cm in opened:
                try:
                    cm.__exit__(None, None, None)
                except Exception:
                    pass
        if state["abort"]:
            return [u for u in urls if u not in done]
        for url, err in refused:            # isolated refusals: real failures
            self._record_failure(url, err or "refused")
        return []

    # ---- product phase (plain HTTP through Scrapy) ----
    def _parse_product_http(self, response):
        url = response.meta["product_url"]
        try:
            product = self._product_extractor()(response, url)
        except Exception as e:
            self._record_failure(url, f"parser failed: {_short_err(e)}")
            return
        missing = _missing_fields(product, self.PRODUCT_REQUIRED_FIELDS)
        if missing:
            self._record_failure(url, f"missing fields {missing}")
            return
        product.setdefault("product_url", url)
        self._record_product(product)
        yield product

    def _product_errback(self, failure):
        self._record_failure(failure.request.meta["product_url"],
                             repr(failure.value))

    # ============================================================
    # Scrapy entrypoint
    # ============================================================
    async def start(self):
        self._prepare_outputs()
        self.logger.info(f"[{self.name}] listing phase (mode={self.mode.value})")

        ok = await asyncio.to_thread(self._crawl_listings)
        if not ok:
            self.logger.warning("Listing crawl returned non-ok; continuing "
                                "with the URLs collected so far")
        await asyncio.to_thread(self._retry_failed_listing_pages)
        self.save_urls()
        self._finalise_listing_report()

        if self._stop.is_set():
            self.logger.warning("Shutdown requested; product phase skipped")
            return
        pool = self.all_product_urls | self.carried_urls
        if not pool:
            self.logger.warning("No product URLs found.")
            return
        self.logger.info(f"Product URLs: {len(self.all_product_urls)} from this run's "
                         f"listing + {len(self.carried_urls - self.all_product_urls)} "
                         f"carried over from earlier runs")
        if not self.SCRAPE_PRODUCTS:
            return

        todo = sorted(u for u in pool if u not in self._done_urls)
        self.logger.info(f"To scrape now: {len(todo)} "
                         f"(skipping {len(pool) - len(todo)} already scraped)")
        if not todo:
            self.logger.info("Nothing new to scrape.")
            return

        if self.PRODUCT_FETCHER == "http":
            for url in todo:
                yield scrapy.Request(url, callback=self._parse_product_http,
                                     errback=self._product_errback,
                                     meta={"product_url": url},
                                     dont_filter=True)
            return

        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue()

        def on_item(product: dict):
            self._record_product(product)
            loop.call_soon_threadsafe(q.put_nowait, product)

        use_http = False
        if self.PRODUCT_FETCHER == "auto":
            use_http = await asyncio.to_thread(self._probe_http_products, todo)

        def producer():
            try:
                rest = todo
                if use_http:
                    rest = self._run_product_phase_http(todo, on_item)
                    if rest:
                        self.logger.warning(f"Plain HTTP stopped working; "
                                            f"{len(rest)} products go to browsers")
                if rest and not self._stop.is_set():
                    self._run_product_phase(rest, on_item)
            except Exception:
                self.logger.exception("Product phase crashed")
            finally:
                loop.call_soon_threadsafe(q.put_nowait, None)

        fut = loop.run_in_executor(None, producer)
        while True:
            item = await q.get()
            if item is None:
                break
            yield item
        await fut

    # ============================================================
    # Completeness verdict
    # ============================================================
    URL_COMPLETE_REASONS = {"empty_tail", "no_next_link", "url_repeated"}

    def _finalise_listing_report(self):
        r = self.listing_report
        r["unique_urls"] = len(self.all_product_urls)
        r["failed_listing_pages"] = len(self.failed_pages)
        reason = str(r.get("reason") or "unknown")
        expected = r.get("expected_total")

        if reason == "time_budget":
            verdict = "PARTIAL"
            why = (f"listing time budget ({self.LISTING_TIME_BUDGET_S // 60} min) "
                   f"used up after {r['unique_urls']} URLs - the next run "
                   f"continues from here")
        elif reason == "categories_done":
            verdict = "PROBABLY_COMPLETE"
            why = (f"all {r.get('categories', 0)} categories crawled "
                   f"({r['unique_urls']} URLs)")
        elif expected:
            missing = expected - r["unique_urls"]
            r["missing_vs_expected"] = missing
            verdict = "COMPLETE" if missing <= 0 else "INCOMPLETE"
            why = f"site says {expected}, collected {r['unique_urls']}"
        elif reason in COMPLETE_REASONS or reason in self.URL_COMPLETE_REASONS \
                or reason.startswith("http_"):
            verdict, why = "COMPLETE", f"site signalled end of list ({reason})"
        elif reason in PROBABLE_REASONS or reason == "no_new_urls":
            verdict = "PROBABLY_COMPLETE"
            why = (f"stopped because nothing new appeared ({reason}); set "
                   f"LISTING_TOTAL_COUNT_SELECTOR or a sentinel to verify")
        else:
            verdict = "INCOMPLETE"
            why = f"stopped early ({reason}) - more products very likely exist"
        if r.get("button_visible_at_end") and verdict != "INCOMPLETE":
            verdict = "INCOMPLETE"
            why = (f"stopped ({reason}) while the Load-more button was still "
                   f"visible - more products exist")
        if self.failed_pages and verdict == "COMPLETE":
            verdict, why = "INCOMPLETE", f"{len(self.failed_pages)} listing pages failed"

        r["verdict"], r["why"] = verdict, why
        log = self.logger.info if verdict == "COMPLETE" else self.logger.warning
        log(f"LISTING {verdict}: {why}")
        try:
            self.report_file.parent.mkdir(parents=True, exist_ok=True)
            with self.report_file.open("w", encoding="utf-8") as f:
                json.dump(r, f, indent=4, ensure_ascii=False, default=str)
        except Exception:
            self.logger.exception("could not write listing report")

    def closed(self, reason):
        self._stop.set()
        self.save_urls()
        self.save_products()
        self.save_state()
        if self.failed_product_urls:
            with self.failed_file.open("w", encoding="utf-8") as f:
                json.dump(self.failed_product_urls, f, indent=4, ensure_ascii=False)
        self.logger.info("========== COMPLETE ==========")
        self.logger.info(f"Total URLs: {len(self.all_product_urls)}")
        self.logger.info(f"Scraped this run: {self._products_this_run} "
                         f"(all runs: {len(self._done_urls)}) -> {self.products_jsonl}")
        self.logger.info(f"Failed: {self.failed_products}")
        self.logger.info(f"Failed listing pages: {self.failed_pages}")
        if self.listing_report.get("verdict"):
            self.logger.info(f"Listing verdict: {self.listing_report['verdict']} "
                             f"- {self.listing_report.get('why')}")

    # ============================================================
    # Hooks
    # ============================================================
    @staticmethod
    def parse_listing_page(resp) -> Set[str]:
        raise NotImplementedError

    @staticmethod
    def extract_product(resp, url: str) -> Optional[dict]:
        raise NotImplementedError

    @staticmethod
    def normalize_url(url: str) -> str:
        """Override (as @staticmethod) to canonicalise product URLs."""
        return url

    def on_listing_page(self, page_idx: int, urls: Set[str], new_urls: Set[str]):
        pass

    def on_product(self, product: dict):
        pass

    def on_product_failed(self, url: str, error: str):
        pass

    # ============================================================
    # Utilities (usable from @staticmethods via BaseInfinityScrollSpider.x)
    # Work with both Scrapling and Scrapy responses.
    # ============================================================
    @staticmethod
    def text(resp, xpath: str) -> Optional[str]:
        for v in resp.xpath(xpath).getall():
            v = str(v).strip()
            if v:
                return v
        return None

    @staticmethod
    def texts(resp, xpath: str, sep: str = " ") -> Optional[str]:
        parts = [str(v).strip() for v in resp.xpath(xpath).getall()]
        joined = sep.join(p for p in parts if p)
        return joined or None

    @staticmethod
    def xpath_string(resp, expr: str) -> Optional[str]:
        """Evaluate an XPath that returns a STRING rather than nodes, e.g.
        substring-before(substring-after(//script[...], '"x":'), ',') or
        normalize-space(//h1). Uses the response's own selector when it
        supports string results, otherwise evaluates the same XPath with
        lxml on the page HTML. Returns the trimmed string or None."""
        try:
            v = resp.xpath(expr).get()
            if v is not None and str(v).strip():
                return str(v).strip()
        except Exception:
            pass
        html = None
        for attr in ("html_content", "text", "body"):
            try:
                html = getattr(resp, attr, None)
            except Exception:
                html = None
            if html:
                break
        if not html:
            return None
        if isinstance(html, bytes):
            html = html.decode("utf-8", "replace")
        try:
            import lxml.html
            out = lxml.html.fromstring(str(html)).xpath(expr)
        except Exception:
            return None
        if isinstance(out, list):
            out = out[0] if out else None
        out = str(out).strip() if out is not None else ""
        return out or None

    @staticmethod
    def meta(resp, name: str) -> Optional[str]:
        return BaseInfinityScrollSpider.text(
            resp, f"//meta[@property='{name}' or @name='{name}']/@content"
        )

    @staticmethod
    def jsonld_product(resp) -> dict:
        return _jsonld_product(resp)

    @staticmethod
    def shopify_product_url(url: str) -> str:
        """/collections/<c>/products/<h> -> /products/<h> (same product)."""
        head, sep, tail = url.partition("/collections/")
        if sep and "/products/" in tail:
            return head + "/products/" + tail.split("/products/", 1)[1]
        return url

    @staticmethod
    def parse_price(value) -> Optional[float]:
        return _parse_price(value)

    @staticmethod
    def discount_percent(current, old) -> Optional[str]:
        cur, prev = _parse_price(current), _parse_price(old)
        if not cur or not prev or prev <= cur:
            return None
        return f"{round((prev - cur) / prev * 100)}%"

    @staticmethod
    def absolute(url: str, base: str) -> str:
        return urljoin(base, url)


# Friendlier alias for new spiders
BaseEcommerceSpider = BaseInfinityScrollSpider
