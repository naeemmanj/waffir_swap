# """
# One spider for every store in the Waffir backend.

#     scrapy crawl waffir -a store="Tokyo Games"

# At start-up it reads the store's record from the Waffir API
# (/api/store/fetch-store-data/) and turns its XPaths into a full
# BaseInfinityScrollSpider configuration: listing mode (url / scroll /
# click), product links, Load-more / Next buttons, categories, and a
# product extractor for every field. Everything else (segments, throttle
# recovery, resume, HTTP/browser auto-switch, completeness verdict) comes
# from the base spider unchanged.

# Optional arguments (-a name=value):
#     store           store name, remote_id or domain           (required)
#     api_url         Waffir API address (default: WAFFIR_API_URL or localhost)
#     categories      off | fallback | always   crawl category_xpath links (default off)
#     segments        on | off      try ?page=N reloads in scroll/click mode (default on)
#     page_param      URL-mode page parameter when no next_page_xpath (default "page={page}")
#     product_fetcher auto | browser | http                       (default auto)
#     listing_only    1             collect product URLs only, skip product pages
#     resume          1             skip products already saved by an earlier run
#     max_pages       N             URL-mode page limit (default 500)

# Output: output/<store_slug>/product_urls.json, products.json, products.jsonl,
#         products_listing_report.json, products_failed.json

# Per-store spiders (spiders/stores.py): subclass WaffirStoreSpider, set
# STORE = "<name in Waffir>" plus any base-spider setting. XPaths still come
# from the API; settings defined in the subclass win over the API values.
# """

# from __future__ import annotations

# import functools
# from pathlib import Path
# from typing import Optional
# from urllib.parse import urljoin, urlsplit

# from waffir_swap.spiders import base_spider as _base

# _NEEDED_BASE_VERSION = 4
# if getattr(_base, "BASE_SPIDER_VERSION", 0) < _NEEDED_BASE_VERSION:
#     raise ImportError(
#         f"waffir_swap/spiders/base_spider.py is out of date (version "
#         f"{getattr(_base, 'BASE_SPIDER_VERSION', 'none')}, need "
#         f"{_NEEDED_BASE_VERSION}+). Copy the latest base_spider.py into "
#         f"{_base.__file__} - and update the other spider files with it."
#     )

# from waffir_swap.spiders.base_spider import (  # noqa: E402
#     COMMON_TRACKER_DOMAINS,
#     BaseInfinityScrollSpider,
#     PaginationMode,
# )
# from waffir_swap.waffir_api import (
#     WaffirApiError,
#     fetch_stores,
#     find_store,
#     slugify,
#     store_status,
# )

# B = BaseInfinityScrollSpider

# # XPaths that return a string instead of nodes, e.g.
# # substring-before(substring-after(//script[...], '"x":'), ',')
# _STRING_FUNCTIONS = ("normalize-space(", "substring", "concat(", "string(",
#                      "translate(", "number(", "sum(", "count(")


# # ============================================================
# # XPath helpers (module level -> picklable for worker processes)
# # ============================================================

# def _clean(value) -> Optional[str]:
#     text = " ".join(str(value).split()) if value is not None else ""
#     return text or None


# def _strip_href(xp: str) -> str:
#     """'//a[...]/@href' -> '//a[...]' (the element, for waiting/clicking)."""
#     xp = (xp or "").strip()
#     return xp[: -len("/@href")] if xp.endswith("/@href") else xp


# def _element_part(xp: str) -> Optional[str]:
#     """Element part of a value XPath, used as a 'page ready' marker:
#     '(//h1)[1]/text()' -> '(//h1)[1]', '//meta[@p]/@content' -> '//meta[@p]'.
#     None for string-function XPaths or when the result isn't valid XPath."""
#     s = (xp or "").strip()
#     if not s or s.startswith(_STRING_FUNCTIONS):
#         return None
#     if s.startswith("(") and s.endswith(")[1]"):
#         s = s[1:-4]
#     cut = s.rfind("/text()")
#     if cut != -1:
#         s = s[:cut]
#     last = s.rfind("/@")
#     if last != -1 and "/" not in s[last + 2:]:
#         s = s[:last]
#     s = s.rstrip("/")
#     try:
#         import lxml.etree
#         lxml.etree.XPath(s)
#     except Exception:
#         return None
#     return s or None


# def _read(resp, xp: str, first: bool = False) -> Optional[str]:
#     """Value of any kind of XPath from the API:
#        * string functions (normalize-space(...), substring-...)  -> that string
#        * attributes (/@content, /@src)                           -> first value
#        * text nodes (/text(), //text())                          -> joined text
#        * elements ((//h6[...])[1])                               -> element text
#     """
#     xp = (xp or "").strip()
#     if not xp:
#         return None
#     if xp.startswith(_STRING_FUNCTIONS):
#         return _clean(B.xpath_string(resp, xp))
#     try:
#         values = [str(v) for v in resp.xpath(xp).getall()]
#     except Exception:
#         values = []
#     if any(v.lstrip().startswith("<") for v in values):        # elements
#         return _clean(B.xpath_string(resp, f"normalize-space(string(({xp})[1]))"))
#     values = [c for c in (_clean(v) for v in values) if c]
#     if not values:
#         return None
#     is_attribute = xp.rfind("/@") > xp.rfind("/text()")
#     if first or is_attribute:
#         return values[0]
#     return _clean(" ".join(values))


# def _api_parse_listing(resp, products_xpath: str) -> set:
#     xp = products_xpath.strip()
#     if "@href" not in xp:
#         xp = xp + "/@href"
#     return {str(h).strip() for h in resp.xpath(xp).getall() if h and str(h).strip()}


# def _api_extract_product(resp, url: str, xp: dict) -> dict:
#     ld = B.jsonld_product(resp)

#     name = _read(resp, xp.get("product_name_xpath")) or ld.get("name") \
#         or B.meta(resp, "og:title")
#     brand = _read(resp, xp.get("brand_name_xpath"), first=True) or ld.get("brand")

#     current_price = (_read(resp, xp.get("current_price_xpath"), first=True)
#                      or ld.get("price")
#                      or B.meta(resp, "product:price:amount")
#                      or B.meta(resp, "og:price:amount"))
#     old_price = _read(resp, xp.get("old_price_xpath"), first=True)
#     if old_price and B.parse_price(old_price) == B.parse_price(current_price):
#         old_price = None                                   # not actually reduced

#     discount = (_read(resp, xp.get("discount_percent_xpath"))
#                 or B.discount_percent(current_price, old_price))

#     currency = (_read(resp, xp.get("currency_xpath"), first=True)
#                 or B.meta(resp, "product:price:currency")
#                 or B.meta(resp, "og:price:currency")
#                 or ld.get("currency") or "SAR")

#     image_url = (_read(resp, xp.get("image_url_xpath"), first=True)
#                  or B.meta(resp, "og:image") or ld.get("image"))
#     if image_url:
#         image_url = urljoin(url, image_url)

#     stock_status = (_read(resp, xp.get("stock_status_xpath"), first=True)
#                     or ld.get("availability"))
#     in_stock = None
#     if xp.get("out_of_stock_xpath"):
#         in_stock = not bool(_read(resp, xp["out_of_stock_xpath"], first=True))
#     elif stock_status:
#         s = stock_status.casefold().replace(" ", "").replace("_", "")
#         if "outofstock" in s or "soldout" in s:
#             in_stock = False
#         elif "instock" in s or "available" in s:
#             in_stock = True

#     canonical = _read(resp, xp.get("product_url_xpath"), first=True)

#     return {
#         "store_name": xp.get("_store_name"),
#         "remote_id": xp.get("_remote_id"),
#         "product_url": url,
#         "canonical_url": urljoin(url, canonical) if canonical else None,
#         "product_name": name,
#         "brand": brand,
#         "current_price": current_price,
#         "old_price": old_price,
#         "discount": discount,
#         "currency": currency,
#         "promo_code": _read(resp, xp.get("promo_code_xpath"), first=True),
#         "stock_status": stock_status,
#         "in_stock": in_stock,
#         "category": _read(resp, xp.get("category_raw_xpath")),
#         "sku": _read(resp, xp.get("sku_or_product_id_xpath"), first=True)
#                or ld.get("sku"),
#         "barcode": _read(resp, xp.get("barcode_xpath"), first=True),
#         "image_url": image_url,
#     }


# # ============================================================
# # The spider
# # ============================================================

# class WaffirStoreSpider(BaseInfinityScrollSpider):
#     name = "waffir"
#     STORE = ""                    # per-store subclasses: name / remote_id / slug

#     @classmethod
#     def from_crawler(cls, crawler, *args, **kwargs):
#         # WAFFIR_API_URL in settings.py (env var and -a api_url=... also work)
#         if not kwargs.get("api_url"):
#             kwargs["api_url"] = crawler.settings.get("WAFFIR_API_URL") or ""
#         return super().from_crawler(crawler, *args, **kwargs)

#     def __init__(self, store: str = "", api_url: str = "",
#                  categories: str = "off", segments: str = "on",
#                  page_param: str = "page={page}", product_fetcher: str = "auto",
#                  listing_only: str = "", resume: str = "", max_pages: str = "",
#                  *args, **kwargs):
#         store = store or self.STORE
#         if not store:
#             raise ValueError('Tell me which store: scrapy crawl waffir -a store="Tokyo Games"'
#                              '   (list them with: python -m waffir_swap.run_store --list)')
#         try:
#             entry = find_store(fetch_stores(api_url or None), store)
#         except WaffirApiError as e:
#             raise ValueError(str(e)) from None

#         status, why = store_status(entry)
#         if status != "ready":
#             raise ValueError(f"Store '{entry.get('store_name')}' can't be crawled: {why}")

#         self._notes: list = []
#         self._configure(entry, categories=categories, segments=segments,
#                         page_param=page_param, product_fetcher=product_fetcher,
#                         listing_only=listing_only, resume=resume, max_pages=max_pages)
#         super().__init__(*args, **kwargs)
#         self.logger.info(f"[waffir] store='{self.store_name}' mode={self.mode.value} "
#                          f"start={self.BASE_URL}{self.START_PATH} -> {self.OUTPUT_DIR}")
#         for note in self._notes:
#             self.logger.warning(f"[waffir] {note}")

#     # ---------- API record -> base-spider settings ----------
#     def _configure(self, entry: dict, *, categories, segments, page_param,
#                    product_fetcher, listing_only, resume, max_pages):
#         x = dict(entry["xpaths"])
#         self.store_name = entry.get("store_name") or "store"
#         self.remote_id = entry.get("remote_id")
#         x["_store_name"], x["_remote_id"] = self.store_name, self.remote_id
#         self._xp = x

#         start = urlsplit(x["start_url"].strip())
#         self.BASE_URL = f"{start.scheme}://{start.netloc}"
#         self.START_PATH = start.path + (f"?{start.query}" if start.query else "")

#         self._products_xpath = x["products_xpath"].strip()
#         item = _strip_href(self._products_xpath)
#         nxt = (x.get("next_page_xpath") or "").strip()
#         mode = (x.get("pagination_mode") or "url").strip().lower()
#         own = self._subclass_settings()
#         if "PAGINATION_MODE" in own:                  # store class knows better
#             mode = PaginationMode(own["PAGINATION_MODE"]).value

#         if mode == "url":
#             self.PAGINATION_MODE = PaginationMode.URL
#             self.LISTING_FETCHER = "http"             # auto-falls back to browser
#             self.LISTING_DISABLE_RESOURCES = True
#             self.LISTING_READY_SELECTOR = item
#             if nxt:                                   # follow the site's Next link
#                 self.LISTING_NEXT_PAGE_XPATH = nxt if "@href" in nxt else nxt + "/@href"
#                 self.LISTING_URL_TEMPLATE = "{base_url}{start_path}"
#             else:                                     # build ?page=N ourselves
#                 sep = "&" if "?" in self.START_PATH else "?"
#                 self.LISTING_URL_TEMPLATE = "{base_url}{start_path}" + sep + page_param
#             self.MAX_PAGES = int(max_pages) if max_pages else 500
#             if x.get("category_xpath"):
#                 self._notes.append("category_xpath is ignored in url mode "
#                                    "(categories work in scroll/click mode)")
#         else:
#             button = _strip_href(nxt) or own.get("LOAD_MORE_BUTTON_SELECTOR", "")
#             if mode == "click" and not button:
#                 self._notes.append("pagination_mode is 'click' but next_page_xpath is "
#                                    "empty - using scroll with Load-more detection")
#                 mode = "scroll"
#             self.PAGINATION_MODE = (PaginationMode.CLICK if mode == "click"
#                                     else PaginationMode.SCROLL)
#             self.LISTING_URL_TEMPLATE = "{base_url}{start_path}"
#             self.LISTING_READY_SELECTOR = item
#             self.LISTING_ITEM_SELECTOR = item
#             self.LOAD_MORE_BUTTON_SELECTOR = button   # click: the button; scroll: tried on stalls
#             self.SCROLL_STALL_CLICK_GENERIC = True
#             self.LISTING_BLOCK_DOMAINS = COMMON_TRACKER_DOMAINS
#             self.PAGINATION_MAX_ROUNDS = 3000
#             if segments.lower() != "off":
#                 self.SCROLL_SEGMENT_PARAM = page_param   # probed once; off if unsupported
#                 self.SCROLL_AUTO_PAGE_STEP = True
#             if x.get("category_xpath") and categories.lower() == "off":
#                 self._notes.append("category_xpath is set but not used (add "
#                                    "-a categories=fallback to crawl categories)")
#             if x.get("category_xpath") and categories.lower() in ("fallback", "always"):
#                 self.LISTING_CATEGORY_XPATH = x["category_xpath"]
#                 self.LISTING_CATEGORY_MODE = categories.lower()

#         # ---------- product phase ----------
#         self.PRODUCT_READY_SELECTOR = (_element_part(x.get("product_name_xpath"))
#                                        or "//h1")
#         required = ["product_name"]
#         if x.get("current_price_xpath"):
#             required.append("current_price")
#         self.PRODUCT_REQUIRED_FIELDS = tuple(required)
#         self.PRODUCT_FETCHER = product_fetcher if product_fetcher in (
#             "auto", "browser", "http") else "auto"
#         self.SCRAPE_PRODUCTS = not listing_only
#         self.RESUME = bool(resume)

#         # ---------- output ----------
#         self.OUTPUT_DIR = Path("output") / slugify(self.store_name)
#         self.URLS_FILENAME = "product_urls.json"
#         self.PRODUCTS_FILENAME = "products.json"

#         # ---------- per-store class settings win over the API ----------
#         for key, value in own.items():
#             setattr(self, key, value)
#         if own:
#             self._notes.append("store class settings: " + ", ".join(sorted(own)))

#     def _subclass_settings(self) -> dict:
#         """UPPERCASE settings defined in subclasses of WaffirStoreSpider."""
#         found: dict = {}
#         for klass in type(self).__mro__:
#             if klass is WaffirStoreSpider:
#                 break
#             for key, value in vars(klass).items():
#                 if key.isupper() and key != "STORE" and key not in found:
#                     found[key] = value
#         return found

#     def _subclass_defines(self, attr: str) -> bool:
#         for klass in type(self).__mro__:
#             if klass is WaffirStoreSpider:
#                 return False
#             if attr in vars(klass):
#                 return True
#         return False

#     # ---------- hooks: XPath-driven, picklable ----------
#     def _listing_parser(self):
#         if self._subclass_defines("parse_listing_page"):
#             return type(self).parse_listing_page
#         return functools.partial(_api_parse_listing,
#                                  products_xpath=self._products_xpath)

#     def _product_extractor(self):
#         if self._subclass_defines("extract_product"):
#             return type(self).extract_product
#         return functools.partial(_api_extract_product, xp=self._xp)

#     # one URL per Shopify product (/collections/x/products/y -> /products/y);
#     # other platforms' URLs are left unchanged
#     @staticmethod
#     def normalize_url(url: str) -> str:
#         return BaseInfinityScrollSpider.shopify_product_url(url)

#     @staticmethod
#     def parse_listing_page(resp) -> set:          # replaced by _listing_parser()
#         raise NotImplementedError

#     @staticmethod
#     def extract_product(resp, url: str):          # replaced by _product_extractor()
#         raise NotImplementedError

"""
One spider for every store in the Waffir backend.

    scrapy crawl waffir -a store="Tokyo Games"

At start-up it reads the store's record from the Waffir API
(/api/store/fetch-store-data/) and turns its XPaths into a full
BaseInfinityScrollSpider configuration: listing mode (url / scroll /
click), product links, Load-more / Next buttons, categories, and a
product extractor for every field. Everything else (segments, throttle
recovery, resume, HTTP/browser auto-switch, completeness verdict) comes
from the base spider unchanged.

Optional arguments (-a name=value):
    store           store name, remote_id or domain           (required)
    api_url         Waffir API address (default: WAFFIR_API_URL or localhost)
    categories      auto | all | fallback | always | off   (default auto)
                    auto = "all" when the store has a category_xpath:
                    read the category links on the start URL, then collect
                    products from every category
    listing_minutes max minutes for collecting product URLs (default 20,
                    0 = no limit); the next run continues where it stopped
    segments        on | off      try ?page=N reloads in scroll/click mode (default on)
    page_param      URL-mode page parameter when no next_page_xpath (default "page={page}")
    product_fetcher auto | browser | http                       (default auto)
    listing_only    1             collect product URLs only, skip product pages
    resume          0             start from scratch. Default: keep earlier
                                  products, skip their URLs, continue listings
    max_pages       N             URL-mode page limit (default 500)

Output: output/<store_slug>/product_urls.json, products.json, products.jsonl,
        products_listing_report.json, products_failed.json

Per-store spiders (spiders/stores.py): subclass WaffirStoreSpider, set
STORE = "<name in Waffir>" plus any base-spider setting. XPaths still come
from the API; settings defined in the subclass win over the API values.
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlsplit

from waffir_swap.spiders import base_spider as _base

_NEEDED_BASE_VERSION = 5
if getattr(_base, "BASE_SPIDER_VERSION", 0) < _NEEDED_BASE_VERSION:
    raise ImportError(
        f"waffir_swap/spiders/base_spider.py is out of date (version "
        f"{getattr(_base, 'BASE_SPIDER_VERSION', 'none')}, need "
        f"{_NEEDED_BASE_VERSION}+). Copy the latest base_spider.py into "
        f"{_base.__file__} - and update the other spider files with it."
    )

from waffir_swap.spiders.base_spider import (  # noqa: E402
    COMMON_TRACKER_DOMAINS,
    BaseInfinityScrollSpider,
    PaginationMode,
)
from waffir_swap.waffir_api import (
    WaffirApiError,
    fetch_stores,
    find_store,
    slugify,
    store_status,
)

B = BaseInfinityScrollSpider

# XPaths that return a string instead of nodes, e.g.
# substring-before(substring-after(//script[...], '"x":'), ',')
_STRING_FUNCTIONS = ("normalize-space(", "substring", "concat(", "string(",
                     "translate(", "number(", "sum(", "count(")


# ============================================================
# XPath helpers (module level -> picklable for worker processes)
# ============================================================

def _clean(value) -> Optional[str]:
    text = " ".join(str(value).split()) if value is not None else ""
    return text or None


def _strip_href(xp: str) -> str:
    """'//a[...]/@href' -> '//a[...]' (the element, for waiting/clicking)."""
    xp = (xp or "").strip()
    return xp[: -len("/@href")] if xp.endswith("/@href") else xp


def _element_part(xp: str) -> Optional[str]:
    """Element part of a value XPath, used as a 'page ready' marker:
    '(//h1)[1]/text()' -> '(//h1)[1]', '//meta[@p]/@content' -> '//meta[@p]'.
    None for string-function XPaths or when the result isn't valid XPath."""
    s = (xp or "").strip()
    if not s or s.startswith(_STRING_FUNCTIONS):
        return None
    if s.startswith("(") and s.endswith(")[1]"):
        s = s[1:-4]
    cut = s.rfind("/text()")
    if cut != -1:
        s = s[:cut]
    last = s.rfind("/@")
    if last != -1 and "/" not in s[last + 2:]:
        s = s[:last]
    s = s.rstrip("/")
    try:
        import lxml.etree
        lxml.etree.XPath(s)
    except Exception:
        return None
    return s or None


def _read(resp, xp: str, first: bool = False) -> Optional[str]:
    """Value of any kind of XPath from the API:
       * string functions (normalize-space(...), substring-...)  -> that string
       * attributes (/@content, /@src)                           -> first value
       * text nodes (/text(), //text())                          -> joined text
       * elements ((//h6[...])[1])                               -> element text
    """
    xp = (xp or "").strip()
    if not xp:
        return None
    if xp.startswith(_STRING_FUNCTIONS):
        return _clean(B.xpath_string(resp, xp))
    try:
        values = [str(v) for v in resp.xpath(xp).getall()]
    except Exception:
        values = []
    if any(v.lstrip().startswith("<") for v in values):        # elements
        return _clean(B.xpath_string(resp, f"normalize-space(string(({xp})[1]))"))
    values = [c for c in (_clean(v) for v in values) if c]
    if not values:
        return None
    is_attribute = xp.rfind("/@") > xp.rfind("/text()")
    if first or is_attribute:
        return values[0]
    return _clean(" ".join(values))


def _api_parse_listing(resp, products_xpath: str) -> set:
    xp = products_xpath.strip()
    if "@href" not in xp:
        xp = xp + "/@href"
    return {str(h).strip() for h in resp.xpath(xp).getall() if h and str(h).strip()}


def _api_extract_product(resp, url: str, xp: dict) -> dict:
    ld = B.jsonld_product(resp)

    name = _read(resp, xp.get("product_name_xpath")) or ld.get("name") \
        or B.meta(resp, "og:title")
    brand = _read(resp, xp.get("brand_name_xpath"), first=True) or ld.get("brand")

    current_price = (_read(resp, xp.get("current_price_xpath"), first=True)
                     or ld.get("price")
                     or B.meta(resp, "product:price:amount")
                     or B.meta(resp, "og:price:amount"))
    old_price = _read(resp, xp.get("old_price_xpath"), first=True)
    if old_price and B.parse_price(old_price) == B.parse_price(current_price):
        old_price = None                                   # not actually reduced

    discount = (_read(resp, xp.get("discount_percent_xpath"))
                or B.discount_percent(current_price, old_price))

    currency = (_read(resp, xp.get("currency_xpath"), first=True)
                or B.meta(resp, "product:price:currency")
                or B.meta(resp, "og:price:currency")
                or ld.get("currency") or "SAR")

    image_url = (_read(resp, xp.get("image_url_xpath"), first=True)
                 or B.meta(resp, "og:image") or ld.get("image"))
    if image_url:
        image_url = urljoin(url, image_url)

    stock_status = (_read(resp, xp.get("stock_status_xpath"), first=True)
                    or ld.get("availability"))
    in_stock = None
    if xp.get("out_of_stock_xpath"):
        in_stock = not bool(_read(resp, xp["out_of_stock_xpath"], first=True))
    elif stock_status:
        s = stock_status.casefold().replace(" ", "").replace("_", "")
        if "outofstock" in s or "soldout" in s:
            in_stock = False
        elif "instock" in s or "available" in s:
            in_stock = True

    canonical = _read(resp, xp.get("product_url_xpath"), first=True)

    return {
        "store_name": xp.get("_store_name"),
        "remote_id": xp.get("_remote_id"),
        "product_url": url,
        "canonical_url": urljoin(url, canonical) if canonical else None,
        "product_name": name,
        "brand": brand,
        "current_price": current_price,
        "old_price": old_price,
        "discount": discount,
        "currency": currency,
        "promo_code": _read(resp, xp.get("promo_code_xpath"), first=True),
        "stock_status": stock_status,
        "in_stock": in_stock,
        "category": _read(resp, xp.get("category_raw_xpath")),
        "sku": _read(resp, xp.get("sku_or_product_id_xpath"), first=True)
               or ld.get("sku"),
        "barcode": _read(resp, xp.get("barcode_xpath"), first=True),
        "image_url": image_url,
    }


# ============================================================
# The spider
# ============================================================

class WaffirStoreSpider(BaseInfinityScrollSpider):
    name = "waffir"
    STORE = ""                    # per-store subclasses: name / remote_id / slug

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        # WAFFIR_API_URL in settings.py (env var and -a api_url=... also work)
        if not kwargs.get("api_url"):
            kwargs["api_url"] = crawler.settings.get("WAFFIR_API_URL") or ""
        return super().from_crawler(crawler, *args, **kwargs)

    def __init__(self, store: str = "", api_url: str = "",
                 categories: str = "auto", segments: str = "on",
                 page_param: str = "page={page}", product_fetcher: str = "auto",
                 listing_only: str = "", resume: str = "", max_pages: str = "",
                 listing_minutes: str = "20",
                 *args, **kwargs):
        store = store or self.STORE
        if not store:
            raise ValueError('Tell me which store: scrapy crawl waffir -a store="Tokyo Games"'
                             '   (list them with: python -m waffir_swap.run_store --list)')
        try:
            entry = find_store(fetch_stores(api_url or None), store)
        except WaffirApiError as e:
            raise ValueError(str(e)) from None

        status, why = store_status(entry)
        if status != "ready":
            raise ValueError(f"Store '{entry.get('store_name')}' can't be crawled: {why}")

        self._notes: list = []
        self._configure(entry, categories=categories, segments=segments,
                        page_param=page_param, product_fetcher=product_fetcher,
                        listing_only=listing_only, resume=resume, max_pages=max_pages,
                        listing_minutes=listing_minutes)
        super().__init__(*args, **kwargs)
        self.logger.info(f"[waffir] store='{self.store_name}' mode={self.mode.value} "
                         f"start={self.BASE_URL}{self.START_PATH} -> {self.OUTPUT_DIR}")
        self.logger.info(
            f"[waffir] categories={self.LISTING_CATEGORY_MODE} | listing budget="
            + (f"{self.LISTING_TIME_BUDGET_S // 60} min" if self.LISTING_TIME_BUDGET_S
               else "none")
            + f" | resume={'on' if self.RESUME else 'off (fresh start)'}")
        for note in self._notes:
            self.logger.warning(f"[waffir] {note}")

    # ---------- API record -> base-spider settings ----------
    def _configure(self, entry: dict, *, categories, segments, page_param,
                   product_fetcher, listing_only, resume, max_pages, listing_minutes):
        x = dict(entry["xpaths"])
        self.store_name = entry.get("store_name") or "store"
        self.remote_id = entry.get("remote_id")
        x["_store_name"], x["_remote_id"] = self.store_name, self.remote_id
        self._xp = x

        start = urlsplit(x["start_url"].strip())
        self.BASE_URL = f"{start.scheme}://{start.netloc}"
        self.START_PATH = start.path + (f"?{start.query}" if start.query else "")

        self._products_xpath = x["products_xpath"].strip()
        item = _strip_href(self._products_xpath)
        nxt = (x.get("next_page_xpath") or "").strip()
        mode = (x.get("pagination_mode") or "url").strip().lower()
        own = self._subclass_settings()
        if "PAGINATION_MODE" in own:                  # store class knows better
            mode = PaginationMode(own["PAGINATION_MODE"]).value

        if mode == "url":
            self.PAGINATION_MODE = PaginationMode.URL
            self.LISTING_FETCHER = "http"             # auto-falls back to browser
            self.LISTING_DISABLE_RESOURCES = True
            self.LISTING_READY_SELECTOR = item
            if nxt:                                   # follow the site's Next link
                self.LISTING_NEXT_PAGE_XPATH = nxt if "@href" in nxt else nxt + "/@href"
                self.LISTING_URL_TEMPLATE = "{base_url}{start_path}"
            else:                                     # build ?page=N ourselves
                sep = "&" if "?" in self.START_PATH else "?"
                self.LISTING_URL_TEMPLATE = "{base_url}{start_path}" + sep + page_param
                self.LISTING_PAGE_PARAM = page_param     # same paging for categories
            self.MAX_PAGES = int(max_pages) if max_pages else 500
        else:
            button = _strip_href(nxt) or own.get("LOAD_MORE_BUTTON_SELECTOR", "")
            if mode == "click" and not button:
                self._notes.append("pagination_mode is 'click' but next_page_xpath is "
                                   "empty - using scroll with Load-more detection")
                mode = "scroll"
            self.PAGINATION_MODE = (PaginationMode.CLICK if mode == "click"
                                    else PaginationMode.SCROLL)
            self.LISTING_URL_TEMPLATE = "{base_url}{start_path}"
            self.LISTING_READY_SELECTOR = item
            self.LISTING_ITEM_SELECTOR = item
            self.LOAD_MORE_BUTTON_SELECTOR = button   # click: the button; scroll: tried on stalls
            self.SCROLL_STALL_CLICK_GENERIC = True
            self.LISTING_BLOCK_DOMAINS = COMMON_TRACKER_DOMAINS
            self.PAGINATION_MAX_ROUNDS = 3000
            if segments.lower() != "off":
                self.SCROLL_SEGMENT_PARAM = page_param   # probed once; off if unsupported
                self.SCROLL_AUTO_PAGE_STEP = True

        # ---------- product phase ----------
        self.PRODUCT_READY_SELECTOR = (_element_part(x.get("product_name_xpath"))
                                       or "//h1")
        required = ["product_name"]
        if x.get("current_price_xpath"):
            required.append("current_price")
        self.PRODUCT_REQUIRED_FIELDS = tuple(required)
        self.PRODUCT_FETCHER = product_fetcher if product_fetcher in (
            "auto", "browser", "http") else "auto"
        self.SCRAPE_PRODUCTS = not listing_only
        self.RESUME = (resume or "1").strip().lower() not in ("0", "false", "no", "off")

        # ---------- categories ----------
        cat_mode = (categories or "auto").strip().lower()
        if cat_mode == "auto":
            cat_mode = "all" if (x.get("category_xpath") or "").strip() else "off"
        if cat_mode not in ("all", "fallback", "always", "off"):
            self._notes.append(f"unknown categories={categories!r}; using 'off'")
            cat_mode = "off"
        if (x.get("category_xpath") or "").strip() and cat_mode != "off":
            self.LISTING_CATEGORY_XPATH = x["category_xpath"].strip()
            self.LISTING_CATEGORY_MODE = cat_mode
        else:
            self.LISTING_CATEGORY_MODE = "off"

        # ---------- time budget for collecting product URLs ----------
        try:
            minutes = float(listing_minutes) if str(listing_minutes).strip() else 20.0
        except ValueError:
            minutes = 20.0
        self.LISTING_TIME_BUDGET_S = int(minutes * 60)

        # ---------- output ----------
        self.OUTPUT_DIR = Path("output") / slugify(self.store_name)
        self.URLS_FILENAME = "product_urls.json"
        self.PRODUCTS_FILENAME = "products.json"

        # ---------- per-store class settings win over the API ----------
        for key, value in own.items():
            setattr(self, key, value)
        if own:
            self._notes.append("store class settings: " + ", ".join(sorted(own)))

    def _subclass_settings(self) -> dict:
        """UPPERCASE settings defined in subclasses of WaffirStoreSpider."""
        found: dict = {}
        for klass in type(self).__mro__:
            if klass is WaffirStoreSpider:
                break
            for key, value in vars(klass).items():
                if key.isupper() and key != "STORE" and key not in found:
                    found[key] = value
        return found

    def _subclass_defines(self, attr: str) -> bool:
        for klass in type(self).__mro__:
            if klass is WaffirStoreSpider:
                return False
            if attr in vars(klass):
                return True
        return False

    # ---------- hooks: XPath-driven, picklable ----------
    def _listing_parser(self):
        if self._subclass_defines("parse_listing_page"):
            return type(self).parse_listing_page
        return functools.partial(_api_parse_listing,
                                 products_xpath=self._products_xpath)

    def _product_extractor(self):
        if self._subclass_defines("extract_product"):
            return type(self).extract_product
        return functools.partial(_api_extract_product, xp=self._xp)

    # one URL per Shopify product (/collections/x/products/y -> /products/y);
    # other platforms' URLs are left unchanged
    @staticmethod
    def normalize_url(url: str) -> str:
        return BaseInfinityScrollSpider.shopify_product_url(url)

    @staticmethod
    def parse_listing_page(resp) -> set:          # replaced by _listing_parser()
        raise NotImplementedError

    @staticmethod
    def extract_product(resp, url: str):          # replaced by _product_extractor()
        raise NotImplementedError