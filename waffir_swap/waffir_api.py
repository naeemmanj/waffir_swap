"""
Client for the Waffir backend (Django) store API.

    GET http://localhost:8000/api/store/fetch-store-data/

returns every store with its XPaths. Spiders read their configuration from
here instead of hard-coding it, so a new store = a new row in Django admin.

Settings (environment variables, all optional):
    WAFFIR_API_URL    API address   (default: http://localhost:8000/api/store/fetch-store-data/)
    WAFFIR_API_TOKEN  DRF token, sent as "Authorization: Token <value>" if set

The last good API response is cached in .waffir_api_cache.json, so spiders
still start if the Django server is briefly down (a warning is printed).
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import List, Optional, Tuple

DEFAULT_API_URL = "http://localhost:8000/api/store/fetch-store-data/"
CACHE_FILE = Path(".waffir_api_cache.json")


class WaffirApiError(RuntimeError):
    pass


def api_url(override: Optional[str] = None) -> str:
    return override or os.environ.get("WAFFIR_API_URL") or DEFAULT_API_URL


def fetch_stores(url: Optional[str] = None, timeout: int = 20,
                 use_cache: bool = True) -> List[dict]:
    """All stores from the Waffir API (falls back to the local cache)."""
    url = api_url(url)
    headers = {"Accept": "application/json"}
    token = os.environ.get("WAFFIR_API_TOKEN")
    if token:
        headers["Authorization"] = f"Token {token}"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read().decode("utf-8"))
        if use_cache:
            try:
                CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False),
                                      encoding="utf-8")
            except OSError:
                pass
    except (urllib.error.URLError, OSError, ValueError) as e:
        if use_cache and CACHE_FILE.exists():
            print(f"[waffir] WARNING: API {url} unreachable ({e}); "
                  f"using cached data from {CACHE_FILE}")
            data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        else:
            raise WaffirApiError(
                f"Could not reach the Waffir API at {url} ({e}).\n"
                f"Start the backend first:  python manage.py runserver"
            ) from e
    # Django REST Framework pagination wraps lists in {"results": [...]}
    if isinstance(data, dict) and "results" in data:
        data = data["results"]
    if not isinstance(data, list):
        raise WaffirApiError(f"Unexpected API response from {url}: {type(data)}")
    return data


def store_status(entry: dict) -> Tuple[str, str]:
    """('ready' | 'no_xpaths' | 'sitemap_only' | 'incomplete', explanation)."""
    xp = entry.get("xpaths")
    if not xp:
        return "no_xpaths", "no XPaths configured in Waffir admin yet"
    if not xp.get("start_url"):
        if xp.get("has_sitemap"):
            return "sitemap_only", ("only a sitemap is configured (no start_url / "
                                    "product XPaths) - not crawlable yet")
        return "incomplete", "start_url is empty"
    if not xp.get("products_xpath"):
        return "incomplete", "products_xpath is empty"
    if not xp.get("product_name_xpath"):
        return "incomplete", "product_name_xpath is empty"
    return "ready", "ok"


def find_store(stores: List[dict], key: str) -> dict:
    """Find one store by name, remote_id or domain (case-insensitive)."""
    k = (key or "").strip().casefold()
    if not k:
        raise WaffirApiError("No store given.")

    def domain(s: dict) -> str:
        u = (s.get("url") or "").casefold()
        return u.split("//", 1)[-1].split("/", 1)[0].removeprefix("www.")

    for test in (
        lambda s: (s.get("store_name") or "").casefold() == k,         # exact name
        lambda s: (s.get("remote_id") or "").casefold() == k,          # remote_id
        lambda s: domain(s) == k.removeprefix("www."),                 # domain
        lambda s: slugify(s.get("store_name") or "") == slugify(k),    # slug
        lambda s: k in (s.get("store_name") or "").casefold(),         # part of name
    ):
        hits = [s for s in stores if test(s)]
        if len(hits) == 1:
            return hits[0]
        if len(hits) > 1:
            names = ", ".join(f"'{h.get('store_name')}'" for h in hits)
            raise WaffirApiError(f"'{key}' matches several stores: {names}. "
                                 f"Use the exact name.")
    raise WaffirApiError(f"No store matching '{key}'. "
                         f"Run:  python -m waffir_swap.run_store --list")


def slugify(name: str) -> str:
    """'L'Occitane KSA' -> 'loccitane_ksa' (for output folders)."""
    out, last_us = [], False
    for ch in (name or "store").casefold():
        if ch.isalnum() and ch.isascii():
            out.append(ch)
            last_us = False
        elif ch in " -_/&.()" and not last_us:
            out.append("_")
            last_us = True
    return "".join(out).strip("_") or "store"