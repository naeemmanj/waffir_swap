"""
Run Waffir stores from the command line.

    python -m waffir_swap.run_store --list                 # every store + status
    python -m waffir_swap.run_store "Tokyo Games"          # one store
    python -m waffir_swap.run_store "Tokyo Games" --listing-only
    python -m waffir_swap.run_store --all                  # every ready store, one by one

Extra spider options are passed through with --opt, e.g.
    python -m waffir_swap.run_store "Whites Pharmacy" --opt categories=fallback

Each store runs in its own `scrapy crawl waffir` process, so one failing
store never stops the others (--all keeps going and prints a summary).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

from waffir_swap.waffir_api import (
    WaffirApiError,
    api_url,
    fetch_stores,
    find_store,
    slugify,
    store_status,
)


def _project_root() -> Path:
    """Folder that holds scrapy.cfg (where `scrapy crawl` must run)."""
    here = Path(__file__).resolve()
    for folder in [here.parent, *here.parents]:
        if (folder / "scrapy.cfg").exists():
            return folder
    return Path.cwd()


def _print_list(stores) -> None:
    ready = 0
    print(f"\nStores from {api_url()}\n")
    print(f"  {'STATUS':13} {'MODE':7} {'STORE':32} DETAIL")
    print(f"  {'-' * 13} {'-' * 7} {'-' * 32} {'-' * 40}")
    for s in stores:
        status, why = store_status(s)
        mode = ((s.get("xpaths") or {}).get("pagination_mode") or "-")
        ready += status == "ready"
        detail = (s.get("xpaths") or {}).get("start_url") if status == "ready" else why
        print(f"  {status:13} {mode:7} {(s.get('store_name') or '?')[:32]:32} {detail}")
    print(f"\n  {ready} of {len(stores)} stores are ready to crawl.\n")


def _project_loads() -> bool:
    """`scrapy list` imports every spider file. If that fails, every crawl
    would fail the same way - show the error once and stop."""
    proc = subprocess.run([sys.executable, "-m", "scrapy", "list"],
                          cwd=_project_root(), capture_output=True, text=True)
    if proc.returncode == 0:
        return True
    lines = [l for l in (proc.stderr or proc.stdout).strip().splitlines() if l.strip()]
    culprit = [l.strip() for l in lines if l.strip().startswith('File "') and "spiders" in l]
    print("\nThe Scrapy project does not load, so no store can run:\n")
    print("   " + (lines[-1] if lines else "unknown error"))
    if culprit:
        print("   in " + culprit[-1])
    print("\nFix that file (usually: copy the latest base_spider.py and spider "
          "files together), then run again.\n")
    return False


def _run_one(name: str, listing_only: bool, opts) -> int:
    cmd = [sys.executable, "-m", "scrapy", "crawl", "waffir", "-a", f"store={name}"]
    if listing_only:
        cmd += ["-a", "listing_only=1"]
    for o in opts or []:
        cmd += ["-a", o]
    print(f"\n=== {name} ===\n$ {' '.join(cmd)}\n", flush=True)
    return subprocess.call(cmd, cwd=_project_root())


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Run Waffir stores")
    ap.add_argument("store", nargs="?", help="store name, remote_id or domain")
    ap.add_argument("--list", action="store_true", help="list stores and their status")
    ap.add_argument("--all", action="store_true", help="run every ready store")
    ap.add_argument("--listing-only", action="store_true",
                    help="collect product URLs only (skip product pages)")
    ap.add_argument("--opt", action="append", metavar="KEY=VALUE",
                    help="extra spider option, e.g. --opt categories=fallback")
    args = ap.parse_args(argv)

    try:
        stores = fetch_stores()
    except WaffirApiError as e:
        print(e)
        return 2

    if args.list or not (args.store or args.all):
        _print_list(stores)
        return 0

    if not _project_loads():
        return 2

    if args.store:
        try:
            entry = find_store(stores, args.store)
        except WaffirApiError as e:
            print(e)
            return 2
        status, why = store_status(entry)
        if status != "ready":
            print(f"'{entry.get('store_name')}' can't be crawled: {why}")
            return 2
        return _run_one(entry["store_name"], args.listing_only, args.opt)

    # --all: one store after another, never stop on a failure
    results = []
    for entry in stores:
        if store_status(entry)[0] != "ready":
            continue
        t0 = time.time()
        code = _run_one(entry["store_name"], args.listing_only, args.opt)
        results.append((entry["store_name"], code, time.time() - t0))
    print("\n=== summary ===")
    for name, code, secs in results:
        out = Path("output") / slugify(name)
        print(f"  {'OK ' if code == 0 else 'ERR'}  {name:32} {secs / 60:5.1f} min  -> {out}")
    return 0 if all(c == 0 for _, c, _ in results) else 1


if __name__ == "__main__":
    sys.exit(main())