"""
Per-store spiders. Each one inherits EVERYTHING from the base spider (via
WaffirStoreSpider) and takes its start URL, pagination mode and XPaths from
the Waffir API. A class here only adds what the API can't express - things
learned from real runs. Settings defined here win over the API values.

A store does NOT need a class here to be crawled - the generic spider runs
any ready store:
    scrapy crawl waffir -a store="Mihyar"

With a class:
    scrapy crawl jarir

Template for a new one:

    class MyStore(WaffirStoreSpider):
        name = "mystore"                    # scrapy crawl mystore
        STORE = "Name exactly as in Waffir" # or remote_id / slug
        # any base-spider setting, e.g.
        LISTING_TOTAL_COUNT_TEXT = "results"   # "1409 results" -> exact verdict
        LISTING_FETCHER = "browser"            # site answers plain HTTP with 403

You can also override parse_listing_page / extract_product (as
@staticmethod) when a store needs custom parsing code.
"""

from waffir_swap.spiders.base_spider import PaginationMode
from waffir_swap.spiders.waffir_store import WaffirStoreSpider


class JarirStore(WaffirStoreSpider):
    """Waffir says 'scroll', but the page shows 12 products and a
    "Load more products" button; ?p=N hangs on this site (seen in a run)."""
    name = "jarir"
    STORE = "Jarir Bookstore"
    PAGINATION_MODE = PaginationMode.CLICK
    LOAD_MORE_BUTTON_SELECTOR = (
        "(//button | //a | //*[@role='button'])"
        "[contains(normalize-space(.), 'Load more')]"
    )
    SCROLL_SEGMENT_PARAM = ""
    PAGINATION_BUTTON_WAIT_MS = 12000


class TedBakerStore(WaffirStoreSpider):
    """"Sale (385 items)" -> exact completeness check and stop at the total."""
    name = "tedbaker"
    STORE = "Ted Baker"
    LISTING_TOTAL_COUNT_TEXT = "items)"
    LISTING_URL_CONTAINS = "/products/"


class EyewaStore(WaffirStoreSpider):
    """The listing gets slower with every card and ?p=N is ignored
    (seen in real runs): skip the page probe, watch page speed."""
    name = "eyewa"
    STORE = "Eyewa"
    SCROLL_SEGMENT_PARAM = ""
    SCROLL_SLOW_PING_MS = 1500