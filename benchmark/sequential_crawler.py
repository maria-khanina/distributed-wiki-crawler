"""Baseline crawler for the benchmark: one request at a time, parsing and
storing every page inline, with no queues, no concurrency and no retries."""

from collections import deque
import urllib.request

from crawler_settings import (
    ARTICLE_URL,
    MONGO_URI,
    START_TITLES,
    USER_AGENT,
)
from lxml import html
from parser import is_article_link
from pymongo import MongoClient
from pymongo.collection import Collection

from benchmark.benchmark_settings import (
    NO_TITLE,
    REQUEST_TIMEOUT_SECONDS,
    SEQUENTIAL_COLLECTION,
    SEQUENTIAL_DATABASE,
    TARGET_PAGES,
)
from benchmark.milestones import MilestoneReporter
from benchmark.stored_page import StoredPage


def fetch_page(url: str) -> bytes | None:
    """Download one page, returning None when the request fails."""
    request = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as response:
            return response.read()
    except OSError:
        return None


def extract_new_links(
    tree: html.HtmlElement,
    url: str,
    seen: set[str],
) -> list[str]:
    """Return article links of the page that were not seen before."""
    tree.make_links_absolute(url)
    new_links: list[str] = []
    for _, _, link, _ in tree.iterlinks():
        clean_link = link.split("#")[0]
        if is_article_link(clean_link) and clean_link not in seen:
            seen.add(clean_link)
            new_links.append(clean_link)
    return new_links


def build_page(tree: html.HtmlElement, url: str) -> StoredPage:
    """Build the MongoDB document with the title and text of a parsed page."""
    titles = tree.xpath("/html/head/title/text()")
    title = titles[0] if titles else NO_TITLE
    return {"url": url, "title": title, "text": tree.text_content()}


class SequentialCrawler:
    """Breadth-first crawl that stops after the target number of stored pages."""

    def __init__(
        self,
        pages: Collection[StoredPage],
        start_urls: list[str],
    ) -> None:
        self.pages = pages
        self.queue: deque[str] = deque(start_urls)
        self.seen: set[str] = set(start_urls)
        self.stored_pages = 0
        self.failed_requests = 0
        self.reporter = MilestoneReporter()

    def crawl_page(self) -> None:
        url = self.queue.popleft()
        body = fetch_page(url)
        if body is None:
            self.failed_requests += 1
            return
        tree = html.fromstring(body)
        self.queue.extend(extract_new_links(tree, url, self.seen))
        self.pages.insert_one(build_page(tree, url))
        self.stored_pages += 1
        self.reporter.report(self.stored_pages)

    def run(self) -> None:
        while self.stored_pages < TARGET_PAGES and self.queue:
            self.crawl_page()


def main() -> None:
    client: MongoClient[StoredPage] = MongoClient(MONGO_URI)
    pages = client[SEQUENTIAL_DATABASE][SEQUENTIAL_COLLECTION]
    pages.drop()
    start_urls: list[str] = [ARTICLE_URL + title for title in START_TITLES]
    crawler = SequentialCrawler(pages, start_urls)
    crawler.run()
    print(f"{crawler.stored_pages} pages stored")
    print(f"{crawler.failed_requests} failed requests")


if __name__ == "__main__":
    main()
