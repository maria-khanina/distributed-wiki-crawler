"""Times how long the Docker Compose crawler takes to fill MongoDB with pages.

Start the services without the seeder, run this script, then run the seeder."""

import time

from crawler_settings import MONGO_COLLECTION, MONGO_DATABASE, MONGO_URI
from pymongo import MongoClient
from pymongo.collection import Collection

from benchmark.benchmark_settings import (
    MAX_WAIT_SECONDS,
    POLL_INTERVAL_SECONDS,
    TARGET_PAGES,
)
from benchmark.milestones import MilestoneReporter
from benchmark.stored_page import StoredPage


def wait_for_target(pages: Collection[StoredPage]) -> int:
    """Poll the page count until the target or the time limit is reached."""
    reporter = MilestoneReporter()
    stored_pages = 0
    while stored_pages < TARGET_PAGES:
        if reporter.elapsed_seconds() > MAX_WAIT_SECONDS:
            break
        stored_pages = pages.count_documents({})
        reporter.report(stored_pages)
        time.sleep(POLL_INTERVAL_SECONDS)
    return stored_pages


def main() -> None:
    client: MongoClient[StoredPage] = MongoClient(MONGO_URI)
    pages = client[MONGO_DATABASE][MONGO_COLLECTION]
    print(f"{wait_for_target(pages)} pages stored")


if __name__ == "__main__":
    main()
