from typing import Any

from crawler_settings import (
    MONGO_COLLECTION,
    MONGO_DATABASE,
    MONGO_URI,
    SHOWN_RECENT_PAGES,
)
from pymongo import MongoClient

PAGE_SEPARATOR = "--------------------"
URI_EXAMPLE = "mongodb://<user>:<password>@localhost:27017/?authSource=admin"


def print_page(page: dict[str, Any]) -> None:
    print(f"URL: {page.get('url')}")
    print(f"Title: {page.get('title')}")
    print(f"S3 key {page.get('s3_key')}")
    print(PAGE_SEPARATOR)


def main() -> None:
    if not MONGO_URI:
        print(f"Set MONGO_URI, e.g. {URI_EXAMPLE}")
        return
    client: MongoClient[dict[str, Any]] = MongoClient(MONGO_URI)
    pages = client[MONGO_DATABASE][MONGO_COLLECTION]
    print(f"Total Documents: {pages.count_documents({})}")
    print(f"\n--- Last {SHOWN_RECENT_PAGES} Pages ---")
    for page in pages.find().sort("_id", -1).limit(SHOWN_RECENT_PAGES):
        print_page(page)


if __name__ == "__main__":
    main()
