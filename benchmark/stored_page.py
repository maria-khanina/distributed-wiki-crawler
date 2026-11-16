"""Document shape written to MongoDB by the sequential baseline crawler."""

from typing import TypedDict


class StoredPage(TypedDict):
    url: str
    title: str
    text: str
