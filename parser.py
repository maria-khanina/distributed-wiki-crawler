from __future__ import annotations

import asyncio
import json
import socket
import time
from typing import Any
from urllib.parse import unquote

import aioboto3
from crawler_settings import (
    ARTICLE_PATH,
    DOMAIN,
    FETCH_QUEUE,
    LINKS_QUEUE,
    LOCAL_CACHE_LIMIT,
    MONGO_COLLECTION,
    MONGO_DATABASE,
    MONGO_URI,
    PARSER_MODE,
    PARSER_TASKS,
    POP_TIMEOUT_SECONDS,
    RECONNECT_DELAY_SECONDS,
    REDIS_HOST,
    REDIS_PORT,
    SEEN_SET,
    SYSTEM_PREFIXES,
    TEXT_QUEUE,
)
from lxml import html
from pymongo import AsyncMongoClient
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from storage import open_storage, read_page


def is_article_link(url: str) -> bool:
    if DOMAIN not in url or ARTICLE_PATH not in url:
        return False
    title = unquote(url).partition(ARTICLE_PATH)[2]
    return not title.startswith(SYSTEM_PREFIXES)


class LinkExtractor:
    def __init__(self, redis_client: Redis) -> None:
        self.redis = redis_client
        self.local_cache: set[str] = set()

    def _collect_links(self, html_bytes: bytes, root_url: str) -> set[str]:
        tree = html.fromstring(html_bytes)
        tree.make_links_absolute(root_url)
        links: set[str] = set()
        for _, _, link, _ in tree.iterlinks():
            clean_url = link.split("#")[0]
            if is_article_link(clean_url):
                links.add(clean_url)
        return links

    async def _keep_new(self, links: set[str]) -> list[str]:
        unknown_links: list[str] = [
            link for link in links if link not in self.local_cache
        ]
        if not unknown_links:
            return []
        pipeline = self.redis.pipeline()
        for link in unknown_links:
            pipeline.sadd(SEEN_SET, link)
        results = await pipeline.execute()
        self.local_cache.update(unknown_links)
        return [link for link, is_new in zip(unknown_links, results) if is_new]

    async def process(
        self,
        html_bytes: bytes,
        url: str,
        task_id: str,
        key: str,
    ) -> None:
        links = self._collect_links(html_bytes, url)
        new_links = await self._keep_new(links)
        if new_links:
            await self.redis.rpush(FETCH_QUEUE, *new_links)
        if len(self.local_cache) > LOCAL_CACHE_LIMIT:
            self.local_cache.clear()
        print(f"[{task_id}] links: found {len(links)}, new {len(new_links)}")


class PageSaver:
    def __init__(self, mongo_client: AsyncMongoClient[dict[str, Any]]) -> None:
        self.pages = mongo_client[MONGO_DATABASE][MONGO_COLLECTION]

    async def process(
        self,
        html_bytes: bytes,
        url: str,
        task_id: str,
        key: str,
    ) -> None:
        tree = html.fromstring(html_bytes)
        titles = tree.xpath("/html/head/title/text()")
        title = titles[0] if titles else "No Title"
        await self.pages.update_one(
            {"url": url},
            {"$set": {
                "title": title,
                "text": tree.text_content(),
                "updated_at": time.time(),
                "s3_key": key,
            }},
            upsert=True,
        )
        print(f"[{task_id}] saved content of {title} article")


PageWorker = LinkExtractor | PageSaver


async def pop_task(redis_client: Redis, queue: str) -> dict[str, str] | None:
    try:
        raw_task = await redis_client.blpop([queue], timeout=POP_TIMEOUT_SECONDS)
    except RedisConnectionError:
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        return None
    if not raw_task:
        return None
    return json.loads(raw_task[1])


async def handle_task(
    storage: aioboto3.S3Client,
    worker: PageWorker,
    task: dict[str, str],
    task_id: str,
) -> None:
    try:
        html_bytes = await read_page(storage, task["s3_key"])
        await worker.process(html_bytes, task["url"], task_id, task["s3_key"])
    except Exception as error:
        print(f"[{task_id}] error: {error}")


async def parse_loop(
    task_id: str,
    worker: PageWorker,
    queue: str,
    session: aioboto3.Session,
) -> None:
    redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    async with open_storage(session) as storage:
        while True:
            task = await pop_task(redis_client, queue)
            if task is not None:
                await handle_task(storage, worker, task, task_id)


def build_worker(
    redis_client: Redis,
    mongo_client: AsyncMongoClient[dict[str, Any]],
) -> PageWorker:
    if PARSER_MODE == "CRAWL":
        return LinkExtractor(redis_client)
    if PARSER_MODE == "DOWNLOAD":
        return PageSaver(mongo_client)
    raise ValueError(f"Unknown PARSER_MODE: {PARSER_MODE}")


async def main() -> None:
    redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    mongo_client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(MONGO_URI)
    session = aioboto3.Session()
    queue = LINKS_QUEUE if PARSER_MODE == "CRAWL" else TEXT_QUEUE
    prefix = f"{socket.gethostname()}-{PARSER_MODE}"
    print(f"Starting in {PARSER_MODE} mode ({PARSER_TASKS} tasks)")
    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(parse_loop(
            f"{prefix}-{index}",
            build_worker(redis_client, mongo_client),
            queue,
            session,
        ))
        for index in range(PARSER_TASKS)
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
