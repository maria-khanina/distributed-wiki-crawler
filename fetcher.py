from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
import time
from typing import TypedDict

import aioboto3
import aiohttp
from botocore.exceptions import ClientError
from crawler_settings import (
    FETCHER_TASKS,
    FETCH_QUEUE,
    LINKS_QUEUE,
    MAX_RETRIES,
    OK_STATUS,
    POP_TIMEOUT_SECONDS,
    RATE_LIMIT_STATUS,
    REDIS_HOST,
    REDIS_PORT,
    REPORT_INTERVAL_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_DELAY_SECONDS,
    RETRY_TTL_SECONDS,
    SERVER_ERROR_STATUS,
    STORAGE_FULL_CODE,
    STORAGE_FULL_PAUSE_SECONDS,
    TEXT_QUEUE,
    USER_AGENT,
)
from redis.asyncio import Redis
from storage import ensure_bucket, open_storage, read_error_code, store_page


class FetchStats(TypedDict):
    fetched: int
    failed: int
    retried: int


STATS: FetchStats = {"fetched": 0, "failed": 0, "retried": 0}
redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def hash_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


async def report_stats() -> None:
    while True:
        await asyncio.sleep(REPORT_INTERVAL_SECONDS)
        print("--- FETCHER REPORT ---")
        print(f"Fetched: {STATS['fetched']}")
        print(f"Failed: {STATS['failed']}")
        print(f"Retried: {STATS['retried']}")
        print("-------------------------")


async def schedule_retry(url: str) -> None:
    retry_key = f"retry:{hash_url(url)}"
    current_try = await redis_client.incr(retry_key)
    if current_try == 1:
        await redis_client.expire(retry_key, RETRY_TTL_SECONDS)
    if current_try > MAX_RETRIES:
        STATS["failed"] += 1
        return
    await asyncio.sleep(RETRY_DELAY_SECONDS)
    await redis_client.rpush(FETCH_QUEUE, url)
    STATS["retried"] += 1


async def download(http: aiohttp.ClientSession, url: str) -> bytes | None:
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    async with http.get(url, timeout=timeout) as response:
        status = response.status
        if status >= SERVER_ERROR_STATUS or status == RATE_LIMIT_STATUS:
            await schedule_retry(url)
            return None
        if status != OK_STATUS:
            STATS["failed"] += 1
            return None
        return await response.read()


async def announce(url: str, key: str, task_id: str) -> None:
    message = json.dumps({
        "url": url,
        "s3_key": key,
        "fetched_at": time.time(),
        "worker": task_id,
    })
    try:
        await redis_client.rpush(LINKS_QUEUE, message)
        await redis_client.rpush(TEXT_QUEUE, message)
    except Exception as error:
        print(f"Redis error: {error}")
    STATS["fetched"] += 1


async def report_storage_error(error: ClientError) -> None:
    error_code = read_error_code(error)
    if error_code != STORAGE_FULL_CODE:
        print(f"S3 Error ({error_code}): {error}")
        return
    print(f"MinIO Disk Full! Pausing for {STORAGE_FULL_PAUSE_SECONDS} seconds...")
    await asyncio.sleep(STORAGE_FULL_PAUSE_SECONDS)


async def save(
    storage: aioboto3.S3Client,
    url: str,
    html: bytes,
    task_id: str,
) -> None:
    key = f"{hash_url(url)}.html"
    try:
        await store_page(storage, key, html)
    except ClientError as error:
        await report_storage_error(error)
        return
    await announce(url, key, task_id)


async def handle_url(
    http: aiohttp.ClientSession,
    storage: aioboto3.S3Client,
    url: str,
    task_id: str,
) -> None:
    try:
        html = await download(http, url)
    except Exception as error:
        print(f"[{task_id}] ERROR on {url}: {error}")
        STATS["failed"] += 1
        return
    if html is not None:
        await save(storage, url, html, task_id)


async def fetch_loop(task_id: str, session: aioboto3.Session) -> None:
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as http:
        async with open_storage(session) as storage:
            while True:
                task = await redis_client.blpop(
                    [FETCH_QUEUE],
                    timeout=POP_TIMEOUT_SECONDS,
                )
                if not task:
                    print("No tasks for fetcher :(")
                    continue
                await handle_url(http, storage, task[1], task_id)


async def main() -> None:
    session = aioboto3.Session()
    report = asyncio.create_task(report_stats())
    await ensure_bucket(session)
    prefix = f"{socket.gethostname()}_{os.getpid()}"
    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(fetch_loop(f"{prefix}_task_{index}", session))
        for index in range(FETCHER_TASKS)
    ]
    await asyncio.gather(report, *tasks)


if __name__ == "__main__":
    asyncio.run(main())
