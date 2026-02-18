import os
import asyncio
import json
import socket
import time
import redis.asyncio as redis
import aioboto3
from botocore.config import Config
from lxml import html
from pymongo import AsyncMongoClient
from urllib.parse import unquote
import gzip


MONGO_URI = os.environ["MONGO_URI"]
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
FETCH_QUEUE = os.getenv("FETCH_QUEUE", "queue:fetch")
PARSE_QUEUE = os.getenv("PARSE_QUEUE", "queue:parse")
SEEN_SET = "seen_urls"

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "html-storage")
AWS_ID = os.getenv("AWS_ACCESS_KEY", "minioadmin")
AWS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

DOMAIN = "he.wikipedia.org"

SYSTEM_PREFIXES = (
    "משתמש:",
    "שיחה:",
    "קובץ:",
    "קטגוריה:",
    "עזרה:",
    "תבנית:",
    "ויקיפדיה:",
    "מיוחד:",
    "פורטל:"
)


class Crawler:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.local_cache = set()
        self.cache_limit = 50_000

    async def process(self, html_bytes, root_url, task_id, s3_key=None):
        tree = html.fromstring(html_bytes)
        tree.make_links_absolute(root_url)

        met_links = set()
        for _, _, link, _ in tree.iterlinks():
            clean_url = link.split("#")[0]
            if DOMAIN not in clean_url or "/wiki/" not in clean_url:
                continue
            decoded_url = unquote(clean_url)
            try:
                title = decoded_url.split("/wiki/")[1]
            except IndexError:
                continue
            if title.startswith(SYSTEM_PREFIXES):
                continue
            met_links.add(clean_url)

        unknown_links = [link for link in met_links if link not in self.local_cache]
        new_links = []

        if unknown_links:
            pipe = self.redis.pipeline()
            for link in unknown_links:
                pipe.sadd(SEEN_SET, link)
            results = await pipe.execute()

            for link, is_new in zip(unknown_links, results):
                self.local_cache.add(link)
                if is_new:
                    new_links.append(link)

        if new_links:
            await self.redis.rpush(FETCH_QUEUE, *new_links)

        if len(self.local_cache) > self.cache_limit:
            self.local_cache.clear()

        print(f"[{task_id}] links: found {len(met_links)}, new {len(new_links)}")


class Downloader:
    def __init__(self, mongo_client):
        self.pages_collection = mongo_client.wikipedia_db.pages

    async def process(self, html_bytes, url, task_id, s3_key):
        tree = html.fromstring(html_bytes)
        
        titles = tree.xpath('/html/head/title/text()')
        title = titles[0] if titles else "No Title"

        text_content = tree.text_content()

        await self.pages_collection.update_one(
            {"url": url},
            {"$set": {
                "title": title,
                "text": text_content,
                "updated_at": time.time(),
                "s3_key": s3_key,
            }}, 
            upsert=True
        )
        print(f"[{task_id}] saved content of {title} article")


async def parser(task_id, worker: Crawler | Downloader, s3_session):
    async with s3_session.client(
        service_name="s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ID,
        aws_secret_access_key=AWS_KEY,
        config=Config(signature_version="s3v4")
    ) as s3_client:

        redis_client = redis.Redis(
            host=REDIS_HOST, 
            port=REDIS_PORT, 
            decode_responses=True,
        )

        while True:
            try:
                task_raw = await redis_client.blpop(PARSE_QUEUE, timeout=5)
            except redis.ConnectionError:
                await asyncio.sleep(2)
                continue

            if not task_raw:
                continue

            task = json.loads(task_raw[1])
            url = task["url"]
            s3_key = task["s3_key"]

            try:
                response = await s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
                compressed_bytes = await response["Body"].read()
                html_bytes = await asyncio.to_thread(
                    gzip.decompress,
                    compressed_bytes,
                )
                await worker.process(html_bytes, url, task_id, s3_key)

            except Exception as e:
                print(f"[{task_id}] error: {e}")


async def main():
    hostname = socket.gethostname()
    mode = os.getenv("PARSER_MODE", "CRAWL")
    tasks_count = int(os.getenv("TASKS_PER_CONTAINER", 10))

    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    m_client = AsyncMongoClient(MONGO_URI)
    s3_session = aioboto3.Session()


    if mode == "CRAWL":
        print(f"Starting in Crawler mode ({tasks_count} tasks)")
        make_worker = lambda: Crawler(r_client)
    elif mode == "DOWNLOAD":
        print(f"Starting in Downloader mode ({tasks_count} tasks)")
        make_worker = lambda: Downloader(m_client)

    tasks = []
    for i in range(tasks_count):
        task_id = f"{hostname}-{mode}-{i}"
        worker = make_worker()
        tasks.append(asyncio.create_task(parser(task_id, worker, s3_session)))
    
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
