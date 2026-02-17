import os

import asyncio
import json
import socket
import redis.asyncio as redis
from botocore.client import Config
import time
import aioboto3
import json
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from pymongo import AsyncMongoClient


MONGO_URI = os.environ["MONGO_URI"]

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
FETCH_QUEUE = os.getenv("FETCH_QUEUE", "queue:fetch")
PARSE_QUEUE = os.getenv("PARSE_QUEUE", "queue:parse")
SEEN_SET = "seen_urls"

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "html-storage")
AWS_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

TASKS_PER_CONTAINER = int(os.getenv("PARSERS_PER_PARSER", 50))

DOMAIN = "he.wikipedia.org"

SYSTEM_PREFIXES = (
    "משתמש:",
    "שיחה:",
    "קובץ:",
    "קטגוריה:",
    "עזרה:",
    "תבנית:",
    "ויקיפדיה:",
)


r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
s3_session = aioboto3.Session()

mongo_client = AsyncMongoClient(MONGO_URI)
db = mongo_client.wikipedia_db
pages_collection = db.pages

LOCAL_SEEN_SET = set()


def crawl_links(html: str, base_url: str):
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.string if soup.title else "No Title"

    links = set()

    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"]
        full_url = urljoin(base_url, href)
        clean_url = full_url.split("#")[0]
        parsed_url = urlparse(clean_url)
        path = parsed_url.path
        title_start = path[6:]
        if parsed_url.netloc in DOMAIN and \
            path.startswith("/wiki/") and not \
            title_start.startswith(SYSTEM_PREFIXES):
                links.add(clean_url)
                
    return title, list(links)


async def parse_pages(task_id):
    loop = asyncio.get_running_loop()
    async with s3_session.client(
        service_name="s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ID,
        aws_secret_access_key=AWS_KEY,
        config=Config(signature_version="s3v4")
    ) as s3_client:
        
        while True:
            task_raw = await r.blpop(PARSE_QUEUE, timeout=5)
            
            if not task_raw:
                print("No tasks yet")
                continue

            task = json.loads(task_raw[1])
            url = task["url"]
            s3_key = task["s3_key"]

            try:
                response = await s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
                html_bytes = await response["Body"].read()
                html_content = html_bytes.decode("utf-8")

                title, crawled_links = await loop.run_in_executor(
                    None, 
                    crawl_links, 
                    html_content,
                    url,
                )

                document = {
                    "url": url,
                    "title": title,
                    "updated_at": time.time(),
                    "outgoing_links_count": len(crawled_links),
                    "content": html_content,
                    "outgoing_links": crawled_links
                }

                await pages_collection.replace_one(
                    {"url": url}, 
                    document, 
                    upsert=True
                )

                pipe = r.pipeline()
                for link in crawled_links:
                    pipe.sadd(SEEN_SET, link)
                
                added_links = await pipe.execute()

                new_links = []
                for link, was_added in zip(crawled_links, added_links):
                    if was_added == 1:
                        new_links.append(link)

                if new_links:
                    await r.rpush(FETCH_QUEUE, *new_links)
                                
                print(
                    f"[{task_id}] Parsed {url}: Found {len(crawled_links)}"
                    f"links ({len(new_links)} new)"
                )

            except Exception as e:
                print(f"[{task_id}] Error processing {url}: {e}")


async def main():
    hostname = socket.gethostname()
    pid = os.getpid()

    print(f"Starting Parser on {hostname} (PID: {pid})")
    print(f"Launching {TASKS_PER_CONTAINER} async tasks...")

    tasks = []
    for i in range(TASKS_PER_CONTAINER):
        full_task_id = f"{hostname}_{pid}_task_{i}"
        tasks.append(asyncio.create_task(parse_pages(full_task_id)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
