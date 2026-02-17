import os

import asyncio
import aiohttp
import aioboto3
import json
import hashlib
import socket
import time
import redis.asyncio as redis
from botocore.client import Config


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
FETCH_QUEUE = os.getenv("FETCH_QUEUE", "queue:fetch")
PARSE_QUEUE = os.getenv("PARSE_QUEUE", "queue:parse")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "html_storage")
AWS_KEY = os.getenv("AWS_ACCESS_KEY", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

MAX_RETRIES = 3
RETRY_TTL = 600

TASKS_PER_CONTAINER = int(os.getenv("TASKS_PER_FETCHER", 50))

r = redis.Redis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    decode_responses=True,
)


s3_session = aioboto3.Session()


headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
                    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}


STATS = {
    "fetched": 0,
    "failed": 0,
    "retried": 0
}


def hash_url(url: str) -> str:
    url_bytes = url.encode('utf-8')
    hash_object = hashlib.sha256(url_bytes)
    return hash_object.hexdigest()


async def make_report():
    while True:
        await asyncio.sleep(60)
        print(f"--- FETCHER REPORT ---")
        print(f"Fetched: {STATS['fetched']}")
        print(f"Failed: {STATS['failed']}")
        print(f"Retried: {STATS['retried']}")
        print("-------------------------")


async def fetch_url(task_id: int):
    async with aiohttp.ClientSession(headers=headers) as http_session:
        async with s3_session.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET,
            config=Config(signature_version='s3v4')
        ) as s3_client:
            while True:
                task_raw = await r.blpop(FETCH_QUEUE, timeout=5)

                if not task_raw:
                    continue

                url = task_raw[1]
                
                try:
                    async with http_session.get(url, timeout=10) as response:
                        retry_key = f"retry:{hash_url(url)}"
                        if response.status >= 500 or response.status == 429:
                            current_try = await r.incr(retry_key)
                            if current_try == 1:
                                await r.expire(retry_key, RETRY_TTL)
                            if current_try <= MAX_RETRIES:
                                await r.rpush(FETCH_QUEUE, url)
                                STATS["retried"] += 1
                            else:
                                STATS["failed"] += 1
                            continue
                        if response.status != 200:
                            STATS["failed"] += 1
                            continue
                        html_content = await response.read()

                    file_name = f"{hash_url(url)}.html"
                    await s3_client.put_object(
                        Bucket=S3_BUCKET, 
                        Key=file_name, 
                        Body=html_content,
                    )

                    message = json.dumps({
                        "url": url,
                        "s3_key": file_name,
                        "fetched_at": time.time(),
                        "worker": task_id,
                    })
                    
                    await r.rpush(PARSE_QUEUE, message)
                    STATS["fetched"] += 1

                except Exception as e:
                    print(f"[{task_id}] ERROR on {url}: {e}")


async def main():
    asyncio.create_task(make_report())
    hostname = socket.gethostname()
    pid = os.getpid()

    async with s3_session.client(
                "s3",
                endpoint_url=S3_ENDPOINT,
                aws_access_key_id=AWS_KEY,
                aws_secret_access_key=AWS_SECRET,
                config=Config(signature_version='s3v4'),
    ) as s3_client:
        try:
            await s3_client.create_bucket(Bucket=S3_BUCKET)
            print(f"Bucket '{S3_BUCKET}' ready.")
        except s3_client.exceptions.BucketAlreadyOwnedByYou:
            pass
        except s3_client.exceptions.BucketAlreadyExists:
            pass

    tasks = []
    for i in range(TASKS_PER_CONTAINER):
        full_task_id = f"{hostname}_{pid}_task_{i}"
        tasks.append(asyncio.create_task(fetch_url(full_task_id)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
