import os

import asyncio
import aiohttp
import boto3
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

TASKS_PER_CONTAINER = int(os.getenv("TASKS_PER_CONTAINER", 50))


r = redis.Redis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    decode_responses=True,
)


s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    config=Config(signature_version='s3v4')
)


headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
                    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}



def hash_url(url: str) -> str:
    url_bytes = url.encode('utf-8')
    hash_object = hashlib.sha256(url_bytes)
    return hash_object.hexdigest()


async def upload_to_s3(file_name: str, content: bytes):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, 
        lambda: s3_client.put_object(
            Bucket=S3_BUCKET, 
            Key=file_name, 
            Body=content,
        )
    )


async def fetch_url(task_id: int):
    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            task_raw = await r.blpop(FETCH_QUEUE, timeout=5)

            if not task_raw:
                print("No tasks detected")
                continue

            url = task_raw[1]
            print(f"Received task to fetch {url}")
            
            try:
                start_time = time.time()
                async with session.get(url, timeout=10) as response:
                    if response.status != 200:
                        text = await response.text()
                        print(f"[{task_id}] Failed {url}: HTTP {response.status} {text}")
                        continue
                    html_content = await response.read()

                file_name = f"{hash_url(url)}.html"
                await upload_to_s3(file_name, html_content)

                message = json.dumps({
                    "url": url,
                    "s3_key": file_name,
                    "fetched_at": time.time(),
                    "worker": task_id,
                })
                
                await r.rpush(PARSE_QUEUE, message)
                
                duration = time.time() - start_time
                print(f"[{task_id}] Fetched {url} in {duration:.2f}s")

            except Exception as e:
                print(f"[{task_id}] ERROR on {url}: {e}")


async def main():
    print("entered main async")
    hostname = socket.gethostname()
    pid = os.getpid()

    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None, 
            lambda: s3_client.create_bucket(Bucket=S3_BUCKET)
        )
        print(f"Bucket '{S3_BUCKET}' ready.")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except s3_client.exceptions.BucketAlreadyExists:
        pass

    print(f"Starting Fetcher Node on {hostname} (PID: {pid})")
    print(f"Launching {TASKS_PER_CONTAINER} async tasks...")

    tasks = []
    for i in range(TASKS_PER_CONTAINER):
        full_task_id = f"{hostname}_{pid}_task_{i}"
        tasks.append(asyncio.create_task(fetch_url(full_task_id)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping fetchers...")
