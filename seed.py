from crawler_settings import (
    ARTICLE_URL,
    FETCH_QUEUE,
    REDIS_HOST,
    REDIS_PORT,
    START_TITLES,
)
import redis


def main() -> None:
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    start_urls: list[str] = [ARTICLE_URL + title for title in START_TITLES]
    print(f"Seeding {len(start_urls)} URLs to '{FETCH_QUEUE}'...")
    pipeline = client.pipeline()
    for url in start_urls:
        pipeline.rpush(FETCH_QUEUE, url)
    pipeline.execute()
    print("Seeding done.")


if __name__ == "__main__":
    main()
