# Distributed Wikipedia Crawler

An asynchronous crawler of the Hebrew Wikipedia built from small services that talk only through Redis queues, so each stage can be scaled on its own: more fetchers when the network is the bottleneck, more parsers when parsing is.

## Architecture

```
seed ──► queue:fetch ──► fetcher ──► MinIO (gzip HTML)
              ▲              │
              │              ├──► queue:links ──► crawler    ──► new article links
              │              └──► queue:text  ──► downloader ──► MongoDB (title, text)
              └──────────────────────────────────────────────────┘
```

All services run in Docker Compose (`compose.yaml`):

- **seed** (`seed.py`) puts the start articles into the fetch queue.
- **fetcher** (`fetcher.py`) runs 50 concurrent `aiohttp` tasks per container. Each task takes a URL from `queue:fetch` with a blocking pop and downloads the page. On a server error or a rate limit (5xx or 429) it waits and puts the URL back into the queue, up to 3 times; the retry counter lives in Redis with a 10-minute expiry. The HTML is gzip-compressed and stored in S3-compatible storage (MinIO through `aioboto3`) under the SHA-256 of the URL, and a message with the URL and the storage key goes to both parse queues, so every page is crawled for links and saved as text. A background task prints the fetched, failed and retried counts every 10 seconds; when the storage is full, the fetcher pauses for a minute.
- **crawler** (`parser.py`, `PARSER_MODE=CRAWL`, 2 replicas with 40 tasks each, CPU-limited) reads the page from storage, extracts the links to other articles (skipping user, talk, file, category, template and other system pages) and keeps only the ones not seen before. Deduplication has two levels: a local set of up to 50,000 recent links in each worker, and a Redis set of all seen links, checked for a whole page at once through a Redis pipeline. New links go back into the fetch queue.
- **downloader** (`parser.py`, `PARSER_MODE=DOWNLOAD`) reads the page from storage and upserts its title and text into MongoDB by URL.
- **redis**, **minio**, **mongo**: the queues and the seen set, the raw HTML, the parsed pages.

Keeping the raw HTML in object storage separates downloading from parsing: a parser can be changed and rerun over the stored pages without fetching Wikipedia again. Storage access is shared by both services in `storage.py`; every constant (queue names, timeouts, retry limits, the start articles) is in `crawler_settings.py`, read from the environment where it can change between deployments.

## Running

A `.env` file next to `compose.yaml` sets `MINIO_USER`, `MINIO_PASSWORD`, `S3_ENDPOINT` (`http://minio:9000`), `S3_BUCKET`, `REDIS_HOST` (`redis`), `MONGO_USER`, `MONGO_PASSWORD` and `MONGO_HOST` (`mongo`). Then:

```
docker compose up -d --build
docker compose logs -f fetcher
MONGO_URI="mongodb://<user>:<password>@localhost:27017/?authSource=admin" python mongo_verification.py
```

`mongo_verification.py` prints the number of stored pages and the last three of them.

## Code quality

The code is checked with `python -m pycodestyle --max-line-length=88 --exclude=.venv,typings,tools .`, `python -m pyright` (strict, with local stubs in `typings/` for `aioboto3`, `botocore`, `lxml.html` and the parts of `redis` the services use) and the AST checker [tools/rules_check.py](tools/rules_check.py) (`python tools/rules_check.py .`): at most 10 statements per function, no magic numbers outside `crawler_settings.py`, type hints on containers, no abbreviations or single-letter names, ordered imports.
