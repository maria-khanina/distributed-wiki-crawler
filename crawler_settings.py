import os

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
FETCH_QUEUE = os.getenv("FETCH_QUEUE", "queue:fetch")
LINKS_QUEUE = os.getenv("LINKS_QUEUE", "queue:links")
TEXT_QUEUE = os.getenv("TEXT_QUEUE", "queue:text")
SEEN_SET = "seen_urls"

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "html-storage")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_SIGNATURE_VERSION = "s3v4"
STORAGE_FULL_CODE = "XMinioStorageFull"
EXISTING_BUCKET_CODES = ("BucketAlreadyOwnedByYou", "BucketAlreadyExists")

MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DATABASE = "wikipedia_db"
MONGO_COLLECTION = "pages"

FETCHER_TASKS = int(os.getenv("FETCHERS_PER_CONTAINER", "50"))
PARSER_TASKS = int(os.getenv("TASKS_PER_CONTAINER", "10"))
PARSER_MODE = os.getenv("PARSER_MODE", "CRAWL")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT_SECONDS = 10
POP_TIMEOUT_SECONDS = 5
REPORT_INTERVAL_SECONDS = 10
RETRY_DELAY_SECONDS = 2
RECONNECT_DELAY_SECONDS = 2
STORAGE_FULL_PAUSE_SECONDS = 60
MAX_RETRIES = 3
RETRY_TTL_SECONDS = 600
OK_STATUS = 200
RATE_LIMIT_STATUS = 429
SERVER_ERROR_STATUS = 500
COMPRESSION_LEVEL = 1

LOCAL_CACHE_LIMIT = 50_000
DOMAIN = "he.wikipedia.org"
ARTICLE_PATH = "/wiki/"
SYSTEM_PREFIXES = (
    "משתמש:",
    "שיחה:",
    "קובץ:",
    "קטגוריה:",
    "עזרה:",
    "תבנית:",
    "ויקיפדיה:",
    "מיוחד:",
    "פורטל:",
)
SHOWN_RECENT_PAGES = 3

ARTICLE_URL = "https://he.wikipedia.org/wiki/"
START_TITLES = (
    "%D7%9B%D7%93%D7%95%D7%A8_%D7%94%D7%90%D7%A8%D7%A5",
    "%D7%9E%D7%95%D7%97",
    "%D7%90%D7%A7%D7%96%D7%99%D7%A1%D7%98"
    "%D7%A0%D7%A6%D7%99%D7%90%D7%9C%D7%99%D7%96%D7%9D",
    "%D7%97%D7%95%D7%9E%D7%95%D7%A1",
    "%D7%9E%D7%9C%D7%97%D7%9E%D7%AA_%D7%94%D7%90%D7%9E%D7%95",
    "%D7%9B%D7%AA%D7%91_%D7%99%D7%93_%D7%95%D7%95%D7%99%D7%A0%D7%99%D7%A5%27",
    "%D7%93%D7%95%D7%91%D7%95%D7%A0%D7%99_%D7%9E%D7%99%D7%9D",
    "%D7%9E%D7%A4%D7%9C%D7%A6%D7%AA_%D7%94%D7%A1%D7%A4%D7%92%D7%98%D7%99_"
    "%D7%94%D7%9E%D7%A2%D7%95%D7%A4%D7%A4%D7%AA",
    "%D7%9E%D7%93%D7%A2%D7%99_%D7%94%D7%9E%D7%97%D7%A9%D7%91",
)
