import os

import redis


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
FETCH_QUEUE = os.getenv("FETCH_QUEUE", "queue:fetch")


start_urls = [
    # Earth (כדור הארץ)
    "https://he.wikipedia.org/wiki/%D7%9B%D7%93%D7%95%D7%A8_%D7%94%D7%90%D7%A8%D7%A5",
    
    # Brain (מוח)
    "https://he.wikipedia.org/wiki/%D7%9E%D7%95%D7%97",
    
    # Existentialism (אקזיסטנציאליזם)
    "https://he.wikipedia.org/wiki/%D7%90%D7%A7%D7%96%D7%99%D7%A1%D7%98%D7%A0%D7%A6%D7%99%D7%90%D7%9C%D7%99%D7%96%D7%9D",
    
    # Hummus (חומוס)
    "https://he.wikipedia.org/wiki/%D7%97%D7%95%D7%9E%D7%95%D7%A1",
    
    # Emu War (מלחמת_האמו)
    "https://he.wikipedia.org/wiki/%D7%9E%D7%9C%D7%97%D7%9E%D7%AA_%D7%94%D7%90%D7%9E%D7%95",
    
    # Voynich Manuscript (כתב_יד_וויניץ')
    "https://he.wikipedia.org/wiki/%D7%9B%D7%AA%D7%91_%D7%99%D7%93_%D7%95%D7%95%D7%99%D7%A0%D7%99%D7%A5%27",
    
    # Tardigrade (דובוני_מים)
    "https://he.wikipedia.org/wiki/%D7%93%D7%95%D7%91%D7%95%D7%A0%D7%99_%D7%9E%D7%99%D7%9D",

    # Flying Spaghetti Monster (מפלצת_הספגטי_המעופפת)
    "https://he.wikipedia.org/wiki/%D7%9E%D7%A4%D7%9C%D7%A6%D7%AA_%D7%94%D7%A1%D7%A4%D7%92%D7%98%D7%99_%D7%94%D7%9E%D7%A2%D7%95%D7%A4%D7%A4%D7%AA",

    # Computer Science (מדעי המחשב)
    "https://he.wikipedia.org/wiki/%D7%9E%D7%93%D7%A2%D7%99_%D7%94%D7%9E%D7%97%D7%A9%D7%91",
]

def main():
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    print(f"Seeding {len(start_urls)} URLs to '{FETCH_QUEUE}'...")

    pipe = r.pipeline()
    for url in start_urls:
        pipe.rpush(FETCH_QUEUE, url)
    
    pipe.execute()
    print("Seeding done.")


if __name__ == "__main__":
    main()
