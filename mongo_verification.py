import os
from pymongo import MongoClient
import urllib.parse


HOST = "localhost"
PORT = 27017
USER = "REMOVED"
PASS = "REMOVED"


uri = f"mongodb://{USER}:{PASS}@{HOST}:{PORT}/?authSource=admin"
client = MongoClient(uri)
db = client["wikipedia_db"]
collection = db["pages"]

count = collection.count_documents({})
print(f"Total Documents: {count}")

print("\n--- Last 3 Pages ---")
cursor = collection.find().sort("_id", -1).limit(3)
for doc in cursor:
    print(f"URL: {doc.get('url')}")
    print(f"Title: {doc.get('title')}")
    print(f"S3 key {doc.get('s3_key')}")
    print("-" * 20)
