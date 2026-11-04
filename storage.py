from __future__ import annotations

import asyncio
import gzip

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError
from crawler_settings import (
    COMPRESSION_LEVEL,
    EXISTING_BUCKET_CODES,
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_SECRET_KEY,
    S3_SIGNATURE_VERSION,
)


def open_storage(session: aioboto3.Session) -> aioboto3.S3Client:
    return session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version=S3_SIGNATURE_VERSION),
    )


def read_error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", "Unknown"))


async def ensure_bucket(session: aioboto3.Session) -> None:
    async with open_storage(session) as storage:
        try:
            await storage.create_bucket(Bucket=S3_BUCKET)
            print(f"Bucket '{S3_BUCKET}' ready.")
        except ClientError as error:
            if read_error_code(error) not in EXISTING_BUCKET_CODES:
                raise


async def store_page(storage: aioboto3.S3Client, key: str, html: bytes) -> None:
    compressed = await asyncio.to_thread(gzip.compress, html, COMPRESSION_LEVEL)
    await storage.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=compressed,
        ContentEncoding="gzip",
        ContentType="text/html",
    )


async def read_page(storage: aioboto3.S3Client, key: str) -> bytes:
    response = await storage.get_object(Bucket=S3_BUCKET, Key=key)
    compressed: bytes = await response["Body"].read()
    return await asyncio.to_thread(gzip.decompress, compressed)
