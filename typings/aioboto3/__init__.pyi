from types import TracebackType
from typing import Any

from botocore.config import Config

class S3Client:
    async def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentEncoding: str,
        ContentType: str,
    ) -> dict[str, Any]: ...
    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...
    async def create_bucket(self, *, Bucket: str) -> dict[str, Any]: ...
    async def __aenter__(self) -> S3Client: ...
    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

class Session:
    def client(
        self,
        service_name: str,
        *,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        config: Config,
    ) -> S3Client: ...
