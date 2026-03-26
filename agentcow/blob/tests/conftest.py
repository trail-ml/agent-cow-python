import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from agentcow.blob.context import CowBlobConfig
from agentcow.blob.interceptor import CowBlobInterceptor

TEST_BUCKET = "test-bucket"
TEST_REGION = "eu-west-1"


@pytest.fixture
def session_id():
    return uuid.uuid4()


@pytest.fixture
def operation_id():
    return uuid.uuid4()


@pytest.fixture
def cow_context_var():
    return ContextVar("cow_blob_ctx", default=None)


@pytest.fixture
def aws_mock():
    with mock_aws():
        yield


@pytest.fixture
def s3_client(aws_mock):
    return boto3.client("s3", region_name=TEST_REGION)


@pytest.fixture
def test_bucket(s3_client):
    params: dict = {"Bucket": TEST_BUCKET}
    if TEST_REGION != "us-east-1":
        params["CreateBucketConfiguration"] = {
            "LocationConstraint": TEST_REGION
        }
    s3_client.create_bucket(**params)
    return TEST_BUCKET


@pytest.fixture
def cow_config(session_id, operation_id):
    return CowBlobConfig(
        session_id=session_id,
        operation_id=operation_id,
        path_prefix="data/",
        scratch_namespace=".cow",
    )


@pytest.fixture
def interceptor_with_ctx(s3_client, test_bucket, cow_context_var, cow_config):
    """Return (interceptor, context_var, config) with hooks registered."""
    interceptor = CowBlobInterceptor(s3_client, cow_context_var)
    return interceptor, cow_context_var, cow_config


# ---------------------------------------------------------------------------
# In-memory fake backend for operations.py tests
# ---------------------------------------------------------------------------


@dataclass
class FakeBlob:
    name: str
    last_updated: datetime | None = None


class FakeBlobBackend:
    """In-memory blob store implementing the duck-typed backend interface
    consumed by agentcow.blob.operations."""

    def __init__(self):
        self._store: dict[str, dict[str, tuple[bytes, datetime]]] = {}
        self._counter = 0

    def _bucket(self, bucket: str) -> dict[str, tuple[bytes, datetime]]:
        return self._store.setdefault(bucket, {})

    def _next_ts(self) -> datetime:
        self._counter += 1
        return datetime(2025, 1, 1, 0, 0, self._counter, tzinfo=timezone.utc)

    def upload_binary(self, data: bytes, bucket: str, key: str) -> None:
        self._bucket(bucket)[key] = (data, self._next_ts())

    def download_as_bytes(self, bucket: str, key: str) -> bytes | None:
        entry = self._bucket(bucket).get(key)
        return entry[0] if entry else None

    def download_as_string(self, bucket: str, key: str) -> str | None:
        data = self.download_as_bytes(bucket, key)
        if data is None:
            return None
        return data.decode("utf-8")

    def delete_file(self, bucket: str, key: str) -> None:
        self._bucket(bucket).pop(key, None)

    def blob_exists(self, bucket: str, key: str) -> bool:
        return key in self._bucket(bucket)

    def list_blobs(self, bucket: str, prefix: str) -> list[FakeBlob]:
        store = self._bucket(bucket)
        return [
            FakeBlob(name=k, last_updated=entry[1])
            for k, entry in sorted(store.items())
            if k.startswith(prefix)
        ]


@pytest.fixture
def backend():
    return FakeBlobBackend()
