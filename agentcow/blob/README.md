# agent-cow — Blob Storage

S3 blob storage backend for [agent-cow](../../README.md). Provides Copy-On-Write interception for S3 operations via boto3 event hooks — writes go to scratch paths, reads resolve through session history, deletes write tombstones instead of removing data.

## Installation

```bash
pip install agent-cow
```

Requires Python 3.10+ and a boto3-compatible S3 client.

## Quick Start

### 1. Create the interceptor

`agent-cow` intercepts your existing boto3 S3 client by registering event hooks. A `ContextVar` carries the active session's COW configuration — when it's set, the hooks redirect S3 operations to scratch paths:

```python
from contextvars import ContextVar
from agentcow.blob import CowBlobInterceptor, CowBlobConfig

cow_blob_ctx: ContextVar[CowBlobConfig | None] = ContextVar("cow_blob_ctx", default=None)

s3_client = boto3.client("s3")
interceptor = CowBlobInterceptor(s3_client, cow_blob_ctx)
```

The interceptor registers `before-parameter-build` event hooks on the S3 client for `PutObject`, `CreateMultipartUpload`, `GetObject`, and `HeadObject`. When the context var is `None`, the hooks are no-ops.

### 2. Start a COW session

Set the context var before your agent runs S3 operations. Each discrete action gets its own `operation_id`:

```python
import uuid
from agentcow.blob import CowBlobConfig

session_id = uuid.uuid4()

config = CowBlobConfig(
    session_id=session_id,
    operation_id=uuid.uuid4(),
    path_prefix="my-org/",
)
cow_blob_ctx.set(config)

# Agent uploads a file — transparently redirected to scratch path:
#   my-org/documents/report.pdf -> my-org/.cow/session_{sid}/blobs/{op_id}/documents/report.pdf
s3_client.put_object(Bucket="my-bucket", Key="my-org/documents/report.pdf", Body=data)

# For the next action, rotate the operation_id
config.operation_id = uuid.uuid4()

# Agent reads a file — resolves through session history,
# returning the scratch version if one exists
s3_client.get_object(Bucket="my-bucket", Key="my-org/documents/report.pdf")
```

Production data is untouched. Other sessions see only the original objects.

### 3. Handle deletes

Deletes use a decorator on your storage backend's `delete_file` method rather than boto3 event hooks:

```python
from agentcow.blob import cow_intercept_delete

class MyStorageBackend:
    def __init__(self, s3_client, interceptor):
        self.cow_interceptor = interceptor

    @cow_intercept_delete
    def delete_file(self, bucket_name, object_name):
        self._s3_client.delete_object(Bucket=bucket_name, Key=object_name)
```

When intercepted, the decorator writes a zero-byte tombstone to `tombstones/{op_id}/{relative_path}` and skips the real delete. Subsequent reads of the deleted path raise `FileNotFoundError`.

### 4. Review and commit

After the session, inspect what the agent did and selectively commit or discard:

```python
from agentcow.blob import (
    get_blob_session_operations,
    get_blob_session_records,
    get_blob_dependencies,
    commit_cow_blobs,
    discard_cow_blobs,
)

ops = get_blob_session_operations(backend, bucket, prefix, ".cow", session_id)
records = get_blob_session_records(backend, bucket, prefix, ".cow", session_id)
deps = get_blob_dependencies(backend, bucket, prefix, ".cow", session_id)
# ops:  [UUID('aaa...'), UUID('bbb...'), UUID('ccc...')]
# records: [CowBlobRecord(final_path='org/file.txt', ...), ...]
# deps: [(UUID('aaa...'), UUID('bbb...')), ...]  — bbb depends on aaa

# Cherry-pick: commit the good operations, discard the rest
commit_cow_blobs(backend, bucket, prefix, ".cow", session_id, operation_ids=[ops[0]])
discard_cow_blobs(backend, bucket, prefix, ".cow", session_id, operation_ids=[ops[1], ops[2]])

# Or commit everything at once
commit_cow_blobs(backend, bucket, prefix, ".cow", session_id)
```

## How It Works

1. **Registers boto3 event hooks** on the singleton S3 client for write and read operations
2. **Redirects writes** to operation-scoped scratch paths under a `.cow` namespace
3. **Resolves reads** through file history, returning the latest scratch version or falling back to production
4. **Records deletes** as zero-byte tombstone objects instead of actually removing anything
5. **Your code doesn't change** — S3 calls still use production keys; the interceptor rewrites them transparently

All scratch data lives under a `.cow` namespace within the org's prefix:

```
{org_id}/.cow/session_{session_id}/blobs/{operation_id}/{relative_path}       # writes
{org_id}/.cow/session_{session_id}/tombstones/{operation_id}/{relative_path}  # deletes
```

The key structure stores information about the operation — `blobs/` means write, `tombstones/` means delete, and `LastModified` provides chronological ordering.

### Bypassing interception

`bypass_cow()` is a context manager that temporarily sets the context var to `None`. Use it when you need to read the production version of a file while a session is active (e.g. computing diffs):

```python
from agentcow.blob import bypass_cow

with bypass_cow(cow_blob_ctx):
    original = s3_client.get_object(Bucket="my-bucket", Key="my-org/documents/report.pdf")
```

### Dependencies

When two operations touch the same file, a dependency edge `(earlier_op, later_op)` is recorded. At query time, `get_blob_dependencies()` rebuilds these edges from S3 keys by grouping records by path and sorting by timestamp.

## API Reference

### Interception

| Function / Class | Description |
|------------------|-------------|
| `CowBlobInterceptor(s3_client, context_var)` | Registers boto3 event hooks for transparent COW path interception on the given S3 client |
| `bypass_cow(context_var)` | Context manager that temporarily disables COW interception for direct production access |
| `cow_intercept_delete` | Decorator that redirects deletes to tombstone writes when a COW session is active |

### Review

| Function | Description |
|----------|-------------|
| `get_blob_session_operations(backend, bucket, prefix, ns, session_id)` | List all uncommitted blob operation UUIDs for a session |
| `get_blob_session_records(backend, bucket, prefix, ns, session_id, operation_ids=None)` | Return `CowBlobRecord` entries for the session, optionally scoped to specific operations |
| `get_blob_dependencies(backend, bucket, prefix, ns, session_id)` | Get `(depends_on, operation_id)` pairs for blob operations in a session |
| `get_blob_operation_diff(backend, bucket, prefix, ns, session_id, operation_id)` | Compute before/after content diff for a single operation |

### Commit / Discard

| Function | Description |
|----------|-------------|
| `commit_cow_blobs(backend, bucket, prefix, ns, session_id, operation_ids=None)` | Commit COW blobs: copy writes to production, apply deletes, clean up scratch. Returns record count |
| `discard_cow_blobs(backend, bucket, prefix, ns, session_id, operation_ids=None)` | Discard COW blobs: delete scratch paths and tombstones without touching production. Returns record count |

### Types

| Type | Description |
|------|-------------|
| `CowBlobConfig` | Dataclass with `session_id`, `operation_id`, `path_prefix`, `scratch_namespace`, `pending_writes`, `dependencies`, `file_history` fields |
| `CowBlobRecord` | Dataclass representing a single pending blob write or delete: `final_path`, `is_delete`, `timestamp`, `bucket_name`, `cow_path`, `operation_id` |
| `BlobOperationDiff` | Dataclass with `before_content`, `after_content`, `is_new_file`, `is_delete` fields |
