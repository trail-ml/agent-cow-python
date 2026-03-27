# Blob COW Tests

## Setup

No external services required. The tests use [moto](https://github.com/getmoto/moto) to mock AWS S3 in-process, and a `FakeBlobBackend` for the operations layer.

Install dependencies (if you haven't already):

```bash
uv sync
```

## Running tests

```bash
uv run pytest agentcow/blob/tests/
```

Run a specific module:

```bash
uv run pytest agentcow/blob/tests/test_paths.py
uv run pytest agentcow/blob/tests/test_interceptor.py
uv run pytest agentcow/blob/tests/test_operations.py
```

## Test modules

| Module | What it tests |
| ---------------------- | --------------------------------------------------------------- |
| `test_paths.py` | Pure path construction and parsing — `to_cow_path`, `to_tombstone_path`, `parse_cow_key`, etc. No I/O. |
| `test_interceptor.py` | S3 client interception via `CowBlobInterceptor` — writes are redirected to COW scratch paths, deletes become tombstones, reads merge COW and base data. Uses moto-mocked S3. |
| `test_operations.py` | Higher-level COW lifecycle — `commit_cow_blobs`, `discard_cow_blobs`, `get_blob_session_records`, diffs, dependencies. Uses `FakeBlobBackend` (in-memory, no moto). |