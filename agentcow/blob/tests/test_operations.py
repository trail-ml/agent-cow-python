import uuid
from datetime import datetime, timezone

from agentcow.blob.operations import (
    commit_cow_blobs,
    discard_cow_blobs,
    get_blob_dependencies,
    get_blob_operation_diff,
    get_blob_session_operations,
)
from agentcow.blob.paths import to_cow_path, to_tombstone_path

PREFIX = "data/"
NS = ".cow"
BUCKET = "test-bucket"


def _seed_cow_write(backend, session_id, operation_id, filename, content):
    """Place a blob at the COW scratch path and return the cow key."""
    cow_key = to_cow_path(PREFIX, session_id, filename, operation_id, NS)
    backend.upload_binary(content.encode(), BUCKET, cow_key)
    return cow_key


def _seed_cow_tombstone(backend, session_id, operation_id, filename):
    """Place a zero-byte tombstone at the COW scratch path."""
    ts_key = to_tombstone_path(PREFIX, session_id, filename, operation_id, NS)
    backend.upload_binary(b"", BUCKET, ts_key)
    return ts_key


class TestGetBlobSessionOperations:
    def test_returns_distinct_op_ids(self, backend, session_id):
        op1 = uuid.uuid4()
        op2 = uuid.uuid4()
        _seed_cow_write(backend, session_id, op1, "a.txt", "a")
        _seed_cow_write(backend, session_id, op2, "b.txt", "b")
        _seed_cow_write(backend, session_id, op1, "c.txt", "c")

        ops = get_blob_session_operations(backend, BUCKET, PREFIX, NS, session_id)
        assert set(ops) == {op1, op2}

    def test_empty_session(self, backend, session_id):
        ops = get_blob_session_operations(backend, BUCKET, PREFIX, NS, session_id)
        assert ops == []


class TestCommitCowBlobs:
    def test_copies_writes_to_production(self, backend, session_id):
        op_id = uuid.uuid4()
        _seed_cow_write(backend, session_id, op_id, "file.txt", "committed")

        count = commit_cow_blobs(backend, BUCKET, PREFIX, NS, session_id)
        assert count == 1
        assert backend.download_as_string(BUCKET, "data/file.txt") == "committed"

    def test_applies_deletes(self, backend, session_id):
        backend.upload_binary(b"original", BUCKET, "data/victim.txt")

        op_id = uuid.uuid4()
        _seed_cow_tombstone(backend, session_id, op_id, "victim.txt")

        commit_cow_blobs(backend, BUCKET, PREFIX, NS, session_id)
        assert backend.blob_exists(BUCKET, "data/victim.txt") is False

    def test_cleans_up_scratch(self, backend, session_id):
        op_id = uuid.uuid4()
        cow_key = _seed_cow_write(backend, session_id, op_id, "file.txt", "data")

        commit_cow_blobs(backend, BUCKET, PREFIX, NS, session_id)
        assert backend.blob_exists(BUCKET, cow_key) is False

    def test_scoped_commit_by_operation_id(self, backend, session_id):
        op1 = uuid.uuid4()
        op2 = uuid.uuid4()
        _seed_cow_write(backend, session_id, op1, "a.txt", "a-content")
        _seed_cow_write(backend, session_id, op2, "b.txt", "b-content")

        count = commit_cow_blobs(
            backend, BUCKET, PREFIX, NS, session_id, operation_ids=[op1]
        )
        assert count == 1
        assert backend.download_as_string(BUCKET, "data/a.txt") == "a-content"
        assert backend.blob_exists(BUCKET, "data/b.txt") is False

    def test_empty_session_returns_zero(self, backend, session_id):
        count = commit_cow_blobs(backend, BUCKET, PREFIX, NS, session_id)
        assert count == 0

    def test_delete_commit_counts_record_when_prod_already_missing(
        self, backend, session_id
    ):
        op_id = uuid.uuid4()
        _seed_cow_tombstone(backend, session_id, op_id, "missing.txt")

        count = commit_cow_blobs(backend, BUCKET, PREFIX, NS, session_id)
        assert count == 1
        assert backend.blob_exists(BUCKET, "data/missing.txt") is False

    def test_scoped_commit_empty_operation_ids_processes_nothing(
        self, backend, session_id
    ):
        op1 = uuid.uuid4()
        op2 = uuid.uuid4()
        k1 = _seed_cow_write(backend, session_id, op1, "a.txt", "a")
        k2 = _seed_cow_write(backend, session_id, op2, "b.txt", "b")

        count = commit_cow_blobs(
            backend, BUCKET, PREFIX, NS, session_id, operation_ids=[]
        )
        assert count == 0
        assert backend.blob_exists(BUCKET, k1) is True
        assert backend.blob_exists(BUCKET, k2) is True
        assert backend.blob_exists(BUCKET, "data/a.txt") is False

    def test_scoped_commit_unknown_operation_ids_processes_nothing(
        self, backend, session_id
    ):
        op1 = uuid.uuid4()
        k1 = _seed_cow_write(backend, session_id, op1, "a.txt", "a")

        count = commit_cow_blobs(
            backend,
            BUCKET,
            PREFIX,
            NS,
            session_id,
            operation_ids=[uuid.uuid4()],
        )
        assert count == 0
        assert backend.blob_exists(BUCKET, k1) is True
        assert backend.blob_exists(BUCKET, "data/a.txt") is False


class TestDiscardCowBlobs:
    def test_removes_scratch_without_touching_production(self, backend, session_id):
        backend.upload_binary(b"prod", BUCKET, "data/file.txt")

        op_id = uuid.uuid4()
        cow_key = _seed_cow_write(backend, session_id, op_id, "file.txt", "scratch")

        count = discard_cow_blobs(backend, BUCKET, PREFIX, NS, session_id)
        assert count == 1
        assert backend.blob_exists(BUCKET, cow_key) is False
        assert backend.download_as_string(BUCKET, "data/file.txt") == "prod"

    def test_removes_tombstones(self, backend, session_id):
        op_id = uuid.uuid4()
        ts_key = _seed_cow_tombstone(backend, session_id, op_id, "file.txt")

        discard_cow_blobs(backend, BUCKET, PREFIX, NS, session_id)
        assert backend.blob_exists(BUCKET, ts_key) is False

    def test_scoped_discard(self, backend, session_id):
        op1 = uuid.uuid4()
        op2 = uuid.uuid4()
        key1 = _seed_cow_write(backend, session_id, op1, "a.txt", "a")
        key2 = _seed_cow_write(backend, session_id, op2, "b.txt", "b")

        discard_cow_blobs(
            backend, BUCKET, PREFIX, NS, session_id, operation_ids=[op1]
        )
        assert backend.blob_exists(BUCKET, key1) is False
        assert backend.blob_exists(BUCKET, key2) is True

    def test_scoped_discard_empty_operation_ids_processes_nothing(
        self, backend, session_id
    ):
        op1 = uuid.uuid4()
        k1 = _seed_cow_write(backend, session_id, op1, "a.txt", "a")

        count = discard_cow_blobs(
            backend, BUCKET, PREFIX, NS, session_id, operation_ids=[]
        )
        assert count == 0
        assert backend.blob_exists(BUCKET, k1) is True

    def test_scoped_discard_unknown_operation_ids_processes_nothing(
        self, backend, session_id
    ):
        op1 = uuid.uuid4()
        k1 = _seed_cow_write(backend, session_id, op1, "a.txt", "a")

        count = discard_cow_blobs(
            backend,
            BUCKET,
            PREFIX,
            NS,
            session_id,
            operation_ids=[uuid.uuid4()],
        )
        assert count == 0
        assert backend.blob_exists(BUCKET, k1) is True


class TestGetBlobDependencies:
    def test_detects_cross_operation_dependency(self, backend, session_id):
        op1 = uuid.uuid4()
        op2 = uuid.uuid4()
        _seed_cow_write(backend, session_id, op1, "shared.txt", "v1")
        _seed_cow_write(backend, session_id, op2, "shared.txt", "v2")

        deps = get_blob_dependencies(backend, BUCKET, PREFIX, NS, session_id)
        op_pairs = {(d[0], d[1]) for d in deps}
        assert len(op_pairs) == 1
        pair = op_pairs.pop()
        assert set(pair) == {op1, op2}

    def test_no_deps_for_independent_files(self, backend, session_id):
        op1 = uuid.uuid4()
        op2 = uuid.uuid4()
        _seed_cow_write(backend, session_id, op1, "a.txt", "a")
        _seed_cow_write(backend, session_id, op2, "b.txt", "b")

        deps = get_blob_dependencies(backend, BUCKET, PREFIX, NS, session_id)
        assert deps == []


class TestGetBlobOperationDiff:
    def test_new_file_diff(self, backend, session_id):
        op_id = uuid.uuid4()
        _seed_cow_write(backend, session_id, op_id, "new.txt", "new-content")

        diff = get_blob_operation_diff(
            backend, BUCKET, PREFIX, NS, session_id, op_id
        )
        assert diff.is_new_file is True
        assert diff.after_content == "new-content"
        assert diff.before_content is None
        assert diff.is_delete is False

    def test_overwrite_existing_file(self, backend, session_id):
        backend.upload_binary(b"original", BUCKET, "data/file.txt")

        op_id = uuid.uuid4()
        _seed_cow_write(backend, session_id, op_id, "file.txt", "updated")

        diff = get_blob_operation_diff(
            backend, BUCKET, PREFIX, NS, session_id, op_id
        )
        assert diff.before_content == "original"
        assert diff.after_content == "updated"
        assert diff.is_new_file is False
        assert diff.is_delete is False

    def test_overwrite_with_identical_bytes_treated_as_no_op_diff(
        self, backend, session_id
    ):
        backend.upload_binary(b"unchanged", BUCKET, "data/file.txt")

        op_id = uuid.uuid4()
        _seed_cow_write(backend, session_id, op_id, "file.txt", "unchanged")

        diff = get_blob_operation_diff(
            backend, BUCKET, PREFIX, NS, session_id, op_id
        )
        assert diff.after_content == "unchanged"
        assert diff.before_content is None
        assert diff.is_new_file is True
        assert diff.is_delete is False

    def test_delete_diff(self, backend, session_id):
        backend.upload_binary(b"doomed", BUCKET, "data/gone.txt")

        op_id = uuid.uuid4()
        _seed_cow_tombstone(backend, session_id, op_id, "gone.txt")

        diff = get_blob_operation_diff(
            backend, BUCKET, PREFIX, NS, session_id, op_id
        )
        assert diff.is_delete is True
        assert diff.after_content is None

    def test_nonexistent_operation_returns_empty_diff(self, backend, session_id):
        diff = get_blob_operation_diff(
            backend, BUCKET, PREFIX, NS, session_id, uuid.uuid4()
        )
        assert diff.before_content is None
        assert diff.after_content is None
        assert diff.is_new_file is False
        assert diff.is_delete is False

    def test_multi_op_diff_uses_previous_cow_version(self, backend, session_id):
        op1 = uuid.uuid4()
        op2 = uuid.uuid4()
        _seed_cow_write(backend, session_id, op1, "file.txt", "first")
        _seed_cow_write(backend, session_id, op2, "file.txt", "second")

        diff = get_blob_operation_diff(
            backend, BUCKET, PREFIX, NS, session_id, op2
        )
        assert diff.before_content == "first"
        assert diff.after_content == "second"
        assert diff.is_new_file is False
