"""
High-level COW blob operations: commit, discard, cleanup, dependencies.

Records are discovered by listing S3 keys under the session prefix
(blobs/ for writes, tombstones/ for deletes) rather than reading
manifest files.
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict

from .paths import cow_session_prefix, parse_cow_key
from .context import BlobOperationDiff, CowBlobRecord

logger = logging.getLogger(__name__)


def _load_records_from_keys(
    backend,
    bucket: str,
    path_prefix: str,
    scratch_namespace: str,
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID] | None = None,
) -> list[CowBlobRecord]:
    """Build CowBlobRecords by listing and parsing S3 keys under the session prefix."""
    prefix = cow_session_prefix(path_prefix, session_id, scratch_namespace)
    blobs = backend.list_blobs(bucket, prefix)

    op_filter = set(operation_ids) if operation_ids is not None else None
    records: list[CowBlobRecord] = []
    for blob in blobs:
        name = blob.name if hasattr(blob, "name") else str(blob)
        parsed = parse_cow_key(name, prefix, path_prefix)
        if parsed is None:
            continue
        op_id, final_path, is_delete = parsed
        if op_filter is not None and op_id not in op_filter:
            continue
        records.append(
            CowBlobRecord(
                final_path=final_path,
                is_delete=is_delete,
                timestamp=blob.last_updated.isoformat() if blob.last_updated else "",
                bucket_name=bucket,
                cow_path=name,
                operation_id=op_id,
            )
        )
    return records


def get_blob_session_operations(
    backend,
    manifest_bucket: str,
    path_prefix: str,
    scratch_namespace: str,
    session_id: uuid.UUID,
) -> list[uuid.UUID]:
    """Return all uncommitted blob operation IDs for a COW session."""
    records = _load_records_from_keys(
        backend, manifest_bucket, path_prefix, scratch_namespace, session_id
    )
    seen: set[uuid.UUID] = set()
    ops: list[uuid.UUID] = []
    for rec in records:
        if rec.operation_id and rec.operation_id not in seen:
            seen.add(rec.operation_id)
            ops.append(rec.operation_id)
    return ops


def get_blob_session_records(
    backend,
    manifest_bucket: str,
    path_prefix: str,
    scratch_namespace: str,
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID] | None = None,
) -> list[CowBlobRecord]:
    """Return blob operation records for a COW session."""
    return _load_records_from_keys(
        backend,
        manifest_bucket,
        path_prefix,
        scratch_namespace,
        session_id,
        operation_ids,
    )


def get_blob_dependencies(
    backend,
    manifest_bucket: str,
    path_prefix: str,
    scratch_namespace: str,
    session_id: uuid.UUID,
) -> list[tuple[uuid.UUID, uuid.UUID]]:
    """Compute blob operation dependencies from session keys.

    When two operations touch the same ``final_path``, the one with the
    earlier timestamp depends-on the later one.

    Returns ``(depends_on, operation_id)`` pairs.
    """
    records = _load_records_from_keys(
        backend, manifest_bucket, path_prefix, scratch_namespace, session_id
    )

    files: dict[str, list[tuple[uuid.UUID, str]]] = defaultdict(list)
    for rec in records:
        if rec.operation_id is not None:
            files[rec.final_path].append((rec.operation_id, rec.timestamp))

    deps: list[tuple[uuid.UUID, uuid.UUID]] = []
    seen: set[tuple[uuid.UUID, uuid.UUID]] = set()
    for entries in files.values():
        if len(entries) < 2:
            continue
        entries.sort(key=lambda e: e[1])
        for i in range(len(entries) - 1):
            pair = (entries[i][0], entries[i + 1][0])
            if pair not in seen:
                seen.add(pair)
                deps.append(pair)
    return deps


def _read_record_content(
    backend,
    bucket: str,
    rec: CowBlobRecord,
) -> str | None:
    if rec.is_delete:
        return None
    return backend.download_as_string(bucket, rec.cow_path)


def get_blob_operation_diff(
    backend,
    manifest_bucket: str,
    path_prefix: str,
    scratch_namespace: str,
    session_id: uuid.UUID,
    operation_id: uuid.UUID,
) -> BlobOperationDiff:
    """Compute before/after content for a single operation on a single file."""
    records = _load_records_from_keys(
        backend, manifest_bucket, path_prefix, scratch_namespace, session_id
    )

    target = next((r for r in records if r.operation_id == operation_id), None)
    if target is None:
        return BlobOperationDiff(
            before_content=None,
            after_content=None,
            is_new_file=False,
            is_delete=False,
        )

    after_content = _read_record_content(backend, manifest_bucket, target)

    file_ops = sorted(
        (
            r
            for r in records
            if r.final_path == target.final_path and r.operation_id is not None
        ),
        key=lambda r: r.timestamp,
    )
    target_idx = next(
        (i for i, r in enumerate(file_ops) if r.operation_id == operation_id), None
    )

    if target_idx is not None and target_idx > 0:
        prev = file_ops[target_idx - 1]
        before_content = _read_record_content(backend, manifest_bucket, prev)
        is_new_file = prev.is_delete
    elif backend.blob_exists(manifest_bucket, target.final_path):
        before_content = backend.download_as_string(manifest_bucket, target.final_path)
        is_new_file = before_content == after_content
        if is_new_file:
            before_content = None
    else:
        before_content = None
        is_new_file = True

    return BlobOperationDiff(
        before_content=before_content,
        after_content=after_content,
        is_new_file=is_new_file,
        is_delete=target.is_delete,
    )


def commit_cow_blobs(
    backend,
    manifest_bucket: str,
    path_prefix: str,
    scratch_namespace: str,
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID] | None = None,
) -> int:
    """Commit COW blobs: copy writes to production, apply deletes, clean up.

    Records are sorted by timestamp so multi-operation commits apply
    in the correct order.  Returns the number of records processed.
    """
    records = _load_records_from_keys(
        backend,
        manifest_bucket,
        path_prefix,
        scratch_namespace,
        session_id,
        operation_ids,
    )
    records.sort(key=lambda r: r.timestamp)

    for rec in records:
        if rec.is_delete:
            if backend.blob_exists(rec.bucket_name, rec.final_path):
                backend.delete_file(rec.bucket_name, rec.final_path)
        else:
            data = backend.download_as_bytes(rec.bucket_name, rec.cow_path)
            if data is not None:
                backend.upload_binary(data, rec.bucket_name, rec.final_path)

    _cleanup_session(
        backend,
        manifest_bucket,
        path_prefix,
        scratch_namespace,
        session_id,
        operation_ids,
    )

    logger.info(
        "Committed %d cow blob records for session %s", len(records), session_id
    )
    return len(records)


def discard_cow_blobs(
    backend,
    manifest_bucket: str,
    path_prefix: str,
    scratch_namespace: str,
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID] | None = None,
) -> int:
    """Discard COW blobs: delete scratch paths and tombstones.

    Returns the number of records processed.
    """
    records = _load_records_from_keys(
        backend,
        manifest_bucket,
        path_prefix,
        scratch_namespace,
        session_id,
        operation_ids,
    )

    _cleanup_session(
        backend,
        manifest_bucket,
        path_prefix,
        scratch_namespace,
        session_id,
        operation_ids,
    )

    logger.info(
        "Discarded %d cow blob records for session %s", len(records), session_id
    )
    return len(records)


def _cleanup_session(
    backend,
    manifest_bucket: str,
    path_prefix: str,
    scratch_namespace: str,
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID] | None = None,
) -> None:
    """Remove COW scratch blobs and tombstones.

    When ``operation_ids`` is None the entire session prefix is wiped.
    When scoped, only the referenced blobs/tombstones are removed.
    """
    if operation_ids is None:
        prefix = cow_session_prefix(path_prefix, session_id, scratch_namespace)
        blobs = backend.list_blobs(manifest_bucket, prefix)
        for blob in blobs:
            backend.delete_file(manifest_bucket, blob.name)
        return

    records = _load_records_from_keys(
        backend,
        manifest_bucket,
        path_prefix,
        scratch_namespace,
        session_id,
        operation_ids,
    )
    for rec in records:
        if rec.cow_path and backend.blob_exists(manifest_bucket, rec.cow_path):
            backend.delete_file(manifest_bucket, rec.cow_path)
