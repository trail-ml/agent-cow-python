"""
Path construction and parsing for COW blob operations.

All functions are pure — no I/O, no driver-specific imports.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import CowBlobConfig


def _extract_prefix_and_ns(
    ctx_or_prefix: CowBlobConfig | str,
    scratch_namespace: str | None = None,
) -> tuple[str, str]:
    if isinstance(ctx_or_prefix, str):
        return ctx_or_prefix, scratch_namespace or ".cow"
    return ctx_or_prefix.path_prefix, ctx_or_prefix.scratch_namespace


def to_cow_path(
    ctx_or_prefix: CowBlobConfig | str,
    session_id: uuid.UUID,
    object_name: str,
    operation_id: uuid.UUID,
    scratch_namespace: str | None = None,
) -> str:
    """Build the operation-scoped scratch path for a COW write."""
    prefix, ns = _extract_prefix_and_ns(ctx_or_prefix, scratch_namespace)
    return f"{prefix}{ns}/session_{session_id}/blobs/{operation_id}/{object_name}"


def to_tombstone_path(
    ctx_or_prefix: CowBlobConfig | str,
    session_id: uuid.UUID,
    object_name: str,
    operation_id: uuid.UUID,
    scratch_namespace: str | None = None,
) -> str:
    """Build the operation-scoped path for a COW delete tombstone."""
    prefix, ns = _extract_prefix_and_ns(ctx_or_prefix, scratch_namespace)
    return f"{prefix}{ns}/session_{session_id}/tombstones/{operation_id}/{object_name}"


def to_manifest_path(
    ctx_or_prefix: CowBlobConfig | str,
    session_id: uuid.UUID,
    operation_id: uuid.UUID,
    scratch_namespace: str | None = None,
) -> str:
    prefix, ns = _extract_prefix_and_ns(ctx_or_prefix, scratch_namespace)
    return f"{prefix}{ns}/session_{session_id}/.ops/{operation_id}.json"


def cow_session_prefix(
    ctx_or_prefix: CowBlobConfig | str,
    session_id: uuid.UUID,
    scratch_namespace: str | None = None,
) -> str:
    prefix, ns = _extract_prefix_and_ns(ctx_or_prefix, scratch_namespace)
    return f"{prefix}{ns}/session_{session_id}/"


def is_infrastructure_path(path: str, scratch_namespace: str = ".cow") -> bool:
    """Return True if the path is inside the COW scratch namespace."""
    return f"{scratch_namespace}/session_" in path


def strip_cow_prefix(
    cow_path: str,
    ctx_or_prefix: CowBlobConfig | str,
    session_id: uuid.UUID,
    scratch_namespace: str | None = None,
) -> str:
    """Recover the production-relative path from a COW path."""
    session_prefix = cow_session_prefix(ctx_or_prefix, session_id, scratch_namespace)
    if not cow_path.startswith(session_prefix):
        raise ValueError(
            f"Path {cow_path!r} does not start with session prefix {session_prefix!r}"
        )
    return cow_path[len(session_prefix) :]


def parse_cow_key(
    key: str, session_prefix: str, path_prefix: str
) -> tuple[uuid.UUID, str, bool] | None:
    """Parse a COW S3 key into (operation_id, final_path, is_delete).

    Returns None for keys that aren't blobs or tombstones (e.g. old manifests).

    Expected formats::

        {session_prefix}blobs/{operation_id}/{relative_path}
        {session_prefix}tombstones/{operation_id}/{relative_path}
    """
    if not key.startswith(session_prefix):
        return None

    rest = key[len(session_prefix) :]

    if rest.startswith("blobs/"):
        is_delete = False
        rest = rest[len("blobs/") :]
    elif rest.startswith("tombstones/"):
        is_delete = True
        rest = rest[len("tombstones/") :]
    else:
        return None

    slash_idx = rest.find("/")
    if slash_idx == -1:
        return None

    op_id_str = rest[:slash_idx]
    relative_path = rest[slash_idx + 1 :]

    try:
        op_id = uuid.UUID(op_id_str)
    except ValueError:
        return None

    final_path = f"{path_prefix}{relative_path}"
    return (op_id, final_path, is_delete)
