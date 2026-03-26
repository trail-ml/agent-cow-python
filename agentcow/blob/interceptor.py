"""
boto3 event hooks for transparent COW blob path interception.

Registers on the S3 client's event system to rewrite Keys when a COW
session is active (context var is set).  Reads resolve through file
history; writes redirect to operation-scoped scratch paths; deletes
write zero-byte tombstone blobs.

The interceptor is fully self-contained — it uses the S3 client directly
for tombstone writes and session listing, with no dependency on
higher-level storage abstractions.
"""

from __future__ import annotations

import functools
import logging
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Optional

from .context import CowBlobConfig, CowBlobRecord
from .paths import cow_session_prefix, parse_cow_key, to_cow_path, to_tombstone_path

logger = logging.getLogger(__name__)


@contextmanager
def bypass_cow(context_var: ContextVar[Any]):
    """Temporarily disable COW interception for the current context.

    Use this when you need to read/write production paths directly
    while a COW session is active (e.g. reading original content for diffs).
    """
    token = context_var.set(None)
    try:
        yield
    finally:
        context_var.reset(token)


def cow_intercept_delete(fn):
    """Decorator that suppresses the wrapped delete when a COW session is active.

    Expects the decorated method's ``self`` to have a ``cow_interceptor``
    attribute (a ``CowBlobInterceptor`` instance).  The first two positional
    args after ``self`` must be ``bucket_name`` and ``object_name``.
    """

    @functools.wraps(fn)
    def wrapper(self, bucket_name, object_name, *args, **kwargs):
        if self.cow_interceptor.record_delete(bucket_name, object_name):
            return
        return fn(self, bucket_name, object_name, *args, **kwargs)

    return wrapper


_WRITE_OPS = ("PutObject", "CreateMultipartUpload")
_READ_OPS = ("GetObject", "HeadObject")


class CowBlobInterceptor:
    """Registers boto3 event hooks for transparent COW path interception.

    When the context var holds an active COW session, write operations
    have their Key rewritten to a scratch path, read operations resolve
    through the session's file history, and deletes write zero-byte
    tombstone blobs instead of actually deleting.

    The interceptor is registered once on the singleton S3 client and
    checks the context var on every call — when no COW session is active,
    the hooks are no-ops.

    File history is derived from S3 key structure (blobs/ and tombstones/
    subdirectories under the session prefix) rather than separate manifest
    files.
    """

    def __init__(
        self,
        s3_client,
        context_var: ContextVar[Any],
    ):
        self._s3_client = s3_client
        self._context_var: ContextVar[Optional[CowBlobConfig]] = context_var

        for op in _WRITE_OPS:
            s3_client.meta.events.register(
                f"before-parameter-build.s3.{op}", self._intercept_write
            )
        for op in _READ_OPS:
            s3_client.meta.events.register(
                f"before-parameter-build.s3.{op}", self._intercept_read
            )

    def _get_ctx(self) -> Optional[CowBlobConfig]:
        return self._context_var.get()

    @staticmethod
    def _should_intercept(ctx: CowBlobConfig, object_name: str) -> bool:
        prefix = ctx.path_prefix
        ns = ctx.scratch_namespace
        if prefix and not object_name.startswith(prefix):
            return False
        rel = object_name[len(prefix) :] if prefix else object_name
        if rel.startswith(f"{ns}/"):
            return False
        return True

    @staticmethod
    def _relative_name(ctx: CowBlobConfig, object_name: str) -> str:
        prefix = ctx.path_prefix
        if prefix and object_name.startswith(prefix):
            return object_name[len(prefix) :]
        return object_name

    @staticmethod
    def _session_id(ctx: CowBlobConfig) -> uuid.UUID:
        assert ctx.session_id is not None
        return ctx.session_id

    @staticmethod
    def _operation_id(ctx: CowBlobConfig) -> uuid.UUID:
        assert ctx.operation_id is not None
        return ctx.operation_id

    @staticmethod
    def _cow_path_for(ctx: CowBlobConfig, object_name: str) -> str:
        return to_cow_path(
            ctx,
            CowBlobInterceptor._session_id(ctx),
            CowBlobInterceptor._relative_name(ctx, object_name),
            CowBlobInterceptor._operation_id(ctx),
        )

    @staticmethod
    def _tombstone_path_for(ctx: CowBlobConfig, object_name: str) -> str:
        return to_tombstone_path(
            ctx,
            CowBlobInterceptor._session_id(ctx),
            CowBlobInterceptor._relative_name(ctx, object_name),
            CowBlobInterceptor._operation_id(ctx),
        )

    @staticmethod
    def _accumulate(
        ctx: CowBlobConfig,
        bucket_name: str,
        object_name: str,
        is_delete: bool = False,
    ) -> None:
        op_id = CowBlobInterceptor._operation_id(ctx)
        history = ctx.file_history.get(object_name)
        if history:
            last_op_id = history[-1][0]
            if last_op_id != op_id:
                ctx.dependencies.append((last_op_id, op_id))

        ctx.file_history.setdefault(object_name, []).append(
            (ctx.operation_id, is_delete)
        )
        cow_path = (
            "" if is_delete else CowBlobInterceptor._cow_path_for(ctx, object_name)
        )
        ctx.pending_writes.append(
            CowBlobRecord(
                final_path=object_name,
                is_delete=is_delete,
                timestamp=datetime.now(timezone.utc).isoformat(),
                bucket_name=bucket_name,
                cow_path=cow_path,
                operation_id=ctx.operation_id,
            )
        )

    def _load_file_history(self, ctx: CowBlobConfig, bucket_name: str) -> None:
        """Populate file_history by listing the session's S3 keys.

        Blobs under ``blobs/`` are writes; keys under ``tombstones/``
        are deletes.  Entries are sorted by ``LastModified``, then by
        full S3 key, when timestamps tie (e.g. mocks with coarse clocks).
        """
        if ctx.history_loaded:
            return
        ctx.history_loaded = True

        prefix = cow_session_prefix(
            ctx.path_prefix, self._session_id(ctx), ctx.scratch_namespace
        )
        paginator = self._s3_client.get_paginator("list_objects_v2")

        entries: list[tuple[str, uuid.UUID, bool, datetime, str]] = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                parsed = parse_cow_key(obj["Key"], prefix, ctx.path_prefix)
                if parsed is None:
                    continue
                op_id, final_path, is_delete = parsed
                entries.append(
                    (final_path, op_id, is_delete, obj["LastModified"], obj["Key"])
                )

        entries.sort(key=lambda e: (e[3], e[4]))

        for final_path, op_id, is_delete, _, _ in entries:
            ctx.file_history.setdefault(final_path, []).append((op_id, is_delete))

    def _resolve_read_path(self, ctx: CowBlobConfig, object_name: str) -> Optional[str]:
        """Return the path to read from, or None if deleted in this session."""
        if not self._should_intercept(ctx, object_name):
            return object_name

        history = ctx.file_history.get(object_name)
        if history:
            last_op_id, is_delete = history[-1]
            if is_delete:
                return None
            return to_cow_path(
                ctx,
                CowBlobInterceptor._session_id(ctx),
                self._relative_name(ctx, object_name),
                last_op_id,
            )

        return object_name

    def _intercept_write(self, params, **kwargs):
        ctx = self._get_ctx()
        if ctx is None or ctx.session_id is None:
            return
        key = params.get("Key", "")
        if not key or not self._should_intercept(ctx, key):
            return
        params["Key"] = self._cow_path_for(ctx, key)
        self._accumulate(ctx, params["Bucket"], key)

    def _intercept_read(self, params, **kwargs):
        ctx = self._get_ctx()
        if ctx is None or ctx.session_id is None:
            return
        key = params.get("Key", "")
        if not key or not self._should_intercept(ctx, key):
            return
        self._load_file_history(ctx, params["Bucket"])
        resolved = self._resolve_read_path(ctx, params["Key"])
        if resolved is None:
            raise FileNotFoundError(
                f"Blob {params['Key']!r} has been deleted in this COW session"
            )
        params["Key"] = resolved

    def record_delete(self, bucket_name: str, object_name: str) -> bool:
        """Record a COW delete if a session is active.

        Writes a zero-byte tombstone blob to S3 and updates the in-memory
        file history. Returns True if the delete was intercepted (caller
        should skip the actual S3 call), False otherwise.
        """
        ctx = self._get_ctx()
        if (
            ctx is None
            or ctx.session_id is None
            or not self._should_intercept(ctx, object_name)
        ):
            return False

        tombstone = self._tombstone_path_for(ctx, object_name)
        self._s3_client.put_object(Bucket=bucket_name, Key=tombstone, Body=b"")

        self._accumulate(ctx, bucket_name, object_name, is_delete=True)
        return True
