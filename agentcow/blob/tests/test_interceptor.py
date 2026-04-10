import time
import uuid

import pytest

from agentcow.blob.context import CowBlobConfig
from agentcow.blob.interceptor import bypass_cow, cow_intercept_delete
from agentcow.blob.paths import to_cow_path, to_tombstone_path


class _StorageBackendWithDecoratedDelete:
    def __init__(self, s3_client, interceptor):
        self._s3 = s3_client
        self.cow_interceptor = interceptor

    @cow_intercept_delete
    def delete_file(self, bucket_name, object_name):
        self._s3.delete_object(Bucket=bucket_name, Key=object_name)


class TestWriteInterception:
    def test_put_object_redirected_to_cow_path(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        ctx_var.set(cfg)

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"hello"
        )

        expected_cow = to_cow_path(cfg, cfg.session_id, "file.txt", cfg.operation_id)

        with bypass_cow(ctx_var):
            resp = s3_client.get_object(Bucket=test_bucket, Key=expected_cow)
            assert resp["Body"].read() == b"hello"

            with pytest.raises(s3_client.exceptions.NoSuchKey):
                s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")

    def test_pending_writes_accumulated(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        ctx_var.set(cfg)

        s3_client.put_object(
            Bucket=test_bucket, Key="data/a.txt", Body=b"a"
        )
        s3_client.put_object(
            Bucket=test_bucket, Key="data/b.txt", Body=b"b"
        )

        assert len(cfg.pending_writes) == 2
        paths = {w.final_path for w in cfg.pending_writes}
        assert paths == {"data/a.txt", "data/b.txt"}

    def test_create_multipart_upload_redirects_key(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        ctx_var.set(cfg)

        resp = s3_client.create_multipart_upload(
            Bucket=test_bucket, Key="data/file.txt"
        )
        upload_id = resp["UploadId"]
        expected_cow = to_cow_path(cfg, cfg.session_id, "file.txt", cfg.operation_id)

        listing = s3_client.list_multipart_uploads(
            Bucket=test_bucket, Prefix=expected_cow
        )
        uploads = listing.get("Uploads") or []
        assert any(u["Key"] == expected_cow for u in uploads)

        assert len(cfg.pending_writes) == 1
        assert cfg.pending_writes[0].final_path == "data/file.txt"

        s3_client.abort_multipart_upload(
            Bucket=test_bucket, Key=expected_cow, UploadId=upload_id
        )


class TestReadInterception:
    def test_read_resolves_through_history(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"original"
        )

        ctx_var.set(cfg)

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"cow-version"
        )

        resp = s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")
        assert resp["Body"].read() == b"cow-version"

    def test_read_falls_through_without_history(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"production"
        )

        ctx_var.set(cfg)

        resp = s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")
        assert resp["Body"].read() == b"production"

    def test_head_object_resolves_through_history(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"original"
        )

        ctx_var.set(cfg)

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"cow-version"
        )

        resp = s3_client.head_object(Bucket=test_bucket, Key="data/file.txt")
        assert int(resp["ContentLength"]) == len(b"cow-version")

    def test_head_object_falls_through_without_history(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"production"
        )

        ctx_var.set(cfg)

        resp = s3_client.head_object(Bucket=test_bucket, Key="data/file.txt")
        assert int(resp["ContentLength"]) == len(b"production")

    def test_head_object_after_delete_raises(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"content"
        )

        ctx_var.set(cfg)
        interceptor.record_delete(test_bucket, "data/file.txt")

        with pytest.raises(FileNotFoundError, match="deleted in this COW session"):
            s3_client.head_object(Bucket=test_bucket, Key="data/file.txt")


class TestDeleteInterception:
    def test_record_delete_writes_tombstone(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        ctx_var.set(cfg)

        result = interceptor.record_delete(test_bucket, "data/file.txt")
        assert result is True

        tombstone = to_tombstone_path(
            cfg, cfg.session_id, "file.txt", cfg.operation_id
        )
        resp = s3_client.get_object(Bucket=test_bucket, Key=tombstone)
        assert resp["Body"].read() == b""

    def test_read_after_delete_raises(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"content"
        )

        ctx_var.set(cfg)
        interceptor.record_delete(test_bucket, "data/file.txt")

        with pytest.raises(FileNotFoundError, match="deleted in this COW session"):
            s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")

    def test_delete_not_intercepted_when_inactive(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx

        result = interceptor.record_delete(test_bucket, "data/file.txt")
        assert result is False


class TestCowInterceptDeleteDecorator:
    def test_decorator_writes_tombstone_and_skips_real_delete(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        backend = _StorageBackendWithDecoratedDelete(s3_client, interceptor)

        s3_client.put_object(Bucket=test_bucket, Key="data/file.txt", Body=b"keep-me")
        ctx_var.set(cfg)

        backend.delete_file(test_bucket, "data/file.txt")

        tombstone = to_tombstone_path(
            cfg, cfg.session_id, "file.txt", cfg.operation_id
        )
        with bypass_cow(ctx_var):
            assert s3_client.get_object(Bucket=test_bucket, Key=tombstone)["Body"].read() == b""
            assert (
                s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")["Body"].read()
                == b"keep-me"
            )

    def test_decorator_calls_delete_object_when_session_inactive(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        backend = _StorageBackendWithDecoratedDelete(s3_client, interceptor)

        s3_client.put_object(Bucket=test_bucket, Key="data/file.txt", Body=b"gone")
        backend.delete_file(test_bucket, "data/file.txt")

        with pytest.raises(s3_client.exceptions.NoSuchKey):
            s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")


class TestNoOpWhenInactive:
    def test_write_passthrough(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"direct"
        )

        resp = s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")
        assert resp["Body"].read() == b"direct"

    def test_read_passthrough(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx

        s3_client.put_object(
            Bucket=test_bucket, Key="mykey", Body=b"value"
        )
        resp = s3_client.get_object(Bucket=test_bucket, Key="mykey")
        assert resp["Body"].read() == b"value"


class TestPathPrefixFiltering:
    def test_key_outside_prefix_not_intercepted(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        ctx_var.set(cfg)

        s3_client.put_object(
            Bucket=test_bucket, Key="other/file.txt", Body=b"outside"
        )

        resp = s3_client.get_object(Bucket=test_bucket, Key="other/file.txt")
        assert resp["Body"].read() == b"outside"
        assert len(cfg.pending_writes) == 0

    def test_key_must_match_full_path_prefix_not_only_shared_prefix_chars(
        self, s3_client, test_bucket, interceptor_with_ctx, session_id, operation_id
    ):
        interceptor, ctx_var, _ = interceptor_with_ctx
        cfg = CowBlobConfig(
            session_id=session_id,
            operation_id=operation_id,
            path_prefix="data/prefix/",
            scratch_namespace=".cow",
        )
        ctx_var.set(cfg)

        s3_client.put_object(Bucket=test_bucket, Key="data/pre", Body=b"raw")

        assert len(cfg.pending_writes) == 0
        assert (
            s3_client.get_object(Bucket=test_bucket, Key="data/pre")["Body"].read()
            == b"raw"
        )


class TestScratchNamespaceBypass:
    def test_cow_namespace_key_not_intercepted(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        ctx_var.set(cfg)

        cow_key = f"data/.cow/session_{cfg.session_id}/some-internal"
        s3_client.put_object(Bucket=test_bucket, Key=cow_key, Body=b"infra")

        resp = s3_client.get_object(Bucket=test_bucket, Key=cow_key)
        assert resp["Body"].read() == b"infra"


class TestBypassCow:
    def test_bypass_temporarily_disables_interception(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        ctx_var.set(cfg)

        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"cow-write"
        )
        assert len(cfg.pending_writes) == 1

        with bypass_cow(ctx_var):
            s3_client.put_object(
                Bucket=test_bucket, Key="data/file.txt", Body=b"direct-write"
            )

        resp = s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")
        assert resp["Body"].read() == b"cow-write"
        assert len(cfg.pending_writes) == 1

    def test_context_restored_after_bypass(
        self, s3_client, test_bucket, interceptor_with_ctx
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        ctx_var.set(cfg)

        with bypass_cow(ctx_var):
            assert ctx_var.get() is None

        assert ctx_var.get() is cfg


class TestMultiOperationHistory:
    def test_later_write_visible_via_s3_history(
        self, s3_client, test_bucket, interceptor_with_ctx, session_id
    ):
        """Pre-populate two cow writes in S3, then verify a fresh config
        reads the latest one (determined by LastModified)."""
        interceptor, ctx_var, cfg = interceptor_with_ctx

        op1 = uuid.UUID("00000000-0000-0000-0000-000000000001")
        op2 = uuid.UUID("00000000-0000-0000-0000-000000000002")

        cow1 = to_cow_path(cfg, cfg.session_id, "file.txt", op1)
        cow2 = to_cow_path(cfg, cfg.session_id, "file.txt", op2)

        with bypass_cow(ctx_var):
            s3_client.put_object(Bucket=test_bucket, Key=cow1, Body=b"version-1")
            time.sleep(0.05)
            s3_client.put_object(Bucket=test_bucket, Key=cow2, Body=b"version-2")

        fresh_cfg = CowBlobConfig(
            session_id=cfg.session_id,
            operation_id=uuid.uuid4(),
            path_prefix="data/",
        )
        ctx_var.set(fresh_cfg)

        resp = s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")
        assert resp["Body"].read() == b"version-2"

    def test_dependencies_tracked_across_operations(
        self, s3_client, test_bucket, interceptor_with_ctx, session_id
    ):
        interceptor, ctx_var, cfg = interceptor_with_ctx
        op1 = uuid.uuid4()
        op2 = uuid.uuid4()

        cfg.operation_id = op1
        ctx_var.set(cfg)
        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"v1"
        )

        cfg.operation_id = op2
        s3_client.put_object(
            Bucket=test_bucket, Key="data/file.txt", Body=b"v2"
        )

        assert (op1, op2) in cfg.dependencies


class TestFileHistoryLoading:
    def test_history_loaded_from_s3_listing(
        self, s3_client, test_bucket, interceptor_with_ctx, session_id
    ):
        """Pre-populate cow keys in S3, then verify _load_file_history picks them up."""
        interceptor, ctx_var, cfg = interceptor_with_ctx
        op_id = cfg.operation_id

        cow_key = to_cow_path(cfg, cfg.session_id, "file.txt", op_id)
        s3_client.put_object(Bucket=test_bucket, Key=cow_key, Body=b"data")

        fresh_cfg = CowBlobConfig(
            session_id=cfg.session_id,
            operation_id=uuid.uuid4(),
            path_prefix="data/",
        )
        ctx_var.set(fresh_cfg)

        s3_client.get_object(Bucket=test_bucket, Key="data/file.txt")

        assert "data/file.txt" in fresh_cfg.file_history
        assert fresh_cfg.history_loaded is True
