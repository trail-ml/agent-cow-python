import uuid

import pytest

from agentcow.blob.context import CowBlobConfig
from agentcow.blob.paths import (
    cow_session_prefix,
    is_infrastructure_path,
    parse_cow_key,
    strip_cow_prefix,
    to_cow_path,
    to_manifest_path,
    to_tombstone_path,
)

SID = uuid.UUID("baaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")


class TestToCowPath:
    def test_with_string_prefix(self):
        result = to_cow_path("data/", SID, "file.txt", OID)
        assert result == f"data/.cow/session_{SID}/blobs/{OID}/file.txt"

    def test_with_empty_prefix(self):
        result = to_cow_path("", SID, "file.txt", OID)
        assert result == f".cow/session_{SID}/blobs/{OID}/file.txt"

    def test_with_config_object(self):
        cfg = CowBlobConfig(path_prefix="pfx/", scratch_namespace=".scratch")
        result = to_cow_path(cfg, SID, "a/b.txt", OID)
        assert result == f"pfx/.scratch/session_{SID}/blobs/{OID}/a/b.txt"

    def test_custom_scratch_namespace(self):
        result = to_cow_path("", SID, "f.txt", OID, scratch_namespace=".ns")
        assert result == f".ns/session_{SID}/blobs/{OID}/f.txt"


class TestToTombstonePath:
    def test_with_string_prefix(self):
        result = to_tombstone_path("data/", SID, "file.txt", OID)
        assert result == f"data/.cow/session_{SID}/tombstones/{OID}/file.txt"

    def test_with_empty_prefix(self):
        result = to_tombstone_path("", SID, "file.txt", OID)
        assert result == f".cow/session_{SID}/tombstones/{OID}/file.txt"


class TestToManifestPath:
    def test_basic(self):
        result = to_manifest_path("data/", SID, OID)
        assert result == f"data/.cow/session_{SID}/.ops/{OID}.json"


class TestCowSessionPrefix:
    def test_basic(self):
        result = cow_session_prefix("data/", SID)
        assert result == f"data/.cow/session_{SID}/"

    def test_empty_prefix(self):
        result = cow_session_prefix("", SID)
        assert result == f".cow/session_{SID}/"

    def test_trailing_slash(self):
        result = cow_session_prefix("data/", SID)
        assert result.endswith("/")


class TestIsInfrastructurePath:
    def test_cow_path_is_infrastructure(self):
        path = f"data/.cow/session_{SID}/blobs/{OID}/file.txt"
        assert is_infrastructure_path(path) is True

    def test_production_path_is_not(self):
        assert is_infrastructure_path("data/file.txt") is False

    def test_custom_namespace(self):
        path = f".scratch/session_{SID}/blobs/{OID}/file.txt"
        assert is_infrastructure_path(path, scratch_namespace=".scratch") is True

    def test_partial_match_not_infrastructure(self):
        assert is_infrastructure_path(".cow/not_a_session/file.txt") is False


class TestStripCowPrefix:
    def test_round_trip(self):
        cow_path = to_cow_path("data/", SID, "file.txt", OID)
        stripped = strip_cow_prefix(cow_path, "data/", SID)
        assert stripped == f"blobs/{OID}/file.txt"

    def test_bad_prefix_raises(self):
        with pytest.raises(ValueError, match="does not start with session prefix"):
            strip_cow_prefix("wrong/path.txt", "data/", SID)


class TestParseCowKey:
    def test_parse_blob_key(self):
        prefix = f"data/.cow/session_{SID}/"
        key = f"{prefix}blobs/{OID}/images/photo.jpg"
        result = parse_cow_key(key, prefix, "data/")
        assert result is not None
        op_id, final_path, is_delete = result
        assert op_id == OID
        assert final_path == "data/images/photo.jpg"
        assert is_delete is False

    def test_parse_tombstone_key(self):
        prefix = f"data/.cow/session_{SID}/"
        key = f"{prefix}tombstones/{OID}/images/photo.jpg"
        result = parse_cow_key(key, prefix, "data/")
        assert result is not None
        op_id, final_path, is_delete = result
        assert op_id == OID
        assert final_path == "data/images/photo.jpg"
        assert is_delete is True

    def test_non_cow_key_returns_none(self):
        prefix = f"data/.cow/session_{SID}/"
        assert parse_cow_key("data/file.txt", prefix, "data/") is None

    def test_manifest_key_returns_none(self):
        prefix = f"data/.cow/session_{SID}/"
        key = f"{prefix}.ops/{OID}.json"
        assert parse_cow_key(key, prefix, "data/") is None

    def test_malformed_uuid_returns_none(self):
        prefix = f"data/.cow/session_{SID}/"
        key = f"{prefix}blobs/not-a-uuid/file.txt"
        assert parse_cow_key(key, prefix, "data/") is None

    def test_no_slash_after_op_id_returns_none(self):
        prefix = f"data/.cow/session_{SID}/"
        key = f"{prefix}blobs/{OID}"
        assert parse_cow_key(key, prefix, "data/") is None

    def test_empty_prefix(self):
        prefix = f".cow/session_{SID}/"
        key = f"{prefix}blobs/{OID}/file.txt"
        result = parse_cow_key(key, prefix, "")
        assert result is not None
        _, final_path, _ = result
        assert final_path == "file.txt"

    def test_path_prefix_without_trailing_slash_joins_to_relative(self):
        prefix = f"data/.cow/session_{SID}/"
        key = f"{prefix}blobs/{OID}/file.txt"
        result = parse_cow_key(key, prefix, "data")
        assert result is not None
        _, final_path, _ = result
        assert final_path == "datafile.txt"
