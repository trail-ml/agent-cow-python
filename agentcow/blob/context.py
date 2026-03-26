from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from agentcow.context import CowConfig


@dataclass
class CowBlobRecord:
    """A single pending blob write or delete.

    Only ``final_path``, ``is_delete``, and ``timestamp`` are serialized
    to the manifest.  The remaining fields are populated at runtime from
    context that is already available (bucket from the caller, operation_id
    from the manifest filename, cow_path from path conventions).
    """

    final_path: str
    is_delete: bool = False
    timestamp: str = ""
    bucket_name: str = ""
    cow_path: str = ""
    operation_id: uuid.UUID | None = None
    group_key: str | None = None


@dataclass
class BlobOperationDiff:
    """Before/after content for a single operation on a single file."""

    before_content: str | None
    after_content: str | None
    is_new_file: bool
    is_delete: bool


@dataclass
class CowBlobConfig(CowConfig):
    """Configuration and runtime state for a COW blob session.

    Parallel to agentcow's CowRequestConfig. The blob interceptor reads
    and writes these fields during interception. Applications can
    subclass to add backend-specific fields (e.g. visible_operations).
    """

    path_prefix: str = ""
    scratch_namespace: str = ".cow"
    group_key: str | None = None
    pending_writes: list[CowBlobRecord] = field(default_factory=list)
    dependencies: list[tuple[uuid.UUID, uuid.UUID]] = field(default_factory=list)
    file_history: dict[str, list[tuple[uuid.UUID, bool, str]]] = field(
        default_factory=dict
    )
    history_loaded: bool = False
