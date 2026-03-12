"""
Base COW configuration shared by all implementations.

No driver-specific imports — only standard Python.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass
class CowConfig:
    """Base configuration shared by all COW implementations.

    Subclassed by each backend (SQL, blob, etc.) to add
    implementation-specific fields.
    """

    session_id: uuid.UUID | None = None
    operation_id: uuid.UUID | None = None

    @property
    def is_cow_requested(self) -> bool:
        return self.session_id is not None
