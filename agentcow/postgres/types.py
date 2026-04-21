"""
Core data structures for PostgreSQL COW change-table rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


CHANGE_TABLE_RESERVED_FIELDS: frozenset[str] = frozenset(
    {"session_id", "operation_id", "_cow_deleted", "_cow_updated_at"}
)


@dataclass
class CowWrite:
    table_name: str
    operation_id: UUID
    primary_key: dict[str, Any]
    data: dict[str, Any]
    is_delete: bool
    updated_at: datetime

    def __hash__(self) -> int:
        return hash(self.get_pk_tuple())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CowWrite):
            return NotImplemented
        return self.get_pk_tuple() == other.get_pk_tuple()

    @classmethod
    def from_row(
        cls,
        table_name: str,
        row: dict[str, Any],
        pk_columns: list[str],
    ) -> "CowWrite":
        primary_key = {column: row.get(column) for column in pk_columns}
        operation_id = row.get("operation_id")
        is_delete = row.get("_cow_deleted", False)
        updated_at = row.get("_cow_updated_at", datetime.now())

        data = {
            key: value
            for key, value in row.items()
            if key not in CHANGE_TABLE_RESERVED_FIELDS
        }

        return cls(
            table_name=table_name,
            operation_id=operation_id,
            primary_key=primary_key,
            data=data,
            is_delete=is_delete,
            updated_at=updated_at,
        )

    def get_pk_tuple(self) -> tuple:
        return (self.table_name, tuple(sorted(self.primary_key.items())))
