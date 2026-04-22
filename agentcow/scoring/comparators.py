"""
Field-level comparators for COW scoring.

The core protocol is :class:`WriteComparator`: given a pair of
:class:`CowWrite` values (GT + agent), produce a
:class:`WriteComparisonResult` with per-field similarity and an overall
match result.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from enum import Enum
from typing import Any, Optional, Protocol
from uuid import UUID

from .types import CHANGE_TABLE_RESERVED_FIELDS, CowWrite

logger = logging.getLogger(__name__)


class FieldCategory(Enum):
    """High-level category for a field, derived from its SQL data type."""

    TEXT = "text"
    NUMERIC = "numeric"
    BOOLEAN = "boolean"
    JSON = "json"
    TIMESTAMP = "timestamp"
    UUID = "uuid"
    OTHER = "other"


_TEXT_TYPES = {"text", "varchar", "character varying", "citext", "character"}
_NUMERIC_TYPES = {
    "integer",
    "bigint",
    "smallint",
    "numeric",
    "double precision",
    "real",
    "decimal",
}
_JSON_TYPES = {"json", "jsonb"}
_TIMESTAMP_TYPES = {
    "timestamp",
    "timestamp without time zone",
    "timestamp with time zone",
    "date",
    "time",
}


def categorize_data_type(data_type: Optional[str]) -> FieldCategory:
    if not data_type:
        return FieldCategory.OTHER
    dt = data_type.lower()
    if dt in _TEXT_TYPES:
        return FieldCategory.TEXT
    if dt in _NUMERIC_TYPES:
        return FieldCategory.NUMERIC
    if dt == "boolean":
        return FieldCategory.BOOLEAN
    if dt in _JSON_TYPES:
        return FieldCategory.JSON
    if dt in _TIMESTAMP_TYPES:
        return FieldCategory.TIMESTAMP
    if dt == "uuid":
        return FieldCategory.UUID
    return FieldCategory.OTHER


@dataclass
class FieldConfig:
    """Per-table PK/FK/datatype metadata and globally ignored fields."""

    pk_columns: dict[str, set[str]] = field(default_factory=dict)
    fk_columns: dict[str, set[str]] = field(default_factory=dict)
    column_types: dict[str, dict[str, str]] = field(default_factory=dict)
    ignored_fields: set[str] = field(
        default_factory=lambda: {
            *CHANGE_TABLE_RESERVED_FIELDS,
            "timestamp",
            "created_at",
            "updated_at",
            "deleted_at",
        }
    )

    def set_table_metadata(
        self,
        table_name: str,
        pk_columns: set[str],
        fk_columns: set[str],
        column_types: Optional[dict[str, str]] = None,
    ) -> None:
        self.pk_columns[table_name] = set(pk_columns)
        self.fk_columns[table_name] = set(fk_columns)
        if column_types is not None:
            self.column_types[table_name] = dict(column_types)

    def is_primary_key(self, table_name: str, field_name: str) -> bool:
        return field_name in self.pk_columns.get(table_name, set())

    def is_foreign_key(self, table_name: str, field_name: str) -> bool:
        return field_name in self.fk_columns.get(table_name, set())

    def is_ignored(self, field_name: str) -> bool:
        return field_name in self.ignored_fields

    def data_type(self, table_name: str, field_name: str) -> Optional[str]:
        return self.column_types.get(table_name, {}).get(field_name)

    def category(self, table_name: str, field_name: str) -> FieldCategory:
        return categorize_data_type(self.data_type(table_name, field_name))

    def is_timestamp(self, table_name: str, field_name: str) -> bool:
        if self.category(table_name, field_name) == FieldCategory.TIMESTAMP:
            return True
        if field_name == "timestamp" or field_name.endswith("_timestamp"):
            return True
        return False

    def should_compare(self, table_name: str, field_name: str) -> bool:
        if self.is_ignored(field_name):
            return False
        if self.is_primary_key(table_name, field_name):
            return False
        if self.is_foreign_key(table_name, field_name):
            return False
        if self.is_timestamp(table_name, field_name):
            return False
        return True


@dataclass
class FieldComparisonResult:
    field_name: str
    matches: bool
    ground_truth_value: Any
    agent_value: Any
    feedback: str
    similarity: float = 0.0


@dataclass
class WriteComparisonResult:
    is_match: bool
    field_results: list[FieldComparisonResult]
    overall_feedback: str
    similarity: float = 0.0

    @property
    def mismatched_fields(self) -> list[FieldComparisonResult]:
        return [r for r in self.field_results if not r.matches]


def _compute_similarity(field_results: list[FieldComparisonResult]) -> float:
    if not field_results:
        return 1.0
    return sum(r.similarity for r in field_results) / len(field_results)


def _build_overall_feedback(
    table_name: str,
    field_results: list[FieldComparisonResult],
    is_match: bool,
) -> str:
    mismatched = [r for r in field_results if not r.matches]
    if is_match and not mismatched:
        return f"{table_name}: exact match"
    if is_match:
        return (
            f"{table_name}: matched above threshold "
            f"({len(mismatched)} field(s) partially differ)"
        )
    if not mismatched:
        return f"{table_name}: similarity below threshold"
    preview = ", ".join(r.field_name for r in mismatched[:3])
    more = f" (+{len(mismatched) - 3} more)" if len(mismatched) > 3 else ""
    return f"{table_name}: field differences in {preview}{more}"


def _json_equal(a: Any, b: Any) -> bool:
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        b_remaining = list(b)
        for item in a:
            found = False
            for i, candidate in enumerate(b_remaining):
                if _json_equal(item, candidate):
                    b_remaining.pop(i)
                    found = True
                    break
            if not found:
                return False
        return True
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a.keys()) != set(b.keys()):
            return False
        return all(_json_equal(a[k], b[k]) for k in a)
    return a == b


def _parse_json(value: Any) -> Any:
    import json

    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


def _coerce_uuid(value: Any) -> Optional[UUID]:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return None


class WriteComparator(Protocol):
    """Callable that compares a GT and agent write given shared context."""

    def compare(
        self,
        ground_truth: CowWrite,
        agent: CowWrite,
        uuid_mapping: dict[UUID, UUID],
        field_config: FieldConfig,
        gt_created_uuids: set[UUID],
        agent_created_uuids: set[UUID],
    ) -> WriteComparisonResult: ...


class DatatypeComparator:
    """Type-aware comparator that routes by SQL data type."""

    def __init__(self, match_threshold: float = 0.8) -> None:
        self.match_threshold = match_threshold

    def compare(
        self,
        ground_truth: CowWrite,
        agent: CowWrite,
        uuid_mapping: dict[UUID, UUID],
        field_config: FieldConfig,
        gt_created_uuids: set[UUID],
        agent_created_uuids: set[UUID],
    ) -> WriteComparisonResult:
        table_name = ground_truth.table_name
        field_results: list[FieldComparisonResult] = []
        all_fields = set(ground_truth.data.keys()) | set(agent.data.keys())

        for field_name in all_fields:
            if not field_config.should_compare(table_name, field_name):
                continue

            gt_value = ground_truth.data.get(field_name)
            agent_value = agent.data.get(field_name)

            if field_config.is_foreign_key(table_name, field_name):
                field_results.append(
                    self._compare_fk_field(
                        field_name,
                        gt_value,
                        agent_value,
                        uuid_mapping,
                        gt_created_uuids,
                        agent_created_uuids,
                    )
                )
            else:
                data_type = field_config.data_type(table_name, field_name)
                field_results.append(
                    self._compare_content_field(
                        field_name, gt_value, agent_value, data_type
                    )
                )

        similarity = _compute_similarity(field_results)
        is_match = similarity >= self.match_threshold
        return WriteComparisonResult(
            is_match=is_match,
            field_results=field_results,
            overall_feedback=_build_overall_feedback(
                table_name, field_results, is_match
            ),
            similarity=similarity,
        )

    def _compare_fk_field(
        self,
        field_name: str,
        gt_value: Any,
        agent_value: Any,
        uuid_mapping: dict[UUID, UUID],
        gt_created_uuids: set[UUID],
        agent_created_uuids: set[UUID],
    ) -> FieldComparisonResult:
        gt_uuid = _coerce_uuid(gt_value)
        agent_uuid = _coerce_uuid(agent_value)

        if gt_uuid is None and agent_uuid is None:
            return FieldComparisonResult(
                field_name=field_name,
                matches=True,
                ground_truth_value=gt_value,
                agent_value=agent_value,
                feedback=f"Field '{field_name}' null in both",
                similarity=1.0,
            )
        if gt_uuid is None or agent_uuid is None:
            return FieldComparisonResult(
                field_name=field_name,
                matches=False,
                ground_truth_value=gt_value,
                agent_value=agent_value,
                feedback=f"Field '{field_name}' null mismatch",
                similarity=0.0,
            )

        if gt_uuid in gt_created_uuids:
            mapped = uuid_mapping.get(gt_uuid)
            if mapped is not None and mapped == agent_uuid:
                return FieldComparisonResult(
                    field_name=field_name,
                    matches=True,
                    ground_truth_value=gt_value,
                    agent_value=agent_value,
                    feedback=f"Field '{field_name}' matches via UUID mapping",
                    similarity=1.0,
                )
            return FieldComparisonResult(
                field_name=field_name,
                matches=False,
                ground_truth_value=gt_value,
                agent_value=agent_value,
                feedback=(
                    f"Field '{field_name}' FK mismatch after UUID mapping: "
                    f"expected {mapped} got {agent_uuid}"
                ),
                similarity=0.0,
            )

        if gt_uuid == agent_uuid:
            return FieldComparisonResult(
                field_name=field_name,
                matches=True,
                ground_truth_value=gt_value,
                agent_value=agent_value,
                feedback=f"Field '{field_name}' matches",
                similarity=1.0,
            )

        return FieldComparisonResult(
            field_name=field_name,
            matches=False,
            ground_truth_value=gt_value,
            agent_value=agent_value,
            feedback=(
                f"Field '{field_name}' FK differs: expected {gt_uuid} got {agent_uuid}"
            ),
            similarity=0.0,
        )

    def _compare_content_field(
        self,
        field_name: str,
        gt_value: Any,
        agent_value: Any,
        data_type: Optional[str],
    ) -> FieldComparisonResult:
        category = categorize_data_type(data_type)

        if gt_value is None and agent_value is None:
            return FieldComparisonResult(
                field_name=field_name,
                matches=True,
                ground_truth_value=gt_value,
                agent_value=agent_value,
                feedback=f"Field '{field_name}' null in both",
                similarity=1.0,
            )

        if gt_value is None or agent_value is None:
            return FieldComparisonResult(
                field_name=field_name,
                matches=False,
                ground_truth_value=gt_value,
                agent_value=agent_value,
                feedback=f"Field '{field_name}' null mismatch",
                similarity=0.0,
            )

        if category == FieldCategory.TEXT:
            similarity = SequenceMatcher(None, str(gt_value), str(agent_value)).ratio()
            matches = gt_value == agent_value
            feedback = (
                f"Field '{field_name}' matches"
                if matches
                else (
                    f"Field '{field_name}' differs: '{gt_value}' vs '{agent_value}' "
                    f"(sim={similarity:.2f})"
                )
            )
            return FieldComparisonResult(
                field_name=field_name,
                matches=matches,
                ground_truth_value=gt_value,
                agent_value=agent_value,
                feedback=feedback,
                similarity=similarity,
            )

        if category == FieldCategory.JSON:
            parsed_gt = _parse_json(gt_value)
            parsed_agent = _parse_json(agent_value)
            matches = _json_equal(parsed_gt, parsed_agent)
            return FieldComparisonResult(
                field_name=field_name,
                matches=matches,
                ground_truth_value=gt_value,
                agent_value=agent_value,
                feedback=(
                    f"Field '{field_name}' JSON matches"
                    if matches
                    else f"Field '{field_name}' JSON differs"
                ),
                similarity=1.0 if matches else 0.0,
            )

        matches = gt_value == agent_value
        return FieldComparisonResult(
            field_name=field_name,
            matches=matches,
            ground_truth_value=gt_value,
            agent_value=agent_value,
            feedback=(
                f"Field '{field_name}' matches"
                if matches
                else f"Field '{field_name}' differs: '{gt_value}' vs '{agent_value}'"
            ),
            similarity=1.0 if matches else 0.0,
        )


class CompositeComparator:
    """Route comparison by table name with a default fallback."""

    def __init__(
        self,
        table_comparators: Optional[dict[str, WriteComparator]] = None,
        default: Optional[WriteComparator] = None,
        match_threshold: float = 0.8,
    ) -> None:
        self.table_comparators = dict(table_comparators or {})
        self.default = default or DatatypeComparator(match_threshold=match_threshold)
        self.match_threshold = match_threshold

    def compare(
        self,
        ground_truth: CowWrite,
        agent: CowWrite,
        uuid_mapping: dict[UUID, UUID],
        field_config: FieldConfig,
        gt_created_uuids: set[UUID],
        agent_created_uuids: set[UUID],
    ) -> WriteComparisonResult:
        comparator = self.table_comparators.get(ground_truth.table_name, self.default)
        return comparator.compare(
            ground_truth,
            agent,
            uuid_mapping,
            field_config,
            gt_created_uuids,
            agent_created_uuids,
        )
