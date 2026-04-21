"""
Core data structures for COW session scoring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Optional
from uuid import UUID

if TYPE_CHECKING:
    from .comparators import (
        FieldComparisonResult,
        WriteComparator,
        WriteComparisonResult,
    )
    from .scorer import ScoringResult


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


@dataclass
class CowNode:
    op_id: UUID
    timestamp: datetime
    rows: list[CowWrite]
    metadata: Optional[dict[str, Any]] = None


@dataclass
class CowGraph:
    nodes: dict[UUID, CowNode] = field(default_factory=dict)

    def all_rows(self) -> list[CowWrite]:
        rows: list[CowWrite] = []
        for node in self.nodes.values():
            rows.extend(node.rows)
        return rows

    def topologically_sorted_nodes(self) -> list[CowNode]:
        return sorted(self.nodes.values(), key=lambda node: (node.timestamp, node.op_id))


@dataclass
class MatchedWrite:
    ground_truth: CowWrite
    agent: CowWrite
    comparison: "WriteComparisonResult"


@dataclass
class MissingWrite:
    ground_truth: CowWrite
    feedback: str


@dataclass
class ExtraWrite:
    agent: CowWrite
    feedback: str


@dataclass
class ScoredNode:
    op_id: UUID
    metadata: Optional[dict[str, Any]]
    structural_utility: float
    structural_score_before: float
    structural_score_after: float
    content_utility: float
    matched_rows: list[MatchedWrite]
    extra_rows: list[CowWrite]


@dataclass
class ScoredGraph:
    nodes: dict[UUID, ScoredNode] = field(default_factory=dict)


@dataclass
class OpUtility:
    op_id: UUID
    metadata: Optional[dict[str, Any]]
    structural_utility: float
    structural_score_before: float
    structural_score_after: float
    content_utility: float
    matched_rows_contributed: int
    extra_rows_contributed: int


@dataclass
class EntityComparison:
    table_name: str
    primary_key: dict[str, Any]
    status: str
    similarity: float
    feedback: str
    field_results: list["FieldComparisonResult"] = field(default_factory=list)

    @property
    def is_match(self) -> bool:
        return self.status == "matched"


@dataclass
class EntityStateResult:
    structural_score: float
    content_score: float
    relationship_score: float
    matched_count: int
    missing_count: int
    extra_count: int
    comparisons: list[EntityComparison] = field(default_factory=list)


@dataclass
class WastefulPair:
    table_name: str
    primary_key: dict[str, Any]
    create_op_id: UUID
    delete_op_id: UUID


@dataclass
class EfficiencyResult:
    efficiency: float
    op_count_ratio: float
    waste_ratio: float
    wasteful_pairs: list[WastefulPair] = field(default_factory=list)


@dataclass
class SessionScoringTerms:
    op_utilities: list[OpUtility]
    structural_score: float
    content_score: float
    relationship_score: float
    efficiency: float
    gt_operation_count: int
    agent_operation_count: int
    matched_row_count: int
    missing_row_count: int
    extra_row_count: int


ScoreFn = Callable[[SessionScoringTerms], float]
FeedbackFn = Callable[["ScoringResult"], str]


@dataclass
class ScoringConfig:
    comparator: Optional["WriteComparator"] = None
    score_fns: dict[str, ScoreFn] = field(default_factory=dict)
    feedback_fn: Optional[FeedbackFn] = None
    collapse: bool = False
    match_threshold: float = 0.8
    ignored_fields: Optional[set[str]] = None
