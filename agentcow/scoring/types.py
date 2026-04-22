"""
Core data structures for COW session scoring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Optional
from uuid import UUID

from ..postgres.types import CHANGE_TABLE_RESERVED_FIELDS, CowWrite

if TYPE_CHECKING:
    from .comparators import (
        FieldComparisonResult,
        WriteComparator,
        WriteComparisonResult,
    )
    from .row_similarity import RowSimilarityFn


__all__ = [
    "CHANGE_TABLE_RESERVED_FIELDS",
    "CowWrite",
    "CowNode",
    "CowGraph",
    "MatchedWrite",
    "MissingWrite",
    "ExtraWrite",
    "ScoredNode",
    "ScoredGraph",
    "OpUtility",
    "EntityComparison",
    "EntityStateResult",
    "WastefulPair",
    "EfficiencyResult",
    "SessionScoringTerms",
    "ScoringResult",
    "ScoreFn",
    "FeedbackFn",
    "ScoringConfig",
]


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
        return sorted(
            self.nodes.values(), key=lambda node: (node.timestamp, node.op_id)
        )


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


@dataclass
class ScoringResult:
    scores: dict[str, float]
    feedback_report: str
    terms: SessionScoringTerms
    scored_graph: ScoredGraph
    matched_writes: list[MatchedWrite] = field(default_factory=list)
    missing_writes: list[MissingWrite] = field(default_factory=list)
    extra_writes: list[ExtraWrite] = field(default_factory=list)
    entity_state_comparisons: list[EntityComparison] = field(default_factory=list)


ScoreFn = Callable[[SessionScoringTerms], float]
FeedbackFn = Callable[[ScoringResult], str]


@dataclass
class ScoringConfig:
    comparator: Optional["WriteComparator"] = None
    row_similarity: Optional[dict[str, "RowSimilarityFn"]] = None
    score_fns: dict[str, ScoreFn] = field(default_factory=dict)
    feedback_fn: Optional[FeedbackFn] = None
    collapse: bool = False
    match_threshold: float = 0.8
    exact_match_threshold: float = 0.9999
    ignored_fields: Optional[set[str]] = None
