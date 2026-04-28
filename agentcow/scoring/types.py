"""
Core data structures for COW session scoring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, TypedDict

from ..postgres.types import CHANGE_TABLE_RESERVED_FIELDS, CowWrite


__all__ = [
    "CHANGE_TABLE_RESERVED_FIELDS",
    "CowWrite",
    "TableMeta",
    "ScoringCounts",
    "ScoringResult",
    "ScoreFn",
]


@dataclass
class TableMeta:
    pk_columns: set[str] = field(default_factory=set)
    fk_columns: set[str] = field(default_factory=set)
    column_types: dict[str, str] = field(default_factory=dict)


class ScoringCounts(TypedDict):
    matched: int
    missing: int
    extra: int
    gt_ops: int
    agent_ops: int


@dataclass
class ScoringResult:
    """Result of scoring an agent session against a ground-truth session.

    ``scores`` is populated from the user-supplied ``score_fns`` registry.
    """

    struct_score: float
    content_score: float
    efficiency: float
    op_struct_scores: dict
    op_content_scores: dict
    counts: ScoringCounts
    scores: dict[str, float] = field(default_factory=dict)


ScoreFn = Callable[[ScoringResult], float]
