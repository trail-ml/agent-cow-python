"""
Core data structures for COW session scoring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from ..postgres.types import CHANGE_TABLE_RESERVED_FIELDS, CowWrite


__all__ = [
    "CHANGE_TABLE_RESERVED_FIELDS",
    "CowWrite",
    "TableMeta",
    "ScoringResult",
    "ScoreFn",
]


@dataclass
class TableMeta:
    pk_columns: set[str] = field(default_factory=set)
    fk_columns: set[str] = field(default_factory=set)
    column_types: dict[str, str] = field(default_factory=dict)


@dataclass
class ScoringResult:
    """Result of scoring an agent session against a ground-truth session.

    ``counts`` keys: ``matched``, ``missing``, ``extra``, ``gt_ops``, ``agent_ops``.
    ``scores`` is populated from the user-supplied ``score_fns`` registry.
    """

    struct_score: float
    content_score: float
    efficiency: float
    op_struct_scores: dict
    op_content_scores: dict
    counts: dict[str, int]
    scores: dict[str, float] = field(default_factory=dict)


ScoreFn = Callable[[ScoringResult], float]
