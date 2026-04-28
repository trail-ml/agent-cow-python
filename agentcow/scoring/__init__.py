"""
COW session scoring.

Mirrors the structure of :mod:`psudeocode.md`:

* :mod:`extraction` — pull rows and table metadata from Postgres.
* :mod:`matching` — pair ground truth entities with agent entities (two-pass internally
  so FK comparisons line up), and detect wasted ops.
* :mod:`compare` — ``WriteComparator`` Protocol and the default
  ``DatatypeComparator`` (which accepts per-table overrides).
* :mod:`scores` — ``struct_score``, ``content_score``, ``efficiency``.
* :mod:`scorer` — the per-op iteration flow plus the public entry points
  ``score_sessions`` and ``score_cow_sessions``.
"""

from .compare import (
    DatatypeComparator,
    RowSimilarityFn,
    WriteComparator,
    from_row_similarity,
)
from .extraction import (
    Executor,
    get_rows_changed,
    get_session_operations,
    get_table_metadata,
)
from .matching import drop_wasted_rows, get_wasted_ops, match_rows
from .sample_evaluators import default_score_fn, f1, precision, recall
from .scorer import (
    DEFAULT_IGNORED_FIELDS,
    score_cow_sessions,
    score_sessions,
)
from .scores import content_score, efficiency, struct_score
from .types import (
    CHANGE_TABLE_RESERVED_FIELDS,
    CowWrite,
    ScoreFn,
    ScoringCounts,
    ScoringResult,
    TableMeta,
)

__all__ = [
    "CHANGE_TABLE_RESERVED_FIELDS",
    "CowWrite",
    "DEFAULT_IGNORED_FIELDS",
    "DatatypeComparator",
    "Executor",
    "RowSimilarityFn",
    "ScoreFn",
    "ScoringCounts",
    "ScoringResult",
    "TableMeta",
    "WriteComparator",
    "content_score",
    "default_score_fn",
    "drop_wasted_rows",
    "efficiency",
    "f1",
    "from_row_similarity",
    "get_rows_changed",
    "get_session_operations",
    "get_table_metadata",
    "get_wasted_ops",
    "match_rows",
    "precision",
    "recall",
    "score_cow_sessions",
    "score_sessions",
    "struct_score",
]
