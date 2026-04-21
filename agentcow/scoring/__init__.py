"""
Generic graph-based COW session scoring library.

This subpackage is deliberately self-contained: no framework glue, no ORM,
and no app-specific config. All database access goes through the
:class:`~agentcow.scoring.extraction.Executor` protocol so callers can layer
their own wrappers on top.
"""

from .comparators import (
    CompositeComparator,
    DatatypeComparator,
    FieldCategory,
    FieldComparisonResult,
    FieldConfig,
    WriteComparator,
    WriteComparisonResult,
    categorize_data_type,
)
from .efficiency import (
    compute_efficiency,
    compute_operation_count_ratio,
    detect_wasteful_pairs,
)
from .entity_state import compute_entity_state_score, flatten_graph
from .extraction import (
    Executor,
    extract_session_graph,
    extract_session_writes,
    get_table_column_types,
    get_table_fk_columns,
    get_table_pk_columns,
)
from .feedback import default_feedback_fn
from .matching import (
    MatchResult,
    collapse_to_final_state,
    find_best_match,
    get_created_uuids,
    match_writes,
    topological_sort_writes,
)
from .op_utility import compute_op_utilities
from .row_similarity import (
    RowSimilarityComparator,
    RowSimilarityFn,
    SimilarityResult,
    from_row_similarity,
)
from .sample_scorers import default_score_fn, f1, precision, recall
from .scorer import (
    build_empty_field_config,
    build_field_config,
    score_cow_sessions,
    score_sessions,
)
from .types import (
    CowGraph,
    CowNode,
    CowWrite,
    EfficiencyResult,
    EntityComparison,
    EntityStateResult,
    ExtraWrite,
    FeedbackFn,
    MatchedWrite,
    MissingWrite,
    OpUtility,
    ScoredGraph,
    ScoredNode,
    ScoreFn,
    ScoringConfig,
    ScoringResult,
    SessionScoringTerms,
    WastefulPair,
)

__all__ = [
    "CowGraph",
    "CowNode",
    "CowWrite",
    "EfficiencyResult",
    "EntityComparison",
    "EntityStateResult",
    "ExtraWrite",
    "MatchedWrite",
    "MissingWrite",
    "OpUtility",
    "ScoredGraph",
    "ScoredNode",
    "SessionScoringTerms",
    "WastefulPair",
    "FeedbackFn",
    "ScoreFn",
    "ScoringConfig",
    "default_feedback_fn",
    "default_score_fn",
    "f1",
    "precision",
    "recall",
    "CompositeComparator",
    "DatatypeComparator",
    "FieldCategory",
    "FieldComparisonResult",
    "FieldConfig",
    "WriteComparator",
    "WriteComparisonResult",
    "categorize_data_type",
    "SimilarityResult",
    "RowSimilarityComparator",
    "RowSimilarityFn",
    "from_row_similarity",
    "Executor",
    "extract_session_graph",
    "extract_session_writes",
    "get_table_column_types",
    "get_table_fk_columns",
    "get_table_pk_columns",
    "MatchResult",
    "collapse_to_final_state",
    "find_best_match",
    "get_created_uuids",
    "match_writes",
    "topological_sort_writes",
    "compute_efficiency",
    "compute_entity_state_score",
    "compute_op_utilities",
    "compute_operation_count_ratio",
    "detect_wasteful_pairs",
    "flatten_graph",
    "ScoringResult",
    "build_empty_field_config",
    "build_field_config",
    "score_cow_sessions",
    "score_sessions",
]
