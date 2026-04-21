"""
Top-level orchestration for graph-based COW session scoring.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional
from uuid import UUID

from .comparators import CompositeComparator, DatatypeComparator, FieldConfig, WriteComparator
from .efficiency import compute_efficiency
from .entity_state import compute_entity_state_score, flatten_graph
from .extraction import (
    Executor,
    extract_session_graph,
    get_table_column_types,
    get_table_fk_columns,
    get_table_pk_columns,
)
from .feedback import default_feedback_fn
from .matching import collapse_to_final_state
from .op_utility import compute_op_utilities
from .sample_scorers import default_score_fn
from .types import (
    CowGraph,
    CowNode,
    CowWrite,
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
    SessionScoringTerms,
)

logger = logging.getLogger(__name__)


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


async def build_field_config(
    executor: Executor,
    schema: str,
    tables: list[str],
    ignored_fields: Optional[set[str]] = None,
) -> FieldConfig:
    config = FieldConfig()
    if ignored_fields is not None:
        config.ignored_fields = set(ignored_fields)

    for table_name in tables:
        pk_cols = await get_table_pk_columns(executor, schema, table_name)
        fk_cols = await get_table_fk_columns(executor, schema, table_name)
        col_types = await get_table_column_types(executor, schema, table_name)
        config.set_table_metadata(
            table_name, set(pk_cols), fk_cols, column_types=col_types
        )
    return config


def build_empty_field_config(
    graphs: list[CowGraph], ignored_fields: Optional[set[str]] = None
) -> FieldConfig:
    config = FieldConfig()
    if ignored_fields is not None:
        config.ignored_fields = set(ignored_fields)
    tables: dict[str, set[str]] = {}
    for graph in graphs:
        for row in graph.all_rows():
            tables.setdefault(row.table_name, set()).update(row.primary_key.keys())
    for table_name, pk_cols in tables.items():
        config.set_table_metadata(table_name, pk_cols, set(), column_types={})
    return config


async def score_sessions(
    ground_truth: CowGraph,
    agent: CowGraph,
    *,
    config: Optional[ScoringConfig] = None,
    executor: Optional[Executor] = None,
    schema: Optional[str] = None,
    field_config: Optional[FieldConfig] = None,
) -> ScoringResult:
    config = config or ScoringConfig()
    comparator: WriteComparator = config.comparator or CompositeComparator(
        default=DatatypeComparator(match_threshold=config.match_threshold)
    )

    gt_graph, agent_graph = ground_truth, agent
    if config.collapse:
        gt_graph = _collapse_graph(gt_graph)
        agent_graph = _collapse_graph(agent_graph)

    if field_config is None:
        all_tables = sorted(
            {row.table_name for row in gt_graph.all_rows() + agent_graph.all_rows()}
        )
        if executor is not None and schema is not None and all_tables:
            field_config = await build_field_config(
                executor, schema, all_tables, ignored_fields=config.ignored_fields
            )
        else:
            field_config = build_empty_field_config(
                [gt_graph, agent_graph], ignored_fields=config.ignored_fields
            )
    elif config.ignored_fields is not None:
        field_config.ignored_fields = set(config.ignored_fields)

    entity_result = compute_entity_state_score(
        gt_graph, agent_graph, comparator, field_config
    )

    matched_agent_pks = _matched_agent_pks(entity_result)
    efficiency_result = compute_efficiency(gt_graph, agent_graph, matched_agent_pks)

    matched_writes, missing_writes, extra_writes = _split_entity_comparisons(
        entity_result, gt_graph, agent_graph, comparator, field_config
    )

    extra_by_op: dict[UUID, int] = {}
    for extra_write in extra_writes:
        extra_by_op[extra_write.agent.operation_id] = (
            extra_by_op.get(extra_write.agent.operation_id, 0) + 1
        )

    op_utilities = compute_op_utilities(
        gt_graph,
        agent_graph,
        comparator,
        field_config,
        matched_writes=matched_writes,
        extra_writes_by_op=extra_by_op,
    )

    terms = SessionScoringTerms(
        op_utilities=op_utilities,
        structural_score=entity_result.structural_score,
        content_score=entity_result.content_score,
        relationship_score=entity_result.relationship_score,
        efficiency=efficiency_result.efficiency,
        gt_operation_count=len(gt_graph.nodes),
        agent_operation_count=len(agent_graph.nodes),
        matched_row_count=len(matched_writes),
        missing_row_count=len(missing_writes),
        extra_row_count=len(extra_writes),
    )

    score_fns: dict[str, ScoreFn] = config.score_fns or {"overall": default_score_fn}
    scores = {name: fn(terms) for name, fn in score_fns.items()}

    scored_graph = _build_scored_graph(
        agent_graph, op_utilities, matched_writes, extra_writes
    )

    result = ScoringResult(
        scores=scores,
        feedback_report="",
        terms=terms,
        scored_graph=scored_graph,
        matched_writes=matched_writes,
        missing_writes=missing_writes,
        extra_writes=extra_writes,
        entity_state_comparisons=list(entity_result.comparisons),
    )
    feedback_fn: FeedbackFn = config.feedback_fn or default_feedback_fn
    result.feedback_report = feedback_fn(result)
    return result


async def score_cow_sessions(
    executor: Executor,
    ground_truth_session_id: UUID,
    agent_session_id: UUID,
    schema: str,
    *,
    config: Optional[ScoringConfig] = None,
    excluded_tables: Optional[set[str]] = None,
) -> ScoringResult:
    gt_graph = await extract_session_graph(
        executor, schema, ground_truth_session_id, excluded_tables=excluded_tables
    )
    agent_graph = await extract_session_graph(
        executor, schema, agent_session_id, excluded_tables=excluded_tables
    )
    return await score_sessions(
        ground_truth=gt_graph,
        agent=agent_graph,
        config=config,
        executor=executor,
        schema=schema,
    )


def _collapse_graph(graph: CowGraph) -> CowGraph:
    collapsed_rows = collapse_to_final_state(flatten_graph(graph))
    if not collapsed_rows:
        return CowGraph()
    first_op = collapsed_rows[0].operation_id
    timestamp = min(row.updated_at for row in collapsed_rows)
    node = CowNode(op_id=first_op, timestamp=timestamp, rows=collapsed_rows)
    return CowGraph(nodes={first_op: node})


def _matched_agent_pks(entity_result: EntityStateResult) -> set[tuple]:
    pks: set[tuple] = set()
    for comparison in entity_result.comparisons:
        if comparison.status != "matched":
            continue
        pks.add((comparison.table_name, tuple(sorted(comparison.primary_key.items()))))
    return pks


def _split_entity_comparisons(
    entity_result: EntityStateResult,
    gt_graph: CowGraph,
    agent_graph: CowGraph,
    comparator: WriteComparator,
    field_config: FieldConfig,
) -> tuple[list[MatchedWrite], list[MissingWrite], list[ExtraWrite]]:
    from .matching import match_writes

    gt_final = collapse_to_final_state(flatten_graph(gt_graph))
    agent_final = collapse_to_final_state(flatten_graph(agent_graph))

    _ = entity_result

    result = match_writes(gt_final, agent_final, comparator, field_config)
    return result.matched_writes, result.missing_writes, result.extra_writes


def _build_scored_graph(
    agent_graph: CowGraph,
    op_utilities: list[OpUtility],
    matched_writes: list[MatchedWrite],
    extra_writes: list[ExtraWrite],
) -> ScoredGraph:
    utility_by_op: dict[UUID, OpUtility] = {utility.op_id: utility for utility in op_utilities}

    matched_by_op: dict[UUID, list[MatchedWrite]] = {}
    for matched_write in matched_writes:
        matched_by_op.setdefault(matched_write.agent.operation_id, []).append(
            matched_write
        )

    extras_by_op: dict[UUID, list[CowWrite]] = {}
    for extra_write in extra_writes:
        extras_by_op.setdefault(extra_write.agent.operation_id, []).append(
            extra_write.agent
        )

    scored: dict[UUID, ScoredNode] = {}
    for node in agent_graph.topologically_sorted_nodes():
        utility = utility_by_op.get(node.op_id)
        scored[node.op_id] = ScoredNode(
            op_id=node.op_id,
            metadata=node.metadata,
            structural_utility=utility.structural_utility if utility else 0.0,
            structural_score_before=utility.structural_score_before if utility else 0.0,
            structural_score_after=utility.structural_score_after if utility else 0.0,
            content_utility=utility.content_utility if utility else 0.0,
            matched_rows=matched_by_op.get(node.op_id, []),
            extra_rows=extras_by_op.get(node.op_id, []),
        )
    return ScoredGraph(nodes=scored)
