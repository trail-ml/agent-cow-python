"""
Op-level utility: the journey-based scoring signals per agent op.
"""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from .comparators import FieldConfig, WriteComparator
from .entity_state import compute_entity_state_score
from .types import CowGraph, CowNode, CowWrite, MatchedWrite, OpUtility


def _graph_from_rows(rows: list[CowWrite]) -> CowGraph:
    graph = CowGraph()
    for row in rows:
        node = graph.nodes.get(row.operation_id)
        if node is None:
            node = CowNode(op_id=row.operation_id, timestamp=row.updated_at, rows=[])
            graph.nodes[row.operation_id] = node
        node.rows.append(row)
        if row.updated_at < node.timestamp:
            node.timestamp = row.updated_at
    return graph


def _content_by_op(
    matched_writes: list[MatchedWrite],
    extra_by_op: dict[UUID, int],
) -> dict[UUID, tuple[float, int]]:
    sums: dict[UUID, float] = {}
    counts: dict[UUID, int] = {}
    for matched_write in matched_writes:
        op_id = matched_write.agent.operation_id
        sums[op_id] = sums.get(op_id, 0.0) + matched_write.comparison.similarity
        counts[op_id] = counts.get(op_id, 0) + 1

    for op_id in extra_by_op:
        counts.setdefault(op_id, 0)

    out: dict[UUID, tuple[float, int]] = {}
    for op_id, count in counts.items():
        if count == 0:
            out[op_id] = (0.0, 0)
        else:
            out[op_id] = (sums[op_id] / count, count)
    return out


def compute_op_utilities(
    gt_graph: CowGraph,
    agent_graph: CowGraph,
    comparator: WriteComparator,
    field_config: FieldConfig,
    matched_writes: list[MatchedWrite],
    extra_writes_by_op: dict[UUID, int],
    seed_uuid_mapping: Optional[dict[UUID, UUID]] = None,
) -> list[OpUtility]:
    utilities: list[OpUtility] = []

    if not agent_graph.nodes:
        return utilities

    ordered_nodes = agent_graph.topologically_sorted_nodes()
    content_by_op = _content_by_op(matched_writes, extra_writes_by_op)

    cumulative_rows: list[CowWrite] = []
    prev_state_score: Optional[float] = None

    for node in ordered_nodes:
        if prev_state_score is None:
            before_graph = _graph_from_rows(list(cumulative_rows))
            before_result = compute_entity_state_score(
                gt_graph,
                before_graph,
                comparator,
                field_config,
                seed_uuid_mapping=seed_uuid_mapping,
            )
            structural_before = before_result.structural_score
        else:
            structural_before = prev_state_score

        cumulative_rows.extend(node.rows)

        after_graph = _graph_from_rows(list(cumulative_rows))
        after_result = compute_entity_state_score(
            gt_graph,
            after_graph,
            comparator,
            field_config,
            seed_uuid_mapping=seed_uuid_mapping,
        )
        structural_after = after_result.structural_score
        prev_state_score = structural_after

        content_utility, matched_count = content_by_op.get(node.op_id, (0.0, 0))
        extra_count = extra_writes_by_op.get(node.op_id, 0)

        utilities.append(
            OpUtility(
                op_id=node.op_id,
                metadata=node.metadata,
                structural_utility=structural_after - structural_before,
                structural_score_before=structural_before,
                structural_score_after=structural_after,
                content_utility=content_utility,
                matched_rows_contributed=matched_count,
                extra_rows_contributed=extra_count,
            )
        )

    return utilities
