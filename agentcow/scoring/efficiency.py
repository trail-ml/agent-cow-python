"""
Efficiency scoring: penalize doing more work than necessary.
"""

from __future__ import annotations

from collections import defaultdict

from .types import CowGraph, CowWrite, EfficiencyResult, WastefulPair


def compute_operation_count_ratio(gt_graph: CowGraph, agent_graph: CowGraph) -> float:
    """Return ``min(1, gt_ops / agent_ops)`` with sane handling of zero ops."""
    agent_ops = len(agent_graph.nodes)
    if agent_ops == 0:
        return 1.0
    gt_ops = len(gt_graph.nodes)
    return min(1.0, gt_ops / agent_ops)


def detect_wasteful_pairs(
    agent_graph: CowGraph,
    matched_agent_pks: set[tuple],
) -> list[WastefulPair]:
    """Find unmatched create/delete pairs within the agent session."""
    by_pk: dict[tuple, list[CowWrite]] = defaultdict(list)
    for row in agent_graph.all_rows():
        by_pk[row.get_pk_tuple()].append(row)

    wasteful: list[WastefulPair] = []
    for pk_tuple, rows in by_pk.items():
        if pk_tuple in matched_agent_pks:
            continue
        rows.sort(key=lambda r: r.updated_at)

        create_row = next((r for r in rows if not r.is_delete), None)
        delete_row = next((r for r in reversed(rows) if r.is_delete), None)
        if create_row is None or delete_row is None:
            continue
        if delete_row.updated_at < create_row.updated_at:
            continue
        if create_row.operation_id == delete_row.operation_id:
            continue

        wasteful.append(
            WastefulPair(
                table_name=create_row.table_name,
                primary_key=dict(create_row.primary_key),
                create_op_id=create_row.operation_id,
                delete_op_id=delete_row.operation_id,
            )
        )
    return wasteful


def compute_efficiency(
    gt_graph: CowGraph,
    agent_graph: CowGraph,
    matched_agent_pks: set[tuple],
) -> EfficiencyResult:
    """Combine op-count ratio and waste detection into a single score."""
    op_count_ratio = compute_operation_count_ratio(gt_graph, agent_graph)
    wasteful = detect_wasteful_pairs(agent_graph, matched_agent_pks)

    agent_ops = max(len(agent_graph.nodes), 1)
    waste_ratio = len(wasteful) / agent_ops
    efficiency = op_count_ratio * max(0.0, 1.0 - waste_ratio)

    return EfficiencyResult(
        efficiency=efficiency,
        op_count_ratio=op_count_ratio,
        waste_ratio=waste_ratio,
        wasteful_pairs=wasteful,
    )
