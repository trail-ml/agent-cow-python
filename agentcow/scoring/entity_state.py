"""
Entity state score: collapse each graph to final entity state, then compare.
"""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from .comparators import FieldConfig, WriteComparator
from .matching import collapse_to_final_state, match_writes
from .types import (
    CowGraph,
    CowWrite,
    EntityComparison,
    EntityStateResult,
    MatchedWrite,
)


def flatten_graph(graph: CowGraph) -> list[CowWrite]:
    """Flatten a :class:`CowGraph` into rows ordered by ``updated_at``."""
    rows = graph.all_rows()
    rows.sort(key=lambda r: r.updated_at)
    return rows


def compute_relationship_score(
    matched_writes: list[MatchedWrite],
    uuid_mapping: dict[UUID, UUID],
    field_config: FieldConfig,
) -> tuple[float, int, int]:
    total = 0
    preserved = 0

    for matched_write in matched_writes:
        gt = matched_write.ground_truth
        agent = matched_write.agent
        for field_name, gt_value in gt.data.items():
            if not field_config.is_foreign_key(gt.table_name, field_name):
                continue
            agent_value = agent.data.get(field_name)
            total += 1
            if gt_value is None and agent_value is None:
                preserved += 1
                continue
            if gt_value is None or agent_value is None:
                continue
            if isinstance(gt_value, UUID) and gt_value in uuid_mapping:
                if uuid_mapping[gt_value] == agent_value:
                    preserved += 1
                continue
            if gt_value == agent_value:
                preserved += 1

    if total == 0:
        return 1.0, 0, 0
    return preserved / total, total, preserved


def compute_entity_state_score(
    gt_graph: CowGraph,
    agent_graph: CowGraph,
    comparator: WriteComparator,
    field_config: FieldConfig,
    seed_uuid_mapping: Optional[dict[UUID, UUID]] = None,
) -> EntityStateResult:
    gt_final = collapse_to_final_state(flatten_graph(gt_graph))
    agent_final = collapse_to_final_state(flatten_graph(agent_graph))

    if not gt_final and not agent_final:
        return EntityStateResult(
            structural_score=1.0,
            content_score=1.0,
            relationship_score=1.0,
            matched_count=0,
            missing_count=0,
            extra_count=0,
        )

    match_result = match_writes(
        gt_final, agent_final, comparator, field_config, seed_uuid_mapping
    )

    matched = len(match_result.matched_writes)
    missing = len(match_result.missing_writes)
    extra = len(match_result.extra_writes)
    total = matched + missing + extra

    structural_score = 1.0 if total == 0 else matched / total
    if matched == 0:
        content_score = 1.0 if total == 0 else 0.0
    else:
        content_score = (
            sum(m.comparison.similarity for m in match_result.matched_writes) / matched
        )

    relationship_score, _total_links, _preserved_links = compute_relationship_score(
        match_result.matched_writes, match_result.uuid_mapping, field_config
    )

    comparisons: list[EntityComparison] = []
    for matched_write in match_result.matched_writes:
        comparisons.append(
            EntityComparison(
                table_name=matched_write.ground_truth.table_name,
                primary_key=dict(matched_write.ground_truth.primary_key),
                status="matched",
                similarity=matched_write.comparison.similarity,
                feedback=matched_write.comparison.overall_feedback,
                field_results=list(matched_write.comparison.field_results),
            )
        )
    for miss in match_result.missing_writes:
        comparisons.append(
            EntityComparison(
                table_name=miss.ground_truth.table_name,
                primary_key=dict(miss.ground_truth.primary_key),
                status="missing",
                similarity=0.0,
                feedback=miss.feedback,
            )
        )
    for extra_write in match_result.extra_writes:
        comparisons.append(
            EntityComparison(
                table_name=extra_write.agent.table_name,
                primary_key=dict(extra_write.agent.primary_key),
                status="extra",
                similarity=0.0,
                feedback=extra_write.feedback,
            )
        )

    return EntityStateResult(
        structural_score=structural_score,
        content_score=content_score,
        relationship_score=relationship_score,
        matched_count=matched,
        missing_count=missing,
        extra_count=extra,
        comparisons=comparisons,
    )
