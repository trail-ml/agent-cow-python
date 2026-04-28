"""
Top-level COW session scoring flow.

Iterate through agent ops in topological order, compute the cumulative struct/content scores at each step,
record the per-op delta, then layer in efficiency and any user-supplied
``score_fns``.
"""

from __future__ import annotations

from typing import Optional
from uuid import UUID

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
from .scores import content_score, efficiency, struct_score
from .types import (
    CHANGE_TABLE_RESERVED_FIELDS,
    CowWrite,
    ScoreFn,
    ScoringResult,
    TableMeta,
)


DEFAULT_IGNORED_FIELDS: set[str] = {
    *CHANGE_TABLE_RESERVED_FIELDS,
    "timestamp",
    "created_at",
    "updated_at",
    "deleted_at",
}


async def score_sessions(
    rows_gt: list[CowWrite],
    rows_a: list[CowWrite],
    table_meta: dict[str, TableMeta],
    *,
    score_fns: Optional[dict[str, ScoreFn]] = None,
    ignored_fields: Optional[set[str]] = None,
    collapse: bool = False,
    comparator: Optional[WriteComparator] = None,
    row_similarity: Optional[dict[str, RowSimilarityFn]] = None,
) -> ScoringResult:
    """Score an agent session against a ground-truth session.

    ``collapse=True`` drops rows from create-then-delete cycles before
    iteration, so the scorer evaluates net intent rather than transient
    activity. Op IDs are preserved on surviving rows.

    Customization (mutually exclusive — pass at most one):

    * ``comparator``: a :class:`WriteComparator` to use everywhere. Build a
      ``DatatypeComparator(table_comparators=...)`` directly when you need
      full control.
    * ``row_similarity``: convenience map of
      ``{table_name: (gt_row, agent_row) -> bool | float}`` callables. Each
      one is wrapped via :func:`from_row_similarity` and dropped into a
      :class:`DatatypeComparator` as a per-table override.
    """
    ignored = ignored_fields if ignored_fields is not None else DEFAULT_IGNORED_FIELDS
    comparator = _resolve_comparator(comparator, row_similarity)

    if collapse:
        rows_gt = drop_wasted_rows(rows_gt)
        rows_a = drop_wasted_rows(rows_a)

    op_ids_gt = get_session_operations(rows_gt)
    op_ids_a = get_session_operations(rows_a)

    rows_by_op: dict[UUID, list[CowWrite]] = {}
    for row in rows_a:
        rows_by_op.setdefault(row.operation_id, []).append(row)

    # Per-op structural utility = delta in cumulative struct score
    cur_struct = struct_score(rows_gt, [], table_meta, ignored, comparator=comparator)
    op_struct: dict[UUID, float] = {}
    applied: list[CowWrite] = []
    for op_id in op_ids_a:
        applied.extend(rows_by_op.get(op_id, []))
        new_struct = struct_score(
            rows_gt, applied, table_meta, ignored, comparator=comparator
        )
        op_struct[op_id] = new_struct - cur_struct
        cur_struct = new_struct

    matched, missing, extra = match_rows(
        rows_gt, rows_a, table_meta, ignored, comparator=comparator
    )
    matched_pks = {agent.get_pk_tuple() for _gt, agent, _sim in matched}
    wasted = get_wasted_ops(rows_a, matched_pks)
    eff = efficiency(op_ids_gt, op_ids_a, wasted)

    # Per-op content utility = mean similarity over matched rows whose
    # agent-side write originated in this op
    sims_by_op: dict[UUID, list[float]] = {}
    for _gt, agent, sim in matched:
        sims_by_op.setdefault(agent.operation_id, []).append(sim)
    op_content: dict[UUID, float] = {
        op_id: sum(sims) / len(sims) for op_id, sims in sims_by_op.items()
    }

    final_content = content_score(
        rows_gt, rows_a, table_meta, ignored, comparator=comparator
    )

    result = ScoringResult(
        struct_score=cur_struct,
        content_score=final_content,
        efficiency=eff,
        op_struct_scores=op_struct,
        op_content_scores=op_content,
        counts={
            "matched": len(matched),
            "missing": len(missing),
            "extra": len(extra),
            "gt_ops": len(op_ids_gt),
            "agent_ops": len(op_ids_a),
        },
    )

    for name, fn in (score_fns or {}).items():
        result.scores[name] = fn(result)
    return result


async def score_cow_sessions(
    executor: Executor,
    ground_truth_session_id: UUID,
    agent_session_id: UUID,
    schema: str,
    *,
    score_fns: Optional[dict[str, ScoreFn]] = None,
    ignored_fields: Optional[set[str]] = None,
    excluded_tables: Optional[set[str]] = None,
    collapse: bool = False,
    comparator: Optional[WriteComparator] = None,
    row_similarity: Optional[dict[str, RowSimilarityFn]] = None,
) -> ScoringResult:
    """Pull both sessions from Postgres, then score."""
    rows_gt = await get_rows_changed(
        executor, schema, ground_truth_session_id, excluded_tables=excluded_tables
    )
    rows_a = await get_rows_changed(
        executor, schema, agent_session_id, excluded_tables=excluded_tables
    )
    tables = sorted({r.table_name for r in rows_gt} | {r.table_name for r in rows_a})
    table_meta = await get_table_metadata(executor, schema, tables)

    return await score_sessions(
        rows_gt,
        rows_a,
        table_meta,
        score_fns=score_fns,
        ignored_fields=ignored_fields,
        collapse=collapse,
        comparator=comparator,
        row_similarity=row_similarity,
    )


def _resolve_comparator(
    comparator: Optional[WriteComparator],
    row_similarity: Optional[dict[str, RowSimilarityFn]],
) -> WriteComparator:
    if comparator is not None and row_similarity:
        raise ValueError("Pass `comparator` or `row_similarity`, not both.")
    if comparator is not None:
        return comparator
    if row_similarity:
        return DatatypeComparator(
            table_comparators={
                t: from_row_similarity(fn) for t, fn in row_similarity.items()
            }
        )
    return DatatypeComparator()
