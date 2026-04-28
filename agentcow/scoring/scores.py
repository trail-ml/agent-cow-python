"""
Score calculations for COW session scoring:

* :func:`struct_score` — fraction of the union of GT/agent entities that matched
* :func:`content_score` — mean field-level similarity across matched rows
* :func:`efficiency` — ``min(1, gt_ops/a_ops) * (1 - wasted_ops/a_ops)``
"""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from .compare import WriteComparator
from .matching import match_rows
from .types import CowWrite, TableMeta


def struct_score(
    rows_gt: list[CowWrite],
    rows_a: list[CowWrite],
    table_meta: dict[str, TableMeta],
    ignored_fields: set[str],
    *,
    comparator: Optional[WriteComparator] = None,
) -> float:
    """Fraction of (matched + missing + extra) that are matched."""
    matched, missing, extra = match_rows(
        rows_gt, rows_a, table_meta, ignored_fields, comparator=comparator
    )
    total = len(matched) + len(missing) + len(extra)
    if total == 0:
        return 1.0
    return len(matched) / total


def content_score(
    rows_gt: list[CowWrite],
    rows_a: list[CowWrite],
    table_meta: dict[str, TableMeta],
    ignored_fields: set[str],
    *,
    comparator: Optional[WriteComparator] = None,
) -> float:
    """Mean per-field similarity across matched rows."""
    if not rows_gt and not rows_a:
        return 1.0

    matched, _missing, _extra = match_rows(
        rows_gt, rows_a, table_meta, ignored_fields, comparator=comparator
    )
    if not matched:
        return 0.0
    return sum(sim for _gt, _a, sim in matched) / len(matched)


def efficiency(
    op_ids_gt: list[UUID],
    op_ids_a: list[UUID],
    wasted_ops: list[UUID],
) -> float:
    """``min(1, gt/a) * (1 - wasted/a)``."""
    a = len(op_ids_a)
    if a == 0:
        return 1.0
    gt = len(op_ids_gt)
    op_ratio = min(1.0, gt / a)
    waste_ratio = len(wasted_ops) / a
    return op_ratio * max(0.0, 1.0 - waste_ratio)
