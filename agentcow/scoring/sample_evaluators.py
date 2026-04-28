"""
Sample :data:`ScoreFn` and :data:`RowSimilarityFn` implementations.

Two flavours of evaluator live here:

* **ScoreFns** — ``(ScoringResult) -> float``. Pass through ``score_fns=...``
  on ``score_sessions`` / ``score_cow_sessions`` to get the metric on
  ``result.scores``.
* **RowSimilarityFns** — ``(gt_row, agent_row) -> bool | float``. Pass
  through ``row_similarity={"table": fn, ...}`` to override the default
  comparator on a per-table basis.
"""

from __future__ import annotations

from typing import Any

from .types import ScoringResult


def precision(result: ScoringResult) -> float:
    matched = result.counts["matched"]
    extra = result.counts["extra"]
    denom = matched + extra
    return matched / denom if denom else 1.0


def recall(result: ScoringResult) -> float:
    matched = result.counts["matched"]
    missing = result.counts["missing"]
    denom = matched + missing
    return matched / denom if denom else 1.0


def f1(result: ScoringResult) -> float:
    p = precision(result)
    r = recall(result)
    return (2 * p * r / (p + r)) if (p + r) else 0.0


def default_score_fn(result: ScoringResult) -> float:
    return (
        result.struct_score * 0.5
        + result.content_score * 0.3
        + result.efficiency * 0.2
    )


def name_match(gt: dict, agent: dict) -> bool:
    """Match rows whose ``name`` field is identical."""
    return gt.get("name") == agent.get("name")


def assertion_name_match(gt: dict, agent: dict) -> bool:
    """Same as :func:`name_match`, but assertion-style.

    ``from_row_similarity`` catches ``AssertionError`` and treats it as a
    mismatch, so any test-style helper drops in unchanged.
    """
    assert gt.get("name") == agent.get("name"), (
        f"name differs: {gt.get('name')!r} vs {agent.get('name')!r}"
    )
    return True


def metadata_priority_match(gt: dict, agent: dict) -> float:
    """Compare two rows by a single key inside a JSON ``metadata`` column.

    Useful when the table has a ``jsonb metadata`` column whose schema is
    flexible but you only care about one field (e.g. ``priority``) lining up.
    Returns 1.0 on match, 0.5 if both sides have a non-null value but they
    differ, 0.0 otherwise — illustrates that a ``RowSimilarityFn`` can return
    a graded float, not just a bool.
    """
    gt_meta: Any = gt.get("metadata") or {}
    agent_meta: Any = agent.get("metadata") or {}
    if not isinstance(gt_meta, dict) or not isinstance(agent_meta, dict):
        return 0.0

    gt_priority = gt_meta.get("priority")
    agent_priority = agent_meta.get("priority")
    if gt_priority == agent_priority:
        return 1.0
    if gt_priority is not None and agent_priority is not None:
        return 0.5
    return 0.0
