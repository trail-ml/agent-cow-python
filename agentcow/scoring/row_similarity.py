"""
Row-level similarity hook for COW scoring.

The lightweight customization seam: override *how similar* two rows are for a
given table with a plain ``(gt_row, agent_row) -> bool | float`` function,
without touching :class:`WriteComparator`, :class:`FieldConfig`, UUID mapping,
or any of the scorer's internals. ``bool`` is the degenerate case where a row
is either fully similar (1.0) or not (0.0); ``float`` is graded similarity in
``[0, 1]``.

:class:`RowSimilarityComparator` routes the user callable into the full scoring
pipeline. The adapter:

* Remaps agent-side foreign-key UUIDs back into GT space before the user
  function runs, so direct ``==`` on FK columns "just works".
* Treats an ``AssertionError`` raised inside the user function as a mismatch
  whose feedback is the assertion message. That makes assertion-style helpers
  drop in unchanged.
* Accepts ``bool``, ``float``, or :class:`SimilarityResult` as a return value
  and builds a full :class:`WriteComparisonResult` from it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Union
from uuid import UUID

from .comparators import (
    FieldComparisonResult,
    FieldConfig,
    WriteComparisonResult,
    _build_overall_feedback,
)
from .types import CowWrite

RowData = Mapping[str, Any]

SYNTHETIC_FIELD_NAME = "__row_similarity__"


@dataclass
class SimilarityResult:
    """Structured return value for :data:`RowSimilarityFn`.

    Use this when you want partial-credit similarity, custom feedback, or a
    per-field breakdown. For the common case, return a plain ``bool`` or
    ``float`` instead.
    """

    similarity: float = 1.0
    match: Optional[bool] = None
    feedback: Optional[str] = None
    field_results: Optional[list[FieldComparisonResult]] = None


RowSimilarityReturn = Union[bool, float, SimilarityResult]
RowSimilarityFn = Callable[[RowData, RowData], RowSimilarityReturn]


class RowSimilarityComparator:
    """Adapt a simple ``(gt_row, agent_row)`` callable into a ``WriteComparator``."""

    def __init__(
        self,
        fn: RowSimilarityFn,
        *,
        match_threshold: float = 0.8,
        remap_foreign_keys: bool = True,
    ) -> None:
        self.fn = fn
        self.match_threshold = match_threshold
        self.remap_foreign_keys = remap_foreign_keys

    def compare(
        self,
        ground_truth: CowWrite,
        agent: CowWrite,
        uuid_mapping: dict[UUID, UUID],
        field_config: FieldConfig,
        gt_created_uuids: set[UUID],
        agent_created_uuids: set[UUID],
    ) -> WriteComparisonResult:
        if self.remap_foreign_keys:
            agent_view = _remap_agent_fks(
                agent.data, uuid_mapping, field_config, ground_truth.table_name
            )
        else:
            agent_view = dict(agent.data)

        gt_view = dict(ground_truth.data)

        assertion_feedback: Optional[str] = None
        try:
            raw: RowSimilarityReturn = self.fn(gt_view, agent_view)
        except AssertionError as exc:
            raw = False
            assertion_feedback = str(exc) or "assertion failed"

        return _normalize_similarity_result(
            raw,
            ground_truth,
            agent,
            self.match_threshold,
            assertion_feedback,
        )


def from_row_similarity(
    fn: RowSimilarityFn,
    *,
    match_threshold: float = 0.8,
    remap_foreign_keys: bool = True,
) -> RowSimilarityComparator:
    """Wrap a ``(gt_row, agent_row) -> bool | float | SimilarityResult`` callable.

    Example::

        def issues_similar(gt, agent) -> bool:
            return (
                gt["name"] == agent["name"]
                and gt["state_id"] == agent["state_id"]
            )

        ScoringConfig(row_similarity={"issues": issues_similar})

    Assertion-style helpers also work directly, since ``AssertionError``
    is caught and converted into mismatch feedback::

        def issues_similar(gt, agent):
            assert gt["name"] == agent["name"], "name differs"
            assert gt["state_id"] == agent["state_id"], "state differs"
            return True
    """
    return RowSimilarityComparator(
        fn,
        match_threshold=match_threshold,
        remap_foreign_keys=remap_foreign_keys,
    )


def _remap_agent_fks(
    agent_data: RowData,
    uuid_mapping: dict[UUID, UUID],
    field_config: FieldConfig,
    table_name: str,
) -> dict[str, Any]:
    remapped: dict[str, Any] = dict(agent_data)
    if not uuid_mapping:
        return remapped

    reverse = {agent_id: gt_id for gt_id, agent_id in uuid_mapping.items()}

    for field_name, value in agent_data.items():
        if not field_config.is_foreign_key(table_name, field_name):
            continue
        uuid_val = _try_uuid(value)
        if uuid_val is not None and uuid_val in reverse:
            remapped[field_name] = reverse[uuid_val]
    return remapped


def _try_uuid(value: Any) -> Optional[UUID]:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except (ValueError, AttributeError):
            return None
    return None


def _normalize_similarity_result(
    raw: RowSimilarityReturn,
    ground_truth: CowWrite,
    agent: CowWrite,
    match_threshold: float,
    assertion_feedback: Optional[str],
) -> WriteComparisonResult:
    table_name = ground_truth.table_name

    if isinstance(raw, SimilarityResult):
        similarity = _clamp(raw.similarity)
        is_match = (
            raw.match if raw.match is not None else similarity >= match_threshold
        )
        field_results = raw.field_results or _synthesize_field_results(
            ground_truth, agent, similarity, is_match, raw.feedback
        )
        feedback = raw.feedback or _build_overall_feedback(
            table_name, field_results, is_match
        )
        return WriteComparisonResult(
            is_match=is_match,
            field_results=field_results,
            overall_feedback=feedback,
            similarity=similarity,
        )

    if isinstance(raw, bool):
        similarity = 1.0 if raw else 0.0
        is_match = raw
    else:
        similarity = _clamp(float(raw))
        is_match = similarity >= match_threshold

    field_results = _synthesize_field_results(
        ground_truth, agent, similarity, is_match, assertion_feedback
    )
    feedback = assertion_feedback or _build_overall_feedback(
        table_name, field_results, is_match
    )
    return WriteComparisonResult(
        is_match=is_match,
        field_results=field_results,
        overall_feedback=feedback,
        similarity=similarity,
    )


def _synthesize_field_results(
    ground_truth: CowWrite,
    agent: CowWrite,
    similarity: float,
    is_match: bool,
    feedback_override: Optional[str],
) -> list[FieldComparisonResult]:
    if feedback_override:
        feedback = feedback_override
    elif is_match:
        feedback = f"{ground_truth.table_name}: custom similarity matched"
    else:
        feedback = f"{ground_truth.table_name}: custom similarity mismatch"

    return [
        FieldComparisonResult(
            field_name=SYNTHETIC_FIELD_NAME,
            matches=is_match,
            ground_truth_value=dict(ground_truth.data),
            agent_value=dict(agent.data),
            feedback=feedback,
            similarity=similarity,
        )
    ]


def _clamp(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value
