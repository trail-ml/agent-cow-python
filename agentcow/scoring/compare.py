"""
Row comparators for COW session scoring.

Two pieces of public API:

* :class:`WriteComparator` — Protocol every comparator implements.
* :class:`DatatypeComparator` — the default. Skips PKs/timestamps/ignored
  fields, pre-remaps FK UUIDs into ground truth space, then dispatches each remaining
  field through ``_TYPE_COMPARATORS`` based on its Postgres ``data_type``.
  Accepts an optional ``table_comparators={table_name: WriteComparator}``
  map so callers can override the rule for one table without touching the
  others — the override runs first, datatype dispatch is the fallback.
"""

from __future__ import annotations

import json
from difflib import SequenceMatcher
from typing import Any, Callable, Protocol, runtime_checkable
from uuid import UUID

from .types import CowWrite, TableMeta


ValueComparator = Callable[[Any, Any], float]
RowSimilarityFn = Callable[[dict, dict], "bool | float"]


@runtime_checkable
class WriteComparator(Protocol):
    """Protocol for anything that can score a (gt, agent) row pair in [0, 1]."""

    def compare(
        self,
        gt: CowWrite,
        agent: CowWrite,
        table_meta: TableMeta,
        uuid_mapping: dict[UUID, UUID],
        ignored_fields: set[str],
    ) -> float: ...


def _compare_text(gt: Any, agent: Any) -> float:
    return SequenceMatcher(None, str(gt), str(agent)).ratio()


def _compare_json(gt: Any, agent: Any) -> float:
    return (
        1.0
        if json.dumps(gt, sort_keys=True) == json.dumps(agent, sort_keys=True)
        else 0.0
    )


def _compare_exact(gt: Any, agent: Any) -> float:
    return 1.0 if gt == agent else 0.0


_TYPE_COMPARATORS: dict[str, ValueComparator] = {
    "text": _compare_text,
    "varchar": _compare_text,
    "character varying": _compare_text,
    "citext": _compare_text,
    "character": _compare_text,
    "json": _compare_json,
    "jsonb": _compare_json,
}

_TIMESTAMP_TYPES = {
    "timestamp",
    "timestamp without time zone",
    "timestamp with time zone",
    "date",
    "time",
}


class DatatypeComparator:
    """Default per-row comparator with optional per-table overrides.

    If ``table_comparators`` is provided and the row's ``table_name`` matches
    a key, delegates to that comparator and skips the built-in logic.
    Otherwise: PKs, timestamps, and ``ignored_fields`` are skipped, FK fields
    are pre-remapped into ground truth UUID space (so that ground truth and agent rows referring
    to the same matched entity compare equal), and each remaining field is
    dispatched through ``_TYPE_COMPARATORS`` based on its Postgres
    ``data_type`` (anything not in the dict falls back to exact equality).
    """

    def __init__(
        self,
        table_comparators: dict[str, WriteComparator] | None = None,
    ) -> None:
        self.table_comparators: dict[str, WriteComparator] = dict(
            table_comparators or {}
        )

    def compare(
        self,
        gt: CowWrite,
        agent: CowWrite,
        table_meta: TableMeta,
        uuid_mapping: dict[UUID, UUID],
        ignored_fields: set[str],
    ) -> float:
        override = self.table_comparators.get(gt.table_name)
        if override is not None:
            return override.compare(gt, agent, table_meta, uuid_mapping, ignored_fields)

        reverse_uuid_mapping = {
            agent_id: gt_id for gt_id, agent_id in uuid_mapping.items()
        }
        all_fields = set(gt.data.keys()) | set(agent.data.keys())

        sims: list[float] = []
        for field_name in all_fields:
            if field_name in ignored_fields:
                continue
            if field_name in table_meta.pk_columns:
                continue

            data_type = (table_meta.column_types.get(field_name) or "").lower()
            if (
                data_type in _TIMESTAMP_TYPES
                or field_name == "timestamp"
                or field_name.endswith("_timestamp")
            ):
                continue

            gt_value = gt.data.get(field_name)
            agent_value = agent.data.get(field_name)

            if (
                field_name in table_meta.fk_columns
                and agent_value in reverse_uuid_mapping
            ):
                agent_value = reverse_uuid_mapping[agent_value]

            if gt_value is None and agent_value is None:
                sims.append(1.0)
                continue
            if gt_value is None or agent_value is None:
                sims.append(0.0)
                continue

            comparator = _TYPE_COMPARATORS.get(data_type, _compare_exact)
            sims.append(comparator(gt_value, agent_value))

        if not sims:
            return 1.0
        return sum(sims) / len(sims)


class _RowSimilarityAdapter:
    """Wraps a :data:`RowSimilarityFn` so it satisfies :class:`WriteComparator`.

    Pre-remaps agent FK UUIDs into ground truth space so direct ``==`` on FK columns
    works inside the user's callable. ``AssertionError`` raised from the
    callable is treated as ``0.0`` so assertion-style helpers drop in
    unchanged.
    """

    def __init__(self, fn: RowSimilarityFn) -> None:
        self.fn = fn

    def compare(
        self,
        gt: CowWrite,
        agent: CowWrite,
        table_meta: TableMeta,
        uuid_mapping: dict[UUID, UUID],
        ignored_fields: set[str],
    ) -> float:
        reverse = {a: g for g, a in uuid_mapping.items()}
        agent_view = {
            k: reverse.get(v, v) if k in table_meta.fk_columns else v
            for k, v in agent.data.items()
        }
        try:
            result = self.fn(dict(gt.data), agent_view)
        except AssertionError:
            return 0.0
        if isinstance(result, bool):
            return 1.0 if result else 0.0
        return max(0.0, min(1.0, float(result)))


def from_row_similarity(fn: RowSimilarityFn) -> WriteComparator:
    """Adapt a ``(gt_row, agent_row) -> bool | float`` into a ``WriteComparator``.

    The two ``dict`` arguments are the row's ``data`` payload (column ->
    value). Agent FK UUIDs have already been remapped into ground truth space when
    a mapping exists. Return values:

    * ``True`` / ``False`` — interpreted as ``1.0`` / ``0.0``.
    * ``float`` — clamped to ``[0, 1]``.
    * raise ``AssertionError`` — interpreted as ``0.0`` (lets you reuse
      assertion-style helpers).
    """
    return _RowSimilarityAdapter(fn)
