"""
Row matching helpers for COW session scoring.

* :func:`match_rows` — pair ground truth entities with their best agent counterpart.
  Internally runs two passes: a first match with no UUID mapping, then a
  second pass using the mapping derived from the first match's pairings, so
  FK comparisons between session-created entities work end-to-end.
* :func:`get_wasted_ops` — agent ops whose only contribution was unmatched
  create-then-delete pairs.
* :func:`drop_wasted_rows` — remove create-then-delete cycles, used by
  ``collapse=True``.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Optional
from uuid import UUID

from .compare import DatatypeComparator, WriteComparator
from .types import CowWrite, TableMeta


def match_rows(
    rows_gt: list[CowWrite],
    rows_a: list[CowWrite],
    table_meta: dict[str, TableMeta],
    ignored_fields: set[str],
    *,
    comparator: Optional[WriteComparator] = None,
) -> tuple[
    list[tuple[CowWrite, CowWrite, float]],
    list[CowWrite],
    list[CowWrite],
]:
    """Pair ground truth entities with their best agent counterpart.

    Both sides are first reduced to one row per primary key (last write wins).
    Then each ground truth entity greedily picks its best unused agent candidate from
    the same table with matching ``is_delete``. We run the greedy pass twice:
    the first builds a UUID mapping from the PK pairings it finds, the second
    re-matches with that mapping so FK columns line up.

    ``comparator`` defaults to :class:`DatatypeComparator`.

    Returns ``(matched, missing, extra)``:

    * ``matched``: list of ``(gt, agent, similarity)`` tuples
    * ``missing``: ground truth entities with no agent counterpart
    * ``extra``: agent entities not paired to any ground truth entity
    """
    comparator = comparator or DatatypeComparator()
    gt_final = _last_write_per_pk(rows_gt)
    agent_final = _last_write_per_pk(rows_a)

    matched, _, _ = _greedy_match(
        gt_final, agent_final, table_meta, ignored_fields, {}, comparator
    )
    uuid_mapping = _map_uuids(matched)
    return _greedy_match(
        gt_final, agent_final, table_meta, ignored_fields, uuid_mapping, comparator
    )


def get_wasted_ops(
    rows_a: list[CowWrite],
    matched_pks: set[tuple],
) -> list[UUID]:
    """Return agent op IDs whose only effect was an unmatched create+delete cycle.

    An op is wasted if every entity it touched was created and later deleted in
    the same session without ever matching a ground truth row.
    """
    wasted_pks = _redundant_pks(rows_a) - matched_pks

    op_to_pks: dict[UUID, set[tuple]] = defaultdict(set)
    for row in rows_a:
        op_to_pks[row.operation_id].add(row.get_pk_tuple())

    return [op_id for op_id, pks in op_to_pks.items() if pks and pks <= wasted_pks]


def drop_wasted_rows(rows: list[CowWrite]) -> list[CowWrite]:
    """Remove rows belonging to create-then-delete cycles within the session.

    Used by ``collapse=True`` to evaluate net intent rather than transient
    activity. Op IDs are preserved on the surviving rows.
    """
    cancelled = _redundant_pks(rows)
    return [r for r in rows if r.get_pk_tuple() not in cancelled]


def _redundant_pks(rows: list[CowWrite]) -> set[tuple]:
    """PKs of entities created and later deleted within these rows."""
    grouped: dict[tuple, list[CowWrite]] = defaultdict(list)
    for row in rows:
        grouped[row.get_pk_tuple()].append(row)

    cancelled: set[tuple] = set()
    for pk_tuple, entries in grouped.items():
        entries.sort(key=lambda r: r.updated_at)
        has_create = any(not r.is_delete for r in entries)
        ends_deleted = entries[-1].is_delete
        if has_create and ends_deleted:
            cancelled.add(pk_tuple)
    return cancelled


def _greedy_match(
    gt_final: list[CowWrite],
    agent_final: list[CowWrite],
    table_meta: dict[str, TableMeta],
    ignored_fields: set[str],
    uuid_mapping: dict[UUID, UUID],
    comparator: WriteComparator,
) -> tuple[
    list[tuple[CowWrite, CowWrite, float]],
    list[CowWrite],
    list[CowWrite],
]:
    agent_by_table: dict[str, list[CowWrite]] = defaultdict(list)
    for agent in agent_final:
        agent_by_table[agent.table_name].append(agent)

    used_agent_keys: set[tuple] = set()
    matched: list[tuple[CowWrite, CowWrite, float]] = []
    missing: list[CowWrite] = []

    for gt in gt_final:
        meta = table_meta.get(gt.table_name) or TableMeta()
        best_agent: Optional[CowWrite] = None
        best_sim = 0.0

        for candidate in agent_by_table.get(gt.table_name, []):
            if candidate.get_pk_tuple() in used_agent_keys:
                continue
            if candidate.is_delete != gt.is_delete:
                continue

            sim = comparator.compare(gt, candidate, meta, uuid_mapping, ignored_fields)
            if best_agent is None or sim > best_sim:
                best_sim = sim
                best_agent = candidate

        if best_agent is None:
            missing.append(gt)
        else:
            matched.append((gt, best_agent, best_sim))
            used_agent_keys.add(best_agent.get_pk_tuple())

    extra = [a for a in agent_final if a.get_pk_tuple() not in used_agent_keys]
    return matched, missing, extra


def _last_write_per_pk(rows: list[CowWrite]) -> list[CowWrite]:
    """Reduce to one row per ``(table, pk)`` — the latest write."""
    latest: dict[tuple, CowWrite] = {}
    for row in rows:
        existing = latest.get(row.get_pk_tuple())
        if existing is None or row.updated_at >= existing.updated_at:
            latest[row.get_pk_tuple()] = row
    return list(latest.values())


def _map_uuids(
    matched: list[tuple[CowWrite, CowWrite, float]],
) -> dict[UUID, UUID]:
    """Build ``{gt_uuid -> agent_uuid}`` from matched primary-key pairs."""
    mapping: dict[UUID, UUID] = {}
    for gt, agent, _sim in matched:
        for column, gt_value in gt.primary_key.items():
            agent_value = agent.primary_key.get(column)
            if isinstance(gt_value, UUID) and isinstance(agent_value, UUID):
                mapping[gt_value] = agent_value
    return mapping
