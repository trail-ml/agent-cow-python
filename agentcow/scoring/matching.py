"""
Row-level matching between GT and agent writes.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
from uuid import UUID

from .comparators import FieldConfig, WriteComparator, WriteComparisonResult
from .types import CowWrite, ExtraWrite, MatchedWrite, MissingWrite


DEFAULT_EXACT_MATCH_THRESHOLD = 0.9999


@dataclass
class MatchResult:
    matched_writes: list[MatchedWrite] = field(default_factory=list)
    missing_writes: list[MissingWrite] = field(default_factory=list)
    extra_writes: list[ExtraWrite] = field(default_factory=list)
    uuid_mapping: dict[UUID, UUID] = field(default_factory=dict)


def collapse_to_final_state(writes: list[CowWrite]) -> list[CowWrite]:
    """Keep only the last write per entity; drop create-then-delete pairs."""
    grouped: dict[tuple, list[CowWrite]] = defaultdict(list)
    for write in writes:
        grouped[write.get_pk_tuple()].append(write)

    final: list[CowWrite] = []
    for rows in grouped.values():
        rows.sort(key=lambda r: r.updated_at)
        last = rows[-1]
        has_create = any(not row.is_delete for row in rows)
        if last.is_delete and has_create:
            continue
        final.append(last)
    return final


def get_created_uuids(writes: list[CowWrite]) -> set[UUID]:
    created: set[UUID] = set()
    for write in writes:
        for value in write.primary_key.values():
            if isinstance(value, UUID):
                created.add(value)
            elif isinstance(value, str):
                try:
                    created.add(UUID(value))
                except (ValueError, AttributeError):
                    pass
    return created


def topological_sort_writes(writes: list[CowWrite]) -> list[CowWrite]:
    return sorted(writes, key=lambda write: write.updated_at)


def find_best_match(
    gt: CowWrite,
    agent_candidates: list[CowWrite],
    comparator: WriteComparator,
    uuid_mapping: dict[UUID, UUID],
    field_config: FieldConfig,
    gt_created_uuids: set[UUID],
    agent_created_uuids: set[UUID],
    used_agent_keys: set[tuple],
    exact_match_threshold: float = DEFAULT_EXACT_MATCH_THRESHOLD,
) -> tuple[Optional[CowWrite], Optional[WriteComparisonResult]]:
    best_agent: Optional[CowWrite] = None
    best_result: Optional[WriteComparisonResult] = None
    best_similarity = -1.0

    for candidate in agent_candidates:
        if candidate.get_pk_tuple() in used_agent_keys:
            continue
        if candidate.table_name != gt.table_name:
            continue
        if candidate.is_delete != gt.is_delete:
            continue

        result = comparator.compare(
            gt,
            candidate,
            uuid_mapping,
            field_config,
            gt_created_uuids,
            agent_created_uuids,
        )

        if result.similarity > best_similarity:
            best_similarity = result.similarity
            best_agent = candidate
            best_result = result
            if result.similarity >= exact_match_threshold:
                break

    return best_agent, best_result


def _run_exact_match_pass(
    gt_writes: list[CowWrite],
    agent_writes: list[CowWrite],
    comparator: WriteComparator,
    field_config: FieldConfig,
    uuid_mapping: dict[UUID, UUID],
    exact_match_threshold: float,
) -> tuple[dict[tuple, tuple[CowWrite, WriteComparisonResult]], set[tuple]]:
    gt_created = get_created_uuids(gt_writes)
    agent_created = get_created_uuids(agent_writes)

    agent_by_table: dict[str, list[CowWrite]] = defaultdict(list)
    for agent_write in agent_writes:
        agent_by_table[agent_write.table_name].append(agent_write)

    matches: dict[tuple, tuple[CowWrite, WriteComparisonResult]] = {}
    used_agent_keys: set[tuple] = set()

    for gt in gt_writes:
        candidates = agent_by_table.get(gt.table_name, [])
        agent, result = find_best_match(
            gt,
            candidates,
            comparator,
            uuid_mapping,
            field_config,
            gt_created,
            agent_created,
            used_agent_keys,
            exact_match_threshold=exact_match_threshold,
        )
        if agent is None or result is None:
            continue
        if result.similarity < exact_match_threshold:
            continue
        matches[gt.get_pk_tuple()] = (agent, result)
        used_agent_keys.add(agent.get_pk_tuple())

    return matches, used_agent_keys


def _update_uuid_mapping(
    uuid_mapping: dict[UUID, UUID],
    matches: dict[tuple, tuple[CowWrite, WriteComparisonResult]],
    gt_writes: list[CowWrite],
) -> None:
    gt_by_key = {write.get_pk_tuple(): write for write in gt_writes}
    for gt_key, (agent, _result) in matches.items():
        gt_write = gt_by_key.get(gt_key)
        if gt_write is None:
            continue
        for column, gt_value in gt_write.primary_key.items():
            agent_value = agent.primary_key.get(column)
            gt_uuid = _try_uuid(gt_value)
            agent_uuid = _try_uuid(agent_value)
            if gt_uuid is not None and agent_uuid is not None:
                uuid_mapping[gt_uuid] = agent_uuid


def _try_uuid(value) -> Optional[UUID]:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except (ValueError, AttributeError):
            return None
    return None


def _run_structural_match_pass(
    gt_writes: list[CowWrite],
    agent_writes: list[CowWrite],
    comparator: WriteComparator,
    field_config: FieldConfig,
    uuid_mapping: dict[UUID, UUID],
    existing_matches: dict[tuple, tuple[CowWrite, WriteComparisonResult]],
    existing_used_agent_keys: set[tuple],
    exact_match_threshold: float,
) -> tuple[dict[tuple, tuple[CowWrite, WriteComparisonResult]], set[tuple]]:
    gt_created = get_created_uuids(gt_writes)
    agent_created = get_created_uuids(agent_writes)

    leftover_gt = [
        write for write in gt_writes if write.get_pk_tuple() not in existing_matches
    ]
    leftover_agent_by_key = {
        agent.get_pk_tuple(): agent
        for agent in agent_writes
        if agent.get_pk_tuple() not in existing_used_agent_keys
    }

    matches: dict[tuple, tuple[CowWrite, WriteComparisonResult]] = {}
    used_agent_keys: set[tuple] = set(existing_used_agent_keys)

    for gt in leftover_gt:
        candidates = [
            agent
            for agent in leftover_agent_by_key.values()
            if agent.table_name == gt.table_name and agent.is_delete == gt.is_delete
        ]
        if not candidates:
            continue

        agent, result = find_best_match(
            gt,
            candidates,
            comparator,
            uuid_mapping,
            field_config,
            gt_created,
            agent_created,
            used_agent_keys,
            exact_match_threshold=exact_match_threshold,
        )
        if agent is None or result is None:
            continue

        matches[gt.get_pk_tuple()] = (agent, result)
        used_agent_keys.add(agent.get_pk_tuple())
        leftover_agent_by_key.pop(agent.get_pk_tuple(), None)

    return matches, used_agent_keys


def match_writes(
    gt_writes: list[CowWrite],
    agent_writes: list[CowWrite],
    comparator: WriteComparator,
    field_config: FieldConfig,
    seed_uuid_mapping: Optional[dict[UUID, UUID]] = None,
    exact_match_threshold: float = DEFAULT_EXACT_MATCH_THRESHOLD,
) -> MatchResult:
    uuid_mapping: dict[UUID, UUID] = dict(seed_uuid_mapping or {})

    exact_matches, exact_used_agent_keys = _run_exact_match_pass(
        gt_writes,
        agent_writes,
        comparator,
        field_config,
        uuid_mapping,
        exact_match_threshold,
    )
    _update_uuid_mapping(uuid_mapping, exact_matches, gt_writes)

    structural_matches, used_agent_keys = _run_structural_match_pass(
        gt_writes,
        agent_writes,
        comparator,
        field_config,
        uuid_mapping,
        exact_matches,
        exact_used_agent_keys,
        exact_match_threshold,
    )
    _update_uuid_mapping(uuid_mapping, structural_matches, gt_writes)

    all_matches: dict[tuple, tuple[CowWrite, WriteComparisonResult]] = {
        **exact_matches,
        **structural_matches,
    }

    matched: list[MatchedWrite] = []
    missing: list[MissingWrite] = []
    gt_by_key = {write.get_pk_tuple(): write for write in gt_writes}

    for gt_key, gt_write in gt_by_key.items():
        if gt_key in all_matches:
            agent, result = all_matches[gt_key]
            matched.append(
                MatchedWrite(ground_truth=gt_write, agent=agent, comparison=result)
            )
        else:
            missing.append(
                MissingWrite(
                    ground_truth=gt_write,
                    feedback=(
                        f"No agent write found for {gt_write.table_name} "
                        f"{gt_write.primary_key}"
                    ),
                )
            )

    extra: list[ExtraWrite] = []
    for agent in agent_writes:
        if agent.get_pk_tuple() in used_agent_keys:
            continue
        extra.append(
            ExtraWrite(
                agent=agent,
                feedback=(
                    f"Extra agent write for {agent.table_name} "
                    f"{agent.primary_key} had no GT counterpart"
                ),
            )
        )

    return MatchResult(
        matched_writes=matched,
        missing_writes=missing,
        extra_writes=extra,
        uuid_mapping=uuid_mapping,
    )
