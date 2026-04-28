"""
Session-level data helpers for COW session scoring.

* :func:`get_rows_changed` — rows changed in a session (pulls from Postgres).
* :func:`get_session_operations` — op IDs in topological order, derived from
  a list of rows already in memory.
* :func:`get_table_metadata` — pk/fk/column-type metadata for the dirty tables.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional
from uuid import UUID

from agentcow.postgres.core import Executor
from agentcow.postgres.core import get_dirty_tables as _get_dirty_tables
from agentcow.postgres.operations import _quote_ident, _quote_literal, _to_uuid

from .types import CowWrite, TableMeta

logger = logging.getLogger(__name__)


def get_session_operations(rows: list[CowWrite]) -> list[UUID]:
    """Return op IDs in topological order (earliest write per op, then op_id)."""
    earliest: dict[UUID, datetime] = {}
    for row in rows:
        existing = earliest.get(row.operation_id)
        if existing is None or row.updated_at < existing:
            earliest[row.operation_id] = row.updated_at
    return sorted(earliest.keys(), key=lambda op_id: (earliest[op_id], op_id))


def _base_name(table_name: str) -> str:
    return table_name if table_name.endswith("_base") else f"{table_name}_base"


async def get_rows_changed(
    executor: Executor,
    schema: str,
    session_id: UUID,
    *,
    excluded_tables: Optional[set[str]] = None,
) -> list[CowWrite]:
    """Return all rows changed by ``session_id``."""
    excluded = excluded_tables or set()

    dirty_tables = [
        table_name
        for table_name in await _get_dirty_tables(executor, session_id, schema)
        if table_name not in excluded
    ]
    logger.info(
        f"COW Scoring: {len(dirty_tables)} dirty tables for session {session_id}"
    )

    rows: list[CowWrite] = []
    for table_name in dirty_tables:
        pk_cols = await _get_pk_columns(executor, schema, table_name)
        if not pk_cols:
            continue

        changes_table = f"{table_name}_changes"
        columns = await _get_changes_table_columns(executor, schema, changes_table)
        if not columns:
            continue

        col_list = ", ".join(_quote_ident(column) for column in columns)
        sql = (
            f"SELECT {col_list} "
            f"FROM {_quote_ident(schema)}.{_quote_ident(changes_table)} "
            f"WHERE session_id = {_to_uuid(session_id)} "
            "ORDER BY _cow_updated_at"
        )
        raw_rows = await executor.execute(sql)

        for raw in raw_rows:
            row = dict(zip(columns, raw))
            rows.append(CowWrite.from_row(table_name, row, pk_cols))

    return rows


async def get_table_metadata(
    executor: Executor,
    schema: str,
    tables: list[str],
) -> dict[str, TableMeta]:
    """Return ``{table_name: TableMeta}`` for the given tables."""
    table_meta: dict[str, TableMeta] = {}
    for table_name in tables:
        pk = await _get_pk_columns(executor, schema, table_name)
        fk = await _get_fk_columns(executor, schema, table_name)
        types = await _get_column_types(executor, schema, table_name)
        table_meta[table_name] = TableMeta(
            pk_columns=set(pk),
            fk_columns=set(fk),
            column_types=dict(types),
        )
    return table_meta


async def _get_pk_columns(
    executor: Executor, schema: str, table_name: str
) -> list[str]:
    base_table = _base_name(table_name)
    sql = (
        "SELECT kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        "  ON tc.constraint_name = kcu.constraint_name "
        " AND tc.table_schema = kcu.table_schema "
        "WHERE tc.constraint_type = 'PRIMARY KEY' "
        f"  AND tc.table_schema = {_quote_literal(schema)} "
        f"  AND (tc.table_name = {_quote_literal(base_table)} "
        f"       OR tc.table_name = {_quote_literal(table_name)}) "
        "ORDER BY kcu.ordinal_position"
    )
    rows = await executor.execute(sql)
    return [r[0] for r in rows if r[0] not in ("session_id", "operation_id")]


async def _get_fk_columns(executor: Executor, schema: str, table_name: str) -> set[str]:
    base_table = _base_name(table_name)
    sql = (
        "SELECT kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        "  ON tc.constraint_name = kcu.constraint_name "
        " AND tc.table_schema = kcu.table_schema "
        "WHERE tc.constraint_type = 'FOREIGN KEY' "
        f"  AND tc.table_schema = {_quote_literal(schema)} "
        f"  AND (tc.table_name = {_quote_literal(base_table)} "
        f"       OR tc.table_name = {_quote_literal(table_name)})"
    )
    rows = await executor.execute(sql)
    return {r[0] for r in rows}


async def _get_column_types(
    executor: Executor, schema: str, table_name: str
) -> dict[str, str]:
    base_table = _base_name(table_name)
    sql = (
        "SELECT column_name, data_type "
        "FROM information_schema.columns "
        f"WHERE table_schema = {_quote_literal(schema)} "
        f"  AND (table_name = {_quote_literal(base_table)} "
        f"       OR table_name = {_quote_literal(table_name)})"
    )
    rows = await executor.execute(sql)
    return {r[0]: r[1] for r in rows}


async def _get_changes_table_columns(
    executor: Executor, schema: str, changes_table: str
) -> list[str]:
    sql = (
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = {_quote_literal(schema)} "
        f"  AND table_name = {_quote_literal(changes_table)} "
        "ORDER BY ordinal_position"
    )
    rows = await executor.execute(sql)
    return [r[0] for r in rows]
