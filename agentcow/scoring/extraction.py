"""
Database extraction for COW session scoring.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from agentcow.postgres.core import Executor
from agentcow.postgres.core import get_dirty_tables as _get_dirty_tables
from agentcow.postgres.operations import _quote_ident, _quote_literal, _to_uuid

from .types import CowGraph, CowNode, CowWrite

logger = logging.getLogger(__name__)


def _base_name(table_name: str) -> str:
    return table_name if table_name.endswith("_base") else f"{table_name}_base"


async def get_table_pk_columns(
    executor: Executor, schema: str, table_name: str
) -> list[str]:
    """Return PK columns for ``table_name``, preferring the ``*_base`` variant."""
    base_table = _base_name(table_name)
    sql = (
        "SELECT kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        "  ON tc.constraint_name = kcu.constraint_name "
        " AND tc.table_schema = kcu.table_schema "
        "WHERE tc.constraint_type = 'PRIMARY KEY' "
        f"  AND tc.table_schema = {_quote_literal(schema)} "
        f"  AND (tc.table_name = {_quote_literal(base_table)} OR tc.table_name = {_quote_literal(table_name)}) "
        "ORDER BY kcu.ordinal_position"
    )
    rows = await executor.execute(sql)
    return [r[0] for r in rows if r[0] not in ("session_id", "operation_id")]


async def get_table_fk_columns(
    executor: Executor, schema: str, table_name: str
) -> set[str]:
    """Return FK column names for ``table_name``."""
    base_table = _base_name(table_name)
    sql = (
        "SELECT kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        "  ON tc.constraint_name = kcu.constraint_name "
        " AND tc.table_schema = kcu.table_schema "
        "WHERE tc.constraint_type = 'FOREIGN KEY' "
        f"  AND tc.table_schema = {_quote_literal(schema)} "
        f"  AND (tc.table_name = {_quote_literal(base_table)} OR tc.table_name = {_quote_literal(table_name)})"
    )
    rows = await executor.execute(sql)
    return {r[0] for r in rows}


async def get_table_column_types(
    executor: Executor, schema: str, table_name: str
) -> dict[str, str]:
    """Return ``{column_name: data_type}`` for ``table_name``."""
    base_table = _base_name(table_name)
    sql = (
        "SELECT column_name, data_type "
        "FROM information_schema.columns "
        f"WHERE table_schema = {_quote_literal(schema)} "
        f"  AND (table_name = {_quote_literal(base_table)} OR table_name = {_quote_literal(table_name)})"
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


def _normalize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                return json.loads(stripped)
            except (ValueError, TypeError):
                return value
    return value


def _try_uuid(value: Any) -> Optional[UUID]:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except (ValueError, AttributeError):
            return None
    return None


async def extract_session_graph(
    executor: Executor,
    schema: str,
    session_id: UUID,
    *,
    excluded_tables: Optional[set[str]] = None,
) -> CowGraph:
    """Extract a :class:`CowGraph` for the given COW session."""
    excluded = excluded_tables or set()

    dirty_tables = [
        table_name
        for table_name in await _get_dirty_tables(executor, session_id, schema)
        if table_name not in excluded
    ]
    logger.info(
        f"COW Scoring: {len(dirty_tables)} dirty tables for session {session_id}"
    )

    pk_cache: dict[str, list[str]] = {}
    for table_name in dirty_tables:
        pk_cols = await get_table_pk_columns(executor, schema, table_name)
        if pk_cols:
            pk_cache[table_name] = pk_cols

    nodes: dict[UUID, CowNode] = {}
    for table_name, pk_cols in pk_cache.items():
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
        rows = await executor.execute(sql)

        for raw in rows:
            row = {column: _normalize_value(value) for column, value in zip(columns, raw)}
            write = CowWrite.from_row(table_name, row, pk_cols)

            op_id = write.operation_id
            if not isinstance(op_id, UUID):
                op_id = _try_uuid(op_id)
            if op_id is None:
                continue
            write.operation_id = op_id

            timestamp = write.updated_at or datetime.utcnow()
            node = nodes.get(op_id)
            if node is None:
                nodes[op_id] = CowNode(op_id=op_id, timestamp=timestamp, rows=[write])
            else:
                node.rows.append(write)
                if timestamp < node.timestamp:
                    node.timestamp = timestamp

    return CowGraph(nodes=nodes)


extract_session_writes = extract_session_graph
