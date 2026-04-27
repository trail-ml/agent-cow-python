"""
Core COW functionality for PostgreSQL.

Provides high-level async functions for managing Copy-On-Write tables.
All functions accept a generic ``Executor`` — no ORM or driver dependency.
"""

import uuid
from contextlib import asynccontextmanager
from graphlib import CycleError, TopologicalSorter
from typing import Any, AsyncIterator, Protocol, TypedDict, runtime_checkable

from .cow_sql_functions import (
    COW_CHANGES_TABLE_NAME_SQL,
    CREATE_DIRTY_TABLES_SQL,
    SETUP_COW_SQL,
    COMMIT_COW_UPSERT_SQL,
    COMMIT_COW_DELETE_SQL,
    COMMIT_COW_CLEANUP_SQL,
    COMMIT_COW_SQL,
    DISCARD_COW_SQL,
    TEARDOWN_COW_SQL,
    GET_DIRTY_CHANGES_TABLES_SQL,
    GET_COW_DEPENDENCIES_SQL,
    GET_SESSION_OPERATIONS_SQL,
    GET_COW_FK_EDGES_SQL,
)
from .operations import (
    COW_FUNCTION_NAMES,
    setup_cow_sql,
    teardown_cow_sql,
    rename_table_sql,
    check_cow_state_sql,
    check_cow_disable_state_sql,
    check_table_is_base_table_sql,
    get_table_pk_cols_sql,
    check_cow_functions_deployed_sql,
    list_user_tables_sql,
    list_base_tables_sql,
    list_changes_tables_sql,
    commit_cow_session_sql,
    discard_cow_session_sql,
    commit_cow_operations_sql,
    discard_cow_operations_sql,
    commit_cow_upsert_sql,
    commit_cow_delete_sql,
    commit_cow_cleanup_sql,
    get_cow_fk_edges_sql,
    alter_fk_constraints_deferrable_sql,
    alter_fk_constraints_not_deferrable_sql,
    get_session_operations_sql,
    get_operation_dependencies_sql,
    set_visible_operations_sql,
    get_dirty_tables_sql,
)
from .context import CowPostgresConfig, build_cow_variable_statements


@runtime_checkable
class Executor(Protocol):
    """Minimal async SQL executor.

    Any object with an ``execute`` method that accepts a SQL string and
    returns rows as ``list[tuple[Any, ...]]`` satisfies this protocol.

    Example adapters::

        # SQLAlchemy AsyncSession
        class SAExecutor:
            def __init__(self, session):
                self._s = session
            async def execute(self, sql):
                from sqlalchemy import text
                r = await self._s.execute(text(sql))
                return [tuple(row) for row in r.fetchall()] if r.returns_rows else []

        # asyncpg Connection
        class PGExecutor:
            def __init__(self, conn):
                self._c = conn
            async def execute(self, sql):
                return [tuple(r) for r in await self._c.fetch(sql)]
    """

    async def execute(self, sql: str) -> list[tuple[Any, ...]]: ...


class CowStatus(TypedDict):
    enabled: bool
    tables_with_cow: list[str]
    changes_tables: list[str]
    cow_functions_deployed: bool


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _get_pk_cols(executor: Executor, schema: str, table_name: str) -> list[str]:
    """Resolve primary-key columns for a table by querying the database."""
    rows = await executor.execute(get_table_pk_cols_sql(schema, table_name))
    pk_cols = [row[0] for row in rows]
    if not pk_cols:
        raise ValueError(f"Table {table_name} in schema {schema} has no primary key.")
    return pk_cols


@asynccontextmanager
async def deferred_fk_constraints(executor: Executor) -> AsyncIterator[None]:
    """Defer FK constraint checks for the duration of the block.

    Use this around multi-table commit loops so that cross-table FK
    references are validated only after all tables have been committed.
    Requires FK constraints to be ``DEFERRABLE INITIALLY IMMEDIATE``
    (see :func:`enable_cow` / :func:`enable_cow_schema` with
    ``allow_deferred_fks=True``).
    """
    await executor.execute("SET CONSTRAINTS ALL DEFERRED")
    yield
    await executor.execute("SET CONSTRAINTS ALL IMMEDIATE")


def _topologically_sort_tables(
    tables: list[str],
    edges: list[tuple[str, str]],
) -> list[str]:
    """Return *tables* ordered so that parents come before children.

    Raises :class:`ValueError` with actionable remediation if the FK
    subgraph contains a cycle.
    """
    sorter: TopologicalSorter[str] = TopologicalSorter()
    for table in tables:
        sorter.add(table)
    for parent, child in edges:
        if parent != child:
            sorter.add(child, parent)
    try:
        return list(sorter.static_order())
    except CycleError as exc:
        cycle = list(dict.fromkeys(exc.args[1]))
        raise ValueError(
            f"Cycle detected among tables {cycle}; commit_cow_session_schema "
            "cannot order inserts/deletes automatically. Either break the cycle "
            "in your schema or pass defer_fk_constraints=True (requires FKs to "
            "be DEFERRABLE; see enable_cow(allow_deferred_fks=True))."
        ) from exc


# ---------------------------------------------------------------------------
# Deployment
# ---------------------------------------------------------------------------


async def deploy_cow_functions(executor: Executor) -> None:
    """Deploy the required PL/pgSQL helper functions to the database."""
    for sql in (
        COW_CHANGES_TABLE_NAME_SQL,
        CREATE_DIRTY_TABLES_SQL,
        SETUP_COW_SQL,
        COMMIT_COW_UPSERT_SQL,
        COMMIT_COW_DELETE_SQL,
        COMMIT_COW_CLEANUP_SQL,
        COMMIT_COW_SQL,
        DISCARD_COW_SQL,
        TEARDOWN_COW_SQL,
        GET_DIRTY_CHANGES_TABLES_SQL,
        GET_COW_DEPENDENCIES_SQL,
        GET_SESSION_OPERATIONS_SQL,
        GET_COW_FK_EDGES_SQL,
    ):
        await executor.execute(sql)


# ---------------------------------------------------------------------------
# Enable / Disable COW
# ---------------------------------------------------------------------------


async def enable_cow(
    executor: Executor,
    table_name: str,
    pk_cols: list[str] | None = None,
    schema: str = "public",
    allow_deferred_fks: bool = False,
) -> None:
    """Enable COW on *table_name*.

    If *pk_cols* is ``None`` they are auto-detected from the database.

    If *allow_deferred_fks* is ``True``, any ``NOT DEFERRABLE`` FK
    constraints on the base table are flipped to
    ``DEFERRABLE INITIALLY IMMEDIATE`` so that callers can use
    :func:`deferred_fk_constraints` or
    ``commit_cow_session_schema(..., defer_fk_constraints=True)``.
    This modifies your schema; by default we leave constraints alone
    and rely on topological-order commits to satisfy FK checks.
    """
    base_table = f"{table_name}_base"

    if pk_cols is None:
        pk_cols = await _get_pk_cols(executor, schema, table_name)

    rows = await executor.execute(check_cow_state_sql(schema, table_name, base_table))
    base_exists, original_is_table, view_exists = rows[0]

    if base_exists and view_exists:
        if allow_deferred_fks:
            await executor.execute(
                alter_fk_constraints_deferrable_sql(schema, base_table)
            )
        return

    if base_exists and not view_exists:
        await executor.execute(setup_cow_sql(schema, base_table, table_name, pk_cols))
    elif original_is_table:
        await executor.execute(rename_table_sql(schema, table_name, base_table))
        await executor.execute(setup_cow_sql(schema, base_table, table_name, pk_cols))
    else:
        raise ValueError(
            f"Table {table_name} not found in schema {schema} as table or view"
        )

    if allow_deferred_fks:
        await executor.execute(alter_fk_constraints_deferrable_sql(schema, base_table))


async def disable_cow(
    executor: Executor,
    table_name: str,
    schema: str = "public",
    revert_deferred_fks: bool = True,
) -> None:
    """Disable COW for *table_name*, restoring the original base table.

    If *revert_deferred_fks* is ``True`` (the default), any
    ``DEFERRABLE INITIALLY IMMEDIATE`` FK constraints on the base table
    are flipped back to ``NOT DEFERRABLE`` before the table is renamed.
    Constraints that are ``INITIALLY DEFERRED`` (explicitly set by the
    schema owner) are left alone.
    """
    base_table = f"{table_name}_base"
    changes_table = f"{table_name}_changes"

    rows = await executor.execute(
        check_cow_disable_state_sql(schema, table_name, base_table, changes_table)
    )
    base_exists, _original_is_table, view_exists, changes_exists = rows[0]

    if not base_exists and not view_exists and not changes_exists:
        return

    if base_exists and revert_deferred_fks:
        await executor.execute(
            alter_fk_constraints_not_deferrable_sql(schema, base_table)
        )

    await executor.execute(teardown_cow_sql(schema, table_name))

    if base_exists:
        check = await executor.execute(
            check_table_is_base_table_sql(schema, table_name)
        )
        if not check:
            await executor.execute(rename_table_sql(schema, base_table, table_name))


async def enable_cow_schema(
    executor: Executor,
    schema: str = "public",
    exclude: set[str] | None = None,
    allow_deferred_fks: bool = False,
) -> list[str]:
    """Enable COW on all user tables in *schema*.

    Tables whose names end with ``_base`` or ``_changes`` are skipped
    automatically, as are any names listed in *exclude*.

    See :func:`enable_cow` for *allow_deferred_fks*.

    Returns the table names that were enabled.
    """
    exclude = exclude or set()
    rows = await executor.execute(list_user_tables_sql(schema))
    already_cow = {
        row[0].removesuffix("_base")
        for row in await executor.execute(list_base_tables_sql(schema))
    }

    enabled: list[str] = []
    for (table_name,) in rows:
        if table_name in exclude or table_name in already_cow:
            continue
        await enable_cow(
            executor,
            table_name,
            schema=schema,
            allow_deferred_fks=allow_deferred_fks,
        )
        enabled.append(table_name)
    return enabled


async def disable_cow_schema(
    executor: Executor,
    schema: str = "public",
    exclude: set[str] | None = None,
    revert_deferred_fks: bool = True,
) -> list[str]:
    """Disable COW on all COW-enabled tables in *schema*.

    Returns the table names that were disabled.
    """
    exclude = exclude or set()
    rows = await executor.execute(list_base_tables_sql(schema))
    disabled: list[str] = []
    for (base_name,) in rows:
        table_name = base_name.removesuffix("_base")
        if table_name in exclude:
            continue
        await disable_cow(
            executor,
            table_name,
            schema=schema,
            revert_deferred_fks=revert_deferred_fks,
        )
        disabled.append(table_name)
    return disabled


# ---------------------------------------------------------------------------
# Session-level commit / discard
# ---------------------------------------------------------------------------


async def commit_cow_session(
    executor: Executor,
    table_name: str,
    session_id: str | uuid.UUID,
    pk_cols: list[str] | None = None,
    schema: str = "public",
) -> None:
    """Commit all COW changes for *session_id* on a single table."""
    base_table = f"{table_name}_base"
    if pk_cols is None:
        pk_cols = await _get_pk_cols(executor, schema, base_table)
    await executor.execute(
        commit_cow_session_sql(schema, base_table, pk_cols, session_id)
    )


async def discard_cow_session(
    executor: Executor,
    table_name: str,
    session_id: str | uuid.UUID,
    schema: str = "public",
) -> None:
    """Discard all COW changes for *session_id* on a single table."""
    base_table = f"{table_name}_base"
    await executor.execute(discard_cow_session_sql(schema, base_table, session_id))


async def get_dirty_tables(
    executor: Executor,
    session_id: str | uuid.UUID,
    schema: str = "public",
) -> list[str]:
    """Get the list of dirty table names for a session."""
    rows = await executor.execute(get_dirty_tables_sql(schema, session_id))
    return [row[0] for row in rows]


async def _get_fk_edges(
    executor: Executor,
    schema: str,
    base_tables: list[str],
) -> list[tuple[str, str]]:
    """Fetch FK edges ``(parent_view, child_view)`` among *base_tables*.

    Returns view-level names (``_base`` suffix stripped). Self-referential
    edges are dropped — they're a within-table ordering concern.
    """
    if not base_tables:
        return []
    rows = await executor.execute(get_cow_fk_edges_sql(schema, base_tables))
    edges: list[tuple[str, str]] = []
    for parent_base, child_base, is_self_ref in rows:
        if is_self_ref:
            continue
        edges.append(
            (
                parent_base.removesuffix("_base"),
                child_base.removesuffix("_base"),
            )
        )
    return edges


async def commit_cow_session_schema(
    executor: Executor,
    session_id: str | uuid.UUID,
    schema: str = "public",
    defer_fk_constraints: bool = False,
) -> list[str]:
    """Commit all dirty tables for a session in a schema.

    By default, tables are committed in two phases ordered by FK
    dependency: upserts are applied parents-first (topological order),
    then deletes are applied children-first (reverse topological order).
    This keeps the database consistent at every intermediate step without
    requiring any schema modifications.

    If the dirty-table subgraph contains an FK cycle, a :class:`ValueError`
    is raised; the caller can either break the cycle in their schema or
    opt in to deferred checks with ``defer_fk_constraints=True`` (which
    requires the FKs to be ``DEFERRABLE`` — see
    ``enable_cow(allow_deferred_fks=True)``).

    Returns the list of table names that were committed (in the order
    they were processed).
    """
    tables = await get_dirty_tables(executor, session_id, schema)
    if not tables:
        return []

    if defer_fk_constraints:
        async with deferred_fk_constraints(executor):
            for table_name in tables:
                await commit_cow_session(
                    executor, table_name, session_id, schema=schema
                )
        return tables

    base_tables = [f"{t}_base" for t in tables]
    edges = await _get_fk_edges(executor, schema, base_tables)
    ordered = _topologically_sort_tables(tables, edges)

    for table_name in ordered:
        base_table = f"{table_name}_base"
        pk_cols = await _get_pk_cols(executor, schema, base_table)
        await executor.execute(
            commit_cow_upsert_sql(schema, base_table, pk_cols, session_id)
        )

    for table_name in reversed(ordered):
        base_table = f"{table_name}_base"
        pk_cols = await _get_pk_cols(executor, schema, base_table)
        await executor.execute(
            commit_cow_delete_sql(schema, base_table, pk_cols, session_id)
        )

    for table_name in ordered:
        base_table = f"{table_name}_base"
        await executor.execute(commit_cow_cleanup_sql(schema, base_table, session_id))

    return ordered


async def discard_cow_session_schema(
    executor: Executor,
    session_id: str | uuid.UUID,
    schema: str = "public",
) -> list[str]:
    """Discard all dirty tables for a session in a schema.

    Returns the list of table names that were discarded.
    """
    tables = await get_dirty_tables(executor, session_id, schema)
    for table_name in tables:
        await discard_cow_session(executor, table_name, session_id, schema=schema)
    return tables


# ---------------------------------------------------------------------------
# Operation-level commit / discard
# ---------------------------------------------------------------------------


async def commit_cow_operations(
    executor: Executor,
    table_name: str,
    session_id: str | uuid.UUID,
    operation_ids: list[str | uuid.UUID],
    pk_cols: list[str] | None = None,
    schema: str = "public",
) -> None:
    """Commit specific operations on a single table."""
    if not operation_ids:
        return
    base_table = f"{table_name}_base"
    if pk_cols is None:
        pk_cols = await _get_pk_cols(executor, schema, base_table)
    await executor.execute(
        commit_cow_operations_sql(
            schema, base_table, pk_cols, session_id, operation_ids
        )
    )


async def discard_cow_operations(
    executor: Executor,
    table_name: str,
    session_id: str | uuid.UUID,
    operation_ids: list[str | uuid.UUID],
    schema: str = "public",
) -> None:
    """Discard specific operations on a single table."""
    if not operation_ids:
        return
    base_table = f"{table_name}_base"
    await executor.execute(
        discard_cow_operations_sql(schema, base_table, session_id, operation_ids)
    )


# ---------------------------------------------------------------------------
# Querying operations
# ---------------------------------------------------------------------------


async def get_session_operations(
    executor: Executor,
    session_id: str | uuid.UUID,
    schema: str = "public",
) -> list[uuid.UUID]:
    """Get all operation IDs in a COW session."""
    rows = await executor.execute(get_session_operations_sql(schema, session_id))
    return [row[0] for row in rows]


async def get_operation_dependencies(
    executor: Executor,
    session_id: str | uuid.UUID,
    schema: str = "public",
) -> list[tuple[uuid.UUID, uuid.UUID]]:
    """Get dependency pairs (depends_on, operation_id) in a session."""
    rows = await executor.execute(get_operation_dependencies_sql(schema, session_id))
    return [(row[0], row[1]) for row in rows]


async def set_visible_operations(
    executor: Executor,
    operation_ids: list[str | uuid.UUID] | None,
) -> None:
    """Set which operations' changes should be visible in subsequent queries."""
    await executor.execute(set_visible_operations_sql(operation_ids))


# ---------------------------------------------------------------------------
# Session variables
# ---------------------------------------------------------------------------


async def apply_cow_variables(
    executor: Executor,
    session_id: str | uuid.UUID,
    operation_id: str | uuid.UUID | None = None,
    visible_operations: list[str | uuid.UUID] | None = None,
) -> None:
    """Set the COW session variables (session_id, operation_id, visible_operations)."""
    for stmt in build_cow_variable_statements(
        session_id, operation_id, visible_operations
    ):
        await executor.execute(stmt)


async def reset_cow_variables(executor: Executor) -> None:
    """Reset all COW session variables to their defaults."""
    await executor.execute("RESET app.session_id")
    await executor.execute("RESET app.operation_id")
    await executor.execute("RESET app.visible_operations")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


async def is_cow_enabled(
    executor: Executor,
    config: CowPostgresConfig,
    schema: str = "public",
) -> bool:
    """Check whether CoW is both requested and properly configured.

    Returns ``True`` only when the request carries a session ID *and* the
    database has the CoW functions deployed with at least one CoW-enabled table.
    """
    if not config.is_active:
        return False

    func_rows = await executor.execute(check_cow_functions_deployed_sql())
    expected = len(COW_FUNCTION_NAMES)
    if (func_rows[0][0] if func_rows else 0) != expected:
        return False

    base_rows = await executor.execute(list_base_tables_sql(schema))
    return len(base_rows) > 0


async def get_cow_status(
    executor: Executor,
    schema: str = "public",
) -> CowStatus:
    """Get the COW status for a schema."""
    func_rows = await executor.execute(check_cow_functions_deployed_sql())
    expected = len(COW_FUNCTION_NAMES)
    cow_functions_deployed = (func_rows[0][0] if func_rows else 0) == expected

    base_rows = await executor.execute(list_base_tables_sql(schema))
    base_tables = [row[0] for row in base_rows]

    changes_rows = await executor.execute(list_changes_tables_sql(schema))
    changes_tables = [row[0] for row in changes_rows]

    tables_with_cow = [t.replace("_base", "") for t in base_tables]

    return CowStatus(
        enabled=len(base_tables) > 0,
        tables_with_cow=tables_with_cow,
        changes_tables=changes_tables,
        cow_functions_deployed=cow_functions_deployed,
    )
