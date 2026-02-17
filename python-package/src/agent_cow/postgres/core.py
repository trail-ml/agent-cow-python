"""
Core COW functionality for PostgreSQL.

This module contains the main functions for enabling/disabling COW on tables,
committing and discarding sessions, and managing COW state.
"""

import uuid
from typing import Type, Optional

from sqlalchemy import text, inspect, Table, MetaData, bindparam
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.types import Text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from .sql_functions import (
    SETUP_COW_SQL,
    COMMIT_COW_SQL,
    DISCARD_COW_SQL,
    TEARDOWN_COW_SQL,
    GET_COW_DEPENDENCIES_SQL,
    COMMIT_COW_OPERATIONS_SQL,
    DISCARD_COW_OPERATIONS_SQL,
    GET_SESSION_OPERATIONS_SQL,
)


def _quote_ident(s: str) -> str:
    """Quote a PostgreSQL identifier."""
    return '"' + s.replace('"', '""') + '"'


def get_tablename(model: Type[DeclarativeBase]) -> str:
    """Get the table name for a model."""
    tablename = getattr(model, "__tablename__", None)
    if tablename is None:
        raise ValueError(f"Model {model} does not have a __tablename__ attribute.")
    return tablename


async def deploy_cow_functions(session: AsyncSession):
    """Deploy the required PL/PGSQL functions to the database."""
    await session.execute(text(SETUP_COW_SQL))
    await session.execute(text(COMMIT_COW_SQL))
    await session.execute(text(DISCARD_COW_SQL))
    await session.execute(text(TEARDOWN_COW_SQL))
    await session.execute(text(GET_COW_DEPENDENCIES_SQL))
    await session.execute(text(COMMIT_COW_OPERATIONS_SQL))
    await session.execute(text(DISCARD_COW_OPERATIONS_SQL))
    await session.execute(text(GET_SESSION_OPERATIONS_SQL))


async def enable_cow_for_model(
    session: AsyncSession,
    model: Type[DeclarativeBase],
    schema: str = "public",
):
    """Enable Copy-On-Write (COW) on the table corresponding to the given SQLAlchemy model."""
    if not hasattr(model, "__tablename__"):
        raise ValueError(f"Model {model} does not have a __tablename__ attribute.")

    original_table = get_tablename(model)
    insp = inspect(model)
    pk_cols = [col.name for col in insp.primary_key]
    if not pk_cols:
        raise ValueError(f"Model {model.__name__} has no primary key defined.")

    await _enable_cow_for_table_name(session, original_table, pk_cols, schema)


async def enable_cow_for_table(
    session: AsyncSession,
    table: Table,
    schema: str = "public",
):
    """Enable Copy-On-Write (COW) for a SQLAlchemy Table object."""
    original_table = table.name
    pk_cols = [col.name for col in table.primary_key.columns]
    if not pk_cols:
        raise ValueError(f"Table {original_table} has no primary key defined.")

    await _enable_cow_for_table_name(session, original_table, pk_cols, schema)


async def _enable_cow_for_table_name(
    session: AsyncSession,
    original_table: str,
    pk_cols: list[str],
    schema: str,
):
    """Internal helper to enable COW for a table by name."""
    base_table = f"{original_table}_base"
    view_name = original_table

    check_result = await session.execute(
        text("""
            SELECT 
                EXISTS(SELECT 1 FROM information_schema.tables 
                       WHERE table_schema = :schema AND table_name = :base_table 
                       AND table_type = 'BASE TABLE') as base_exists,
                EXISTS(SELECT 1 FROM information_schema.tables 
                       WHERE table_schema = :schema AND table_name = :original_table 
                       AND table_type = 'BASE TABLE') as original_is_table,
                EXISTS(SELECT 1 FROM information_schema.views 
                       WHERE table_schema = :schema AND table_name = :original_table) as view_exists
            """),
        {
            "schema": schema,
            "base_table": base_table,
            "original_table": original_table,
        },
    )
    row = check_result.fetchone()
    if row is None:
        raise ValueError(f"Unable to check state for table {original_table}")

    base_exists = row[0]
    original_is_table = row[1]
    view_exists = row[2]

    if base_exists and view_exists:
        return

    if base_exists and not view_exists:
        await session.execute(
            text(
                "SELECT setup_cow(:schema, :base_table, :view_name, :pk_cols)"
            ).bindparams(bindparam("pk_cols", type_=PG_ARRAY(Text))),
            {
                "schema": schema,
                "base_table": base_table,
                "view_name": view_name,
                "pk_cols": pk_cols,
            },
        )
        return

    if original_is_table:
        rename_sql = f"ALTER TABLE {_quote_ident(schema)}.{_quote_ident(original_table)} RENAME TO {_quote_ident(base_table)}"
        await session.execute(text(rename_sql))

        await session.execute(
            text(
                "SELECT setup_cow(:schema, :base_table, :view_name, :pk_cols)"
            ).bindparams(bindparam("pk_cols", type_=PG_ARRAY(Text))),
            {
                "schema": schema,
                "base_table": base_table,
                "view_name": view_name,
                "pk_cols": pk_cols,
            },
        )
        return

    raise ValueError(
        f"Table {original_table} not found in schema {schema} as table or view"
    )


async def enable_cow_for_registry(
    session: AsyncSession,
    registry,
    schema: str = "public",
    exclude_models: Optional[list[Type[DeclarativeBase]]] = None,
):
    """Enable Copy-On-Write (COW) for all models in a given SQLAlchemy registry."""
    if exclude_models is None:
        exclude_models = []

    models = []
    for mapper in registry.mappers:
        model = mapper.class_
        if model not in exclude_models and hasattr(model, "__tablename__"):
            models.append(model)

    from sqlalchemy.exc import NoInspectionAvailable
    import collections

    dependencies = collections.defaultdict(set)
    model_map = {get_tablename(m): m for m in models}

    for model in models:
        try:
            insp = inspect(model)
        except NoInspectionAvailable:
            continue

        for column in insp.columns:
            for fk in column.foreign_keys:
                target_full = fk.target_fullname
                if "." in target_full:
                    parts = target_full.split(".")
                    if len(parts) >= 2:
                        target_table = parts[-2]
                        if target_table in model_map:
                            dependencies[model].add(model_map[target_table])

    sorted_models = []
    visited = set()
    temp_mark = set()

    def visit(n):
        if n in temp_mark:
            return
        if n not in visited:
            temp_mark.add(n)
            for m in dependencies[n]:
                visit(m)
            temp_mark.remove(n)
            visited.add(n)
            sorted_models.append(n)

    for model in models:
        if model not in visited:
            visit(model)

    for model in sorted_models:
        await enable_cow_for_model(session, model, schema=schema)


async def enable_cow_for_metadata(
    session: AsyncSession,
    metadata: MetaData,
    schema: str = "public",
    exclude_tables: Optional[list[str]] = None,
    registry=None,
):
    """Enable Copy-On-Write (COW) for unmapped tables in a MetaData object."""
    import collections

    if exclude_tables is None:
        exclude_tables = []

    mapped_table_names: set[str] = set()
    if registry is not None:
        for mapper in registry.mappers:
            if hasattr(mapper, "local_table") and mapper.local_table is not None:
                mapped_table_names.add(mapper.local_table.name)

    all_tables = [
        t
        for t in metadata.tables.values()
        if t.name not in exclude_tables and t.name not in mapped_table_names
    ]

    dependencies: dict[str, set[str]] = collections.defaultdict(set)
    table_map = {t.name: t for t in all_tables}

    for table in all_tables:
        for fk in table.foreign_keys:
            target_table_name = fk.column.table.name
            if target_table_name in table_map and target_table_name != table.name:
                dependencies[table.name].add(target_table_name)

    sorted_table_names: list[str] = []
    visited: set[str] = set()
    temp_mark: set[str] = set()

    def visit(name: str):
        if name in temp_mark:
            return
        if name not in visited:
            temp_mark.add(name)
            for dep in dependencies[name]:
                visit(dep)
            temp_mark.remove(name)
            visited.add(name)
            sorted_table_names.append(name)

    for table in all_tables:
        if table.name not in visited:
            visit(table.name)

    for table_name in sorted_table_names:
        table = table_map[table_name]
        await enable_cow_for_table(session, table, schema=schema)


async def commit_cow_session(
    session: AsyncSession,
    models: list[Type[DeclarativeBase]] | Type[DeclarativeBase],
    session_id: uuid.UUID,
    schema: str = "public",
):
    """Commit changes from a COW workspace/session to the base table."""
    from sqlalchemy.exc import NoInspectionAvailable
    import collections

    if not isinstance(models, list):
        models = [models]

    dependencies = collections.defaultdict(set)
    model_map = {get_tablename(m): m for m in models}

    for model in models:
        try:
            insp = inspect(model)
        except NoInspectionAvailable:
            continue

        for column in insp.columns:
            for fk in column.foreign_keys:
                target_full = fk.target_fullname
                if "." in target_full:
                    parts = target_full.split(".")
                    if len(parts) >= 2:
                        target_table = parts[-2]
                        if target_table in model_map:
                            dependencies[model].add(model_map[target_table])

    sorted_models = []
    visited = set()
    temp_mark = set()

    def visit(n):
        if n in temp_mark:
            return
        if n not in visited:
            temp_mark.add(n)
            for m in dependencies[n]:
                visit(m)
            temp_mark.remove(n)
            visited.add(n)
            sorted_models.append(n)

    for model in models:
        if model not in visited:
            visit(model)

    for model in sorted_models:
        table_name = get_tablename(model)
        base_table = f"{table_name}_base"

        insp = inspect(model)
        pk_cols = [col.name for col in insp.primary_key]
        if not pk_cols:
            raise ValueError(f"Model {model.__name__} has no primary key.")

        await session.execute(
            text("SELECT commit_cow(:schema, :base_table, :pk_cols, :sid)").bindparams(
                bindparam("pk_cols", type_=PG_ARRAY(Text))
            ),
            {
                "schema": schema,
                "base_table": base_table,
                "pk_cols": pk_cols,
                "sid": session_id,
            },
        )


async def commit_cow_table(
    session: AsyncSession,
    table: Table,
    session_id: uuid.UUID,
    schema: str = "public",
):
    """Commit changes from a COW workspace/session to the base table for a Table object."""
    table_name = table.name
    base_table = f"{table_name}_base"

    pk_cols = [col.name for col in table.primary_key.columns]
    if not pk_cols:
        raise ValueError(f"Table {table_name} has no primary key.")

    await session.execute(
        text("SELECT commit_cow(:schema, :base_table, :pk_cols, :sid)").bindparams(
            bindparam("pk_cols", type_=PG_ARRAY(Text))
        ),
        {
            "schema": schema,
            "base_table": base_table,
            "pk_cols": pk_cols,
            "sid": session_id,
        },
    )


async def commit_metadata_changes(
    session: AsyncSession,
    metadata: MetaData,
    session_id: uuid.UUID,
    schema: str = "public",
):
    """Auto-detects and commits all tables (including association tables) that have changes."""
    import collections

    result = await session.execute(
        text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :schema 
              AND table_name LIKE '%_changes'
        """),
        {"schema": schema},
    )
    existing_changes_tables = {row[0] for row in result}

    dirty_tables: list[Table] = []
    processed_tables: set[str] = set()

    for table in metadata.tables.values():
        table_name = table.name
        changes_table = f"{table_name}_changes"

        if (
            table_name in processed_tables
            or changes_table not in existing_changes_tables
        ):
            continue

        processed_tables.add(table_name)

        has_changes = await session.execute(
            text(
                f"SELECT 1 FROM {_quote_ident(schema)}.{_quote_ident(changes_table)} WHERE session_id = :sid LIMIT 1"
            ),
            {"sid": session_id},
        )

        if has_changes.scalar():
            dirty_tables.append(table)

    if not dirty_tables:
        return

    dependencies: dict[str, set[str]] = collections.defaultdict(set)
    table_map = {t.name: t for t in dirty_tables}

    for table in dirty_tables:
        for fk in table.foreign_keys:
            target_table_name = fk.column.table.name
            if target_table_name in table_map and target_table_name != table.name:
                dependencies[table.name].add(target_table_name)

    sorted_table_names: list[str] = []
    visited: set[str] = set()
    temp_mark: set[str] = set()

    def visit(name: str):
        if name in temp_mark:
            return
        if name not in visited:
            temp_mark.add(name)
            for dep in dependencies[name]:
                visit(dep)
            temp_mark.remove(name)
            visited.add(name)
            sorted_table_names.append(name)

    for table in dirty_tables:
        if table.name not in visited:
            visit(table.name)

    for table_name in sorted_table_names:
        table = table_map[table_name]
        await commit_cow_table(session, table, session_id, schema)


async def commit_registry_changes(
    session: AsyncSession,
    registry,
    session_id: uuid.UUID,
    schema: str = "public",
    metadata: Optional[MetaData] = None,
):
    """Auto-detects and commits all models that have changes in the given COW session."""
    result = await session.execute(
        text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :schema 
              AND table_name LIKE '%_changes'
        """),
        {"schema": schema},
    )
    existing_changes_tables = {row[0] for row in result}

    dirty_models = []
    processed_tables: set[str] = set()

    for mapper in registry.mappers:
        model = mapper.class_
        if not hasattr(model, "__tablename__"):
            continue

        table_name = get_tablename(model)
        changes_table = f"{table_name}_changes"

        if (
            table_name in processed_tables
            or changes_table not in existing_changes_tables
        ):
            continue

        processed_tables.add(table_name)

        has_changes = await session.execute(
            text(
                f"SELECT 1 FROM {_quote_ident(schema)}.{_quote_ident(changes_table)} WHERE session_id = :sid LIMIT 1"
            ),
            {"sid": session_id},
        )

        if has_changes.scalar():
            dirty_models.append(model)

    if dirty_models:
        await commit_cow_session(session, dirty_models, session_id, schema)

    if metadata is not None:
        dirty_tables: list[Table] = []

        for table in metadata.tables.values():
            table_name = table.name
            changes_table = f"{table_name}_changes"

            if (
                table_name in processed_tables
                or changes_table not in existing_changes_tables
            ):
                continue

            processed_tables.add(table_name)

            has_changes = await session.execute(
                text(
                    f"SELECT 1 FROM {_quote_ident(schema)}.{_quote_ident(changes_table)} WHERE session_id = :sid LIMIT 1"
                ),
                {"sid": session_id},
            )

            if has_changes.scalar():
                dirty_tables.append(table)

        for table in dirty_tables:
            await commit_cow_table(session, table, session_id, schema)


async def discard_cow_session(
    session: AsyncSession,
    model: Type[DeclarativeBase],
    session_id: uuid.UUID,
    schema: str = "public",
):
    """Discard all changes made in a specific COW workspace/session."""
    table_name = get_tablename(model)
    base_table = f"{table_name}_base"

    await session.execute(
        text("SELECT discard_cow(:schema, :base_table, :sid)"),
        {"schema": schema, "base_table": base_table, "sid": session_id},
    )


async def disable_cow_for_model(
    session: AsyncSession,
    model: Type[DeclarativeBase],
    schema: str = "public",
):
    """Disable COW for the given model, rolling back the database structure."""
    if not hasattr(model, "__tablename__"):
        raise ValueError(f"Model {model} does not have a __tablename__ attribute.")

    original_table = get_tablename(model)
    await _disable_cow_for_table_name(session, original_table, schema)


async def disable_cow_for_table(
    session: AsyncSession,
    table: Table,
    schema: str = "public",
):
    """Disable COW for a SQLAlchemy Table object."""
    await _disable_cow_for_table_name(session, table.name, schema)


async def _disable_cow_for_table_name(
    session: AsyncSession,
    original_table: str,
    schema: str,
):
    """Internal helper to disable COW for a table by name."""
    view_name = original_table
    base_table = f"{original_table}_base"
    changes_table = f"{original_table}_changes"

    check_result = await session.execute(
        text("""
            SELECT 
                EXISTS(SELECT 1 FROM information_schema.tables 
                       WHERE table_schema = :schema AND table_name = :base_table 
                       AND table_type = 'BASE TABLE') as base_exists,
                EXISTS(SELECT 1 FROM information_schema.tables 
                       WHERE table_schema = :schema AND table_name = :original_table 
                       AND table_type = 'BASE TABLE') as original_exists,
                EXISTS(SELECT 1 FROM information_schema.views 
                       WHERE table_schema = :schema AND table_name = :original_table) as view_exists,
                EXISTS(SELECT 1 FROM information_schema.tables 
                       WHERE table_schema = :schema AND table_name = :changes_table) as changes_exists
            """),
        {
            "schema": schema,
            "base_table": base_table,
            "original_table": original_table,
            "changes_table": changes_table,
        },
    )
    row = check_result.fetchone()
    if row is None:
        return

    base_exists = row[0]
    view_exists = row[2]
    changes_exists = row[3]

    if not base_exists and not view_exists and not changes_exists:
        return

    await session.execute(
        text("SELECT teardown_cow(:schema, :view_name)"),
        {"schema": schema, "view_name": view_name},
    )

    if base_exists:
        original_check = await session.execute(
            text("""
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = :schema AND table_name = :original_table
                AND table_type = 'BASE TABLE'
                """),
            {"schema": schema, "original_table": original_table},
        )
        if not original_check.scalar():
            rename_sql = f"ALTER TABLE {_quote_ident(schema)}.{_quote_ident(base_table)} RENAME TO {_quote_ident(original_table)}"
            await session.execute(text(rename_sql))


async def disable_cow_for_registry(
    session: AsyncSession,
    registry,
    schema: str = "public",
):
    """Disable COW for all models in a given SQLAlchemy registry."""
    for mapper in registry.mappers:
        model = mapper.class_
        if hasattr(model, "__tablename__"):
            await disable_cow_for_model(session, model, schema=schema)


async def disable_cow_for_metadata(
    session: AsyncSession,
    metadata: MetaData,
    schema: str = "public",
    registry=None,
):
    """Disable COW for unmapped tables in a MetaData object."""
    mapped_table_names: set[str] = set()
    if registry is not None:
        for mapper in registry.mappers:
            if hasattr(mapper, "local_table") and mapper.local_table is not None:
                mapped_table_names.add(mapper.local_table.name)

    unmapped_tables = [
        t for t in metadata.tables.values() if t.name not in mapped_table_names
    ]

    for table in unmapped_tables:
        await disable_cow_for_table(session, table, schema=schema)


async def get_cow_status(
    session: AsyncSession,
    schema: str,
) -> dict:
    """Get the COW status for a schema, including which tables have COW enabled."""
    functions_result = await session.execute(text("""
            SELECT COUNT(*) FROM pg_proc 
            WHERE proname IN ('setup_cow', 'commit_cow', 'discard_cow', 'teardown_cow')
        """))
    functions_count = functions_result.scalar()
    cow_functions_deployed = functions_count == 4

    base_tables_result = await session.execute(
        text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :schema 
              AND table_name LIKE '%_base'
              AND table_type = 'BASE TABLE'
        """),
        {"schema": schema},
    )
    base_tables = [row[0] for row in base_tables_result]

    changes_result = await session.execute(
        text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :schema 
              AND table_name LIKE '%_changes'
        """),
        {"schema": schema},
    )
    changes_tables = [row[0] for row in changes_result]

    tables_with_cow = [t.replace("_base", "") for t in base_tables]
    enabled = len(base_tables) > 0

    return {
        "enabled": enabled,
        "tables_with_cow": tables_with_cow,
        "changes_tables": changes_tables,
        "cow_functions_deployed": cow_functions_deployed,
    }
