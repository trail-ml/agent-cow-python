"""
Operation-level COW management for PostgreSQL.

This module provides fine-grained control over individual operations within
a COW session, including dependency tracking, partial commits, and selective visibility.
"""

import uuid
from typing import Type, Optional

from sqlalchemy import text, inspect, Table, MetaData, bindparam
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY, UUID as PG_UUID
from sqlalchemy.types import Text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from .core import get_tablename, _quote_ident


async def get_session_operations(
    session: AsyncSession,
    session_id: uuid.UUID,
    schema: str,
) -> list[uuid.UUID]:
    """Get all operation IDs in a COW session."""
    result = await session.execute(
        text("SELECT operation_id FROM get_cow_session_operations(:schema, :sid)"),
        {"schema": schema, "sid": session_id},
    )
    return [row[0] for row in result]


async def get_operation_dependencies(
    session: AsyncSession,
    session_id: uuid.UUID,
    schema: str,
) -> list[tuple[uuid.UUID, uuid.UUID]]:
    """Get dependency pairs between operations in a COW session."""
    result = await session.execute(
        text(
            "SELECT depends_on, operation_id FROM get_cow_dependencies(:schema, :sid)"
        ),
        {"schema": schema, "sid": session_id},
    )
    return [(row[0], row[1]) for row in result]


# Alias for backward compatibility
get_cow_dependencies = get_operation_dependencies


async def set_visible_operations(
    session: AsyncSession,
    operation_ids: list[uuid.UUID] | None,
):
    """Set which operations' changes should be visible in subsequent queries."""
    if operation_ids:
        ops_str = ",".join(str(op) for op in operation_ids)
        await session.execute(text(f"SET LOCAL app.visible_operations = '{ops_str}'"))
    else:
        await session.execute(text("SET LOCAL app.visible_operations = ''"))


async def commit_cow_operations(
    session: AsyncSession,
    models: list[Type[DeclarativeBase]] | Type[DeclarativeBase],
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID],
    schema: str = "public",
):
    """Commit specific operations from a COW session to the base tables."""
    from sqlalchemy.exc import NoInspectionAvailable
    import collections

    if not operation_ids:
        return

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
            text(
                "SELECT commit_cow_operations(:schema, :base_table, :pk_cols, :sid, :ops)"
            ).bindparams(
                bindparam("pk_cols", type_=PG_ARRAY(Text)),
                bindparam("ops", type_=PG_ARRAY(PG_UUID)),
            ),
            {
                "schema": schema,
                "base_table": base_table,
                "pk_cols": pk_cols,
                "sid": session_id,
                "ops": operation_ids,
            },
        )


async def commit_cow_table_operations(
    session: AsyncSession,
    table: Table,
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID],
    schema: str = "public",
):
    """Commit specific operations from a COW session for a Table object."""
    if not operation_ids:
        return

    table_name = table.name
    base_table = f"{table_name}_base"

    pk_cols = [col.name for col in table.primary_key.columns]
    if not pk_cols:
        raise ValueError(f"Table {table_name} has no primary key.")

    await session.execute(
        text(
            "SELECT commit_cow_operations(:schema, :base_table, :pk_cols, :sid, :ops)"
        ).bindparams(
            bindparam("pk_cols", type_=PG_ARRAY(Text)),
            bindparam("ops", type_=PG_ARRAY(PG_UUID)),
        ),
        {
            "schema": schema,
            "base_table": base_table,
            "pk_cols": pk_cols,
            "sid": session_id,
            "ops": operation_ids,
        },
    )


async def discard_cow_operations(
    session: AsyncSession,
    models: list[Type[DeclarativeBase]] | Type[DeclarativeBase],
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID],
    schema: str = "public",
):
    """Discard specific operations from a COW session."""
    if not operation_ids:
        return

    if not isinstance(models, list):
        models = [models]

    for model in models:
        table_name = get_tablename(model)
        base_table = f"{table_name}_base"

        await session.execute(
            text(
                "SELECT discard_cow_operations(:schema, :base_table, :sid, :ops)"
            ).bindparams(
                bindparam("ops", type_=PG_ARRAY(PG_UUID)),
            ),
            {
                "schema": schema,
                "base_table": base_table,
                "sid": session_id,
                "ops": operation_ids,
            },
        )


async def discard_cow_table_operations(
    session: AsyncSession,
    table: Table,
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID],
    schema: str = "public",
):
    """Discard specific operations from a COW session for a Table object."""
    if not operation_ids:
        return

    table_name = table.name
    base_table = f"{table_name}_base"

    await session.execute(
        text(
            "SELECT discard_cow_operations(:schema, :base_table, :sid, :ops)"
        ).bindparams(
            bindparam("ops", type_=PG_ARRAY(PG_UUID)),
        ),
        {
            "schema": schema,
            "base_table": base_table,
            "sid": session_id,
            "ops": operation_ids,
        },
    )


async def commit_registry_operations(
    session: AsyncSession,
    registry,
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID],
    schema: str = "public",
    metadata: Optional[MetaData] = None,
):
    """Auto-detects and commits specific operations for models that have changes."""
    if not operation_ids:
        return

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
                f"SELECT 1 FROM {_quote_ident(schema)}.{_quote_ident(changes_table)} "
                f"WHERE session_id = :sid AND operation_id = ANY(:ops) LIMIT 1"
            ),
            {"sid": session_id, "ops": operation_ids},
        )

        if has_changes.scalar():
            dirty_models.append(model)

    if dirty_models:
        await commit_cow_operations(
            session, dirty_models, session_id, operation_ids, schema
        )

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
                    f"SELECT 1 FROM {_quote_ident(schema)}.{_quote_ident(changes_table)} "
                    f"WHERE session_id = :sid AND operation_id = ANY(:ops) LIMIT 1"
                ),
                {"sid": session_id, "ops": operation_ids},
            )

            if has_changes.scalar():
                dirty_tables.append(table)

        for table in dirty_tables:
            await commit_cow_table_operations(
                session, table, session_id, operation_ids, schema
            )


async def discard_registry_operations(
    session: AsyncSession,
    registry,
    session_id: uuid.UUID,
    operation_ids: list[uuid.UUID],
    schema: str = "public",
    metadata: Optional[MetaData] = None,
):
    """Auto-detects and discards specific operations for models that have changes."""
    if not operation_ids:
        return

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
    processed_tables: set[str] = set()

    for mapper in registry.mappers:
        model = mapper.class_
        if not hasattr(model, "__tablename__"):
            continue

        table_name = get_tablename(model)
        changes_table = f"{table_name}_changes"

        if changes_table in existing_changes_tables:
            processed_tables.add(table_name)
            await discard_cow_operations(
                session, model, session_id, operation_ids, schema
            )

    if metadata is not None:
        for table in metadata.tables.values():
            table_name = table.name
            changes_table = f"{table_name}_changes"

            if (
                table_name in processed_tables
                or changes_table not in existing_changes_tables
            ):
                continue

            await discard_cow_table_operations(
                session, table, session_id, operation_ids, schema
            )
