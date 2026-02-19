"""
SQLAlchemy integration example for agent-cow.

This file serves two purposes:

    1. Production-ready utilities for integrating agent-cow with SQLAlchemy,
       covering the full gap between the driver-agnostic library API and what
       a real deployment needs. Each section is standalone -- copy what you need.

    2. A runnable demo (at the bottom) that shows session isolation, commit,
       and discard using those utilities.

Sections:
    1. Executor adapter           -- wraps AsyncSession for agent-cow
    2. Session listener            -- re-applies SET LOCAL after every commit
    3. COW session context manager -- recommended entry point
    4. Request header parsing      -- extracts COW config from HTTP headers
    5. Model-aware enable/disable  -- topological FK ordering
    6. Schema-level commit/discard -- auto-detects dirty tables
    7. Migration helpers           -- disable COW before, re-enable after
    8. Runnable demo               -- python -m agentcow.postgres.examples.sqlalchemy_example

Requirements:
    pip install agent-cow sqlalchemy asyncpg
    A running PostgreSQL instance with: CREATE DATABASE agent_cow_example;
"""

from __future__ import annotations

import asyncio
import collections
import logging
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

from sqlalchemy import Column, ForeignKey, Integer, String, event, inspect, select, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, relationship

from agentcow.postgres import (
    Executor,
    apply_cow_variables,
    build_cow_variable_statements,
    commit_cow_session,
    deploy_cow_functions,
    disable_cow,
    discard_cow_session,
    enable_cow,
    get_cow_status,
)

logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost/agent_cow_example"


# =============================================================================
# Models
# =============================================================================


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    projects = relationship("Project", back_populates="owner")


class Project(Base):
    __tablename__ = "projects"
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    title = Column(String(200), nullable=False)
    owner = relationship("User", back_populates="projects")


ALL_MODELS: list[type[Base]] = [User, Project]


# =============================================================================
# 1. Executor adapter
# =============================================================================


class SAExecutor:
    """Wraps a SQLAlchemy AsyncSession to satisfy the agent-cow Executor protocol."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def execute(self, sql: str) -> list[tuple[Any, ...]]:
        result = await self._session.execute(text(sql))
        if result.returns_rows:
            return [tuple(row) for row in result.fetchall()]
        return []


assert isinstance(SAExecutor.__new__(SAExecutor), Executor)


# =============================================================================
# 2. Session listener
# =============================================================================
# SET LOCAL is transaction-scoped in PostgreSQL. After commit(), the session
# variables are lost. This listener re-applies them on every new transaction.
# =============================================================================


def _apply_cow_variables_sync(
    connection,
    session_id: uuid.UUID,
    operation_id: uuid.UUID | None = None,
    visible_operations: list[uuid.UUID] | None = None,
) -> None:
    for stmt in build_cow_variable_statements(
        session_id, operation_id, visible_operations
    ):
        connection.execute(text(stmt))


def setup_cow_session_listener(
    session: AsyncSession,
    session_id: uuid.UUID,
    operation_id: uuid.UUID | None = None,
    visible_operations: list[uuid.UUID] | None = None,
) -> None:
    """Attach an event listener that re-sets COW variables after every commit.

    Call this once per session. Every subsequent transaction on this session
    will automatically have the COW variables set.
    """

    @event.listens_for(session.sync_session, "after_begin")
    def _after_begin(sync_session, transaction, connection):
        _apply_cow_variables_sync(
            connection, session_id, operation_id, visible_operations
        )


# =============================================================================
# 3. COW session context manager
# =============================================================================


@asynccontextmanager
async def cow_session(
    session_maker: async_sessionmaker[AsyncSession],
    session_id: uuid.UUID,
    operation_id: uuid.UUID | None = None,
    visible_operations: list[uuid.UUID] | None = None,
) -> AsyncIterator[AsyncSession]:
    """Context manager that yields a SQLAlchemy session with COW variables set.

    The session listener ensures variables survive across commits. This is
    the recommended way to use agent-cow with SQLAlchemy::

        async with cow_session(maker, sid, op_id) as session:
            session.add(User(name="Bessie"))
            await session.commit()
            # COW variables are still active here
            users = await session.execute(select(User))
    """
    async with session_maker() as session:
        ex = SAExecutor(session)
        await apply_cow_variables(ex, session_id, operation_id, visible_operations)
        setup_cow_session_listener(session, session_id, operation_id, visible_operations)
        yield session


# =============================================================================
# 4. Request header parsing
# =============================================================================


@dataclass
class CowRequestConfig:
    """COW configuration parsed from HTTP request headers."""

    session_id: uuid.UUID | None = None
    operation_id: uuid.UUID | None = None
    visible_operations: list[uuid.UUID] | None = None

    @property
    def is_cow_requested(self) -> bool:
        return self.session_id is not None


def parse_cow_headers(request) -> CowRequestConfig:
    """Parse COW configuration from HTTP request headers.

    Expected headers::

        x-agent-session-id: UUID
        x-operation-id: UUID
        x-visible-operations: comma-separated UUIDs

    Invalid values are logged and treated as absent.
    Works with FastAPI, Starlette, or any object with a ``.headers`` dict.
    """
    if request is None:
        return CowRequestConfig()

    def _uuid(header: str) -> uuid.UUID | None:
        value = request.headers.get(header)
        if not value:
            return None
        try:
            return uuid.UUID(value)
        except ValueError:
            logger.warning("Invalid %s header: %r", header, value)
            return None

    def _uuid_list(header: str) -> list[uuid.UUID] | None:
        value = request.headers.get(header)
        if not value:
            return None
        try:
            return [uuid.UUID(v.strip()) for v in value.split(",") if v.strip()]
        except ValueError:
            logger.warning("Invalid %s header: %r", header, value)
            return None

    return CowRequestConfig(
        session_id=_uuid("x-agent-session-id"),
        operation_id=_uuid("x-operation-id"),
        visible_operations=_uuid_list("x-visible-operations"),
    )


# =============================================================================
# 5. Model-aware enable/disable with FK ordering
# =============================================================================


def _toposort_models(models: list[type[DeclarativeBase]]) -> list[type[DeclarativeBase]]:
    """Topologically sort models so FK parents come before children."""
    model_map: dict[str, type[DeclarativeBase]] = {}
    for m in models:
        tbl = getattr(m, "__tablename__", None)
        if tbl:
            model_map[tbl] = m

    deps: dict[type, set[type]] = collections.defaultdict(set)
    for model in models:
        for col in inspect(model).columns:
            for fk in col.foreign_keys:
                parts = fk.target_fullname.split(".")
                if len(parts) >= 2:
                    target = parts[-2]
                    if target in model_map and model_map[target] is not model:
                        deps[model].add(model_map[target])

    result: list[type[DeclarativeBase]] = []
    visited: set[type] = set()
    visiting: set[type] = set()

    def visit(node: type):
        if node in visiting:
            return
        if node not in visited:
            visiting.add(node)
            for dep in deps[node]:
                visit(dep)
            visiting.discard(node)
            visited.add(node)
            result.append(node)

    for m in models:
        visit(m)
    return result


async def enable_cow_for_models(
    session: AsyncSession,
    models: list[type[DeclarativeBase]],
    schema: str = "public",
) -> list[str]:
    """Enable COW on multiple models in FK-safe order.

    Returns the list of table names that were enabled.
    """
    ex = SAExecutor(session)
    enabled = []
    for model in _toposort_models(models):
        table_name = model.__tablename__
        pk_cols = [col.name for col in inspect(model).primary_key]
        if not pk_cols:
            raise ValueError(f"Model {model.__name__} has no primary key")
        await enable_cow(ex, table_name, pk_cols=pk_cols, schema=schema)
        enabled.append(table_name)
    return enabled


async def disable_cow_for_models(
    session: AsyncSession,
    models: list[type[DeclarativeBase]],
    schema: str = "public",
) -> list[str]:
    """Disable COW on multiple models. Returns table names that were disabled."""
    ex = SAExecutor(session)
    disabled = []
    for model in models:
        await disable_cow(ex, model.__tablename__, schema=schema)
        disabled.append(model.__tablename__)
    return disabled


# =============================================================================
# 6. Schema-level commit/discard with dirty-table detection
# =============================================================================


async def commit_cow_session_all(
    session: AsyncSession,
    session_id: uuid.UUID,
    models: list[type[DeclarativeBase]],
    schema: str = "public",
) -> list[str]:
    """Commit a COW session across all models that have changes.

    Discovers which tables have pending changes, topologically sorts them,
    and commits in FK-safe order. Returns table names that were committed.
    """
    ex = SAExecutor(session)

    result = await session.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name LIKE '%\\_changes'"
        ),
        {"schema": schema},
    )
    existing_changes = {row[0] for row in result}

    dirty_models = []
    for model in models:
        changes_table = f"{model.__tablename__}_changes"
        if changes_table not in existing_changes:
            continue
        has_rows = await session.execute(
            text(
                f'SELECT 1 FROM "{schema}"."{changes_table}" '
                f"WHERE session_id = :sid LIMIT 1"
            ),
            {"sid": session_id},
        )
        if has_rows.scalar():
            dirty_models.append(model)

    if not dirty_models:
        return []

    committed = []
    for model in _toposort_models(dirty_models):
        table_name = model.__tablename__
        pk_cols = [col.name for col in inspect(model).primary_key]
        await commit_cow_session(
            ex, table_name, session_id, pk_cols=pk_cols, schema=schema
        )
        committed.append(table_name)
    return committed


async def discard_cow_session_all(
    session: AsyncSession,
    session_id: uuid.UUID,
    schema: str = "public",
) -> None:
    """Discard a COW session across all changes tables in a schema."""
    result = await session.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name LIKE '%\\_changes'"
        ),
        {"schema": schema},
    )
    for (changes_table,) in result:
        await session.execute(
            text(
                f'DELETE FROM "{schema}"."{changes_table}" WHERE session_id = :sid'
            ),
            {"sid": session_id},
        )


# =============================================================================
# 7. Migration helpers
# =============================================================================
# COW replaces tables with views. ALTER TABLE on a view fails, so you must
# disable COW before migrations and re-enable after.
# =============================================================================


async def disable_cow_before_migration(
    session: AsyncSession,
    models: list[type[DeclarativeBase]],
    schema: str = "public",
) -> bool:
    """Disable COW if enabled. Returns True if COW was active."""
    ex = SAExecutor(session)
    status = await get_cow_status(ex, schema=schema)
    if not status["enabled"]:
        return False
    await disable_cow_for_models(session, models, schema=schema)
    await session.commit()
    return True


async def reenable_cow_after_migration(
    session: AsyncSession,
    models: list[type[DeclarativeBase]],
    schema: str = "public",
) -> None:
    """Re-enable COW after a migration. Call only if disable returned True."""
    ex = SAExecutor(session)
    await deploy_cow_functions(ex)
    await enable_cow_for_models(session, models, schema=schema)
    await session.commit()


# =============================================================================
# Usage patterns (commented)
# =============================================================================
#
# --- FastAPI dependency ---
#
#     from fastapi import FastAPI, Request, Depends
#
#     app = FastAPI()
#     engine = create_async_engine("postgresql+asyncpg://...")
#     session_maker = async_sessionmaker(engine, expire_on_commit=False)
#
#     async def get_db(request: Request):
#         config = parse_cow_headers(request)
#         if config.is_cow_requested:
#             async with cow_session(
#                 session_maker,
#                 config.session_id,
#                 config.operation_id,
#                 config.visible_operations,
#             ) as session:
#                 yield session
#         else:
#             async with session_maker() as session:
#                 yield session
#
#     @app.post("/api/users")
#     async def create_user(session: AsyncSession = Depends(get_db)):
#         session.add(User(name="Bessie"))
#         await session.commit()
#         return {"status": "ok"}
#
# --- Alembic env.py ---
#
#     async def run_async_migrations():
#         async with connectable.connect() as connection:
#             async with AsyncSession(bind=connection) as session:
#                 was_enabled = await disable_cow_before_migration(
#                     session, all_models, schema="my_schema"
#                 )
#             await connection.run_sync(do_run_migrations)
#             if was_enabled:
#                 async with AsyncSession(bind=connection) as session:
#                     await reenable_cow_after_migration(
#                         session, all_models, schema="my_schema"
#                     )


# =============================================================================
# 8. Runnable demo
# =============================================================================


engine = create_async_engine(DATABASE_URL)
session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def setup_database():
    print("\n--- Setting up database ---")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_maker() as session:
        ex = SAExecutor(session)
        await deploy_cow_functions(ex)
        await session.commit()

        enabled = await enable_cow_for_models(session, ALL_MODELS)
        await session.commit()
        print(f"  COW enabled on: {enabled}")

        status = await get_cow_status(ex)
        print(f"  Status: {status}")


async def demo_session_isolation():
    print("\n--- Session isolation ---")

    sid_a = uuid.uuid4()
    sid_b = uuid.uuid4()

    async with cow_session(session_maker, sid_a, uuid.uuid4()) as session:
        session.add(User(name="Bessie", email="bessie@greenacres.farm"))
        await session.commit()
        print(f"  Session A created Bessie")

    async with cow_session(session_maker, sid_b, uuid.uuid4()) as session:
        session.add(User(name="Clyde", email="clyde@greenacres.farm"))
        await session.commit()
        print(f"  Session B created Clyde")

    async with cow_session(session_maker, sid_a, uuid.uuid4()) as session:
        users = (await session.execute(select(User))).scalars().all()
        print(f"  Session A sees: {[u.name for u in users]}")

    async with cow_session(session_maker, sid_b, uuid.uuid4()) as session:
        users = (await session.execute(select(User))).scalars().all()
        print(f"  Session B sees: {[u.name for u in users]}")

    async with session_maker() as session:
        users = (await session.execute(select(User))).scalars().all()
        print(f"  Production sees: {[u.name for u in users]}")

    async with session_maker() as session:
        committed = await commit_cow_session_all(session, sid_a, ALL_MODELS)
        await session.commit()
        print(f"  Committed session A ({committed})")

    async with session_maker() as session:
        await discard_cow_session_all(session, sid_b)
        await session.commit()
        print(f"  Discarded session B")

    async with session_maker() as session:
        users = (await session.execute(select(User))).scalars().all()
        print(f"  Production now sees: {[u.name for u in users]}")


async def demo_cross_table():
    print("\n--- Cross-table COW (User + Project) ---")

    sid = uuid.uuid4()
    op_id = uuid.uuid4()

    async with cow_session(session_maker, sid, op_id) as session:
        result = await session.execute(
            select(User).where(User.name == "Bessie")
        )
        bessie = result.scalar_one()
        session.add(Project(owner_id=bessie.id, title="Pasture Expansion"))
        await session.commit()
        print(f"  Created project for Bessie in COW session")

    async with session_maker() as session:
        projects = (await session.execute(select(Project))).scalars().all()
        print(f"  Production projects: {[p.title for p in projects]}")

    async with session_maker() as session:
        committed = await commit_cow_session_all(session, sid, ALL_MODELS)
        await session.commit()
        print(f"  Committed ({committed})")

    async with session_maker() as session:
        projects = (await session.execute(select(Project))).scalars().all()
        print(f"  Production projects: {[p.title for p in projects]}")


async def cleanup():
    async with session_maker() as session:
        await disable_cow_for_models(session, ALL_MODELS)
        await session.commit()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    print("\n--- Cleaned up ---")


async def main():
    print("=" * 60)
    print("  agent-cow SQLAlchemy Example")
    print("=" * 60)
    try:
        await setup_database()
        await demo_session_isolation()
        await demo_cross_table()
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await cleanup()
        await engine.dispose()
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
