"""
Basic usage example for agent-cow with SQLAlchemy.

This example demonstrates:
1. Creating a simple SQLAlchemy adapter for the Executor protocol
2. Setting up COW on a table
3. Making changes in an isolated session
4. Committing / discarding those changes

Requirements:
- PostgreSQL running locally
- Database created: CREATE DATABASE agent_cow_example;
- Install: pip install agent-cow[sqlalchemy]
"""

import asyncio
import uuid

from sqlalchemy import Column, Integer, String, text, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

from agentcow.postgres.core import (
    Executor,
    deploy_cow_functions,
    enable_cow,
    commit_cow_session,
    discard_cow_session,
    apply_cow_variables,
    get_cow_status,
)

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost/agent_cow_example"

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100))


engine = create_async_engine(DATABASE_URL, echo=True)
async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# ---------------------------------------------------------------------------
# SQLAlchemy adapter — the only glue code needed
# ---------------------------------------------------------------------------


class SAExecutor:
    """Wraps an AsyncSession to satisfy the agent-cow Executor protocol."""

    def __init__(self, session: AsyncSession):
        self._s = session

    async def execute(self, sql: str) -> list[tuple]:
        result = await self._s.execute(text(sql))
        if result.returns_rows:
            return [tuple(row) for row in result.fetchall()]
        return []


assert isinstance(SAExecutor.__new__(SAExecutor), Executor)


# ---------------------------------------------------------------------------
# Demo helpers
# ---------------------------------------------------------------------------


async def setup_database():
    print("\n=== Setting up database ===")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session_maker() as session:
        ex = SAExecutor(session)

        print("Deploying COW functions...")
        await deploy_cow_functions(ex)
        await session.commit()

        print("Enabling COW for users table...")
        await enable_cow(ex, "users")
        await session.commit()

        status = await get_cow_status(ex)
        print(f"COW Status: {status}")

    print("Setup complete\n")


async def demonstrate_session_isolation():
    print("\n=== Demonstrating Session Isolation ===")

    session_a_id = uuid.uuid4()
    session_b_id = uuid.uuid4()

    print(f"\nSession A ({session_a_id})")
    async with async_session_maker() as session:
        ex = SAExecutor(session)
        await apply_cow_variables(ex, session_a_id, uuid.uuid4())

        user_a = User(name="Alice", email="alice@example.com")
        session.add(user_a)
        await session.commit()
        print(f"  Created user: {user_a.name}")

    print(f"\nSession B ({session_b_id})")
    async with async_session_maker() as session:
        ex = SAExecutor(session)
        await apply_cow_variables(ex, session_b_id, uuid.uuid4())

        user_b = User(name="Bob", email="bob@example.com")
        session.add(user_b)
        await session.commit()
        print(f"  Created user: {user_b.name}")

    print("\nSession A's view:")
    async with async_session_maker() as session:
        ex = SAExecutor(session)
        await apply_cow_variables(ex, session_a_id, uuid.uuid4())
        result = await session.execute(select(User))
        users = result.scalars().all()
        print(f"  Sees {len(users)} user(s): {[u.name for u in users]}")

    print("\nSession B's view:")
    async with async_session_maker() as session:
        ex = SAExecutor(session)
        await apply_cow_variables(ex, session_b_id, uuid.uuid4())
        result = await session.execute(select(User))
        users = result.scalars().all()
        print(f"  Sees {len(users)} user(s): {[u.name for u in users]}")

    print("\nProduction view (no COW session):")
    async with async_session_maker() as session:
        result = await session.execute(select(User))
        users = result.scalars().all()
        print(f"  Sees {len(users)} user(s)")

    print(f"\nCommitting Session A...")
    async with async_session_maker() as session:
        ex = SAExecutor(session)
        await commit_cow_session(ex, "users", session_a_id)
        await session.commit()

    print("\nProduction view after committing Session A:")
    async with async_session_maker() as session:
        result = await session.execute(select(User))
        users = result.scalars().all()
        print(f"  Sees {len(users)} user(s): {[u.name for u in users]}")

    print(f"\nDiscarding Session B...")
    async with async_session_maker() as session:
        ex = SAExecutor(session)
        await discard_cow_session(ex, "users", session_b_id)
        await session.commit()

    print("Session isolation demo complete\n")


async def demonstrate_commit_discard():
    print("\n=== Demonstrating Commit and Discard ===")

    session_id = uuid.uuid4()

    print(f"Making changes in session {session_id}...")
    async with async_session_maker() as session:
        ex = SAExecutor(session)
        await apply_cow_variables(ex, session_id, uuid.uuid4())
        user = User(name="Charlie", email="charlie@example.com")
        session.add(user)
        await session.commit()
        print(f"  Created user: {user.name}")

    print("\nProduction view (before commit):")
    async with async_session_maker() as session:
        result = await session.execute(select(User).filter(User.name == "Charlie"))
        users = result.scalars().all()
        print(f"  Charlie exists: {len(users) > 0}")

    print(f"\nCommitting session...")
    async with async_session_maker() as session:
        ex = SAExecutor(session)
        await commit_cow_session(ex, "users", session_id)
        await session.commit()

    print("\nProduction view (after commit):")
    async with async_session_maker() as session:
        result = await session.execute(select(User).filter(User.name == "Charlie"))
        users = result.scalars().all()
        print(f"  Charlie exists: {len(users) > 0}")

    print("Commit/discard demo complete\n")


async def cleanup():
    print("\n=== Cleaning up ===")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    print("Database cleaned up\n")


async def main():
    print("\n" + "=" * 60)
    print("  agent-cow Basic Usage Example")
    print("=" * 60)

    try:
        await setup_database()
        await demonstrate_session_isolation()
        await demonstrate_commit_discard()
    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await cleanup()
        await engine.dispose()

    print("=" * 60)
    print("  Example Complete!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
