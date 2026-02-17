"""
Basic usage example for agent-cow.

This example demonstrates:
1. Setting up COW on a simple User model
2. Making changes in an isolated session
3. Committing those changes to the base table

Requirements:
- PostgreSQL running locally
- Database created: CREATE DATABASE agent_cow_example;
- Install: pip install agent-cow
"""

import asyncio
import uuid
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

from agent_cow import (
    deploy_cow_functions,
    enable_cow_for_model,
    commit_cow_session,
    discard_cow_session,
    apply_cow_variables_async,
    get_cow_status,
)

# Database configuration
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost/agent_cow_example"

# Define SQLAlchemy model
Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100))


# Create async session factory
engine = create_async_engine(DATABASE_URL, echo=True)
async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def setup_database():
    """Create tables and deploy COW functions."""
    print("\n=== Setting up database ===")

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Deploy COW functions and enable COW on User model
    async with async_session_maker() as session:
        print("Deploying COW functions...")
        await deploy_cow_functions(session)
        await session.commit()

        print("Enabling COW for User model...")
        await enable_cow_for_model(session, User, schema="public")
        await session.commit()

        # Check status
        status = await get_cow_status(session, "public")
        print(f"COW Status: {status}")

    print("✓ Database setup complete\n")


async def demonstrate_session_isolation():
    """Demonstrate isolated sessions that don't affect each other."""
    print("\n=== Demonstrating Session Isolation ===")

    # Create two different session IDs
    session_a_id = uuid.uuid4()
    session_b_id = uuid.uuid4()

    # Session A: Create a user
    print(f"\nSession A (ID: {session_a_id})")
    async with async_session_maker() as session:
        await apply_cow_variables_async(session, session_a_id, uuid.uuid4())

        user_a = User(name="Alice", email="alice@example.com")
        session.add(user_a)
        await session.commit()
        print(f"  Created user: {user_a.name}")

    # Session B: Create a different user
    print(f"\nSession B (ID: {session_b_id})")
    async with async_session_maker() as session:
        await apply_cow_variables_async(session, session_b_id, uuid.uuid4())

        user_b = User(name="Bob", email="bob@example.com")
        session.add(user_b)
        await session.commit()
        print(f"  Created user: {user_b.name}")

    # Check what each session sees
    print("\nSession A's view:")
    async with async_session_maker() as session:
        await apply_cow_variables_async(session, session_a_id, uuid.uuid4())

        from sqlalchemy import select

        result = await session.execute(select(User))
        users = result.scalars().all()
        print(f"  Sees {len(users)} user(s): {[u.name for u in users]}")

    print("\nSession B's view:")
    async with async_session_maker() as session:
        await apply_cow_variables_async(session, session_b_id, uuid.uuid4())

        result = await session.execute(select(User))
        users = result.scalars().all()
        print(f"  Sees {len(users)} user(s): {[u.name for u in users]}")

    # Production view (no session) - should see no users yet
    print("\nProduction view (no COW session):")
    async with async_session_maker() as session:
        from sqlalchemy import select

        result = await session.execute(select(User))
        users = result.scalars().all()
        print(f"  Sees {len(users)} user(s)")

    # Commit session A
    print(f"\n✓ Committing Session A...")
    async with async_session_maker() as session:
        await commit_cow_session(session, User, session_a_id, schema="public")
        await session.commit()

    # Check production view again
    print("\nProduction view after committing Session A:")
    async with async_session_maker() as session:
        from sqlalchemy import select

        result = await session.execute(select(User))
        users = result.scalars().all()
        print(f"  Sees {len(users)} user(s): {[u.name for u in users]}")

    # Discard session B
    print(f"\n✓ Discarding Session B...")
    async with async_session_maker() as session:
        await discard_cow_session(session, User, session_b_id, schema="public")
        await session.commit()

    print("✓ Session isolation demo complete\n")


async def demonstrate_commit_discard():
    """Demonstrate committing and discarding changes."""
    print("\n=== Demonstrating Commit and Discard ===")

    session_id = uuid.uuid4()

    # Make some changes
    print(f"Making changes in session {session_id}...")
    async with async_session_maker() as session:
        await apply_cow_variables_async(session, session_id, uuid.uuid4())

        user = User(name="Charlie", email="charlie@example.com")
        session.add(user)
        await session.commit()
        print(f"  Created user: {user.name}")

    # Check production - should not see Charlie yet
    print("\nProduction view (before commit):")
    async with async_session_maker() as session:
        from sqlalchemy import select

        result = await session.execute(select(User).filter(User.name == "Charlie"))
        users = result.scalars().all()
        print(f"  Charlie exists: {len(users) > 0}")

    # Commit the session
    print(f"\n✓ Committing session...")
    async with async_session_maker() as session:
        await commit_cow_session(session, User, session_id, schema="public")
        await session.commit()

    # Check production again
    print("\nProduction view (after commit):")
    async with async_session_maker() as session:
        from sqlalchemy import select

        result = await session.execute(select(User).filter(User.name == "Charlie"))
        users = result.scalars().all()
        print(f"  Charlie exists: {len(users) > 0}")

    print("✓ Commit/discard demo complete\n")


async def cleanup():
    """Clean up the database."""
    print("\n=== Cleaning up ===")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    print("✓ Database cleaned up\n")


async def main():
    """Run all examples."""
    print("\n" + "=" * 60)
    print("  agent-cow Basic Usage Example")
    print("=" * 60)

    try:
        await setup_database()
        await demonstrate_session_isolation()
        await demonstrate_commit_discard()
    except Exception as e:
        print(f"\n❌ Error: {e}")
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
