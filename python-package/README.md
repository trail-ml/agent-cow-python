# agent-cow (Python / PostgreSQL)

**PostgreSQL Copy-On-Write for AI agent workspace isolation**

[![PyPI](https://img.shields.io/pypi/v/agent-cow.svg)](https://pypi.org/project/agent-cow/)
[![Python Versions](https://img.shields.io/pypi/pyversions/agent-cow.svg)](https://pypi.org/project/agent-cow/)

Full server-side COW support via SQLAlchemy, PostgreSQL views, and triggers. See the [root README](../README.md) for an overview of the project.

## Installation

```bash
pip install agent-cow
```

Requires Python 3.10+ and a PostgreSQL database.

## Quick Start

```python
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from agent_cow import (
    deploy_cow_functions,
    enable_cow_for_model,
    commit_cow_session,
    apply_cow_variables_async,
)

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def setup():
    async with async_session() as session:
        await deploy_cow_functions(session)
        await session.commit()

        await enable_cow_for_model(session, User)
        await session.commit()

async def agent_session():
    session_id = uuid.uuid4()
    operation_id = uuid.uuid4()

    async with async_session() as session:
        await apply_cow_variables_async(session, session_id, operation_id)

        user = User(name="Test User")
        session.add(user)
        await session.commit()

    async with async_session() as session:
        await commit_cow_session(session, User, session_id)
        await session.commit()
```

## How It Works

1. **Renames your table** from `users` to `users_base`
2. **Creates a changes table** `users_changes` to store session-specific modifications
3. **Creates a COW view** named `users` that merges base + changes
4. **Your code doesn't change** -- SQLAlchemy still queries `users` (now a view)

When you set `app.session_id` and `app.operation_id` variables, all writes go to the changes table. Reads automatically merge base data with your session's changes.

## Features

### Session-Level Isolation

```python
await apply_cow_variables_async(session_a, session_id_a, op_id_a)
await apply_cow_variables_async(session_b, session_id_b, op_id_b)
```

### Operation Tracking

```python
await apply_cow_variables_async(session, session_id, op_id_1)
user = User(name="Alice")
session.add(user)
await session.commit()

await apply_cow_variables_async(session, session_id, op_id_2)
user.name = "Alice Smith"
await session.commit()

await commit_cow_operations(session, User, session_id, [op_id_1])
```

### Dependency Detection

```python
deps = await get_operation_dependencies(session, session_id, schema)
```

### Atomic Commit or Discard

```python
await commit_cow_session(session, User, session_id)
await discard_cow_session(session, User, session_id)
```

## API Reference

### Core Functions

- `deploy_cow_functions(session)` -- Deploy COW SQL functions (one-time setup)
- `enable_cow_for_model(session, model)` -- Enable COW on a SQLAlchemy model
- `enable_cow_for_table(session, table)` -- Enable COW on a Table object
- `enable_cow_for_registry(session, registry)` -- Enable COW on all models in a registry
- `commit_cow_session(session, models, session_id)` -- Commit all session changes
- `discard_cow_session(session, model, session_id)` -- Discard all session changes
- `disable_cow_for_model(session, model)` -- Disable COW and restore original table

### Operation-Level Functions

- `apply_cow_variables_async(session, session_id, operation_id)` -- Set COW session vars
- `get_session_operations(session, session_id, schema)` -- List all operations in a session
- `get_operation_dependencies(session, session_id, schema)` -- Get operation dependencies
- `commit_cow_operations(session, models, session_id, operation_ids)` -- Commit specific operations
- `discard_cow_operations(session, models, session_id, operation_ids)` -- Discard specific operations

### Session Management

- `CowRequestConfig` -- Dataclass for COW configuration
- `parse_cow_headers_from_request(request)` -- Parse COW config from HTTP headers
- `setup_cow_session_listener(session, session_id, operation_id)` -- Auto-set vars on transaction start

## FastAPI Integration

```python
from fastapi import FastAPI, Request, Depends
from agent_cow import parse_cow_headers_from_request, apply_cow_variables_async

app = FastAPI()

async def get_db_session(request: Request):
    cow_config = parse_cow_headers_from_request(request)

    async with async_session() as session:
        if cow_config.is_cow_enabled:
            await apply_cow_variables_async(
                session,
                cow_config.agent_session_id,
                cow_config.operation_id,
                cow_config.visible_operations
            )
        yield session

@app.post("/users")
async def create_user(session: AsyncSession = Depends(get_db_session)):
    user = User(name="New User")
    session.add(user)
    await session.commit()
    return {"id": user.id}
```

Send requests with headers:

```bash
curl -X POST http://localhost:8000/users \
  -H "x-agent-session-id: 550e8400-e29b-41d4-a716-446655440000" \
  -H "x-operation-id: 660e8400-e29b-41d4-a716-446655440000"
```

## Performance

- **Read overhead**: COW views add minimal overhead (simple UNION ALL)
- **Write overhead**: Slightly slower due to trigger execution
- **Storage**: Changes tables consume additional space until committed
- **Best for**: Workloads with more reads than writes, or where isolation is critical

## Limitations

- Requires primary keys on all COW-enabled tables
- Session variables are transaction-scoped (automatically reset after commit)

## Development

```bash
cd python-package
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
pytest tests/
```
