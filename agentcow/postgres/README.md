# agent-cow — PostgreSQL

PostgreSQL backend for [agent-cow](../../README.md). Provides database-level Copy-On-Write via PL/pgSQL functions, change-tracking tables, and views — works with any async PostgreSQL driver.

## Installation

```bash
pip install agent-cow
```

Requires Python 3.10+ and PostgreSQL 14+.

For SQLAlchemy support:

```bash
pip install agent-cow[sqlalchemy]
```

## Quick Start

### 1. Create an executor

`agent-cow` works with any async PostgreSQL driver through the `Executor` protocol — just wrap your connection with an `async execute(sql) -> list[tuple]` method:

```python
# asyncpg
class AsyncpgExecutor:
    def __init__(self, conn):
        self._conn = conn

    async def execute(self, sql: str) -> list[tuple]:
        return [tuple(r) for r in await self._conn.fetch(sql)]


# SQLAlchemy AsyncSession
class SAExecutor:
    def __init__(self, session):
        self._s = session

    async def execute(self, sql: str) -> list[tuple]:
        from sqlalchemy import text
        result = await self._s.execute(text(sql))
        return [tuple(row) for row in result.fetchall()] if result.returns_rows else []
```

### 2. One-time setup

Deploy the COW functions to your database and enable COW on your tables:

```python
from agentcow.postgres import deploy_cow_functions, enable_cow_schema

executor = AsyncpgExecutor(conn)

await deploy_cow_functions(executor)
await enable_cow_schema(executor, schema="public", exclude={"alembic_version"})
# Returns: ["users", "orders", "products", ...]
```

If you only need COW on specific tables:

```python
from agentcow.postgres import enable_cow

await enable_cow(executor, "users")
await enable_cow(executor, "orders")
```

### 3. Run an agent session

Each agent session gets a `session_id`. Each discrete action the agent takes gets an `operation_id`. These need to be set on the database connection before the agent runs queries — typically by passing them as HTTP request headers from your agent orchestrator to your backend, where middleware applies them to the connection.

For example, your agent sends headers like:

```
X-Cow-Session-Id: 550e8400-e29b-41d4-a716-446655440000
X-Cow-Operation-Id: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
```

And your backend middleware calls `apply_cow_variables` on every request (see [Web Framework Integration](#web-framework-integration) below). The agent rotates the `operation_id` for each discrete action so you can review and cherry-pick changes later.

Here's the core mechanism — however you pass the IDs, this is what happens on the database side:

```python
import uuid
from agentcow.postgres import apply_cow_variables

session_id = uuid.uuid4()

operation_id = uuid.uuid4()
await apply_cow_variables(executor, session_id, operation_id)

# Agent runs its queries as normal — INSERT, UPDATE, DELETE all go
# to the changes table. SELECT merges base data with session changes.
await executor.execute("INSERT INTO users (name, email) VALUES ('Bessie', 'bessie@sunnymeadow.farm')")

# For the next action, rotate the operation_id
operation_id = uuid.uuid4()
await apply_cow_variables(executor, session_id, operation_id)

await executor.execute("UPDATE users SET email = 'bessie@rollinghills.farm' WHERE name = 'Bessie'")
```

Production data is untouched. Other sessions see only the base data.

### 4. Review and commit

After the session, inspect what the agent did and selectively commit or discard:

```python
from agentcow.postgres import (
    get_session_operations,
    get_operation_dependencies,
    commit_cow_operations,
    discard_cow_operations,
    commit_cow_session,
)

ops = await get_session_operations(executor, session_id)
deps = await get_operation_dependencies(executor, session_id)
# ops:  [UUID('aaa...'), UUID('bbb...'), UUID('ccc...')]
# deps: [(UUID('aaa...'), UUID('bbb...')), ...]  — bbb depends on aaa

# Cherry-pick: commit the good operations, discard the rest
await commit_cow_operations(executor, "users", session_id, [ops[0]])
await discard_cow_operations(executor, "users", session_id, [ops[1], ops[2]])

# Or commit everything at once
await commit_cow_session(executor, "users", session_id)
```

## How It Works

1. **Renames your table** from `users` to `users_base`
2. **Creates a changes table** `users_changes` to store session-specific modifications
3. **Creates a COW view** named `users` that merges base + changes
4. **Your code doesn't change** — queries still target `users` (now a view)

When you set `app.session_id` and `app.operation_id` via `SET LOCAL`, all writes go to the changes table. Reads automatically merge base data with your session's changes. Other sessions (and production) see only the base data. This all happens at the SQL layer — no application-level query routing required.

See the [interactive demo](https://www.agent-cow.com) for a worked example of a farm inventory management system where an agent makes both good and bad decisions.

## Web Framework Integration

Here's how to wire `agent-cow` into a FastAPI app so that any request carrying COW headers writes to an isolated session:

```python
import uuid
from fastapi import FastAPI, Request
from agentcow.postgres import apply_cow_variables

app = FastAPI()

@app.middleware("http")
async def cow_middleware(request: Request, call_next):
    session_id = request.headers.get("x-cow-session-id")
    operation_id = request.headers.get("x-cow-operation-id")

    if session_id:
        executor = get_executor_from_request(request)  # your DB connection helper
        await apply_cow_variables(
            executor,
            session_id=uuid.UUID(session_id),
            operation_id=uuid.UUID(operation_id) if operation_id else uuid.uuid4(),
        )

    response = await call_next(request)
    return response
```

You can also build the `SET LOCAL` statements directly for use in raw connection middleware:

```python
from agentcow.postgres import build_cow_variable_statements

stmts = build_cow_variable_statements(session_id, operation_id)
# ["SET LOCAL app.session_id = '...'", "SET LOCAL app.operation_id = '...'"]
for stmt in stmts:
    await conn.execute(stmt)
```

## API Reference

### Setup (one-time)

| Function | Description |
|----------|-------------|
| `deploy_cow_functions(executor)` | Deploy COW PL/pgSQL functions to the database |
| `enable_cow(executor, table_name, *, pk_cols=None, schema="public")` | Enable COW on a single table. Primary keys are auto-detected if not provided |
| `enable_cow_schema(executor, *, schema="public", exclude=None)` | Enable COW on all user tables in a schema. Returns list of enabled table names |

### Per-Request

| Function | Description |
|----------|-------------|
| `apply_cow_variables(executor, session_id, operation_id=None, visible_operations=None)` | Set COW session variables for the current transaction |
| `reset_cow_variables(executor)` | Reset all COW session variables to defaults |
| `build_cow_variable_statements(session_id, operation_id=None, visible_operations=None)` | Build raw `SET LOCAL` SQL strings (for use without an executor) |

### Review

| Function | Description |
|----------|-------------|
| `get_session_operations(executor, session_id, *, schema="public")` | List all operation UUIDs in a session |
| `get_operation_dependencies(executor, session_id, *, schema="public")` | Get `(depends_on, operation_id)` pairs for a session |
| `set_visible_operations(executor, operation_ids)` | Filter which operations' changes are visible in subsequent reads |
| `get_cow_status(executor, *, schema="public")` | Get COW status: deployed functions, enabled tables, changes tables |
| `is_cow_enabled(executor, config, *, schema="public")` | Check if COW is both requested and properly configured |

### Commit / Discard

| Function | Description |
|----------|-------------|
| `commit_cow_session(executor, table_name, session_id, *, pk_cols=None, schema="public")` | Commit all session changes to the base table |
| `discard_cow_session(executor, table_name, session_id, *, schema="public")` | Discard all session changes |
| `commit_cow_operations(executor, table_name, session_id, operation_ids, *, pk_cols=None, schema="public")` | Commit specific operations (cherry-pick) |
| `discard_cow_operations(executor, table_name, session_id, operation_ids, *, schema="public")` | Discard specific operations |

### Teardown

| Function | Description |
|----------|-------------|
| `disable_cow(executor, table_name, *, schema="public")` | Disable COW on a table, restoring the original base table |
| `disable_cow_schema(executor, *, schema="public", exclude=None)` | Disable COW on all COW-enabled tables in a schema |

### Types

| Type | Description |
|------|-------------|
| `Executor` | Protocol — any object with `async execute(sql: str) -> list[tuple]` |
| `CowRequestConfig` | Dataclass with `agent_session_id`, `operation_id`, `visible_operations` fields |
| `CowStatus` | TypedDict with `enabled`, `tables_with_cow`, `changes_tables`, `cow_functions_deployed` fields |
