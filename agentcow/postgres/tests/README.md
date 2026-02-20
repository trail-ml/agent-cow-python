# PostgreSQL COW Tests

## Setup

Start a local Postgres container:

```bash
./agentcow/postgres/tests/start-test-db.sh
```

This creates a Docker container (`agent-cow-pg`) with Postgres 18 on port 5432.
The script is idempotent — it reuses an existing container if one already exists.

## Running tests

```bash
uv run pytest agentcow/postgres/tests/
```

## Connecting to the test database

From another terminal:

```bash
docker exec -it agent-cow-pg psql -U postgres -d agent_cow_test
```

Or from your host (if `psql` is installed locally):

```bash
psql -h localhost -p 5432 -U postgres -d agent_cow_test
```

You can also connect with any GUI tool (TablePlus, DBeaver, pgAdmin, etc.)
using `localhost:5432`, user `postgres`, password `postgres`.

## Useful psql commands

### Listing objects

| Command      | What it shows                                     |
| ------------ | ------------------------------------------------- |
| `\dt`        | All tables (`_base` and `_changes` tables)        |
| `\dv`        | All views (the COW overlay views)                 |
| `\df`        | All functions (COW trigger + management functions) |
| `\d <name>`  | Columns/structure of a specific table or view     |
| `\di`        | All indexes                                       |
| `\x`         | Toggle expanded row display (for wide tables)     |
| `\q`         | Quit psql                                         |

### Inspecting COW state

```sql
-- Raw base table data (bypasses COW)
SELECT * FROM items_base;

-- COW changes overlay
SELECT * FROM items_changes;

-- Merged view (what the application sees)
SELECT * FROM items;

-- Which sessions have pending changes
SELECT DISTINCT session_id, operation_id FROM items_changes;

-- Soft-deleted rows
SELECT * FROM items_changes WHERE _cow_deleted = true;
```

### Simulating a COW session in psql

```sql
SET app.session_id = 'some-uuid-here';
SET app.operation_id = 'some-uuid-here';

-- Now the view returns session-specific data
SELECT * FROM items;

-- Clear session
RESET app.session_id;
RESET app.operation_id;
```

## Debugging mid-test

Add a `breakpoint()` in any test to pause execution while the database is still alive:

```python
@pytest.mark.asyncio
async def test_something(executor, postgresql):
    await deploy_cow_functions(executor)
    # ...
    info = postgresql.info
    print(f"psql -h {info.host} -p {info.port} -d {info.dbname} -U {info.user}")
    breakpoint() # Or just a breakpoint in your IDE. Use the debugger to check values, tables, etc
```

Then connect from another terminal using the printed `psql` command.

## Environment variables

All connection defaults can be overridden:

| Variable      | Default            |
| ------------- | ------------------ |
| `PG_HOST`     | `localhost`        |
| `PG_PORT`     | `5432`             |
| `PG_USER`     | `postgres`         |
| `PG_PASSWORD` | `postgres`         |
| `PG_DBNAME`   | `agent_cow_test`   |
