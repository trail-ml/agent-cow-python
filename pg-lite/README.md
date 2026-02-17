# @agent-cow/pg-lite

Copy-on-Write (CoW) isolation for agentic database operations, built on [PGlite](https://github.com/electric-sql/pglite).

Agent writes go to separate **changes tables** instead of modifying production data directly. Changes can be reviewed, committed, or discarded — giving humans full control over what an AI agent persists.

## Install

```bash
npm install @agent-cow/pg-lite @electric-sql/pglite
```

## How it works

When you register a table for CoW, three objects are created in PostgreSQL:

| Object | Naming | Purpose |
|---|---|---|
| **Base table** | `{table}_base` | Production data. Protected from agent writes. |
| **Changes table** | `{table}_changes` | Stores all agent modifications with session/operation tracking. |
| **View** | `{table}` | Merges base + changes. This is what the agent reads and writes to. |

INSTEAD OF triggers on the view intercept INSERT/UPDATE/DELETE and redirect them to the changes table when a session is active. When no session is set, writes go directly to the base table.

## Usage

```typescript
import { CowManager } from '@agent-cow/pg-lite';

const cow = new CowManager({ dataDir: 'idb://my-app' });
await cow.init();

// Create your base tables (use the _base suffix)
await cow.exec(`
  CREATE TABLE users_base (
    user_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT
  );
`);

// Register the table for CoW — creates changes table, view, and triggers
await cow.setupTable('users', 'user_id');

// Start a session
const sessionId = crypto.randomUUID();
const operationId = crypto.randomUUID();

// Write through the CoW view (triggers redirect to changes table)
await cow.withSession(sessionId, operationId, async () => {
  await cow.query(
    'INSERT INTO users (user_id, name, email) VALUES ($1, $2, $3)',
    ['u1', 'Alice', 'alice@example.com']
  );
});

// Read the agent's view (base + changes merged)
await cow.withSession(sessionId, undefined, async () => {
  const users = await cow.query('SELECT * FROM users');
  console.log(users); // [{ user_id: 'u1', name: 'Alice', email: 'alice@example.com' }]
});

// Base table is untouched
const base = await cow.query('SELECT * FROM users_base');
console.log(base); // []

// Inspect pending changes
const changes = await cow.getChanges('users', sessionId);

// Commit to production
await cow.commit('users', 'user_id', sessionId, [operationId]);

// Or discard
// await cow.discard('users', sessionId, [operationId]);
```

## API

### `new CowManager(options?)`

Create a new CoW manager.

- `options.dataDir` — PGlite data directory (default: `'idb://agent-cow'`)
- `options.instance` — Provide your own PGlite instance instead of creating one

### `cow.init()`

Initialize the database connection and deploy CoW SQL functions.

### `cow.setupTable(viewName, pkColumn)`

Register an existing `{viewName}_base` table for CoW. Creates the changes table, merged view, and INSTEAD OF triggers.

### `cow.setSession(sessionId, operationId?)`

Set the active session context. Subsequent queries through the view will read/write changes for this session.

### `cow.clearSession()`

Clear the session context. Queries go directly to base tables.

### `cow.withSession(sessionId, operationId, fn)`

Run a function within a session context. Automatically clears the session afterwards.

### `cow.query<T>(sql, params?)`

Execute a SQL query and return rows.

### `cow.exec(sql)`

Execute a SQL statement (no return value).

### `cow.getChanges<T>(viewName, sessionId)`

Get all pending changes for a table in a session.

### `cow.commit(viewName, pkColumn, sessionId, operationIds)`

Merge selected operations into the base table and remove them from changes.

### `cow.commitAll(sessionId, operationIds)`

Commit operations across all registered tables.

### `cow.discard(viewName, sessionId, operationIds)`

Remove selected operations from the changes table without applying them.

### `cow.discardAll(sessionId, operationIds)`

Discard operations across all registered tables.

### `cow.getDependencies(sessionId)`

Compute operation dependencies for a session. Returns `{ depends_on, operation_id }[]`. Dependencies are detected for:
- **Same-row**: two operations touch the same primary key in the same table
- **Cross-table**: an operation references a row created by another operation via foreign key

### `cow.getRegisteredTables()`

Returns the list of tables registered for CoW.

### `cow.reset()`

Close the database and delete all IndexedDB data.

### `cow.close()`

Close the database connection.

## Raw SQL

If you need the SQL functions directly (e.g. for a non-PGlite Postgres), the raw SQL strings are exported:

```typescript
import {
  SETUP_COW_SQL,
  COMMIT_COW_OPERATIONS_SQL,
  DISCARD_COW_OPERATIONS_SQL,
  GET_COW_DEPENDENCIES_SQL,
} from '@agent-cow/pg-lite';
```

## License

MIT
