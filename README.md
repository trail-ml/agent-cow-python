# agent-cow

**Database Copy-On-Write for AI agent workspace isolation**

[![PyPI](https://img.shields.io/pypi/v/agent-cow.svg)](https://pypi.org/project/agent-cow/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Read the full article: [Copy-on-Write in Agentic Systems](https://www.trail-ml.com/blog/agent-cow)
> Try the interactive demo: [www.agent-cow.com](https://www.agent-cow.com)

## Installation

```bash
pip install agent-cow
```

Requires Python 3.10+ and a PostgreSQL database.

## Copy-on-Write for Agentic Systems

Alignment is an open problem in AI safety, and [misalignment during agent execution may not always be obvious](https://www.cold-takes.com/why-ai-alignment-could-be-hard-with-modern-deep-learning/). At best, a misaligned agent is annoying (ie. if the agent does something other than what the user wants it to do) and at worst, dangerous (i.e. leading to sensitive data loss, tool misuse, and [other harms](https://www.anthropic.com/research/agentic-misalignment)). Rather than tackling the alignment problem directly, this repo focuses on minimizing potential harm a misaligned agent can cause. 

`agent-cow` provides database-level Copy-On-Write (CoW) functionality - enabling **workspace isolation** for AI agents, and protecting production data from unintended or misaligned agent actions. When an agent makes database changes during a session, those changes are isolated and can be reviewed, then committed or discarded -- without affecting production data until the user approves.

Changes are stored in a separate table, rather than allowing agents to modify production data directly. During a session, the agent's changes appear 'merged' with the original data (it 'believes' they have been applied). At the end of a session, these changes can be visualized in a dependency graph and selectively committed by the user. 

**This approach has various benefits**: 
- **Changes can be reviewed at the end of a session**, rather than needing to repeatedly 'accept' each action as it is executed. This minimizes the direct human supervision required while improving the safeguards in place. 
- Mistakes are less consequential, since the **agent can't write directly to the main/production data**. If some changes are good but others aren't, users can cherry-pick operations they wish to keep.
- **Misalignment patterns become more visible**. When reviewing changes at the end of a session, users can clearly identify where the agent deviated from intended behavior and adjust the system prompt or agent configuration accordingly to prevent similar issues in future sessions.
- **Multiple agents or agent sessions** can run simultaneously on isolated copies without interfering with each other.

## How It Works

1. **Renames your table** from `users` to `users_base`
2. **Creates a changes table** `users_changes` to store session-specific modifications
3. **Creates a COW view** named `users` that merges base + changes
4. **Your code doesn't change** — queries still target `users` (now a view)

When you set `app.session_id` and `app.operation_id` variables, all writes go to the changes table. Reads automatically merge base data with your session's changes. Other sessions (and production) see only the base data.

See the [interactive demo](https://www.agent-cow.com) for a worked example of an inventory management system where an agent makes both good and bad decisions.

## Quick Start

```python
import uuid
from agent_cow.postgres import (
    Executor,
    deploy_cow_functions,
    enable_cow,
    commit_cow_session,
    apply_cow_variables,
    get_cow_status,
)

# Any object with an async execute(sql) -> list[tuple] method works
class MyExecutor:
    def __init__(self, conn):
        self._conn = conn
    async def execute(self, sql: str) -> list[tuple]:
        return [tuple(r) for r in await self._conn.fetch(sql)]

executor = MyExecutor(conn)

# One-time setup
await deploy_cow_functions(executor)
await enable_cow(executor, "users")

# Agent session
session_id = uuid.uuid4()
operation_id = uuid.uuid4()
await apply_cow_variables(executor, session_id, operation_id)

# ... agent makes changes ...

# Review, then commit or discard
await commit_cow_session(executor, "users", session_id)
```

## Backends

| Backend | Status |
|---------|--------|
| **PostgreSQL** | Available |
| **Blob/File Storage** | In progress |
| **MySQL** | Planned |
| **SQLite** | Planned |
| **MongoDB** | Planned |

Looking for the **TypeScript/PGlite** implementation? See [@agent-cow/pg-lite](https://github.com/trail-ml/agent-cow-pglite).

## API Reference

### Core Functions

- `deploy_cow_functions(executor)` — Deploy COW SQL functions (one-time setup)
- `enable_cow(executor, table_name)` — Enable COW on a table
- `disable_cow(executor, table_name)` — Disable COW and restore original table
- `commit_cow_session(executor, table_name, session_id)` — Commit all session changes
- `discard_cow_session(executor, table_name, session_id)` — Discard all session changes
- `get_cow_status(executor)` — Get COW status for a schema

### Operation-Level Functions

- `apply_cow_variables(executor, session_id, operation_id)` — Set COW session variables
- `get_session_operations(executor, session_id)` — List all operations in a session
- `get_operation_dependencies(executor, session_id)` — Get operation dependency graph
- `commit_cow_operations(executor, table_name, session_id, operation_ids)` — Commit specific operations
- `discard_cow_operations(executor, table_name, session_id, operation_ids)` — Discard specific operations

### Session Management

- `CowRequestConfig` — Dataclass for COW configuration
- `parse_cow_headers_from_request(request)` — Parse COW config from HTTP headers
- `build_cow_variable_statements(session_id, operation_id)` — Build SET LOCAL SQL statements

## Development

```bash
git clone https://github.com/trail-ml/agent-cow.git
cd agent-cow
pip install -e ".[dev]"
pytest agent_cow/postgres/tests/ -v
```

## Contributing

We welcome contributions! For questions, bug reports, or feature requests, please [open an issue](https://github.com/trail-ml/agent-cow/issues).

## License

MIT License.

## Credits

Created and maintained by [trail](https://trail-ml.com).
