# agent-cow

**Database Copy-On-Write for AI agent workspace isolation**

[![PyPI](https://img.shields.io/pypi/v/agent-cow.svg)](https://pypi.org/project/agent-cow/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Read the full article: [Copy-on-Write in Agentic Systems](https://www.trail-ml.com/blog/agent-cow)
> Try the interactive demo: [www.agent-cow.com](https://www.agent-cow.com)

## Copy-on-Write for Agentic Systems

Alignment is an open problem in AI safety, and [misalignment during agent execution may not always be obvious](https://www.cold-takes.com/why-ai-alignment-could-be-hard-with-modern-deep-learning/). At best, a misaligned agent is annoying (ie. if the agent does something other than what the user wants it to do) and at worst, dangerous (i.e. leading to sensitive data loss, tool misuse, and [other harms](https://www.anthropic.com/research/agentic-misalignment)). Rather than tackling the alignment problem directly, this repo focuses on minimizing potential harm a misaligned agent can cause. 

`agent-cow` provides database-level Copy-On-Write (CoW) functionality - enabling **workspace isolation** for AI agents, and protecting production data from unintended or misaligned agent actions. When an agent makes database changes during a session, those changes are isolated and can be reviewed, then committed or discarded -- without affecting production data until the user approves.

Changes are stored in a separate table, rather than allowing agents to modify production data directly. During a session, the agent's changes appear ‘merged’ with the original data (it ‘believes’ they have been applied). At the end of a session, these changes can be visualized in a dependency graph and selectively committed by the user. 

**This approach has various benefits**: 
- **Changes can be reviewed at the end of a session**, rather than needing to repeatedly ‘accept’ each action as it is executed. This minimizes the direct human supervision required while improving the safeguards in place. 
- Mistakes are less consequential, since the **agent can’t write directly to the main/production data**. If some changes are good but others aren't, users can cherry-pick operations they wish to keep.
- **Misalignment patterns become more visible**. When reviewing changes at the end of a session, users can clearly identify where the agent deviated from intended behavior and adjust the system prompt or agent configuration accordingly to prevent similar issues in future sessions.
- **Multiple agents or agent sessions** can run simultaneously on isolated copies without interfering with each other.

## Implementations

| Implementation | Language | Install | Details |
|----------------|----------|---------|---------|
| [**PostgreSQL**](python-package/) | Python | `pip install agent-cow` | Full server-side support via SQLAlchemy, views, and triggers |
| [**PGlite**](pglite/) | TypeScript | `npm install agent-cow` | Postgres in WebAssembly, runs in the browser or Node.js |

| Planned | Status |
|---------|--------|
| **Blob/File Storage** | In progress |
| **MySQL** | Coming soon |
| **SQLite** | Coming soon |
| **MongoDB** | Coming soon |

## How It Works

Under the hood, all implementations use the same approach:

1. **Base layer** stores production data
2. **Changes layer** stores session-specific modifications
3. **Unified interface** merges base + changes transparently
4. **Write interception** routes mutations to the changes layer instead of the base
5. **Session isolation** ensures each session only sees its own changes

When a session is active, writes go to the changes layer. Reads automatically merge base data with the session's changes. Other sessions (and production) see only the base data.

See the [interactive demo](https://www.agent-cow.com) for a worked example of an inventory management system where an agent makes both good and bad decisions.

## Repository Structure

```
agent-cow/
  python-package/   Python (PostgreSQL) implementation, published to PyPI
  pglite/           TypeScript (PGlite) implementation, published to npm
```

## Contributing

We welcome contributions! See the README in each implementation folder for development setup.

- [Python/PostgreSQL contributing](python-package/)
- [TypeScript/PGlite contributing](pglite/)

For questions, bug reports, or feature requests, please [open an issue](../../issues).

## License

MIT License.

## Credits

Created and maintained by [trail](https://trail-ml.com).
