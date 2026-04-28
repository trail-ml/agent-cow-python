"""agent-cow: Database Copy-On-Write for AI agent workspace isolation."""

__version__ = "0.1.7"

from .context import CowConfig

__all__ = ["CowConfig"]

# blob subpackage available via `from agentcow.blob import ...`
