"""
COW session management.

Building the SQL statements that configure session-level variables.

No driver-specific imports — only standard Python.
"""

import uuid
from typing import Optional
from dataclasses import dataclass


@dataclass
class CowRequestConfig:
    """Configuration for a COW session."""

    agent_session_id: Optional[uuid.UUID] = None
    operation_id: Optional[uuid.UUID] = None
    visible_operations: Optional[list[uuid.UUID]] = None

    @property
    def is_cow_requested(self) -> bool:
        return self.agent_session_id is not None

    def to_session_kwargs(self) -> dict:
        return {
            "agent_session_id": self.agent_session_id,
            "operation_id": self.operation_id,
            "visible_operations": self.visible_operations,
        }


def build_cow_variable_statements(
    agent_session_id: uuid.UUID | str,
    operation_id: uuid.UUID | str | None = None,
    visible_operations: list[uuid.UUID | str] | None = None,
) -> list[str]:
    """Build SQL statements for setting COW session variables.

    Returns a list of ``SET LOCAL …`` strings ready for any driver.
    """
    statements = [f"SET LOCAL app.session_id = '{agent_session_id}'"]

    if operation_id:
        statements.append(f"SET LOCAL app.operation_id = '{operation_id}'")

    if visible_operations:
        ops_str = ",".join(str(op) for op in visible_operations)
        statements.append(f"SET LOCAL app.visible_operations = '{ops_str}'")

    return statements
