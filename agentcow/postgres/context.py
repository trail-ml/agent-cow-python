"""
SQL COW session configuration.

No driver-specific imports — only standard Python.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from agentcow.context import CowConfig
from agentcow.postgres.operations import _validate_uuid


@dataclass
class CowPostgresConfig(CowConfig):
    """Configuration for a SQL COW session."""

    visible_operations: list[uuid.UUID] | None = None

    def to_session_kwargs(self) -> dict:
        return {
            "agent_session_id": self.session_id,
            "operation_id": self.operation_id,
            "visible_operations": self.visible_operations,
        }


def build_cow_variable_statements(
    agent_session_id: uuid.UUID | str,
    operation_id: uuid.UUID | str | None = None,
    visible_operations: list[uuid.UUID | str] | None = None,
) -> list[str]:
    """Build SQL statements for setting COW session variables.

    Returns a list of ``SET LOCAL ...`` strings ready for any driver.
    """
    statements = [f"SET LOCAL app.session_id = '{_validate_uuid(agent_session_id)}'"]

    if operation_id:
        statements.append(
            f"SET LOCAL app.operation_id = '{_validate_uuid(operation_id)}'"
        )

    if visible_operations:
        ops_str = ",".join(str(_validate_uuid(op)) for op in visible_operations)
        statements.append(f"SET LOCAL app.visible_operations = '{ops_str}'")

    return statements
