"""
COW session management.

Utilities for parsing COW session state from HTTP request headers and
building the SQL statements that configure session-level variables.

No driver-specific imports — only standard Python.
"""

import uuid
import logging
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CowRequestConfig:
    """Configuration for COW session parsed from request headers."""

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


def parse_cow_headers_from_request(request) -> CowRequestConfig:
    """Parse COW configuration from HTTP request headers."""
    if request is None:
        return CowRequestConfig()

    agent_session_id = _parse_uuid_header(request, "x-agent-session-id")
    operation_id = _parse_uuid_header(request, "x-operation-id")
    visible_operations = _parse_uuid_list_header(request, "x-visible-operations")

    return CowRequestConfig(
        agent_session_id=agent_session_id,
        operation_id=operation_id,
        visible_operations=visible_operations,
    )


def _parse_uuid_header(request, header_name: str) -> Optional[uuid.UUID]:
    header_value = request.headers.get(header_name)
    if not header_value:
        return None

    try:
        return uuid.UUID(header_value)
    except ValueError:
        logger.warning(f"Invalid {header_name} header: {header_value!r}")
        return None


def _parse_uuid_list_header(request, header_name: str) -> Optional[list[uuid.UUID]]:
    header_value = request.headers.get(header_name)
    if not header_value:
        return None

    try:
        return [
            uuid.UUID(op_id.strip())
            for op_id in header_value.split(",")
            if op_id.strip()
        ]
    except ValueError:
        logger.warning(f"Invalid {header_name} header: {header_value!r}")
        return None


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
