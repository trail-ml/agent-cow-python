"""
COW session management for PostgreSQL.

This module provides utilities for managing COW session state, including parsing
HTTP headers, setting session variables, and managing session lifecycle with event listeners.
"""

import uuid
import logging
from typing import Optional
from dataclasses import dataclass

from sqlalchemy import text, event
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


@dataclass
class CowRequestConfig:
    """Configuration for COW (Copy-On-Write) session from request headers."""
    agent_session_id: Optional[uuid.UUID] = None
    operation_id: Optional[uuid.UUID] = None
    visible_operations: Optional[list[uuid.UUID]] = None

    @property
    def is_cow_enabled(self) -> bool:
        """Returns True if COW mode is active (agent_session_id is set)."""
        return self.agent_session_id is not None

    def to_session_kwargs(self) -> dict:
        """Returns kwargs dict for passing to get_session/create_session."""
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
    """Parse a single UUID from a request header."""
    header_value = request.headers.get(header_name)
    if not header_value:
        return None

    try:
        return uuid.UUID(header_value)
    except ValueError:
        logger.warning(f"Invalid {header_name} header: {header_value!r}")
        return None


def _parse_uuid_list_header(request, header_name: str) -> Optional[list[uuid.UUID]]:
    """Parse a comma-separated list of UUIDs from a request header."""
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
    agent_session_id: uuid.UUID,
    operation_id: Optional[uuid.UUID] = None,
    visible_operations: Optional[list[uuid.UUID]] = None,
) -> list[str]:
    """Build SQL statements for setting COW session variables."""
    statements = [f"SET LOCAL app.session_id = '{agent_session_id}'"]

    if operation_id:
        statements.append(f"SET LOCAL app.operation_id = '{operation_id}'")

    if visible_operations:
        ops_str = ",".join(str(op) for op in visible_operations)
        statements.append(f"SET LOCAL app.visible_operations = '{ops_str}'")

    return statements


def apply_cow_variables_sync(
    connection,
    agent_session_id: uuid.UUID,
    operation_id: Optional[uuid.UUID] = None,
    visible_operations: Optional[list[uuid.UUID]] = None,
) -> None:
    """Apply COW session variables synchronously (for use in event listeners)."""
    for stmt in build_cow_variable_statements(
        agent_session_id, operation_id, visible_operations
    ):
        connection.execute(text(stmt))


async def apply_cow_variables_async(
    session: AsyncSession,
    agent_session_id: uuid.UUID,
    operation_id: Optional[uuid.UUID] = None,
    visible_operations: Optional[list[uuid.UUID]] = None,
) -> None:
    """Apply COW session variables asynchronously."""
    for stmt in build_cow_variable_statements(
        agent_session_id, operation_id, visible_operations
    ):
        await session.execute(text(stmt))


def setup_cow_session_listener(
    session: AsyncSession,
    agent_session_id: uuid.UUID,
    operation_id: Optional[uuid.UUID] = None,
    visible_operations: Optional[list[uuid.UUID]] = None,
) -> None:
    """Set up an event listener to ensure COW session variables are set after each transaction begins."""

    @event.listens_for(session.sync_session, "after_begin")
    def set_cow_variables_after_begin(session, transaction, connection):
        """Re-set COW session variables when a new transaction begins."""
        apply_cow_variables_sync(
            connection, agent_session_id, operation_id, visible_operations
        )
