"""Session management for Hudi CLI connections."""

from __future__ import annotations


class NotConnectedError(Exception):
    """Raised when a command requires a table connection but none exists."""

    pass


class SessionManager:
    """Tracks the currently connected Hudi table path.

    Since each CLI invocation is a fresh JVM, this just remembers
    which path to auto-connect to across MCP tool calls.
    """

    def __init__(self):
        self._connected_path: str | None = None

    @property
    def connected_path(self) -> str | None:
        return self._connected_path

    @property
    def is_connected(self) -> bool:
        return self._connected_path is not None

    def connect(self, path: str) -> None:
        self._connected_path = path

    def disconnect(self) -> None:
        self._connected_path = None

    def require_connection(self) -> str:
        """Return the connected path, or raise if not connected."""
        if self._connected_path is None:
            raise NotConnectedError(
                "Not connected to any Hudi table. Use the connect_to_table tool first."
            )
        return self._connected_path

    def build_command_list(self, commands: list[str]) -> list[str]:
        """Prepend connect command to a list of commands."""
        path = self.require_connection()
        return [f"connect --path {path}"] + commands
