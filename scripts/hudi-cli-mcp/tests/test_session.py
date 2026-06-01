"""Tests for session management."""

import pytest

from hudi_cli.session import NotConnectedError, SessionManager


class TestSessionManager:
    def test_initial_state(self):
        session = SessionManager()
        assert not session.is_connected
        assert session.connected_path is None

    def test_connect(self):
        session = SessionManager()
        session.connect("/tmp/table")
        assert session.is_connected
        assert session.connected_path == "/tmp/table"

    def test_disconnect(self):
        session = SessionManager()
        session.connect("/tmp/table")
        session.disconnect()
        assert not session.is_connected

    def test_disconnect_when_not_connected(self):
        session = SessionManager()
        session.disconnect()  # Should not raise

    def test_require_connection_when_connected(self):
        session = SessionManager()
        session.connect("/tmp/table")
        assert session.require_connection() == "/tmp/table"

    def test_require_connection_when_not_connected(self):
        session = SessionManager()
        with pytest.raises(NotConnectedError, match="Not connected"):
            session.require_connection()

    def test_build_command_list(self):
        session = SessionManager()
        session.connect("/tmp/table")
        commands = session.build_command_list(["commits show", "desc"])
        assert commands == [
            "connect --path /tmp/table",
            "commits show",
            "desc",
        ]

    def test_build_command_list_not_connected(self):
        session = SessionManager()
        with pytest.raises(NotConnectedError):
            session.build_command_list(["commits show"])

    def test_reconnect_updates_path(self):
        session = SessionManager()
        session.connect("/tmp/table1")
        session.connect("/tmp/table2")
        assert session.connected_path == "/tmp/table2"
