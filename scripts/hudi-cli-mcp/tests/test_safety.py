"""Tests for the safety manager and confirmation token system."""

import time
from unittest.mock import patch

import pytest

from hudi_cli.commands import RiskLevel
from hudi_cli.safety import (
    SafetyManager,
    TokenExpiredError,
    TokenNotFoundError,
)


class TestPendingOperation:
    def test_create_operation(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="commit rollback --commit 123",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Roll back commit 123",
        )
        assert op.command == "commit rollback --commit 123"
        assert op.risk_level == RiskLevel.HIGH
        assert op.table_path == "/tmp/table"
        assert op.description == "Roll back commit 123"
        assert op.token  # Non-empty UUID
        assert op.dry_run_result is None
        assert not op.is_expired
        assert op.seconds_remaining > 0

    def test_create_with_dry_run(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Run clean",
            dry_run_result='{"tables": []}',
        )
        assert op.dry_run_result == '{"tables": []}'

    def test_to_dict(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="compaction schedule",
            risk_level=RiskLevel.MEDIUM,
            table_path="/tmp/table",
            description="Schedule compaction",
        )
        d = op.to_dict()
        assert d["token"] == op.token
        assert d["command"] == "compaction schedule"
        assert d["risk_level"] == "medium"
        assert d["table_path"] == "/tmp/table"
        assert d["description"] == "Schedule compaction"
        assert d["dry_run_result"] is None
        assert isinstance(d["expires_in_seconds"], int)
        assert d["expires_in_seconds"] > 0


class TestSafetyManagerConfirm:
    def test_confirm_valid_token(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Run clean",
        )
        confirmed = safety.confirm(op.token)
        assert confirmed.command == "cleans run"
        assert confirmed.table_path == "/tmp/table"

    def test_confirm_consumes_token(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Run clean",
        )
        safety.confirm(op.token)
        with pytest.raises(TokenNotFoundError, match="not found"):
            safety.confirm(op.token)

    def test_confirm_unknown_token(self):
        safety = SafetyManager()
        with pytest.raises(TokenNotFoundError, match="not found"):
            safety.confirm("nonexistent-token")

    def test_confirm_expired_token(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Run clean",
        )
        # Expire the token by moving time forward
        with patch("hudi_cli.safety.time.time", return_value=op.created_at + 400):
            with pytest.raises(TokenNotFoundError):
                # Expired tokens are cleaned up before lookup
                safety.confirm(op.token)


class TestSafetyManagerCancel:
    def test_cancel_valid_token(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Run clean",
        )
        cancelled = safety.cancel(op.token)
        assert cancelled.command == "cleans run"

    def test_cancel_removes_token(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Run clean",
        )
        safety.cancel(op.token)
        with pytest.raises(TokenNotFoundError):
            safety.cancel(op.token)

    def test_cancel_unknown_token(self):
        safety = SafetyManager()
        with pytest.raises(TokenNotFoundError, match="not found"):
            safety.cancel("nonexistent-token")


class TestSafetyManagerListPending:
    def test_empty_list(self):
        safety = SafetyManager()
        assert safety.list_pending() == []

    def test_list_returns_pending(self):
        safety = SafetyManager()
        op1 = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Run clean",
        )
        op2 = safety.prepare_operation(
            command="compaction schedule",
            risk_level=RiskLevel.MEDIUM,
            table_path="/tmp/table",
            description="Schedule compaction",
        )
        pending = safety.list_pending()
        assert len(pending) == 2
        tokens = {op.token for op in pending}
        assert op1.token in tokens
        assert op2.token in tokens

    def test_list_excludes_expired(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Run clean",
        )
        with patch("hudi_cli.safety.time.time", return_value=op.created_at + 400):
            pending = safety.list_pending()
            assert len(pending) == 0

    def test_list_excludes_confirmed(self):
        safety = SafetyManager()
        op = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table",
            description="Run clean",
        )
        safety.confirm(op.token)
        assert len(safety.list_pending()) == 0


class TestMultipleOperations:
    def test_multiple_tokens_independent(self):
        safety = SafetyManager()
        op1 = safety.prepare_operation(
            command="cleans run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table1",
            description="Clean table 1",
        )
        op2 = safety.prepare_operation(
            command="compaction run",
            risk_level=RiskLevel.HIGH,
            table_path="/tmp/table2",
            description="Compact table 2",
        )
        # Confirm one, cancel the other
        confirmed = safety.confirm(op1.token)
        assert confirmed.table_path == "/tmp/table1"

        cancelled = safety.cancel(op2.token)
        assert cancelled.table_path == "/tmp/table2"

    def test_unique_tokens(self):
        safety = SafetyManager()
        tokens = set()
        for i in range(20):
            op = safety.prepare_operation(
                command=f"cleans run",
                risk_level=RiskLevel.HIGH,
                table_path="/tmp/table",
                description=f"Op {i}",
            )
            tokens.add(op.token)
        assert len(tokens) == 20
