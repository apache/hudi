"""Tests for write operation tools."""

import json
from unittest.mock import MagicMock

import pytest

from hudi_cli.commands import RiskLevel
from hudi_cli.executor import ExecutionResult
from hudi_cli.parser import ParsedOutput
from hudi_cli.safety import SafetyManager
from hudi_cli.session import SessionManager
from tools.write_ops import (
    create_savepoint,
    delete_markers,
    delete_savepoint,
    delete_table_configs,
    manage_metadata,
    recover_table_configs,
    repair_table,
    rollback_commit,
    rollback_to_savepoint,
    run_clean,
    run_compaction,
    run_clustering,
    schedule_compaction,
    schedule_clustering,
    toggle_lock_audit,
    trigger_archival,
    unschedule_compaction,
    update_table_configs,
    upgrade_or_downgrade_table,
)


def _mock_executor(return_code=0):
    """Create a mock executor that returns a successful result."""
    executor = MagicMock()
    result = ExecutionResult(
        raw_output="OK",
        parsed=ParsedOutput(messages=["Command succeeded"]),
        return_code=return_code,
        duration_seconds=1.0,
    )
    executor.execute.return_value = result
    return executor


def _connected_session(path="/tmp/table"):
    session = SessionManager()
    session.connect(path)
    return session


class TestLowRiskOperations:
    """LOW risk operations should execute immediately without confirmation."""

    def test_create_savepoint_executes_immediately(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(create_savepoint("20240101120000", executor, session, safety))
        assert result["success"] is True
        assert result["risk_level"] == "low"
        assert "pending_confirmation" not in result.get("status", "")
        # No pending operations
        assert len(safety.list_pending()) == 0

    def test_toggle_lock_audit_enable(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(toggle_lock_audit(True, executor, session, safety))
        assert result["success"] is True
        assert result["risk_level"] == "low"

    def test_toggle_lock_audit_disable(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(toggle_lock_audit(False, executor, session, safety))
        assert result["success"] is True


class TestMediumRiskOperations:
    """MEDIUM risk operations should return a confirmation token."""

    def test_schedule_compaction_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(schedule_compaction(executor, session, safety))
        assert result["status"] == "pending_confirmation"
        assert result["risk_level"] == "medium"
        assert "token" in result
        assert len(safety.list_pending()) == 1

    def test_schedule_clustering_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(schedule_clustering(executor, session, safety))
        assert result["status"] == "pending_confirmation"
        assert result["risk_level"] == "medium"

    def test_unschedule_compaction_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(unschedule_compaction("20240101120000", executor, session, safety))
        assert result["status"] == "pending_confirmation"

    def test_delete_savepoint_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(delete_savepoint("20240101120000", executor, session, safety))
        assert result["status"] == "pending_confirmation"

    def test_delete_markers_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(delete_markers("20240101120000", executor, session, safety))
        assert result["status"] == "pending_confirmation"

    def test_manage_metadata_create(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(manage_metadata("create", executor, session, safety))
        assert result["status"] == "pending_confirmation"

    def test_manage_metadata_invalid_action(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(manage_metadata("invalid", executor, session, safety))
        assert result["success"] is False
        assert "Invalid action" in result["error"]

    def test_trigger_archival_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(trigger_archival(executor, session, safety))
        assert result["status"] == "pending_confirmation"

    def test_repair_table_dryrun(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(
            repair_table("addpartitionmeta", executor, session, safety, dry_run=True)
        )
        assert result["status"] == "pending_confirmation"
        assert "DRY RUN" in result["description"]

    def test_repair_table_invalid_type(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(repair_table("invalid", executor, session, safety))
        assert result["success"] is False
        assert "Invalid repair type" in result["error"]

    def test_recover_table_configs_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(recover_table_configs(executor, session, safety))
        assert result["status"] == "pending_confirmation"


class TestHighRiskOperations:
    """HIGH risk operations should return a token and include preview data."""

    def test_rollback_commit_returns_token_with_preview(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(rollback_commit("20240101120000", executor, session, safety))
        assert result["status"] == "pending_confirmation"
        assert result["risk_level"] == "high"
        assert result["dry_run_result"] is not None
        assert "DESTRUCTIVE" in result["description"]

    def test_rollback_to_savepoint_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(
            rollback_to_savepoint("20240101120000", executor, session, safety)
        )
        assert result["status"] == "pending_confirmation"
        assert result["risk_level"] == "high"

    def test_run_compaction_returns_token_with_preview(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(run_compaction(executor, session, safety))
        assert result["status"] == "pending_confirmation"
        assert result["dry_run_result"] is not None

    def test_run_compaction_with_instant(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(
            run_compaction(executor, session, safety, instant_time="20240101120000")
        )
        assert result["status"] == "pending_confirmation"

    def test_run_clustering_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(run_clustering(executor, session, safety))
        assert result["status"] == "pending_confirmation"

    def test_run_clean_returns_token_with_preview(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(run_clean(executor, session, safety))
        assert result["status"] == "pending_confirmation"
        assert result["dry_run_result"] is not None

    def test_update_table_configs_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(
            update_table_configs("/tmp/props.properties", executor, session, safety)
        )
        assert result["status"] == "pending_confirmation"

    def test_delete_table_configs_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(
            delete_table_configs("key1,key2", executor, session, safety)
        )
        assert result["status"] == "pending_confirmation"

    def test_upgrade_table_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(
            upgrade_or_downgrade_table("upgrade", 5, executor, session, safety)
        )
        assert result["status"] == "pending_confirmation"

    def test_downgrade_table_returns_token(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(
            upgrade_or_downgrade_table("downgrade", 4, executor, session, safety)
        )
        assert result["status"] == "pending_confirmation"

    def test_invalid_direction(self):
        executor = _mock_executor()
        session = _connected_session()
        safety = SafetyManager()

        result = json.loads(
            upgrade_or_downgrade_table("sideways", 5, executor, session, safety)
        )
        assert result["success"] is False
        assert "Invalid direction" in result["error"]


class TestNotConnected:
    """All write operations should fail gracefully when not connected."""

    def test_create_savepoint_not_connected(self):
        executor = _mock_executor()
        session = SessionManager()
        safety = SafetyManager()

        result = json.loads(create_savepoint("20240101120000", executor, session, safety))
        assert result["success"] is False
        assert "Not connected" in result["error"]

    def test_rollback_not_connected(self):
        executor = _mock_executor()
        session = SessionManager()
        safety = SafetyManager()

        result = json.loads(rollback_commit("20240101120000", executor, session, safety))
        assert result["success"] is False
        assert "Not connected" in result["error"]
