"""Tests for write command classification and validation."""

import pytest

from hudi_cli.commands import (
    CommandNotAllowedError,
    RiskLevel,
    get_risk_level,
    is_readonly_command,
    is_write_command,
    validate_command,
    validate_write_command,
)


class TestIsWriteCommand:
    def test_low_risk_commands(self):
        assert is_write_command("savepoint create --commit 123")
        assert is_write_command("locks audit enable")
        assert is_write_command("locks audit disable")

    def test_medium_risk_commands(self):
        assert is_write_command("compaction schedule")
        assert is_write_command("clustering schedule")
        assert is_write_command("compaction unschedule --instant 123")
        assert is_write_command("savepoint delete --commit 123")
        assert is_write_command("marker delete --commit 123")
        assert is_write_command("metadata create")
        assert is_write_command("metadata delete")
        assert is_write_command("metadata delete-record-index")
        assert is_write_command("metadata init")
        assert is_write_command("trigger archival")
        assert is_write_command("repair addpartitionmeta --dryrun true")
        assert is_write_command("repair corrupted clean files")
        assert is_write_command("repair migrate-partition-meta")
        assert is_write_command("locks audit cleanup --dryRun true")
        assert is_write_command("table recover-configs")

    def test_high_risk_commands(self):
        assert is_write_command("commit rollback --commit 123")
        assert is_write_command("savepoint rollback --savepoint 123")
        assert is_write_command("cleans run")
        assert is_write_command("compaction run")
        assert is_write_command("compaction scheduleAndExecute")
        assert is_write_command("compaction repair --instant 123")
        assert is_write_command("clustering run")
        assert is_write_command("clustering scheduleAndExecute")
        assert is_write_command("repair deduplicate --duplicatedPartitionPath p")
        assert is_write_command("repair overwrite-hoodie-props --new-props-file f")
        assert is_write_command("repair deprecated partition")
        assert is_write_command("rename partition --oldPartition a --newPartition b")
        assert is_write_command("create --path /tmp/t --tableName t")
        assert is_write_command("table update-configs --props-file f")
        assert is_write_command("table delete-configs --comma-separated-configs k")
        assert is_write_command("bootstrap run --srcPath s --targetPath t")
        assert is_write_command("upgrade table --toVersion 5")
        assert is_write_command("downgrade table --toVersion 4")

    def test_read_only_commands_not_write(self):
        assert not is_write_command("commits show --limit 10")
        assert not is_write_command("desc")
        assert not is_write_command("stats wa")
        assert not is_write_command("compactions show all")
        assert not is_write_command("cleans show")

    def test_unknown_commands_not_write(self):
        assert not is_write_command("some random command")
        assert not is_write_command("")

    def test_case_insensitive(self):
        assert is_write_command("COMMIT ROLLBACK --commit 123")
        assert is_write_command("Compaction Schedule")
        assert is_write_command("SAVEPOINT CREATE --commit 123")


class TestGetRiskLevel:
    def test_low_risk(self):
        assert get_risk_level("savepoint create --commit 123") == RiskLevel.LOW
        assert get_risk_level("locks audit enable") == RiskLevel.LOW

    def test_medium_risk(self):
        assert get_risk_level("compaction schedule") == RiskLevel.MEDIUM
        assert get_risk_level("metadata delete") == RiskLevel.MEDIUM
        assert get_risk_level("trigger archival") == RiskLevel.MEDIUM

    def test_high_risk(self):
        assert get_risk_level("commit rollback --commit 123") == RiskLevel.HIGH
        assert get_risk_level("cleans run") == RiskLevel.HIGH
        assert get_risk_level("create --path /tmp/t") == RiskLevel.HIGH

    def test_unknown_command_returns_none(self):
        assert get_risk_level("commits show") is None
        assert get_risk_level("desc") is None
        assert get_risk_level("unknown command") is None

    def test_case_insensitive(self):
        assert get_risk_level("COMMIT ROLLBACK --commit 123") == RiskLevel.HIGH


class TestValidateWriteCommand:
    def test_valid_write_command_passes(self):
        validate_write_command("commit rollback --commit 123")  # Should not raise
        validate_write_command("savepoint create --commit 123")

    def test_read_only_command_rejected(self):
        with pytest.raises(CommandNotAllowedError, match="not a recognized write"):
            validate_write_command("commits show --limit 10")

    def test_unknown_command_rejected(self):
        with pytest.raises(CommandNotAllowedError, match="not a recognized write"):
            validate_write_command("some random dangerous command")


class TestWriteAndReadSeparation:
    """Verify that write commands are NOT accepted by read-only validation and vice versa."""

    def test_write_commands_blocked_by_readonly_validator(self):
        with pytest.raises(CommandNotAllowedError):
            validate_command("commit rollback --commit 123")
        with pytest.raises(CommandNotAllowedError):
            validate_command("cleans run")

    def test_read_commands_not_in_write_set(self):
        assert not is_write_command("commits show --limit 10")
        assert not is_write_command("desc")

    def test_read_commands_pass_readonly_validator(self):
        validate_command("commits show --limit 10")  # Should not raise

    def test_no_overlap_between_read_and_write(self):
        """No command should be both read-only and write."""
        # connect is special — it's in READONLY but not in WRITE
        assert is_readonly_command("connect --path /tmp/t")
        assert not is_write_command("connect --path /tmp/t")
