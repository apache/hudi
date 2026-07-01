"""Tests for command validation."""

import pytest

from hudi_cli.commands import (
    CommandNotAllowedError,
    build_command,
    is_readonly_command,
    validate_command,
    validate_commands,
)


class TestIsReadonlyCommand:
    def test_allowed_commands(self):
        assert is_readonly_command("commits show --limit 10")
        assert is_readonly_command("commit showpartitions --commit 123")
        assert is_readonly_command("desc")
        assert is_readonly_command("connect --path /tmp/table")
        assert is_readonly_command("stats wa")
        assert is_readonly_command("show fsview all")
        assert is_readonly_command("metadata list-partitions")
        assert is_readonly_command("timeline show active --limit 5")
        assert is_readonly_command("compactions show all")
        assert is_readonly_command("cleans show --limit 5")

    def test_blocked_commands(self):
        assert not is_readonly_command("commit rollback --commit 123")
        assert not is_readonly_command("savepoint rollback --savepoint 123")
        assert not is_readonly_command("cleans run")
        assert not is_readonly_command("compaction run --tableName t")
        assert not is_readonly_command("compaction schedule")
        assert not is_readonly_command("repair addpartitionmeta")
        assert not is_readonly_command("marker delete")
        assert not is_readonly_command("metadata delete")
        assert not is_readonly_command("upgrade table --toVersion 5")
        assert not is_readonly_command("downgrade table --toVersion 4")
        assert not is_readonly_command("clustering schedule")
        assert not is_readonly_command("create --path /tmp/t --tableName t")

    def test_case_insensitive(self):
        assert is_readonly_command("COMMITS SHOW --limit 10")
        assert is_readonly_command("Desc")
        assert not is_readonly_command("COMMIT ROLLBACK --commit 123")


class TestValidateCommand:
    def test_allowed_command_passes(self):
        validate_command("commits show --limit 10")  # Should not raise

    def test_blocked_command_raises(self):
        with pytest.raises(CommandNotAllowedError, match="not allowed"):
            validate_command("commit rollback --commit 123")


class TestValidateCommands:
    def test_all_allowed(self):
        validate_commands(["commits show", "stats wa", "desc"])

    def test_connect_always_allowed(self):
        validate_commands(["connect --path /tmp/t"])

    def test_one_blocked_raises(self):
        with pytest.raises(CommandNotAllowedError):
            validate_commands(["commits show", "commit rollback --commit 123"])


class TestBuildCommand:
    def test_basic(self):
        assert build_command("commits show") == "commits show"

    def test_with_kwargs(self):
        result = build_command("commits show", limit=10, desc=True)
        assert result == "commits show --limit 10 --desc"

    def test_none_skipped(self):
        result = build_command("commits show", limit=None, sortBy="time")
        assert result == "commits show --sortBy time"

    def test_false_skipped(self):
        result = build_command("commits show", desc=False)
        assert result == "commits show"

    def test_empty_string_skipped(self):
        result = build_command("commits show", sortBy="")
        assert result == "commits show"
