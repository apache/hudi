#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Tests for command validation."""

import pytest

from hudi_cli_mcp.commands import (
    CommandNotAllowedError,
    build_command,
    is_readonly_command,
    quirk_hint,
    quote_arg,
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

    def test_write_command_raises_write_hint(self):
        # A known write command must be reported as a write op, not "unknown".
        with pytest.raises(CommandNotAllowedError, match="write operation"):
            validate_command("commit rollback --commit 123")

    def test_unknown_command_raises_recognized_hint(self):
        # A nonexistent command must NOT be mislabeled as a write command; the
        # hint should steer the caller to real read commands.
        with pytest.raises(CommandNotAllowedError, match="not a recognized"):
            validate_command("metadata list-index")

    def test_unknown_command_hint_steers_to_desc_property(self):
        try:
            validate_command("metadata list-index")
        except CommandNotAllowedError as e:
            # Steer to the property that actually lists metadata indexes.
            assert "hoodie.table.metadata.partitions" in str(e)
            assert "desc" in str(e)
            assert "write operation" not in str(e)


class TestValidateCommands:
    def test_all_allowed(self):
        validate_commands(["commits show", "stats wa", "desc"])

    def test_user_supplied_connect_rejected(self):
        # The session owns the connected path; a connect smuggled into a batch
        # would silently re-target a different table.
        with pytest.raises(CommandNotAllowedError, match="connect"):
            validate_commands(["connect --path /tmp/t"])

    def test_one_blocked_raises(self):
        with pytest.raises(CommandNotAllowedError):
            validate_commands(["commits show", "commit rollback --commit 123"])

    def test_embedded_newline_rejected(self):
        # A newline would smuggle an unvalidated second command past the allowlist.
        with pytest.raises(CommandNotAllowedError):
            validate_command("commits show --limit 5\nsavepoint rollback --savepoint 1")


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

    def test_value_with_space_is_quoted(self):
        # An unquoted path with a space would be split into two CLI arguments.
        result = build_command("table update-configs", **{"props-file": "/my dir/t.props"})
        assert result == 'table update-configs --props-file "/my dir/t.props"'


class TestWordBoundaryMatching:
    def test_prefix_requires_word_boundary(self):
        # "desc" is allowed; an unrelated command sharing the prefix is not.
        assert is_readonly_command("desc")
        assert is_readonly_command("desc --verbose")
        assert not is_readonly_command("described-elsewhere")
        assert not is_readonly_command("descX")

    def test_readonly_prefix_does_not_leak_into_longer_command(self):
        # "commits show" must not accept an invented longer command word.
        assert is_readonly_command("commits show --limit 5")
        assert not is_readonly_command("commits showevil --limit 5")


class TestQuirkHints:
    def test_stats_filesizes_has_hint(self):
        hint = quirk_hint("stats filesizes")
        assert hint and "show fsview all" in hint

    def test_metadata_list_partitions_has_hint(self):
        assert quirk_hint("metadata list-partitions --sparkMaster local") is not None

    def test_compaction_schedule_hint_not_applied_to_schedule_and_execute(self):
        assert quirk_hint("compaction schedule") is not None
        assert quirk_hint("compaction scheduleAndExecute") is None

    def test_normal_command_has_no_hint(self):
        assert quirk_hint("commits show --limit 10") is None


class TestQuoteArg:
    def test_plain_value_unchanged(self):
        assert quote_arg("/tmp/table") == "/tmp/table"

    def test_value_with_space_quoted(self):
        assert quote_arg("/data/my table") == '"/data/my table"'
