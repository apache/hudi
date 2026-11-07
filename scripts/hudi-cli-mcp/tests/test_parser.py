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

"""Tests for the CLI output parser."""

import os
from pathlib import Path

from hudi_cli_mcp.parser import parse_cli_output

SAMPLE_DIR = Path(os.path.dirname(__file__)) / "sample_outputs"


def _read_sample(name: str) -> str:
    return (SAMPLE_DIR / name).read_text()


class TestParseCommitsShow:
    def test_parses_table_headers(self):
        raw = _read_sample("commits_show.txt")
        result = parse_cli_output(raw)

        assert len(result.tables) == 1
        table = result.tables[0]
        assert "CommitTime" in table.headers
        assert "Total Bytes Written" in table.headers
        assert "Total Errors" in table.headers

    def test_parses_table_rows(self):
        raw = _read_sample("commits_show.txt")
        result = parse_cli_output(raw)

        table = result.tables[0]
        assert table.row_count == 3
        assert table.rows[0]["CommitTime"] == "20240315120000"
        assert table.rows[0]["Total Bytes Written"] == "434.2 MB"
        assert table.rows[1]["CommitTime"] == "20240315110000"
        assert table.rows[2]["Total Records Written"] == "50000"

    def test_filters_noise_lines(self):
        raw = _read_sample("commits_show.txt")
        result = parse_cli_output(raw)

        # SLF4J, Main called, and hudi-> should be filtered
        for msg in result.messages:
            assert "SLF4J" not in msg
            assert "Main called" not in msg

    def test_captures_connect_message(self):
        raw = _read_sample("commits_show.txt")
        result = parse_cli_output(raw)

        # "Metadata for table trips loaded" should be in messages
        assert any("Metadata for table" in m for m in result.messages)


class TestParseDescTable:
    def test_parses_property_value_table(self):
        raw = _read_sample("desc_table.txt")
        result = parse_cli_output(raw)

        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.headers == ["Property", "Value"]
        assert table.row_count == 5

    def test_parses_property_values(self):
        raw = _read_sample("desc_table.txt")
        result = parse_cli_output(raw)

        table = result.tables[0]
        props = {row["Property"]: row["Value"] for row in table.rows}
        assert props["hoodie.table.name"] == "trips"
        assert props["hoodie.table.type"] == "COPY_ON_WRITE"
        assert props["hoodie.table.version"] == "6"


class TestParseConnectOnly:
    def test_no_tables(self):
        raw = _read_sample("connect_only.txt")
        result = parse_cli_output(raw)

        assert len(result.tables) == 0

    def test_has_connect_message(self):
        raw = _read_sample("connect_only.txt")
        result = parse_cli_output(raw)

        assert any("Metadata for table" in m for m in result.messages)


class TestParseEmptyTable:
    def test_parses_headers_with_no_rows(self):
        raw = _read_sample("empty_table.txt")
        result = parse_cli_output(raw)

        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.headers == ["CommitTime", "Total Bytes Written", "Total Records"]
        assert table.row_count == 0
        assert table.rows == []


class TestParseMultipleTables:
    def test_parses_two_tables(self):
        raw = _read_sample("multiple_tables.txt")
        result = parse_cli_output(raw)

        assert len(result.tables) == 2

    def test_first_table_is_desc(self):
        raw = _read_sample("multiple_tables.txt")
        result = parse_cli_output(raw)

        table = result.tables[0]
        assert table.headers == ["Property", "Value"]
        assert table.row_count == 2

    def test_second_table_is_commits(self):
        raw = _read_sample("multiple_tables.txt")
        result = parse_cli_output(raw)

        table = result.tables[1]
        assert "CommitTime" in table.headers
        assert table.row_count == 2
        assert table.rows[0]["CommitTime"] == "20240315120000"


class TestToDict:
    def test_serializable_output(self):
        raw = _read_sample("commits_show.txt")
        result = parse_cli_output(raw)
        d = result.to_dict()

        assert "tables" in d
        assert "messages" in d
        assert len(d["tables"]) == 1
        assert d["tables"][0]["row_count"] == 3
        assert isinstance(d["tables"][0]["rows"][0], dict)


class TestEdgeCases:
    def test_empty_input(self):
        result = parse_cli_output("")
        assert len(result.tables) == 0
        assert len(result.messages) == 0

    def test_only_noise(self):
        raw = "SLF4J: something\nMain called\nWARNING: something else\n"
        result = parse_cli_output(raw)
        assert len(result.tables) == 0
        assert len(result.messages) == 0

    def test_raw_preserved(self):
        raw = "hello world"
        result = parse_cli_output(raw)
        assert result.raw == raw


class TestSilentFailureMarkers:
    """Real CLI outputs that previously slipped through as success."""

    def test_failed_colon_prefix_is_error(self):
        from hudi_cli_mcp.parser import _is_error_line as is_error_line

        assert is_error_line('Failed: Could not create savepoint "20260806230427155".')
        assert is_error_line('Failed: Could not delete savepoint "20260806230427155".')

    def test_commit_not_found_is_error(self):
        from hudi_cli_mcp.parser import _is_error_line as is_error_line

        assert is_error_line(
            "Commit 20260806230427155 not found in Commits "
            "org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2"
        )

    def test_plain_output_not_flagged(self):
        from hudi_cli_mcp.parser import _is_error_line as is_error_line

        assert not is_error_line('The commit "20260806230427155" has been savepointed.')
        assert not is_error_line("Savepoint \"20260806230427155\" deleted.")
