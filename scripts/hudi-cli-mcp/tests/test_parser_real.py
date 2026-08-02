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

"""Parser tests against real hudi-cli 1.3.0 output.

The other fixtures use only the heavy ``╠═══╪═══╣`` inter-row border. Real
hudi-cli 1.3.0 uses a *light* ``╟───┼───╢`` border between data rows, which the
original parser did not recognize -- so every data row after the first became its
own single-row "table" with its values misread as headers. These fixtures use the
real border so that regression cannot come back unnoticed.
"""

import os
from pathlib import Path

from hudi_cli.parser import parse_cli_output

SAMPLE_DIR = Path(os.path.dirname(__file__)) / "sample_outputs"


def _read(name: str) -> str:
    return (SAMPLE_DIR / name).read_text()


class TestRealCommitsShow:
    def test_single_table_with_all_rows(self):
        result = parse_cli_output(_read("commits_show_real.txt"))
        # Must be ONE table with all three commits, not three single-row tables.
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.headers[0] == "CommitTime"
        assert table.row_count == 3
        assert [r["CommitTime"] for r in table.rows] == [
            "20260802183502",
            "20260802183459",
            "20260802183449",
        ]

    def test_no_error_lines(self):
        result = parse_cli_output(_read("commits_show_real.txt"))
        assert result.errors == []


class TestRealDesc:
    def test_single_property_table(self):
        result = parse_cli_output(_read("desc_real.txt"))
        # A property table must parse as one table, not one table per property.
        assert len(result.tables) == 1
        table = result.tables[0]
        assert table.headers == ["Property", "Value"]
        assert table.row_count == 3
        props = {r["Property"]: r["Value"] for r in table.rows}
        assert props["hoodie.table.name"] == "trips_mcp"
        assert props["hoodie.table.type"] == "COPY_ON_WRITE"


class TestErrorCapture:
    def test_error_line_surfaced_not_dropped(self):
        raw = (
            "hudi->Metadata for table t loaded\n"
            "org.apache.hudi.exception.HoodieException: compaction can only be run "
            "for table type MERGE_ON_READ\n"
        )
        result = parse_cli_output(raw)
        assert any("HoodieException" in e for e in result.errors)
        # The error must not leak into the plain messages stream.
        assert not any("HoodieException" in m for m in result.messages)

    def test_can_only_be_run_marker(self):
        raw = "Compaction can only be run for table type MERGE_ON_READ\n"
        result = parse_cli_output(raw)
        assert len(result.errors) == 1
