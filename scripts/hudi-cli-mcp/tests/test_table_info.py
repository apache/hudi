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

"""Tests for the typed read tools (server-side extraction/aggregation).

Each test encodes a real model failure observed live: reading the timeline
layout version as the table version, miscounting commit rows, and averaging
file sizes over historical slices.
"""

import json
from unittest.mock import MagicMock

from hudi_cli_mcp.executor import ExecutionResult
from hudi_cli_mcp.parser import ParsedOutput, ParsedTable
from hudi_cli_mcp.session import SessionManager
from hudi_cli_mcp.tools.table_info import (
    get_commit_summary,
    get_file_size_stats,
    get_table_info,
    human_size,
    parse_size,
)


def _executor_with(tables):
    executor = MagicMock()
    executor.execute.return_value = ExecutionResult(
        raw_output="",
        parsed=ParsedOutput(tables=tables),
        return_code=0,
        duration_seconds=0.1,
    )
    return executor


def _session(path="/tmp/table"):
    s = SessionManager()
    s.connect(path)
    return s


def _props_table(props):
    return ParsedTable(
        headers=["Property", "Value"],
        rows=[{"Property": k, "Value": v} for k, v in props.items()],
    )


class TestParseSize:
    def test_units(self):
        assert parse_size("427.1 KB") == int(427.1 * 1024)
        assert parse_size("1.2 MB") == int(1.2 * 1024 * 1024)
        assert parse_size("0.0 B") == 0
        assert parse_size("not a size") is None

    def test_human_roundtrip(self):
        assert human_size(437350) == "427.1 KB"


class TestGetTableInfo:
    def test_extracts_table_version_not_layout_version(self):
        # The exact live failure: models read hoodie.timeline.layout.version (2)
        # as the table version instead of hoodie.table.version (9).
        executor = _executor_with([_props_table({
            "hoodie.table.name": "trips_demo",
            "hoodie.table.type": "MERGE_ON_READ",
            "hoodie.table.version": "9",
            "hoodie.timeline.layout.version": "2",
            "hoodie.table.partition.fields": "city",
            "hoodie.table.recordkey.fields": "uuid",
            "hoodie.table.ordering.fields": "ts",
            "hoodie.table.metadata.partitions": "column_stats,files,partition_stats",
        })])
        d = json.loads(get_table_info(executor, _session()))
        assert d["success"] is True
        assert d["table_version"] == "9"
        assert d["table_type"] == "MERGE_ON_READ"
        assert d["partition_fields"] == ["city"]
        assert d["metadata_indexes"] == ["column_stats", "files", "partition_stats"]

    def test_error_when_no_properties(self):
        executor = _executor_with([])
        d = json.loads(get_table_info(executor, _session()))
        assert d["success"] is False

    def test_explicit_path_connects_session(self):
        executor = _executor_with([_props_table({"hoodie.table.name": "t"})])
        session = SessionManager()
        d = json.loads(get_table_info(executor, session, path="/data/other table"))
        assert d["success"] is True
        assert session.connected_path == "/data/other table"
        (commands,), _ = executor.execute.call_args
        assert commands[0] == 'connect --path "/data/other table"'


def _commits_table(n):
    rows = [
        {
            "CommitTime": f"2026080623040{i:02d}",
            "Total Bytes Written": "1.2 MB",
            "Total Files Added": "0",
            "Total Files Updated": "4",
            "Total Partitions Written": "4",
            "Total Records Written": "80",
            "Total Update Records Written": "3",
            "Total Errors": "0",
        }
        for i in range(n)
    ]
    return ParsedTable(
        headers=["CommitTime", "Total Bytes Written", "Total Files Added",
                 "Total Files Updated", "Total Partitions Written",
                 "Total Records Written", "Total Update Records Written",
                 "Total Errors"],
        rows=rows,
    )


class TestGetCommitSummary:
    def test_commit_count_is_computed_not_model_counted(self):
        # The exact live failure: 10 rows in front of the model, it said "4".
        executor = _executor_with([_commits_table(10)])
        d = json.loads(get_commit_summary(executor, _session(), limit=5))
        assert d["success"] is True
        assert d["commit_count"] == 10
        assert len(d["recent"]) == 5
        assert d["recent"][0]["records_written"] == 80
        assert d["recent"][0]["bytes_written"] == int(1.2 * 1024 * 1024)

    def test_non_instant_rows_ignored(self):
        table = _commits_table(3)
        table.rows.append({"CommitTime": "CommitTime", "Total Records Written": "x"})
        executor = _executor_with([table])
        d = json.loads(get_commit_summary(executor, _session()))
        assert d["commit_count"] == 3


def _fsview_rows():
    # Two slices of the SAME file group (historical + latest) plus one other
    # file group: stats must dedupe to the latest slice per (partition, fileId).
    return ParsedTable(
        headers=["Partition", "FileId", "Base-Instant", "Data-File",
                 "Data-File Size", "Num Delta Files", "Total Delta File Size",
                 "Delta Files"],
        rows=[
            {"Partition": "city=sf", "FileId": "f1", "Base-Instant": "20260806230427155",
             "Data-File": "a.parquet", "Data-File Size": "427.1 KB", "Num Delta Files": "0"},
            {"Partition": "city=sf", "FileId": "f1", "Base-Instant": "20260806230419854",
             "Data-File": "old.parquet", "Data-File Size": "100.0 KB", "Num Delta Files": "0"},
            {"Partition": "city=ny", "FileId": "f2", "Base-Instant": "20260806230427155",
             "Data-File": "b.parquet", "Data-File Size": "1.0 MB", "Num Delta Files": "2"},
        ],
    )


class TestGetFileSizeStats:
    def test_latest_slice_per_file_group(self):
        executor = _executor_with([_fsview_rows()])
        d = json.loads(get_file_size_stats(executor, _session()))
        assert d["success"] is True
        # 2 current file groups (the 100KB historical slice must NOT count)
        assert d["base_file_count"] == 2
        assert d["min_bytes"] == int(427.1 * 1024)
        assert d["max_bytes"] == 1024 * 1024
        assert d["log_file_count"] == 2
        assert d["files_per_partition"] == {"city=sf": 1, "city=ny": 1}

    def test_error_when_no_base_files(self):
        executor = _executor_with([])
        d = json.loads(get_file_size_stats(executor, _session()))
        assert d["success"] is False
