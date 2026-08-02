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

"""Tests for the executor's safety and result semantics.

These don't launch a real JVM; they exercise the guards and the result object.
``/bin/echo`` is used as a stand-in binary so ``HudiCliExecutor`` construction
passes its existence check without shelling out to Hudi.
"""

import hudi_cli.executor as executor_mod
from hudi_cli.executor import ExecutionResult, HudiCliExecutor, LazyHudiCliExecutor
from hudi_cli.parser import ParsedOutput


def _executor() -> HudiCliExecutor:
    return HudiCliExecutor(
        cli_bin="/bin/echo",
        spark_home="/x",
        cli_bundle_jar="/x/cli.jar",
        spark_bundle_jar="/x/spark.jar",
    )


class TestNewlineGuard:
    def test_embedded_newline_command_rejected_without_running(self):
        result = _executor().execute(["commits show\nsavepoint rollback --savepoint 1"])
        assert result.return_code == -1
        assert not result.is_success()
        assert any("injection" in e.lower() for e in result.parsed.errors)


class TestIsSuccess:
    def test_zero_return_code_but_error_line_is_failure(self):
        result = ExecutionResult(
            raw_output="",
            parsed=ParsedOutput(errors=["org...HoodieException: boom"]),
            return_code=0,
            duration_seconds=1.0,
        )
        assert result.is_success() is False

    def test_clean_zero_return_code_is_success(self):
        result = ExecutionResult(
            raw_output="", parsed=ParsedOutput(), return_code=0, duration_seconds=1.0
        )
        assert result.is_success() is True

    def test_timeout_is_failure(self):
        result = ExecutionResult(
            raw_output="",
            parsed=ParsedOutput(),
            return_code=-1,
            duration_seconds=1.0,
            timed_out=True,
        )
        assert result.is_success() is False


class TestOutputCap:
    def test_rows_capped(self, monkeypatch):
        monkeypatch.setattr(executor_mod, "DEFAULT_MAX_ROWS", 2)
        from hudi_cli.parser import ParsedTable

        big = ParsedTable(headers=["c"], rows=[{"c": str(i)} for i in range(10)])
        result = ExecutionResult(
            raw_output="",
            parsed=ParsedOutput(tables=[big]),
            return_code=0,
            duration_seconds=1.0,
        )
        d = result.to_dict()
        assert len(d["tables"][0]["rows"]) == 2
        assert d["tables"][0]["truncated"] is True
        assert "notice" in d


class TestLazyExecutor:
    def test_missing_config_returns_error_not_raise(self, monkeypatch):
        # No env set -> real executor construction would raise; the lazy wrapper
        # must instead return a readable error result so the server stays up.
        for var in ("SPARK_HOME", "CLI_BUNDLE_JAR", "SPARK_BUNDLE_JAR", "HUDI_CLI_BIN"):
            monkeypatch.delenv(var, raising=False)
        result = LazyHudiCliExecutor().execute(["desc"])
        assert result.return_code == -1
        assert any("not configured" in e.lower() for e in result.parsed.errors)
