# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""AST-level SQL guardrail behavior: read-only enforcement + LIMIT capping."""

from __future__ import annotations

import pytest

from hudi_agent_gateway.tools.guardrails import enforce_guardrails
from hudi_agent_gateway.tools.registry import ToolInputError

CAP = 100


def test_plain_select_gets_limit_injected() -> None:
    out = enforce_guardrails("SELECT a FROM t", row_cap=CAP)
    assert f"LIMIT {CAP}" in out


def test_existing_smaller_limit_is_preserved() -> None:
    out = enforce_guardrails("SELECT a FROM t LIMIT 10", row_cap=CAP)
    assert "LIMIT 10" in out and f"LIMIT {CAP}" not in out


def test_oversized_limit_is_lowered() -> None:
    out = enforce_guardrails("SELECT a FROM t LIMIT 100000", row_cap=CAP)
    assert f"LIMIT {CAP}" in out and "100000" not in out


def test_cte_select_is_allowed() -> None:
    out = enforce_guardrails("WITH x AS (SELECT a FROM t) SELECT * FROM x", row_cap=CAP)
    assert out.upper().startswith("WITH") and f"LIMIT {CAP}" in out


def test_union_is_allowed_and_capped() -> None:
    out = enforce_guardrails("SELECT a FROM t UNION ALL SELECT a FROM u", row_cap=CAP)
    assert "UNION" in out.upper() and f"LIMIT {CAP}" in out


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN UPDATE SET a = 1",
        "CREATE TABLE t2 AS SELECT * FROM t",
        "DROP TABLE t",
        "TRUNCATE TABLE t",
        "GRANT SELECT ON t TO alice",
        "SET SESSION query_max_run_time = '1m'",
        "EXPLAIN SELECT 1",
        "CALL system.runtime.kill_query('q')",
        "USE hudi.default",
    ],
)
def test_non_select_statements_are_rejected(sql: str) -> None:
    with pytest.raises(ToolInputError):
        enforce_guardrails(sql, row_cap=CAP)


def test_select_wrapping_dml_is_rejected() -> None:
    # SELECT-rooted, so only the defense-in-depth tree walk can catch it.
    with pytest.raises(ToolInputError, match="Insert"):
        enforce_guardrails("WITH t AS (INSERT INTO x VALUES (1)) SELECT * FROM t", row_cap=CAP)


def test_multiple_statements_are_rejected() -> None:
    with pytest.raises(ToolInputError, match="one statement"):
        enforce_guardrails("SELECT 1; SELECT 2", row_cap=CAP)


def test_garbage_is_rejected() -> None:
    with pytest.raises(ToolInputError):
        enforce_guardrails("please show me the data", row_cap=CAP)


def test_comment_smuggled_dml_is_rejected() -> None:
    with pytest.raises(ToolInputError):
        enforce_guardrails("/* harmless */ DELETE FROM t -- SELECT", row_cap=CAP)


def test_string_literal_containing_dml_is_fine() -> None:
    out = enforce_guardrails("SELECT 'DELETE FROM t' AS s", row_cap=CAP)
    assert "DELETE FROM t" in out  # inside the literal


def test_quoted_identifiers_round_trip() -> None:
    out = enforce_guardrails('SELECT "weird col" FROM "my table"', row_cap=CAP)
    assert '"weird col"' in out
