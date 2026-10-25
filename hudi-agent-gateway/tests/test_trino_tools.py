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

from __future__ import annotations

import json

from hudi_agent_gateway.tools.registry import ToolRegistry
from hudi_agent_gateway.tools.trino_client import QueryResult
from hudi_agent_gateway.tools.trino_tools import shape_result


async def _call(registry: ToolRegistry, name: str, **kwargs: object) -> dict:
    return json.loads(await registry.get(name).handler(**kwargs))


async def test_query_lakehouse_shapes_result(registry: ToolRegistry, fake_trino) -> None:
    out = await _call(registry, "query_lakehouse", sql="SELECT city FROM trips")
    assert out["columns"] == ["city", "trips"]
    assert out["row_count"] == 3
    assert out["truncated"] is False
    # the guardrail-injected LIMIT reached the client
    assert "LIMIT 100" in fake_trino.executed[0]


async def test_query_lakehouse_guardrail_error_returned_not_raised(registry: ToolRegistry) -> None:
    out = await _call(registry, "query_lakehouse", sql="DROP TABLE trips")
    assert "error" in out and "read-only" in out["error"]
    assert "hint" in out


async def test_query_lakehouse_trino_error_returned(registry: ToolRegistry, fake_trino) -> None:
    fake_trino.fail_with("SYNTAX_ERROR: line 1: mismatched input")
    out = await _call(registry, "query_lakehouse", sql="SELECT nope FROM trips")
    assert out["error"].startswith("query failed")


async def test_query_lakehouse_timeout_gets_actionable_hint(
    registry: ToolRegistry, fake_trino
) -> None:
    """Timeouts must NOT get the generic 'fix the SQL' hint -- the SQL is fine."""
    from hudi_agent_gateway.tools.trino_client import TrinoTimeoutError

    fake_trino.error = TrinoTimeoutError("query exceeded the 120s timeout")
    out = await _call(registry, "query_lakehouse", sql="SELECT count(*) FROM trips")
    assert out["error"].startswith("query failed")
    assert "GATEWAY_SQL_TIMEOUT_SECONDS" in out["hint"]
    assert "Fix the SQL" not in out["hint"]


async def test_truncation_notice(registry: ToolRegistry, fake_trino) -> None:
    fake_trino.result = QueryResult(
        columns=["s"], rows=[["x" * 100] for _ in range(200)]
    )
    out = await _call(registry, "query_lakehouse", sql="SELECT s FROM big")
    assert out["truncated"] is True
    assert "notice" in out
    assert out["row_count"] == 200  # original fetched count is preserved
    assert len(out["rows"]) < 200


async def test_row_capped_result_reports_truncated(registry: ToolRegistry, fake_trino) -> None:
    """A result that filled the row cap is truncated even when it fits in bytes."""
    fake_trino.result = QueryResult(columns=["c"], rows=[["v"] for _ in range(100)])
    out = await _call(registry, "query_lakehouse", sql="SELECT c FROM big")
    assert out["truncated"] is True
    assert "row cap" in out["notice"]
    assert len(out["rows"]) == 100  # the byte-size loop did not fire


async def test_list_tables_defaults(registry: ToolRegistry, fake_trino) -> None:
    await _call(registry, "list_tables")
    sql = fake_trino.executed[0]
    assert '"hudi".information_schema.tables' in sql
    assert "table_schema = 'default'" in sql


async def test_list_tables_explicit(registry: ToolRegistry, fake_trino) -> None:
    await _call(registry, "list_tables", catalog="other", schema_name="s2")
    sql = fake_trino.executed[0]
    assert '"other".information_schema.tables' in sql
    assert "table_schema = 's2'" in sql


async def test_describe_table_name_forms(registry: ToolRegistry, fake_trino) -> None:
    await _call(registry, "describe_table", table="trips")
    assert '"hudi".information_schema.columns' in fake_trino.executed[0]
    await _call(registry, "describe_table", table="cat.sch.trips")
    assert '"cat".information_schema.columns' in fake_trino.executed[1]
    out = await _call(registry, "describe_table", table="a.b.c.d")
    assert "error" in out


async def test_describe_missing_table(registry: ToolRegistry, fake_trino) -> None:
    fake_trino.result = QueryResult(columns=["column_name"], rows=[])
    out = await _call(registry, "describe_table", table="ghost")
    assert "not found" in out["error"]


async def test_identifier_injection_rejected(registry: ToolRegistry, fake_trino) -> None:
    """Quoted breakouts in catalog/schema/table params return errors, never reach Trino."""
    out = await _call(registry, "list_tables", catalog='hudi" --', schema_name="default")
    assert "invalid catalog name" in out["error"]
    out = await _call(registry, "list_tables", schema_name="x' OR '1'='1")
    assert "invalid schema name" in out["error"]
    out = await _call(registry, "describe_table", table="a.b.c'; DROP TABLE t --")
    assert "invalid table name" in out["error"]
    assert fake_trino.executed == []  # nothing was ever sent


async def test_shape_result_failsafe_when_metadata_exceeds_cap() -> None:
    """Zero rows left but payload still too big -> bounded fallback payload."""
    result = QueryResult(columns=[f"col_{i}" for i in range(500)], rows=[["v"] * 500])
    text = shape_result(result, max_bytes=400, sql="SELECT " + "x" * 5000, row_cap=100)
    assert len(text.encode()) <= 400
    body = json.loads(text)
    assert body["truncated"] is True and body["rows"] == []
    assert body["column_count"] == 500
    # extreme cap: degrades to a bare bounded error payload
    tiny = shape_result(result, max_bytes=80, sql="SELECT 1", row_cap=100)
    assert len(tiny.encode()) <= 80 and json.loads(tiny)["truncated"] is True
