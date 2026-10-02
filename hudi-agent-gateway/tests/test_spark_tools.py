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

"""Spark Thrift backend tools: same contract as the Trino backend, Spark SQL shape."""

from __future__ import annotations

import json

from hudi_agent_gateway.tools.registry import ToolRegistry
from hudi_agent_gateway.tools.trino_client import QueryResult


async def _call(registry: ToolRegistry, name: str, **kwargs: object) -> dict:
    return json.loads(await registry.get(name).handler(**kwargs))


async def test_query_lakehouse_shapes_result(spark_registry: ToolRegistry, fake_spark) -> None:
    out = await _call(spark_registry, "query_lakehouse", sql="SELECT city FROM trips")
    assert out["columns"] == ["city", "trips"]
    assert out["row_count"] == 3
    assert out["truncated"] is False
    # the guardrail-injected LIMIT reached the client
    assert "LIMIT 100" in fake_spark.executed[0]


async def test_query_lakehouse_accepts_backquoted_identifiers(
    spark_registry: ToolRegistry, fake_spark
) -> None:
    """Spark SQL uses backticks; the guardrails must parse and preserve them."""
    out = await _call(
        spark_registry, "query_lakehouse", sql="SELECT `city` FROM `default`.`trips`"
    )
    assert "error" not in out
    assert "`default`.`trips`" in fake_spark.executed[0].replace('"', "`")


async def test_query_lakehouse_guardrail_error_returned_not_raised(
    spark_registry: ToolRegistry,
) -> None:
    out = await _call(spark_registry, "query_lakehouse", sql="DROP TABLE trips")
    assert "error" in out and "read-only" in out["error"]
    assert "Spark SQL" in out["hint"]


async def test_query_lakehouse_spark_error_returned(
    spark_registry: ToolRegistry, fake_spark
) -> None:
    fake_spark.fail_with("AnalysisException: cannot resolve 'nope'")
    out = await _call(spark_registry, "query_lakehouse", sql="SELECT nope FROM trips")
    assert out["error"].startswith("query failed")


async def test_query_lakehouse_timeout_gets_actionable_hint(
    spark_registry: ToolRegistry, fake_spark
) -> None:
    """Timeouts must NOT get the generic 'fix the SQL' hint -- the SQL is fine."""
    from hudi_agent_gateway.tools.spark_client import SparkTimeoutError

    fake_spark.error = SparkTimeoutError("query exceeded the 120s timeout")
    out = await _call(spark_registry, "query_lakehouse", sql="SELECT count(*) FROM trips")
    assert out["error"].startswith("query failed")
    assert "GATEWAY_SQL_TIMEOUT_SECONDS" in out["hint"]
    assert "Fix the SQL" not in out["hint"]


async def test_truncation_notice(spark_registry: ToolRegistry, fake_spark) -> None:
    fake_spark.result = QueryResult(columns=["s"], rows=[["x" * 100] for _ in range(200)])
    out = await _call(spark_registry, "query_lakehouse", sql="SELECT s FROM big")
    assert out["truncated"] is True
    assert "notice" in out
    assert out["row_count"] == 200  # original fetched count is preserved
    assert len(out["rows"]) < 200


async def test_row_capped_result_reports_truncated(
    spark_registry: ToolRegistry, fake_spark
) -> None:
    """A result that filled the row cap is truncated even when it fits in bytes."""
    fake_spark.result = QueryResult(columns=["c"], rows=[["v"] for _ in range(100)])
    out = await _call(spark_registry, "query_lakehouse", sql="SELECT c FROM big")
    assert out["truncated"] is True
    assert "row cap" in out["notice"]
    assert len(out["rows"]) == 100  # the byte-size loop did not fire


async def test_list_tables_defaults(spark_registry: ToolRegistry, fake_spark) -> None:
    await _call(spark_registry, "list_tables")
    assert fake_spark.executed[0] == "SHOW TABLES IN `default`"


async def test_list_tables_explicit(spark_registry: ToolRegistry, fake_spark) -> None:
    await _call(spark_registry, "list_tables", database="warehouse2")
    assert fake_spark.executed[0] == "SHOW TABLES IN `warehouse2`"


async def test_describe_table_name_forms(spark_registry: ToolRegistry, fake_spark) -> None:
    await _call(spark_registry, "describe_table", table="trips")
    assert fake_spark.executed[0] == "DESCRIBE TABLE `default`.`trips`"
    await _call(spark_registry, "describe_table", table="db2.trips")
    assert fake_spark.executed[1] == "DESCRIBE TABLE `db2`.`trips`"
    out = await _call(spark_registry, "describe_table", table="a.b.c")
    assert "error" in out  # spark tools accept table or database.table only


async def test_describe_missing_table_empty_result(
    spark_registry: ToolRegistry, fake_spark
) -> None:
    fake_spark.result = QueryResult(columns=["col_name"], rows=[])
    out = await _call(spark_registry, "describe_table", table="ghost")
    assert "not found" in out["error"]


async def test_describe_missing_table_server_error_mapped(
    spark_registry: ToolRegistry, fake_spark
) -> None:
    """Spark raises for a missing table; the error maps to the same 'not found' payload."""
    fake_spark.fail_with(
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `ghost` cannot be found."
    )
    out = await _call(spark_registry, "describe_table", table="ghost")
    assert "not found" in out["error"]
    assert "list_tables" in out["hint"]


async def test_identifier_injection_rejected(spark_registry: ToolRegistry, fake_spark) -> None:
    """Quoted breakouts in database/table params return errors, never reach Spark."""
    out = await _call(spark_registry, "list_tables", database="x` --")
    assert "invalid database name" in out["error"]
    out = await _call(spark_registry, "describe_table", table="a.b`; DROP TABLE t --")
    assert "invalid table name" in out["error"]
    assert fake_spark.executed == []  # nothing was ever sent


async def test_tool_surface_matches_trino_backend(
    spark_registry: ToolRegistry, registry: ToolRegistry
) -> None:
    """Both engines expose the identical tool names: the agent prompt, MCP
    clients, and the UI never care which engine is behind the gateway."""
    spark_names = {t["name"] for t in spark_registry.listing()}
    trino_names = {t["name"] for t in registry.listing()}
    assert spark_names == trino_names == {"query_lakehouse", "list_tables", "describe_table"}
