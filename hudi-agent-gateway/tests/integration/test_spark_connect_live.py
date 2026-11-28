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

"""Read-only tests for a Spark Connect endpoint.

Set GATEWAY_IT_SPARK_CONNECT_URI. Table tests also need GATEWAY_IT_SPARK_TABLE (schema.table).
GATEWAY_IT_LLM=1 also enables the real Agent loop. No fixture writes here.
"""

import json
import os
import uuid

import pytest
from fastmcp import Client

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.mcp_server import build_mcp
from hudi_agent_gateway.tools import build_client, build_registry

URI = os.environ.get("GATEWAY_IT_SPARK_CONNECT_URI", "")
TABLE = os.environ.get("GATEWAY_IT_SPARK_TABLE", "")
pytestmark = pytest.mark.skipif(not URI, reason="set Connect URI")
requires_table = pytest.mark.skipif(not TABLE, reason="set existing test table")


@pytest.fixture()
def connect_registry():
    s = GatewaySettings(
        sql_backend="spark-connect",
        spark_connect_uri=URI,
    )
    client = build_client(s)
    try:
        yield s, build_registry(s, client)
    finally:
        client.close()


@requires_table
async def test_tools_and_mcp_on_connect(connect_registry):
    _, registry = connect_registry
    schema, table = TABLE.split(".")
    described = json.loads(await registry.get("describe_table").handler(table=TABLE))
    assert "error" not in described and described["row_count"] > 0
    tables = json.loads(await registry.get("list_tables").handler(schema_name=schema))
    assert "error" not in tables and table in [row[1] for row in tables["rows"]]
    # describe_table validates identifiers before the SELECT is constructed.
    sql = f"SELECT count(*) AS rows FROM `{schema}`.`{table}`"
    out = json.loads(await registry.get("query_lakehouse").handler(sql=sql))
    assert "error" not in out and out["row_count"] == 1
    async with Client(build_mcp(registry)) as mcp:
        result = await mcp.call_tool("query_lakehouse", {"sql": sql})
        assert json.loads(result.content[0].text)["rows"] == out["rows"]
        result = await mcp.call_tool("query_lakehouse", {"sql": f"DELETE FROM {TABLE}"})
        assert "read-only" in json.loads(result.content[0].text)["error"]


async def test_query_types_on_connect(connect_registry):
    _, registry = connect_registry
    # Two distinct instants sharing the same local time during the New York DST fold.
    sql = """
        SELECT array(named_struct('amount',
                   cast('123456789012345678901234567890.1200' AS DECIMAL(38,4)))) AS items,
               timestamp_micros(1730611800123456) AS ts1,
               timestamp_micros(1730615400123456) AS ts2,
               to_timestamp_ntz('2024-11-03 01:30:00.123456') AS ntz,
               map(array(1, 2), unhex('00ff')) AS blobs,
               DATE '2024-11-03' AS day, cast(NULL AS STRING) AS missing
    """
    async with Client(build_mcp(registry)) as mcp:
        response = await mcp.call_tool("query_lakehouse", {"sql": sql})
    result = json.loads(response.content[0].text)
    assert "error" not in result, result
    assert result["column_types"] == [
        "array<struct<`amount`:decimal(38,4)>>", "timestamp", "timestamp", "timestamp_ntz",
        "map<array<int>,binary>", "date", "string",
    ]
    assert result["rows"] == [[
        [{"amount": "123456789012345678901234567890.1200"}], "2024-11-03T05:30:00.123456+00:00",
        "2024-11-03T06:30:00.123456+00:00", "2024-11-03T01:30:00.123456", [[[1, 2], "AP8="]],
        "2024-11-03", None,
    ]]


async def test_nullable_and_duplicate_structs_on_connect(connect_registry):
    _, registry = connect_registry
    query = registry.get("query_lakehouse").handler
    result = json.loads(await query(sql="""
        SELECT IF(id = 0, NULL, struct(1 AS k)) AS s,
               array(IF(id = 0, NULL, struct(1 AS k))) AS a,
               map('v', IF(id = 0, NULL, struct(1 AS k))) AS m,
               array(struct(id AS x, 2 AS x)) AS dup
        FROM range(2) ORDER BY id
    """))
    assert "error" not in result, result
    assert result["rows"] == [
        [None, [None], {"v": None}, [[0, 2]]],
        [{"k": 1}, [{"k": 1}], {"v": {"k": 1}}, [[1, 2]]],
    ]
    empty = json.loads(await query(sql="""
        SELECT struct(1 AS x, 2 AS x) AS dup, struct('v' AS `a:int,b`) AS special,
               struct(1 AS a, 'v' AS b) AS ordinary, struct(1 AS `x``y`) AS quoted
        WHERE false
    """))
    assert empty["rows"] == []
    assert empty["column_types"] == [
        "struct<`x`:int,`x`:int>", "struct<`a:int,b`:string>", "struct<`a`:int,`b`:string>",
        "struct<`x``y`:int>",
    ]


async def test_reserved_struct_field_type_round_trip(connect_registry):
    _, registry = connect_registry
    query = registry.get("query_lakehouse").handler
    result = json.loads(await query(sql="SELECT named_struct('select', 7) AS s"))
    assert result["rows"] == [[{"select": 7}]]
    dtype, = result["column_types"]
    assert dtype == "struct<`select`:int>"
    cast = json.loads(await query(sql=f"SELECT CAST(NULL AS {dtype}) AS s"))
    assert cast["column_types"] == [dtype] and cast["rows"] == [[None]]


async def test_nonfinite_numbers_on_connect(connect_registry):
    _, registry = connect_registry
    query = registry.get("query_lakehouse").handler
    text = await query(sql="""
        SELECT CAST('NaN' AS FLOAT) AS f,
               map(CAST('Infinity' AS DOUBLE), array(CAST('-Infinity' AS DOUBLE), 1D)) AS m
    """)

    result = json.loads(text)
    assert result["rows"] == [["NaN", [["Infinity", ["-Infinity", 1.0]]]]]


async def test_extended_years_and_bigint_keys_on_connect(connect_registry):
    _, registry = connect_registry
    query = registry.get("query_lakehouse").handler
    result = json.loads(await query(sql="""
        SELECT DATE '+10000-01-01' AS d, timestamp_micros(-62167219200000000) AS ts,
               to_timestamp_ntz('0000-01-01 01:00:00') AS ntz,
               map(9007199254740992L, 'a', 9007199254740993L, 'b') AS m
    """))
    assert "error" not in result, result
    assert result["column_types"] == ["date", "timestamp", "timestamp_ntz", "map<bigint,string>"]
    assert result["rows"] == [[
        "+10000-01-01", "0000-01-01T00:00:00+00:00", "0000-01-01T01:00:00",
        {"9007199254740992": "a", "9007199254740993": "b"},
    ]]


@pytest.mark.skipif(os.environ.get("GATEWAY_IT_LLM") != "1", reason="set GATEWAY_IT_LLM=1")
@requires_table
async def test_agent_on_connect(connect_registry):
    from langgraph.checkpoint.memory import InMemorySaver

    from hudi_agent_gateway.agent import build_agent
    from hudi_agent_gateway.llm import build_chat_model

    s, registry = connect_registry
    agent = build_agent(build_chat_model(s), registry, InMemorySaver(), s)
    out = await agent.ainvoke(
        {"messages": [("user", f"Describe the columns of {TABLE}, using your tools.")]},
        config={"configurable": {"thread_id": "connect-it"}},
    )
    assert any(getattr(m, "name", None) == "describe_table" for m in out["messages"])


@pytest.mark.skipif(not os.environ.get("GATEWAY_IT_HTTP_URL"), reason="set GATEWAY_IT_HTTP_URL")
@requires_table
async def test_http_mcp_on_connect():
    base = os.environ["GATEWAY_IT_HTTP_URL"].rstrip("/")
    schema, table = TABLE.split(".")
    sql = f"SELECT count(*) AS rows FROM `{schema}`.`{table}`"
    async with Client(base + "/mcp") as mcp:
        result = await mcp.call_tool("query_lakehouse", {"sql": sql})
        data = json.loads(result.content[0].text)
        assert "error" not in data and data["rows"][0][0] > 0
        result = await mcp.call_tool("query_lakehouse", {"sql": f"DELETE FROM {TABLE}"})
        assert "read-only" in json.loads(result.content[0].text)["error"]


@pytest.mark.skipif(
    not os.environ.get("GATEWAY_IT_HTTP_URL") or os.environ.get("GATEWAY_IT_LLM") != "1",
    reason="set GATEWAY_IT_HTTP_URL and GATEWAY_IT_LLM=1",
)
@requires_table
async def test_http_agent_on_connect():
    import httpx

    base = os.environ["GATEWAY_IT_HTTP_URL"].rstrip("/")
    session_id = "connect-http-it-" + uuid.uuid4().hex
    schema, table = TABLE.split(".")
    sql = f"SELECT COUNT(*) AS rows FROM `{schema}`.`{table}`"
    async with httpx.AsyncClient(timeout=180) as client:
        response = await client.post(
            base + "/v1/chat",
            json={"message": f"Describe the columns of {TABLE} using your describe_table tool.",
                  "session_id": session_id},
        )
        response.raise_for_status()
        result = response.json()
        assert any(call["name"] == "describe_table" and not call["is_error"]
                   for call in result["tool_trace"])
        response = await client.post(
            base + "/v1/chat",
            json={"message": f"Use query_lakehouse to run {sql}.", "session_id": session_id},
        )
        response.raise_for_status()
        trace = response.json()["tool_trace"]
        queries = [json.loads(call["result_preview"]) for call in trace
                   if call["name"] == "query_lakehouse" and not call["is_error"]]
        assert any(query["rows"][0][0] > 0 for query in queries if query.get("rows"))
