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

"""Read-only tests on an existing table in the Helm Connect endpoint.

Set GATEWAY_IT_SPARK_CONNECT_URI and GATEWAY_IT_SPARK_TABLE (schema.table).
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
pytestmark = pytest.mark.skipif(
    not URI or not TABLE, reason="set Connect URI and existing test table"
)


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


@pytest.mark.skipif(os.environ.get("GATEWAY_IT_LLM") != "1", reason="set GATEWAY_IT_LLM=1")
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
