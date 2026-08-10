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

"""The MCP surface, including the mounted-app lifespan regression."""

from __future__ import annotations

import json

import httpx
from fastmcp import Client

from hudi_agent_gateway.app import create_app
from hudi_agent_gateway.mcp_server import build_mcp


async def test_mcp_lists_and_calls_tools(registry) -> None:
    mcp = build_mcp(registry)
    async with Client(mcp) as client:
        tools = await client.list_tools()
        assert {t.name for t in tools} == {"query_lakehouse", "list_tables", "describe_table"}
        result = await client.call_tool(
            "query_lakehouse", {"sql": "SELECT city, count(*) FROM trips GROUP BY city"}
        )
        payload = json.loads(result.content[0].text)
        assert payload["row_count"] == 3


async def test_mounted_mcp_serves_over_http(settings, fake_trino) -> None:
    """Regression for the FastMCP lifespan gotcha: without running the MCP
    sub-app's lifespan inside the outer app's, /mcp requests hang/fail."""
    app = create_app(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c, app.router.lifespan_context(app):
        app.state.trino_client = fake_trino
        resp = await c.post(
            "/mcp/",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0"},
                },
            },
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 200
        assert "hudi-lakehouse" in resp.text


async def test_mcp_disabled(settings, fake_trino) -> None:
    settings = settings.model_copy(update={"mcp_enabled": False})
    app = create_app(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c, app.router.lifespan_context(app):
        resp = await c.post("/mcp/", json={})
        assert resp.status_code == 404
