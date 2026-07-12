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

import httpx
import pytest

from hudi_agent_gateway.app import create_app


@pytest.fixture()
async def client(settings, fake_trino, monkeypatch):
    app = create_app(settings.model_copy(update={"mcp_enabled": False}))
    # The app builds its own TrinoClient; swap in the fake after startup.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c, app.router.lifespan_context(app):
        app.state.trino_client = fake_trino
        yield c, app


async def test_health(client) -> None:
    c, _ = client
    resp = await c.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


async def test_ready_reports_dependency_failures(client) -> None:
    c, app = client
    app.state.trino_client.fail_with("connection refused")

    class NotReady:
        async def check(self):
            return False, "unreachable (http://x)"

    app.state.llm_readiness = NotReady()
    resp = await c.get("/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert body["ready"] is False
    assert body["checks"]["trino"]["ok"] is False
    assert body["checks"]["llm"]["ok"] is False


async def test_ready_ok(client) -> None:
    c, app = client

    class Ready:
        async def check(self):
            return True, "reachable"

    app.state.llm_readiness = Ready()
    resp = await c.get("/ready")
    assert resp.status_code == 200
    assert resp.json()["ready"] is True


async def test_tools_listing(client) -> None:
    c, _ = client
    resp = await c.get("/v1/tools")
    tools = resp.json()["tools"]
    assert {t["name"] for t in tools} == {"query_lakehouse", "list_tables", "describe_table"}
    for tool in tools:
        assert tool["description"]
        assert "properties" in tool["input_schema"]


async def test_info(client) -> None:
    c, _ = client
    resp = await c.get("/v1/info")
    body = resp.json()
    assert body["name"] == "hudi-agent-gateway"
    assert body["provider"] == "ollama"
    assert body["catalog"] == "hudi"
    assert body["schema"] == "default"
    assert body["trino_url"] == "http://fake-trino:8080"
    assert body["trino_ui_url"] == "http://fake-trino:8080/ui/"
    assert body["mcp_enabled"] is False  # fixture disables MCP
