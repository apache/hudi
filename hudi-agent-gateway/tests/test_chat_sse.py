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

"""/v1/chat behavior with a scripted model: JSON path and SSE event stream."""

from __future__ import annotations

import json

import httpx
import pytest
from langchain_core.messages import AIMessage

from hudi_agent_gateway.agent import build_agent
from hudi_agent_gateway.app import create_app
from tests.conftest import TOOL_CALL_THEN_ANSWER, FixedAgents, scripted_model


@pytest.fixture()
async def client(settings, registry, fake_trino):
    app = create_app(settings.model_copy(update={"mcp_enabled": False}))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c, app.router.lifespan_context(app):
        app.state.trino_client = fake_trino
        app.state.registry = registry
        app.state.agents = FixedAgents(
            build_agent(
                scripted_model(TOOL_CALL_THEN_ANSWER),
                registry,
                app.state.sessions.checkpointer,
                settings,
            )
        )
        yield c, app


def _parse_sse(text: str) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []
    text = text.replace("\r\n", "\n")
    for frame in text.split("\n\n"):
        event, data = "message", []
        for line in frame.split("\n"):
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].strip())
        if data:
            events.append((event, json.loads("\n".join(data))))
    return events


async def test_non_streaming_chat_returns_trace(client) -> None:
    c, _ = client
    resp = await c.post(
        "/v1/chat", json={"message": "trips per city?", "session_id": "s-json"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "100 trips" in body["message"]
    assert body["session_id"] == "s-json"
    assert len(body["tool_trace"]) == 1
    trace = body["tool_trace"][0]
    assert trace["name"] == "query_lakehouse"
    assert "sql" in trace["args"]
    assert "chennai" in trace["result_preview"]
    assert trace["is_error"] is False


async def test_streaming_chat_event_order(client) -> None:
    c, _ = client
    resp = await c.post(
        "/v1/chat",
        json={"message": "trips per city?", "session_id": "s-sse", "stream": True},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    events = _parse_sse(resp.text)
    kinds = [e for e, _ in events]

    assert "tool_call" in kinds and "tool_result" in kinds and kinds[-1] == "done"
    assert kinds.index("tool_call") < kinds.index("tool_result")

    tool_call = next(d for e, d in events if e == "tool_call")
    assert tool_call["name"] == "query_lakehouse"
    tool_result = next(d for e, d in events if e == "tool_result")
    assert "chennai" in tool_result["preview"]
    done = events[-1][1]
    assert "100 trips" in done["message"]
    # token events accumulate to the same final text
    tokens = "".join(d["text"] for e, d in events if e == "token")
    assert tokens == done["message"]


async def test_streaming_error_event_on_model_failure(client, settings, registry) -> None:
    c, app = client

    class ExplodingModel(type(scripted_model([AIMessage(content="x")]))):
        def _generate(self, *args, **kwargs):
            raise RuntimeError("model exploded")

        async def _agenerate(self, *args, **kwargs):
            raise RuntimeError("model exploded")

    app.state.agents = FixedAgents(
        build_agent(
            ExplodingModel(messages=iter([])), registry, app.state.sessions.checkpointer, settings
        )
    )
    resp = await c.post(
        "/v1/chat", json={"message": "boom", "session_id": "s-err", "stream": True}
    )
    events = _parse_sse(resp.text)
    assert events[-1][0] == "error"
    assert events[-1][1]["code"] == "agent_error"


async def test_validation_rejects_empty_message(client) -> None:
    c, _ = client
    resp = await c.post("/v1/chat", json={"message": "", "session_id": "s"})
    assert resp.status_code == 422
