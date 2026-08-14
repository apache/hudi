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

"""Shared fixtures: settings, a fake Trino client, and a scripted chat model.

Nothing here talks to a network; the whole suite runs offline.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools import build_registry
from hudi_agent_gateway.tools.trino_client import QueryResult, TrinoQueryError


@pytest.fixture()
def settings(monkeypatch: pytest.MonkeyPatch) -> GatewaySettings:
    # Isolate from the developer's environment.
    for var in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
        monkeypatch.delenv(var, raising=False)
    return GatewaySettings(
        llm_provider="ollama",
        trino_host="fake-trino",
        sql_row_cap=100,
        tool_result_max_bytes=2000,
    )


class FakeTrinoClient:
    """Records executed SQL; replays canned results or errors."""

    def __init__(self) -> None:
        self.executed: list[str] = []
        self.result = QueryResult(
            columns=["city", "trips"],
            rows=[["chennai", 33], ["san_francisco", 34], ["sao_paulo", 33]],
        )
        self.error: Exception | None = None

    async def execute(self, sql: str, *, timeout: float, max_rows: int) -> QueryResult:
        self.executed.append(sql)
        if self.error is not None:
            raise self.error
        return self.result

    async def ping(self, timeout: float = 5.0) -> bool:
        return self.error is None

    def fail_with(self, message: str) -> None:
        self.error = TrinoQueryError(message)


@pytest.fixture()
def fake_trino() -> FakeTrinoClient:
    return FakeTrinoClient()


@pytest.fixture()
def registry(settings: GatewaySettings, fake_trino: FakeTrinoClient):
    return build_registry(settings, trino_client=fake_trino)  # type: ignore[arg-type]


class ScriptedChatModel(GenericFakeChatModel):
    """A fake chat model that replays a fixed sequence of AIMessages, accepts
    ``bind_tools`` (returning itself), and streams each scripted message as a
    single chunk -- including tool calls, which the stock fake drops when the
    message content is empty."""

    def bind_tools(self, tools: Sequence[Any], **kwargs: Any) -> ScriptedChatModel:
        return self

    def _stream(self, messages: Any, stop: Any = None, run_manager: Any = None, **kwargs: Any):
        import json as _json

        from langchain_core.messages import AIMessageChunk
        from langchain_core.outputs import ChatGenerationChunk

        try:
            message = next(self.messages)
        except StopIteration:
            return
        tool_call_chunks = [
            {
                "name": tc["name"],
                "args": _json.dumps(tc.get("args") or {}),
                "id": tc.get("id"),
                "index": i,
                "type": "tool_call_chunk",
            }
            for i, tc in enumerate(getattr(message, "tool_calls", None) or [])
        ]
        chunk = AIMessageChunk(content=message.content, tool_call_chunks=tool_call_chunks)
        yield ChatGenerationChunk(message=chunk)


def scripted_model(messages: list[AIMessage]) -> ScriptedChatModel:
    return ScriptedChatModel(messages=iter(messages))


class FixedAgents:
    """AgentCache stand-in that returns one prebuilt agent for any model."""

    def __init__(self, agent: Any) -> None:
        self.agent = agent
        self.requested: list[str | None] = []

    def get(self, model: str | None = None) -> Any:
        self.requested.append(model)
        return self.agent


TOOL_CALL_THEN_ANSWER = [
    AIMessage(
        content="",
        tool_calls=[
            {
                "id": "call_1",
                "name": "query_lakehouse",
                "args": {"sql": "SELECT city, count(*) AS trips FROM trips GROUP BY city"},
            }
        ],
    ),
    AIMessage(
        content="There are 100 trips across 3 cities: "
        "chennai 33, san_francisco 34, sao_paulo 33."
    ),
]
