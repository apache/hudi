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

"""Define-once, expose-twice: LangChain and MCP derive from one registration."""

from __future__ import annotations

import json
import logging
from typing import Annotated

import pytest
from fastmcp import Client, FastMCP
from pydantic import Field

from hudi_agent_gateway.tools.registry import ToolRegistry, is_tool_error


@pytest.fixture()
def simple_registry() -> ToolRegistry:
    registry = ToolRegistry()

    @registry.register("echo_tool", "Echoes its input.")
    async def echo_tool(
        text: Annotated[str, Field(description="Text to echo back.")],
    ) -> str:
        return json.dumps({"echo": text})

    return registry


def test_langchain_derivation(simple_registry: ToolRegistry) -> None:
    tools = simple_registry.langchain_tools()
    assert len(tools) == 1
    tool = tools[0]
    assert tool.name == "echo_tool"
    assert tool.description == "Echoes its input."
    schema = tool.tool_call_schema.model_json_schema()
    assert schema["properties"]["text"]["description"] == "Text to echo back."
    assert schema["required"] == ["text"]


async def test_mcp_derivation_matches(simple_registry: ToolRegistry) -> None:
    mcp: FastMCP = FastMCP("test")
    simple_registry.attach_to_mcp(mcp)
    async with Client(mcp) as client:
        tools = await client.list_tools()
        assert [t.name for t in tools] == ["echo_tool"]
        assert tools[0].description == "Echoes its input."
        assert tools[0].inputSchema["properties"]["text"]["description"] == "Text to echo back."


async def test_same_payload_via_both_paths(simple_registry: ToolRegistry) -> None:
    lc_result = await simple_registry.langchain_tools()[0].ainvoke({"text": "hi"})

    mcp: FastMCP = FastMCP("test")
    simple_registry.attach_to_mcp(mcp)
    async with Client(mcp) as client:
        mcp_result = await client.call_tool("echo_tool", {"text": "hi"})
    assert json.loads(lc_result) == {"echo": "hi"}
    assert json.loads(mcp_result.content[0].text) == {"echo": "hi"}


async def test_invocation_logged_once(
    simple_registry: ToolRegistry, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger="hudi_agent_gateway.tools"):
        await simple_registry.get("echo_tool").handler(text="hi")
    records = [r for r in caplog.records if r.getMessage() == "tool_invocation"]
    assert len(records) == 1
    fields = records[0].extra_fields  # type: ignore[attr-defined]
    assert fields["tool"] == "echo_tool"
    assert fields["outcome"] == "ok"
    assert fields["duration_ms"] >= 0


def test_duplicate_registration_rejected(simple_registry: ToolRegistry) -> None:
    with pytest.raises(ValueError, match="already registered"):
        simple_registry.register("echo_tool", "dup")


def test_is_tool_error_parses_not_substring_matches() -> None:
    assert is_tool_error(json.dumps({"error": "boom", "hint": "fix it"}))
    # a successful result whose DATA mentions "error" (e.g. a column named
    # `error`) is not an error
    assert not is_tool_error(json.dumps({"columns": ["error"], "rows": [[1]]}))
    assert not is_tool_error("not json at all")
