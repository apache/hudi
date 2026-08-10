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

"""The tool registry: define a lakehouse tool ONCE, expose it everywhere.

A tool is a typed async function whose parameters use
``Annotated[..., Field(description=...)]``. From that single definition the
registry derives:

- LangChain ``StructuredTool``s for the in-process agent loop, and
- FastMCP tool registrations for external MCP clients,

and wraps every invocation with a structured JSON log record (name, duration,
outcome) -- the same trace regardless of which surface called the tool.
"""

from __future__ import annotations

import functools
import inspect
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel

from hudi_agent_gateway.log import log_event

logger = logging.getLogger("hudi_agent_gateway.tools")

ToolHandler = Callable[..., Awaitable[str]]


class ToolInputError(Exception):
    """Raised by guardrails/handlers for invalid tool input; carries a hint for the model."""

    def __init__(self, message: str, hint: str = "") -> None:
        super().__init__(message)
        self.hint = hint


@dataclass(frozen=True)
class GatewayTool:
    name: str
    description: str
    handler: ToolHandler


def is_tool_error(content: str) -> bool:
    """Whether a tool result string is an ``{"error": ...}`` payload.

    Parses rather than substring-matches: a successful result whose data
    merely mentions "error" (e.g. a column named ``error``) is not an error.
    """
    try:
        parsed = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        return False
    return isinstance(parsed, dict) and "error" in parsed


def _with_invocation_logging(name: str, fn: ToolHandler) -> ToolHandler:
    @functools.wraps(fn)
    async def wrapped(*args: Any, **kwargs: Any) -> str:
        start = time.monotonic()
        outcome = "ok"
        try:
            result = await fn(*args, **kwargs)
            if is_tool_error(result):
                outcome = "tool_error"
            return result
        except Exception:
            outcome = "exception"
            raise
        finally:
            log_event(
                logger,
                "tool_invocation",
                tool=name,
                args={k: v for k, v in kwargs.items()},
                duration_ms=round((time.monotonic() - start) * 1000, 1),
                outcome=outcome,
            )

    return wrapped


class ToolRegistry:
    """Order-preserving registry of :class:`GatewayTool`."""

    def __init__(self) -> None:
        self._tools: dict[str, GatewayTool] = {}

    def register(self, name: str, description: str) -> Callable[[ToolHandler], ToolHandler]:
        if name in self._tools:
            raise ValueError(f"tool {name!r} is already registered")

        def deco(fn: ToolHandler) -> ToolHandler:
            wrapped = _with_invocation_logging(name, fn)
            self._tools[name] = GatewayTool(name=name, description=description, handler=wrapped)
            return fn

        return deco

    def get(self, name: str) -> GatewayTool:
        return self._tools[name]

    def __len__(self) -> int:
        return len(self._tools)

    def langchain_tools(self) -> list[BaseTool]:
        return [
            StructuredTool.from_function(
                coroutine=tool.handler,
                name=tool.name,
                description=tool.description,
                infer_schema=True,
            )
            for tool in self._tools.values()
        ]

    def attach_to_mcp(self, mcp: Any) -> None:
        """Register every tool on a FastMCP server instance."""
        for tool in self._tools.values():
            mcp.tool(tool.handler, name=tool.name, description=tool.description)

    def listing(self) -> list[dict[str, Any]]:
        """Tool metadata for ``GET /v1/tools``: name, description, and JSON schema."""
        out: list[dict[str, Any]] = []
        for lc_tool in self.langchain_tools():
            schema = lc_tool.tool_call_schema
            out.append(
                {
                    "name": lc_tool.name,
                    "description": lc_tool.description,
                    "input_schema": schema.model_json_schema()
                    if inspect.isclass(schema) and issubclass(schema, BaseModel)
                    else schema,
                }
            )
        return out
