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

"""Request/response and SSE payload schemas for the HTTP API."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, description="The user's message.")
    session_id: str = Field(
        min_length=1,
        max_length=128,
        description="Conversation id; reuse it for multi-turn memory.",
    )
    stream: bool = Field(default=False, description="Stream the response as SSE.")
    model: str | None = Field(
        default=None,
        max_length=200,
        description="Model to use (within the configured provider); default when omitted.",
    )


class ToolTraceEntry(BaseModel):
    name: str
    args: dict[str, Any] = Field(default_factory=dict)
    result_preview: str = ""
    is_error: bool = False


class ChatResponse(BaseModel):
    session_id: str
    message: str
    tool_trace: list[ToolTraceEntry] = Field(default_factory=list)


# --- SSE event payloads (each is the JSON body of one `data:` line) ---


class TokenEvent(BaseModel):
    text: str


class ToolCallEvent(BaseModel):
    id: str
    name: str
    args: dict[str, Any] = Field(default_factory=dict)


class ToolResultEvent(BaseModel):
    id: str
    name: str
    preview: str
    is_error: bool = False


class DoneEvent(BaseModel):
    session_id: str
    message: str


class ErrorEvent(BaseModel):
    code: Literal["llm_error", "agent_error", "recursion_limit"]
    message: str


class ModelsResponse(BaseModel):
    provider: str
    default: str
    models: list[str]


class DependencyStatus(BaseModel):
    ok: bool
    detail: str = ""


class ReadyResponse(BaseModel):
    ready: bool
    checks: dict[str, DependencyStatus]


class InfoResponse(BaseModel):
    name: str
    version: str
    provider: str
    model: str
    catalog: str
    schema_: str = Field(alias="schema")
    # In-cluster/network endpoints, for the UI's connect panel.
    trino_url: str
    trino_ui_url: str
    mcp_enabled: bool

    model_config = {"populate_by_name": True}
