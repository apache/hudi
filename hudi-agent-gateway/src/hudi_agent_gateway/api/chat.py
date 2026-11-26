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

"""POST /v1/chat: the agent-loop inference endpoint (JSON or SSE)."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import aclosing
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage, ToolMessage
from langgraph.errors import GraphRecursionError
from sse_starlette.sse import EventSourceResponse, ServerSentEvent

from hudi_agent_gateway.api.models import (
    ChatRequest,
    ChatResponse,
    DoneEvent,
    ErrorEvent,
    StatusEvent,
    TokenEvent,
    ToolCallEvent,
    ToolResultEvent,
    ToolTraceEntry,
)
from hudi_agent_gateway.cancellation import ToolCancellationScope, tool_cancellation_scope
from hudi_agent_gateway.log import log_event, request_id_var, session_id_var
from hudi_agent_gateway.tools.registry import is_tool_error

logger = logging.getLogger("hudi_agent_gateway.chat")


def _message_text(msg: Any) -> str:
    text = getattr(msg, "text", None)
    if isinstance(text, str):
        return text
    if callable(text):  # langchain-core < 1.x compatibility
        return text()
    return str(getattr(msg, "content", ""))


router = APIRouter()

_PREVIEW_CHARS = 500


def _agent_config(request: Request, session_id: str) -> dict[str, Any]:
    settings = request.app.state.settings
    return {
        "configurable": {"thread_id": session_id},
        "recursion_limit": settings.agent_max_iterations,
    }


def _tool_trace_from_messages(messages: list[Any]) -> list[ToolTraceEntry]:
    """Reconstruct the tool trace from the messages appended this turn."""
    calls: dict[str, ToolTraceEntry] = {}
    trace: list[ToolTraceEntry] = []
    for msg in messages:
        if isinstance(msg, AIMessage):
            for tc in msg.tool_calls or []:
                entry = ToolTraceEntry(name=tc["name"], args=tc.get("args") or {})
                calls[tc.get("id") or ""] = entry
                trace.append(entry)
        elif isinstance(msg, ToolMessage):
            matched = calls.get(msg.tool_call_id or "")
            if matched is not None:
                content = msg.content if isinstance(msg.content, str) else str(msg.content)
                matched.result_preview = content[:_PREVIEW_CHARS]
                matched.is_error = is_tool_error(content)
    return trace


@router.post("/v1/chat")
async def chat(request: Request, body: ChatRequest) -> Any:
    request_id_var.set(uuid.uuid4().hex[:12])
    session_id_var.set(body.session_id)
    state = request.app.state
    state.sessions.touch(body.session_id)
    log_event(logger, "chat_request", stream=body.stream, message_chars=len(body.message))

    wants_stream = body.stream or "text/event-stream" in request.headers.get("accept", "")
    if wants_stream:
        return EventSourceResponse(_stream_chat(request, body))

    inputs = {"messages": [HumanMessage(body.message)]}
    try:
        async with asyncio.timeout(state.settings.chat_timeout_seconds):
            agent = state.agents.get(body.model)
            result = await agent.ainvoke(inputs, config=_agent_config(request, body.session_id))
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=_timeout_message(request)) from e
    except GraphRecursionError as e:
        raise HTTPException(
            status_code=422,
            detail="The agent hit its iteration limit before finishing. Try a narrower question.",
        ) from e
    except Exception as e:
        logger.exception("agent invocation failed")
        raise HTTPException(status_code=502, detail=f"agent failed: {e}") from e

    messages = result["messages"]
    final = messages[-1]
    final_text = _message_text(final)
    # Trace only this turn: messages after the HumanMessage we just appended.
    turn_start = max(
        i for i, m in enumerate(messages) if isinstance(m, HumanMessage)
    )
    return ChatResponse(
        session_id=body.session_id,
        message=final_text,
        tool_trace=_tool_trace_from_messages(messages[turn_start:]),
    )


async def _stream_chat(request: Request, body: ChatRequest) -> AsyncIterator[ServerSentEvent]:
    state = request.app.state
    inputs = {"messages": [HumanMessage(body.message)]}
    final_text_parts: list[str] = []
    yield _status_event("Preparing response…")
    scope = ToolCancellationScope()
    scope_token = tool_cancellation_scope.set(scope)
    try:
        async with asyncio.timeout(state.settings.chat_timeout_seconds):
            agent = state.agents.get(body.model)
            async with aclosing(agent.astream(
                inputs,
                config=_agent_config(request, body.session_id),
                stream_mode=["messages", "updates"],
            )) as stream:
                async for mode, chunk in stream:
                    if mode == "messages":
                        msg_chunk, _meta = chunk
                        if isinstance(msg_chunk, AIMessageChunk):
                            text = _message_text(msg_chunk)
                            if text:
                                final_text_parts.append(text)
                                yield ServerSentEvent(
                                    event="token", data=TokenEvent(text=text).model_dump_json()
                                )
                    elif mode == "updates" and isinstance(chunk, dict):
                        for event in _updates_to_events(chunk):
                            yield event
    except TimeoutError:
        log_event(logger, "chat_timeout", timeout_seconds=state.settings.chat_timeout_seconds)
        yield ServerSentEvent(
            event="error",
            data=ErrorEvent(
                code="chat_timeout", message=_timeout_message(request)
            ).model_dump_json(),
        )
        return
    except asyncio.CancelledError:
        log_event(logger, "chat_cancelled")
        raise
    except GraphRecursionError:
        yield ServerSentEvent(
            event="error",
            data=ErrorEvent(
                code="recursion_limit",
                message="The agent hit its iteration limit before finishing.",
            ).model_dump_json(),
        )
        return
    except Exception as e:  # noqa: BLE001 - stream must terminate with an error event
        logger.exception("agent stream failed")
        yield ServerSentEvent(
            event="error",
            data=ErrorEvent(code="agent_error", message=str(e)).model_dump_json(),
        )
        return
    finally:
        # LangGraph may run tools in tasks that outlive its streaming iterator.
        # Cancel only this turn's tasks when the stream ends or disconnects.
        scope.cancel()
        tool_cancellation_scope.reset(scope_token)

    yield ServerSentEvent(
        event="done",
        data=DoneEvent(
            session_id=body.session_id, message="".join(final_text_parts)
        ).model_dump_json(),
    )


def _timeout_message(request: Request) -> str:
    seconds = request.app.state.settings.chat_timeout_seconds
    return (f"The response exceeded the {seconds:g}s time limit. Start a new session "
            "before retrying, because this session may contain an incomplete tool call.")


def _status_event(message: str) -> ServerSentEvent:
    return ServerSentEvent(event="status", data=StatusEvent(message=message).model_dump_json())


def _updates_to_events(update: dict[str, Any]) -> list[ServerSentEvent]:
    events: list[ServerSentEvent] = []
    if "pre_model_hook" in update:
        events.append(_status_event("Waiting for model…"))
    agent_update = update.get("agent") or {}
    for msg in agent_update.get("messages", []):
        if isinstance(msg, AIMessage):
            for tc in msg.tool_calls or []:
                events.append(
                    ServerSentEvent(
                        event="tool_call",
                        data=ToolCallEvent(
                            id=tc.get("id") or "", name=tc["name"], args=tc.get("args") or {}
                        ).model_dump_json(),
                    )
                )
    tools_update = update.get("tools") or {}
    for msg in tools_update.get("messages", []):
        if isinstance(msg, ToolMessage):
            content = msg.content if isinstance(msg.content, str) else str(msg.content)
            events.append(
                ServerSentEvent(
                    event="tool_result",
                    data=ToolResultEvent(
                        id=msg.tool_call_id or "",
                        name=msg.name or "",
                        preview=content[:_PREVIEW_CHARS],
                        is_error=is_tool_error(content),
                    ).model_dump_json(),
                )
            )
    return events
