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

"""The lakehouse agent: a LangGraph ReAct loop over the tool registry."""

from __future__ import annotations

from typing import Any, TypeGuard

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, AnyMessage, trim_messages
from langchain_core.messages.utils import count_tokens_approximately
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.prebuilt import create_react_agent

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools.registry import ToolRegistry

_SYSTEM_PROMPT = """\
You are the Apache Hudi lakehouse analyst. You answer questions about data in \
an Apache Hudi lakehouse by querying it through Trino using your tools. The \
default catalog is `{catalog}` and the default schema is `{schema}`; tables \
are Hudi tables registered in the catalog's metastore.

Tool strategy:
- For unfamiliar tables, call `list_tables` and `describe_table` BEFORE writing SQL.
- `query_lakehouse` accepts exactly ONE read-only SELECT statement (Trino SQL). \
Qualify names as catalog.schema.table when querying outside the defaults.
- A server-side row cap of {row_cap} rows is enforced. Prefer aggregations, \
GROUP BY, and explicit LIMIT over raw row dumps.
- If a result has `truncated: true`, tell the user the result was truncated and \
suggest how to narrow the query.
- If a query fails, read the error, fix the SQL, and retry (at most two retries).

Grounding rules:
- Answer ONLY from tool results. Never invent table names, column names, or values.
- State row counts when reporting results.
- Be concise. Use markdown tables for tabular answers.
{extra}"""


def build_system_prompt(settings: GatewaySettings) -> str:
    extra = f"\n{settings.system_prompt_extra}" if settings.system_prompt_extra else ""
    return _SYSTEM_PROMPT.format(
        catalog=settings.trino_catalog,
        schema=settings.trino_schema,
        row_cap=settings.sql_row_cap,
        extra=extra,
    )


class AgentCache:
    """Lazily builds one agent per model name (all sharing the same tools,
    checkpointer, and prompt), so a deployment configured with one provider
    can still serve any of that provider's models per request."""

    _MAX_AGENTS = 8

    def __init__(self, registry: ToolRegistry, checkpointer: BaseCheckpointSaver,
                 settings: GatewaySettings) -> None:
        self._registry = registry
        self._checkpointer = checkpointer
        self._settings = settings
        self._agents: dict[str, Any] = {}

    def get(self, model: str | None = None) -> Any:
        from hudi_agent_gateway.llm import build_chat_model

        name = model or self._settings.llm_model
        if name not in self._agents:
            if len(self._agents) >= self._MAX_AGENTS:
                self._agents.pop(next(iter(self._agents)))
            self._agents[name] = build_agent(
                build_chat_model(self._settings, name),
                self._registry,
                self._checkpointer,
                self._settings,
            )
        return self._agents[name]


def _is_broken_thinking_block(block: Any) -> TypeGuard[dict[str, Any]]:
    return isinstance(block, dict) and block.get("type") == "thinking" and "thinking" not in block


def repair_thinking_blocks(messages: list[AnyMessage]) -> list[AnyMessage]:
    """Restore the `thinking` field on streamed adaptive-thinking blocks.

    Anthropic's adaptive-thinking models can return a thinking block whose text
    is empty (signature only). When such a turn is streamed, langchain-anthropic
    merges the chunks into a block with no `thinking` key at all, and the API
    rejects the message on the next agent-loop call with
    "thinking.thinking: Field required". Re-adding the empty field is lossless.
    """
    repaired: list[AnyMessage] = []
    for msg in messages:
        content = msg.content
        if (
            isinstance(msg, AIMessage)
            and isinstance(content, list)
            and any(_is_broken_thinking_block(b) for b in content)
        ):
            blocks = [
                {**b, "thinking": ""} if _is_broken_thinking_block(b) else b
                for b in content
            ]
            msg = msg.model_copy(update={"content": blocks})
        repaired.append(msg)
    return repaired


def build_agent(
    model: BaseChatModel,
    registry: ToolRegistry,
    checkpointer: BaseCheckpointSaver,
    settings: GatewaySettings,
) -> Any:
    max_messages = settings.max_messages_per_session

    def trim_llm_input(state: dict[str, Any]) -> dict[str, Any]:
        # Bound what reaches the model without rewriting checkpointed history.
        trimmed = trim_messages(
            state["messages"],
            strategy="last",
            token_counter=len,
            max_tokens=max_messages,
            start_on="human",
            include_system=True,
            allow_partial=False,
        )
        return {"llm_input_messages": repair_thinking_blocks(trimmed)}

    return create_react_agent(
        model=model,
        tools=registry.langchain_tools(),
        prompt=build_system_prompt(settings),
        checkpointer=checkpointer,
        pre_model_hook=trim_llm_input,
    )


_ = count_tokens_approximately  # re-exported for callers wanting token-based trimming later
