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

"""The real LangGraph agent wired to the real registry with a scripted model."""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver

from hudi_agent_gateway.agent import build_agent, build_system_prompt
from tests.conftest import TOOL_CALL_THEN_ANSWER, scripted_model


async def test_loop_executes_tool_and_answers(settings, registry, fake_trino) -> None:
    agent = build_agent(scripted_model(TOOL_CALL_THEN_ANSWER), registry, InMemorySaver(), settings)
    result = await agent.ainvoke(
        {"messages": [HumanMessage("How many trips per city?")]},
        config={"configurable": {"thread_id": "t1"}},
    )
    messages = result["messages"]
    # The tool actually executed against the fake Trino client...
    assert fake_trino.executed and "LIMIT" in fake_trino.executed[0]
    tool_msgs = [m for m in messages if isinstance(m, ToolMessage)]
    assert len(tool_msgs) == 1
    assert "chennai" in str(tool_msgs[0].content)
    # ...and the final answer is the scripted closing message.
    assert isinstance(messages[-1], AIMessage)
    assert "100 trips" in messages[-1].text()


async def test_checkpointer_preserves_history_across_turns(settings, registry) -> None:
    saver = InMemorySaver()
    first = scripted_model(
        [AIMessage(content="The trips table has 100 rows across 3 cities.")]
    )
    agent = build_agent(first, registry, saver, settings)
    await agent.ainvoke(
        {"messages": [HumanMessage("How many rows in trips?")]},
        config={"configurable": {"thread_id": "same-thread"}},
    )
    # Second turn on the SAME thread: the model sees prior history.
    second = scripted_model([AIMessage(content="As I said: 100.")])
    agent2 = build_agent(second, registry, saver, settings)
    result = await agent2.ainvoke(
        {"messages": [HumanMessage("Remind me?")]},
        config={"configurable": {"thread_id": "same-thread"}},
    )
    texts = [m.text() for m in result["messages"] if hasattr(m, "text")]
    assert any("100 rows across 3 cities" in t for t in texts)  # history retained
    assert len(result["messages"]) >= 4  # two human + two ai


def test_system_prompt_interpolation(settings) -> None:
    prompt = build_system_prompt(settings)
    assert "`hudi`" in prompt and "`default`" in prompt
    assert str(settings.sql_row_cap) in prompt
    assert "markdown tables" in prompt


def test_agent_cache_builds_per_model_and_caps(settings, registry) -> None:
    from langgraph.checkpoint.memory import InMemorySaver

    from hudi_agent_gateway.agent import AgentCache

    cache = AgentCache(registry, InMemorySaver(), settings)
    default_agent = cache.get(None)
    assert cache.get(None) is default_agent            # cached
    other = cache.get("some-other-model")
    assert other is not default_agent                  # distinct per model
    for i in range(10):                                # exceeds the cap without error
        cache.get(f"m{i}")
    assert cache.get("m9") is cache.get("m9")


async def test_loop_repairs_broken_thinking_block_before_next_model_call(
    settings, registry
) -> None:
    """A streamed adaptive-thinking turn stored with a signature-only thinking
    block must reach the model repaired on the following loop iteration."""
    from typing import Any

    from tests.conftest import ScriptedChatModel

    seen: list[list[Any]] = []

    class RecordingModel(ScriptedChatModel):
        def _stream(self, messages: Any, stop: Any = None, run_manager: Any = None, **kwargs: Any):
            seen.append(list(messages))
            yield from super()._stream(messages, stop, run_manager, **kwargs)

        def _generate(self, messages: Any, stop: Any = None, run_manager: Any = None, **kw: Any):
            seen.append(list(messages))
            return super()._generate(messages, stop=stop, run_manager=run_manager, **kw)

    model = RecordingModel(messages=iter([
        AIMessage(
            content=[{"type": "thinking", "signature": "sig=="}],  # no `thinking` key
            tool_calls=[{"id": "call_1", "name": "list_tables", "args": {}}],
        ),
        AIMessage(content="Answer grounded in the listing."),
    ]))
    agent = build_agent(model, registry, InMemorySaver(), settings)
    await agent.ainvoke(
        {"messages": [HumanMessage("What tables exist?")]},
        config={"configurable": {"thread_id": "thinking-repair"}},
    )

    second_call_ai = [m for m in seen[1] if isinstance(m, AIMessage)]
    blocks = second_call_ai[0].content
    assert blocks[0]["type"] == "thinking" and blocks[0]["thinking"] == ""


def test_repair_thinking_blocks_restores_missing_field() -> None:
    from hudi_agent_gateway.agent import repair_thinking_blocks

    broken = AIMessage(content=[
        {"type": "thinking", "signature": "sig=="},           # streamed merge dropped `thinking`
        {"type": "thinking", "thinking": "kept", "signature": "s2"},
        {"type": "text", "text": "hi"},
    ])
    human = HumanMessage("q")
    plain = AIMessage(content="just text")

    out = repair_thinking_blocks([human, broken, plain])

    assert out[0] is human and out[2] is plain                # untouched passthrough
    assert out[1].content[0] == {"type": "thinking", "signature": "sig==", "thinking": ""}
    assert out[1].content[1]["thinking"] == "kept"            # present field untouched
    assert broken.content[0] == {"type": "thinking", "signature": "sig=="}  # original not mutated
