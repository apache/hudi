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

"""Live-stack integration tests, skipped unless the environment provides:

- ``GATEWAY_IT_TRINO_HOST`` (e.g. ``localhost`` with a port-forward to the
  local-dev hudi-trino service) -- enables the tool tests;
- additionally ``GATEWAY_IT_LLM=1`` -- enables one real agent round-trip
  using the gateway's configured provider.
"""

from __future__ import annotations

import json
import os

import pytest

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools import build_registry

TRINO_HOST = os.environ.get("GATEWAY_IT_TRINO_HOST", "")
LLM_ENABLED = os.environ.get("GATEWAY_IT_LLM", "") == "1"

pytestmark = pytest.mark.skipif(
    not TRINO_HOST, reason="set GATEWAY_IT_TRINO_HOST to run live-stack tests"
)


@pytest.fixture()
def live_settings() -> GatewaySettings:
    return GatewaySettings(
        trino_host=TRINO_HOST,
        trino_port=int(os.environ.get("GATEWAY_IT_TRINO_PORT", "8080")),
    )


@pytest.fixture()
def live_registry(live_settings: GatewaySettings):
    return build_registry(live_settings)


async def test_list_tables_live(live_registry) -> None:
    out = json.loads(await live_registry.get("list_tables").handler())
    tables = [row[1] for row in out["rows"]]
    assert "trips" in tables


async def test_describe_table_live(live_registry) -> None:
    out = json.loads(await live_registry.get("describe_table").handler(table="trips"))
    columns = [row[0] for row in out["rows"]]
    assert {"trip_id", "city", "fare"} <= set(columns)


async def test_query_lakehouse_live(live_registry) -> None:
    out = json.loads(
        await live_registry.get("query_lakehouse").handler(
            sql="SELECT city, count(*) AS trips FROM trips GROUP BY city ORDER BY city"
        )
    )
    assert out["row_count"] == 3
    assert sum(row[1] for row in out["rows"]) == 100


@pytest.mark.skipif(not LLM_ENABLED, reason="set GATEWAY_IT_LLM=1 to run the LLM round-trip")
async def test_agent_round_trip_live(live_settings) -> None:
    from langgraph.checkpoint.memory import InMemorySaver

    from hudi_agent_gateway.agent import build_agent
    from hudi_agent_gateway.llm import build_chat_model

    registry = build_registry(live_settings)
    agent = build_agent(
        build_chat_model(live_settings), registry, InMemorySaver(), live_settings
    )
    result = await agent.ainvoke(
        {"messages": [("user", "How many rows are in the trips table? Answer with the number.")]},
        config={"configurable": {"thread_id": "it"}},
    )
    assert "100" in result["messages"][-1].text()
