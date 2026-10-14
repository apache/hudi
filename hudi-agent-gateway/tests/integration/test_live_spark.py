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

"""Live Spark Thrift Server integration tests, skipped unless the environment
provides:

- ``GATEWAY_IT_SPARK_HOST`` (e.g. ``localhost`` with a Spark Thrift Server on
  port 10000 serving a Hudi ``trips`` table like the local-dev example writes)
  -- enables the tool tests;
- ``GATEWAY_IT_SPARK_PORT`` to override the port (default 10000).

Requires the ``spark`` extra: ``pip install 'hudi-agent-gateway[spark]'``.
"""

from __future__ import annotations

import json
import os

import pytest

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools import build_registry

SPARK_HOST = os.environ.get("GATEWAY_IT_SPARK_HOST", "")

pytestmark = pytest.mark.skipif(
    not SPARK_HOST, reason="set GATEWAY_IT_SPARK_HOST to run live Spark Thrift tests"
)


@pytest.fixture()
def live_settings() -> GatewaySettings:
    return GatewaySettings(
        engine="spark",
        spark_host=SPARK_HOST,
        spark_port=int(os.environ.get("GATEWAY_IT_SPARK_PORT", "10000")),
    )


@pytest.fixture()
def live_registry(live_settings: GatewaySettings):
    return build_registry(live_settings)


async def test_list_tables_live(live_registry) -> None:
    out = json.loads(await live_registry.get("list_tables").handler())
    # SHOW TABLES IN returns (namespace, tableName, isTemporary)
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


async def test_guardrails_reject_write_live(live_registry) -> None:
    out = json.loads(
        await live_registry.get("query_lakehouse").handler(sql="DROP TABLE trips")
    )
    assert "read-only" in out["error"]
