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

"""The connector abstraction: registry wiring and the extensibility contract."""

from __future__ import annotations

import typing

import pytest

from hudi_agent_gateway.config import GatewaySettings, LakehouseEngine
from hudi_agent_gateway.tools import connector_names, get_connector
from hudi_agent_gateway.tools.common import QueryResult
from hudi_agent_gateway.tools.connector import (
    _REGISTRY,
    EngineInfo,
    LakehouseClient,
    LakehouseConnector,
    PromptContext,
    register_connector,
)
from hudi_agent_gateway.tools.connector import (
    connector_names as reg_names,
)
from hudi_agent_gateway.tools.connector import (
    get_connector as reg_get,
)


def test_builtin_connectors_registered() -> None:
    assert set(connector_names()) >= {"trino", "spark"}


def test_every_configured_engine_has_a_connector() -> None:
    """The GATEWAY_ENGINE Literal and the connector registry must not drift."""
    engines = set(typing.get_args(LakehouseEngine))
    assert engines == {"trino", "spark"}
    for engine in engines:
        assert get_connector(engine).name == engine


def test_unknown_engine_raises_with_registered_list() -> None:
    with pytest.raises(ValueError, match="unknown lakehouse engine 'duckdb'"):
        get_connector("duckdb")


def test_default_engine_resolves_to_trino() -> None:
    settings = GatewaySettings()
    assert get_connector(settings.engine).name == "trino"


def test_trino_and_spark_expose_the_same_tool_surface(registry, spark_registry) -> None:
    trino_tools = {t["name"] for t in registry.listing()}
    spark_tools = {t["name"] for t in spark_registry.listing()}
    assert trino_tools == spark_tools == {"query_lakehouse", "list_tables", "describe_table"}


def test_readiness_detail_default_reachable_and_enriched() -> None:
    spark = get_connector("spark")

    class _Client:
        last_error = "TTransportException: Could not connect"

    s = GatewaySettings(engine="spark", spark_host="sts", spark_port=10009)
    assert spark.readiness_detail(s, _Client(), ok=True) == "reachable"
    detail = spark.readiness_detail(s, _Client(), ok=False)
    assert "cannot reach spark thrift server at sts:10009" in detail
    assert "Could not connect" in detail  # last_error enrichment


def test_engine_info_shapes() -> None:
    ti = get_connector("trino").engine_info(GatewaySettings())
    assert ti.engine == "trino" and ti.web_ui_url and ti.sql_url.startswith("http://")
    si = get_connector("spark").engine_info(GatewaySettings(engine="spark", spark_host="h"))
    assert si.engine == "spark" and si.web_ui_url is None
    assert si.sql_url.startswith("jdbc:hive2://h:")


def test_a_third_party_connector_plugs_in_with_no_core_changes() -> None:
    """The whole point: a new engine is one self-contained, self-registering
    class -- no edits to app/meta/agent/config wiring."""

    @register_connector
    class _DummyConnector(LakehouseConnector):
        name = "dummy-test-engine"

        def create_client(self, settings: GatewaySettings) -> LakehouseClient:
            class _C:
                async def execute(self, sql, *, timeout, max_rows):
                    return QueryResult(columns=["ok"], rows=[[1]])

                async def ping(self, timeout=5.0):
                    return True

                def close(self):
                    pass

            return _C()

        def register_tools(self, registry, client, settings) -> None:
            registry.register("ping_engine", "dummy tool")(lambda: client.ping())

        def prompt_context(self, settings) -> PromptContext:
            return PromptContext("Dummy", "Dummy SQL", "one db", "qualify db.table")

        def unreachable_detail(self, settings) -> str:
            return "cannot reach dummy"

        def engine_info(self, settings) -> EngineInfo:
            return EngineInfo("dummy-test-engine", "c", "s", "dummy://", None)

    try:
        assert reg_get("dummy-test-engine").name == "dummy-test-engine"
        assert "dummy-test-engine" in reg_names()
        # the connector satisfies the structural client protocol too
        client = reg_get("dummy-test-engine").create_client(GatewaySettings())
        assert isinstance(client, LakehouseClient)
    finally:
        _REGISTRY.pop("dummy-test-engine", None)


def test_duplicate_registration_is_rejected() -> None:
    with pytest.raises(ValueError, match="already registered"):

        @register_connector
        class _Dupe(LakehouseConnector):
            name = "trino"  # collides with the built-in

            def create_client(self, settings):  # pragma: no cover
                ...

            def register_tools(self, registry, client, settings):  # pragma: no cover
                ...

            def prompt_context(self, settings):  # pragma: no cover
                ...

            def unreachable_detail(self, settings):  # pragma: no cover
                ...

            def engine_info(self, settings):  # pragma: no cover
                ...
