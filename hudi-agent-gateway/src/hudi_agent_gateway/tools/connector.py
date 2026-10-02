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

"""The lakehouse connector abstraction and registry.

A *connector* is everything the gateway needs to talk to one SQL engine
(Trino, a Spark Thrift Server, ...). It bundles the four engine-specific
concerns that would otherwise scatter as ``if settings.engine == ...``
branches across the app, the readiness/info endpoints, and the agent prompt:

1. building the SQL client (``create_client``),
2. registering the lakehouse tools over that client (``register_tools``),
3. the agent's prompt framing (``prompt_context`` -- dialect, namespace model),
4. the operational surfaces (``engine_info`` for ``/v1/info``,
   ``unreachable_detail`` / ``readiness_detail`` for ``/ready``).

Adding a new engine is therefore a single self-contained module:

    from hudi_agent_gateway.tools.connector import (
        LakehouseConnector, register_connector, PromptContext, EngineInfo,
    )

    @register_connector
    class MyEngineConnector(LakehouseConnector):
        name = "myengine"          # must match a GatewaySettings.engine value
        def create_client(self, settings): ...
        def register_tools(self, registry, client, settings): ...
        def prompt_context(self, settings): ...
        def unreachable_detail(self, settings): ...
        def engine_info(self, settings): ...

...then import the module from ``tools/__init__.py`` so it self-registers.
Nothing else in the codebase changes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools.common import QueryResult
from hudi_agent_gateway.tools.registry import ToolRegistry


@runtime_checkable
class LakehouseClient(Protocol):
    """The minimal surface every engine client exposes to the tools and app.

    Structural (duck-typed): the Trino and Spark clients satisfy it without
    inheriting, and tests can substitute a fake with the same three methods.
    """

    async def execute(self, sql: str, *, timeout: float, max_rows: int) -> QueryResult: ...

    async def ping(self, timeout: float = 5.0) -> bool: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class PromptContext:
    """Engine-specific framing for the agent system prompt."""

    engine_name: str  # e.g. "Trino" / "a Spark Thrift Server"
    dialect: str  # human label of the SQL dialect, e.g. "Trino SQL"
    namespace_line: str  # one sentence describing the default catalog/db
    qualify_line: str  # how to qualify names outside the defaults


@dataclass(frozen=True)
class EngineInfo:
    """Engine metadata surfaced by ``GET /v1/info`` and the UI connect panel."""

    engine: str
    catalog: str
    schema: str
    sql_url: str
    web_ui_url: str | None


class LakehouseConnector(ABC):
    """One SQL engine's integration. Stateless -- one instance is registered
    per engine and reused for the process lifetime."""

    #: engine key; MUST equal one of ``GatewaySettings.engine``'s values.
    name: str

    @abstractmethod
    def create_client(self, settings: GatewaySettings) -> LakehouseClient:
        """Construct the engine client (execute/ping/close)."""

    @abstractmethod
    def register_tools(
        self, registry: ToolRegistry, client: LakehouseClient, settings: GatewaySettings
    ) -> None:
        """Register query_lakehouse / list_tables / describe_table on ``registry``."""

    @abstractmethod
    def prompt_context(self, settings: GatewaySettings) -> PromptContext:
        """Engine framing for the agent system prompt."""

    @abstractmethod
    def unreachable_detail(self, settings: GatewaySettings) -> str:
        """Human message for ``/ready`` when the engine is not reachable."""

    @abstractmethod
    def engine_info(self, settings: GatewaySettings) -> EngineInfo:
        """Metadata for ``GET /v1/info``."""

    def readiness_detail(
        self, settings: GatewaySettings, client: LakehouseClient, ok: bool
    ) -> str:
        """The ``/ready`` detail string. Default: ``reachable`` when ok, else
        ``unreachable_detail`` enriched with the client's ``last_error`` if it
        exposes one. Override for fully custom behavior."""
        if ok:
            return "reachable"
        detail = self.unreachable_detail(settings)
        last_error = getattr(client, "last_error", "")
        return f"{detail} ({last_error})" if last_error else detail


_REGISTRY: dict[str, LakehouseConnector] = {}


def register_connector(cls: type[LakehouseConnector]) -> type[LakehouseConnector]:
    """Class decorator: instantiate ``cls`` and register it under ``cls.name``."""
    connector = cls()
    if connector.name in _REGISTRY:
        raise ValueError(f"connector {connector.name!r} is already registered")
    _REGISTRY[connector.name] = connector
    return cls


def get_connector(name: str) -> LakehouseConnector:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise ValueError(
            f"unknown lakehouse engine {name!r}; registered engines: {connector_names()}"
        ) from None


def connector_names() -> list[str]:
    return sorted(_REGISTRY)
