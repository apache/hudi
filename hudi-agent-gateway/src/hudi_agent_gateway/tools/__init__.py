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

"""Tool assembly.

The lakehouse backend is chosen by ``GATEWAY_ENGINE`` and resolved through the
connector registry (:mod:`hudi_agent_gateway.tools.connector`). Importing a
connector class runs its ``@register_connector`` decorator, so it becomes
discoverable via ``get_connector`` / ``build_registry``.

To add an engine: write a connector module (see ``connector.py`` for the
contract) and add one import line for its class below -- that is the only
change to shared code.
"""

from __future__ import annotations

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools.connector import (
    LakehouseClient,
    connector_names,
    get_connector,
)
from hudi_agent_gateway.tools.registry import ToolRegistry
from hudi_agent_gateway.tools.schema_cache import SchemaCache

# Built-in connectors. The import registers them (decorator side effect); the
# re-export makes them part of the package's public surface.
from hudi_agent_gateway.tools.spark_tools import SparkConnector
from hudi_agent_gateway.tools.trino_tools import TrinoConnector

__all__ = [
    "SchemaCache",
    "SparkConnector",
    "TrinoConnector",
    "build_registry",
    "build_schema_cache",
    "connector_names",
    "create_lakehouse_client",
    "get_connector",
]


def create_lakehouse_client(settings: GatewaySettings) -> LakehouseClient:
    """Construct the SQL client for the configured ``GATEWAY_ENGINE``."""
    return get_connector(settings.engine).create_client(settings)


def build_schema_cache(
    settings: GatewaySettings, client: LakehouseClient
) -> SchemaCache | None:
    """The schema cache behind ``GATEWAY_SCHEMA_HINTS``; None when off."""
    if settings.schema_hints == "off":
        return None
    connector = get_connector(settings.engine)
    return SchemaCache(
        fetch=lambda: connector.fetch_schema(client, settings),
        ttl_seconds=settings.schema_cache_ttl_seconds,
        max_tables=settings.schema_max_tables,
        max_columns=settings.schema_max_columns,
    )


def build_registry(
    settings: GatewaySettings,
    client: LakehouseClient | None = None,
    schema_cache: SchemaCache | None = None,
) -> ToolRegistry:
    """Build the tool registry for the configured engine.

    ``client`` is injectable for tests; when omitted a real client is created
    for the engine. ``schema_cache`` (optional) lets the tools enrich
    not-found errors with real names. Tool registration is delegated to the
    engine's connector.
    """
    connector = get_connector(settings.engine)
    registry = ToolRegistry()
    connector.register_tools(
        registry,
        client or connector.create_client(settings),
        settings,
        schema_cache=schema_cache,
    )
    return registry
