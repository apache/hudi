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

"""Lakehouse tools backed by Trino.

Handlers return JSON strings. Expected failures (guardrail rejections, query
errors, timeouts) are returned as ``{"error": ..., "hint": ...}`` payloads
rather than raised, so the agent can read the error and self-correct, and MCP
clients get a useful result instead of a protocol error.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import Field

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools.common import (
    error_payload,
    schema_error_hint,
    shape_result,
    split_table_name,
    validate_identifier,
)
from hudi_agent_gateway.tools.connector import (
    EngineInfo,
    LakehouseClient,
    LakehouseConnector,
    PromptContext,
    register_connector,
)
from hudi_agent_gateway.tools.guardrails import enforce_guardrails
from hudi_agent_gateway.tools.registry import ToolInputError, ToolRegistry
from hudi_agent_gateway.tools.trino_client import (
    TrinoClient,
    TrinoQueryError,
    TrinoTimeoutError,
)

__all__ = ["TrinoConnector", "register", "shape_result"]

_QUERY_DESC = (
    "Run a single read-only SELECT statement (Trino SQL) against the lakehouse "
    "and return the result as JSON. A server-side row cap is enforced; results "
    "may be truncated (indicated by `truncated: true`)."
)
_LIST_TABLES_DESC = (
    "List tables in the lakehouse. Optionally filter by catalog and schema; "
    "defaults to the gateway's configured catalog and schema."
)
_DESCRIBE_DESC = (
    "Describe a table's columns and types. Accepts `table`, `schema.table`, or "
    "`catalog.schema.table`."
)


def register(
    registry: ToolRegistry,
    client: TrinoClient,
    settings: GatewaySettings,
    schema_cache: Any = None,
) -> None:
    async def _not_found_hint(message: str) -> str:
        """Schema-aware hint for name errors (GATEWAY_SCHEMA_HINTS=errors|both)."""
        if schema_cache is None or settings.schema_hints not in ("errors", "both"):
            return ""
        return schema_error_hint(message, await schema_cache.get())

    @registry.register("query_lakehouse", _QUERY_DESC)
    async def query_lakehouse(
        sql: Annotated[
            str, Field(description="A single read-only SELECT statement in Trino SQL.")
        ],
    ) -> str:
        try:
            safe_sql = enforce_guardrails(sql, row_cap=settings.sql_row_cap)
            result = await client.execute(
                safe_sql, timeout=settings.sql_timeout_seconds, max_rows=settings.sql_row_cap
            )
        except ToolInputError as e:
            return error_payload(str(e), e.hint)
        except TrinoTimeoutError as e:
            return error_payload(
                f"query failed: {e}",
                f"The query hit the gateway's {settings.sql_timeout_seconds:.0f}s limit -- "
                "narrow it (partition filter, pre-aggregate, fewer columns) or raise "
                "GATEWAY_SQL_TIMEOUT_SECONDS.",
            )
        except TrinoQueryError as e:
            hint = await _not_found_hint(str(e))
            return error_payload(f"query failed: {e}", hint or "Fix the SQL and try again.")
        return shape_result(
            result,
            max_bytes=settings.tool_result_max_bytes,
            sql=safe_sql,
            row_cap=settings.sql_row_cap,
        )

    @registry.register("list_tables", _LIST_TABLES_DESC)
    async def list_tables(
        catalog: Annotated[
            str, Field(description="Catalog to list from; empty for the default.")
        ] = "",
        schema_name: Annotated[
            str, Field(description="Schema to list from; empty for the default.")
        ] = "",
    ) -> str:
        try:
            cat = validate_identifier(catalog or settings.trino_catalog, "catalog")
            sch = validate_identifier(schema_name or settings.trino_schema, "schema")
        except ToolInputError as e:
            return error_payload(str(e), e.hint)
        sql = (
            f'SELECT table_schema, table_name, table_type FROM "{cat}".information_schema.tables '
            f"WHERE table_schema = '{sch}' ORDER BY table_name"
        )
        try:
            result = await client.execute(
                sql, timeout=settings.sql_timeout_seconds, max_rows=settings.sql_row_cap
            )
        except TrinoQueryError as e:
            return error_payload(f"listing tables failed: {e}")
        return shape_result(
            result, max_bytes=settings.tool_result_max_bytes, sql=sql, row_cap=settings.sql_row_cap
        )

    @registry.register("describe_table", _DESCRIBE_DESC)
    async def describe_table(
        table: Annotated[
            str, Field(description="Table name: table, schema.table, or catalog.schema.table.")
        ],
    ) -> str:
        try:
            cat, sch, tbl = split_table_name(table, settings.trino_catalog, settings.trino_schema)
            validate_identifier(cat, "catalog")
            validate_identifier(sch, "schema")
            validate_identifier(tbl, "table")
        except ToolInputError as e:
            return error_payload(str(e), e.hint)
        sql = (
            "SELECT column_name, data_type, is_nullable "
            f'FROM "{cat}".information_schema.columns '
            f"WHERE table_schema = '{sch}' AND table_name = '{tbl}' ORDER BY ordinal_position"
        )
        try:
            result = await client.execute(
                sql, timeout=settings.sql_timeout_seconds, max_rows=settings.sql_row_cap
            )
        except TrinoQueryError as e:
            return error_payload(f"describe failed: {e}")
        if result.row_count == 0:
            return error_payload(
                f"table {cat}.{sch}.{tbl} not found",
                "Call list_tables to see available tables.",
            )
        return shape_result(
            result, max_bytes=settings.tool_result_max_bytes, sql=sql, row_cap=settings.sql_row_cap
        )


@register_connector
class TrinoConnector(LakehouseConnector):
    """Trino coordinator with the Hudi connector (the default engine)."""

    name = "trino"

    def create_client(self, settings: GatewaySettings) -> TrinoClient:
        return TrinoClient(settings)

    def register_tools(
        self,
        registry: ToolRegistry,
        client: LakehouseClient,
        settings: GatewaySettings,
        schema_cache: Any = None,
    ) -> None:
        register(registry, client, settings, schema_cache)  # type: ignore[arg-type]

    async def fetch_schema(
        self, client: LakehouseClient, settings: GatewaySettings
    ) -> dict[str, list[tuple[str, str]]]:
        """One information_schema query covering every table in the schema."""
        sql = (
            "SELECT table_name, column_name, data_type "
            f'FROM "{settings.trino_catalog}".information_schema.columns '
            f"WHERE table_schema = '{settings.trino_schema}' "
            "ORDER BY table_name, ordinal_position"
        )
        max_rows = settings.schema_max_tables * settings.schema_max_columns
        result = await client.execute(
            sql, timeout=settings.sql_timeout_seconds, max_rows=max_rows
        )
        schema: dict[str, list[tuple[str, str]]] = {}
        for table, column, dtype in result.rows:
            schema.setdefault(table, []).append((column, dtype))
        return schema

    def prompt_context(self, settings: GatewaySettings) -> PromptContext:
        return PromptContext(
            engine_name="Trino",
            dialect="Trino SQL",
            namespace_line=(
                f"The default catalog is `{settings.trino_catalog}` and the default schema "
                f"is `{settings.trino_schema}`; tables are Hudi tables registered in the "
                "catalog's metastore."
            ),
            qualify_line=(
                "Qualify names as catalog.schema.table when querying outside the defaults."
            ),
        )

    def unreachable_detail(self, settings: GatewaySettings) -> str:
        return f"cannot reach trino at {settings.trino_host}:{settings.trino_port}"

    def engine_info(self, settings: GatewaySettings) -> EngineInfo:
        base = f"http://{settings.trino_host}:{settings.trino_port}"
        return EngineInfo(
            engine="trino",
            catalog=settings.trino_catalog,
            schema=settings.trino_schema,
            sql_url=base,
            web_ui_url=f"{base}/ui/",
        )
