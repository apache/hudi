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

"""Lakehouse tools backed by a Spark Thrift Server.

Same tool surface and error contract as the Trino backend
(:mod:`hudi_agent_gateway.tools.trino_tools`): handlers return JSON strings,
and expected failures (guardrail rejections, query errors, timeouts) are
returned as ``{"error": ..., "hint": ...}`` payloads rather than raised, so
the agent can read the error and self-correct.

Spark differences, kept behind the shared contract:

- SQL is validated and rendered in the ``spark`` sqlglot dialect.
- Spark has no ``information_schema``: ``list_tables`` uses ``SHOW TABLES IN``
  and ``describe_table`` uses ``DESCRIBE TABLE``.
- Identifiers are quoted with backticks.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools.common import error_payload, shape_result, validate_identifier
from hudi_agent_gateway.tools.connector import (
    EngineInfo,
    LakehouseClient,
    LakehouseConnector,
    PromptContext,
    register_connector,
)
from hudi_agent_gateway.tools.guardrails import enforce_guardrails
from hudi_agent_gateway.tools.registry import ToolInputError, ToolRegistry
from hudi_agent_gateway.tools.spark_client import (
    SparkQueryError,
    SparkThriftClient,
    SparkTimeoutError,
)

__all__ = ["SparkConnector", "register"]

_QUERY_DESC = (
    "Run a single read-only SELECT statement (Spark SQL) against the lakehouse "
    "and return the result as JSON. A server-side row cap is enforced; results "
    "may be truncated (indicated by `truncated: true`)."
)
_LIST_TABLES_DESC = (
    "List tables in the lakehouse. Optionally filter by database; "
    "defaults to the gateway's configured database."
)
_DESCRIBE_DESC = (
    "Describe a table's columns and types. Accepts `table` or `database.table`."
)

# Spark 4.x error class and the Spark 3.x message form. Deliberately narrow:
# a generic phrase like "cannot be found" could misclassify unrelated errors.
_NOT_FOUND_MARKERS = (
    "TABLE_OR_VIEW_NOT_FOUND",
    "Table or view not found",
)


def _split_table_name(table: str, settings: GatewaySettings) -> tuple[str, str]:
    parts = table.split(".")
    if len(parts) == 1:
        return settings.spark_database, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ToolInputError(
        f"invalid table name {table!r}", hint="Use table or database.table."
    )


def register(registry: ToolRegistry, client: SparkThriftClient, settings: GatewaySettings) -> None:
    @registry.register("query_lakehouse", _QUERY_DESC)
    async def query_lakehouse(
        sql: Annotated[
            str, Field(description="A single read-only SELECT statement in Spark SQL.")
        ],
    ) -> str:
        try:
            safe_sql = enforce_guardrails(sql, row_cap=settings.sql_row_cap, dialect="spark")
            result = await client.execute(
                safe_sql, timeout=settings.sql_timeout_seconds, max_rows=settings.sql_row_cap
            )
        except ToolInputError as e:
            return error_payload(str(e), e.hint)
        except SparkTimeoutError as e:
            return error_payload(
                f"query failed: {e}",
                f"The query hit the gateway's {settings.sql_timeout_seconds:.0f}s limit -- "
                "narrow it (partition filter, pre-aggregate, fewer columns) or raise "
                "GATEWAY_SQL_TIMEOUT_SECONDS.",
            )
        except SparkQueryError as e:
            return error_payload(f"query failed: {e}", "Fix the SQL and try again.")
        return shape_result(
            result,
            max_bytes=settings.tool_result_max_bytes,
            sql=safe_sql,
            row_cap=settings.sql_row_cap,
        )

    @registry.register("list_tables", _LIST_TABLES_DESC)
    async def list_tables(
        database: Annotated[
            str, Field(description="Database to list from; empty for the default.")
        ] = "",
    ) -> str:
        try:
            db = validate_identifier(database or settings.spark_database, "database")
        except ToolInputError as e:
            return error_payload(str(e), e.hint)
        sql = f"SHOW TABLES IN `{db}`"
        try:
            result = await client.execute(
                sql, timeout=settings.sql_timeout_seconds, max_rows=settings.sql_row_cap
            )
        except SparkQueryError as e:
            return error_payload(f"listing tables failed: {e}")
        return shape_result(
            result, max_bytes=settings.tool_result_max_bytes, sql=sql, row_cap=settings.sql_row_cap
        )

    @registry.register("describe_table", _DESCRIBE_DESC)
    async def describe_table(
        table: Annotated[
            str, Field(description="Table name: table or database.table.")
        ],
    ) -> str:
        try:
            db, tbl = _split_table_name(table, settings)
            validate_identifier(db, "database")
            validate_identifier(tbl, "table")
        except ToolInputError as e:
            return error_payload(str(e), e.hint)
        sql = f"DESCRIBE TABLE `{db}`.`{tbl}`"
        try:
            result = await client.execute(
                sql, timeout=settings.sql_timeout_seconds, max_rows=settings.sql_row_cap
            )
        except SparkQueryError as e:
            if any(marker.lower() in str(e).lower() for marker in _NOT_FOUND_MARKERS):
                return error_payload(
                    f"table {db}.{tbl} not found",
                    "Call list_tables to see available tables.",
                )
            return error_payload(f"describe failed: {e}")
        if result.row_count == 0:
            return error_payload(
                f"table {db}.{tbl} not found",
                "Call list_tables to see available tables.",
            )
        return shape_result(
            result, max_bytes=settings.tool_result_max_bytes, sql=sql, row_cap=settings.sql_row_cap
        )


@register_connector
class SparkConnector(LakehouseConnector):
    """Spark SQL over the HiveServer2 Thrift protocol (a Spark Thrift Server,
    or Apache Kyuubi -- same wire protocol). Reads MOR snapshots and the Hudi
    1.x unstructured types (BLOB/VECTOR/VARIANT) that Trino cannot serve."""

    name = "spark"

    def create_client(self, settings: GatewaySettings) -> SparkThriftClient:
        return SparkThriftClient(settings)

    def register_tools(
        self, registry: ToolRegistry, client: LakehouseClient, settings: GatewaySettings
    ) -> None:
        register(registry, client, settings)  # type: ignore[arg-type]

    def prompt_context(self, settings: GatewaySettings) -> PromptContext:
        return PromptContext(
            engine_name="a Spark Thrift Server",
            dialect="Spark SQL",
            namespace_line=(
                f"The default database is `{settings.spark_database}`; tables are Hudi "
                "tables registered in the server's metastore."
            ),
            qualify_line=(
                "Qualify names as database.table when querying outside the default database."
            ),
        )

    def unreachable_detail(self, settings: GatewaySettings) -> str:
        return f"cannot reach spark thrift server at {settings.spark_host}:{settings.spark_port}"

    def engine_info(self, settings: GatewaySettings) -> EngineInfo:
        return EngineInfo(
            engine="spark",
            catalog="spark_catalog",
            schema=settings.spark_database,
            sql_url=(
                f"jdbc:hive2://{settings.spark_host}:{settings.spark_port}/{settings.spark_database}"
            ),
            web_ui_url=None,
        )
