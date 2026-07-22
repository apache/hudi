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

import json
import re
from typing import Annotated, Any

from pydantic import Field

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools.guardrails import enforce_guardrails
from hudi_agent_gateway.tools.registry import ToolInputError, ToolRegistry
from hudi_agent_gateway.tools.trino_client import (
    QueryResult,
    TrinoClient,
    TrinoQueryError,
    TrinoTimeoutError,
)

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


def shape_result(result: QueryResult, *, max_bytes: int, sql: str, row_cap: int) -> str:
    # A result that filled the row cap was almost certainly cut off by it
    # (LIMIT rewrite + fetchmany), so report it truncated -- the agent warns
    # the user instead of presenting a capped answer as complete.
    row_capped = result.row_count >= row_cap
    payload: dict[str, Any] = {
        "sql": sql,
        "columns": result.columns,
        "rows": result.rows,
        "row_count": result.row_count,
        "truncated": row_capped,
    }
    if row_capped:
        payload["notice"] = (
            f"Result hit the {row_cap}-row cap and may be incomplete. "
            "Narrow the query (filters, aggregation) for an exact answer."
        )
    text = json.dumps(payload, default=str)
    while len(text.encode()) > max_bytes and payload["rows"]:
        keep = max(1, len(payload["rows"]) // 2)
        if keep == len(payload["rows"]):
            keep -= 1
        payload["rows"] = payload["rows"][:keep]
        payload["truncated"] = True
        payload["notice"] = (
            f"Result truncated to {len(payload['rows'])} of {result.row_count} fetched rows "
            "to fit the size limit. Narrow the query (filters, aggregation, fewer columns)."
        )
        text = json.dumps(payload, default=str)
    if len(text.encode()) > max_bytes:
        # Fail-safe: even zero rows can bust the limit (huge sql text, very
        # many columns, or a tiny configured cap). Return a bounded payload.
        text = json.dumps({
            "sql": sql[:100],
            "column_count": len(result.columns),
            "rows": [],
            "row_count": result.row_count,
            "truncated": True,
            "notice": "Result exceeded the size limit; rows and column names omitted. "
            "Narrow the query or raise the result size limit.",
        })
        if len(text.encode()) > max_bytes:
            text = json.dumps({"error": "result exceeded the size limit", "truncated": True})
    return text


def _error(message: str, hint: str = "") -> str:
    payload = {"error": message}
    if hint:
        payload["hint"] = hint
    return json.dumps(payload)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$-]*$")


def _validate_identifier(value: str, kind: str) -> str:
    """Reject anything that could escape quoting when interpolated into SQL.

    catalog/schema/table names arrive over HTTP and MCP; only plain
    identifiers are accepted -- everything else is an input error.
    """
    if not _IDENTIFIER_RE.match(value):
        raise ToolInputError(
            f"invalid {kind} name {value!r}",
            hint="Names may contain only letters, digits, '_', '$' or '-', "
            "and must not start with a digit.",
        )
    return value


def _split_table_name(table: str, settings: GatewaySettings) -> tuple[str, str, str]:
    parts = table.split(".")
    if len(parts) == 1:
        return settings.trino_catalog, settings.trino_schema, parts[0]
    if len(parts) == 2:
        return settings.trino_catalog, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise ToolInputError(
        f"invalid table name {table!r}", hint="Use table, schema.table, or catalog.schema.table."
    )


def register(registry: ToolRegistry, client: TrinoClient, settings: GatewaySettings) -> None:
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
            return _error(str(e), e.hint)
        except TrinoTimeoutError as e:
            return _error(
                f"query failed: {e}",
                f"The query hit the gateway's {settings.sql_timeout_seconds:.0f}s limit -- "
                "narrow it (partition filter, pre-aggregate, fewer columns) or raise "
                "GATEWAY_SQL_TIMEOUT_SECONDS.",
            )
        except TrinoQueryError as e:
            return _error(f"query failed: {e}", "Fix the SQL and try again.")
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
            cat = _validate_identifier(catalog or settings.trino_catalog, "catalog")
            sch = _validate_identifier(schema_name or settings.trino_schema, "schema")
        except ToolInputError as e:
            return _error(str(e), e.hint)
        sql = (
            f'SELECT table_schema, table_name, table_type FROM "{cat}".information_schema.tables '
            f"WHERE table_schema = '{sch}' ORDER BY table_name"
        )
        try:
            result = await client.execute(
                sql, timeout=settings.sql_timeout_seconds, max_rows=settings.sql_row_cap
            )
        except TrinoQueryError as e:
            return _error(f"listing tables failed: {e}")
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
            cat, sch, tbl = _split_table_name(table, settings)
            _validate_identifier(cat, "catalog")
            _validate_identifier(sch, "schema")
            _validate_identifier(tbl, "table")
        except ToolInputError as e:
            return _error(str(e), e.hint)
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
            return _error(f"describe failed: {e}")
        if result.row_count == 0:
            return _error(
                f"table {cat}.{sch}.{tbl} not found",
                "Call list_tables to see available tables.",
            )
        return shape_result(
            result, max_bytes=settings.tool_result_max_bytes, sql=sql, row_cap=settings.sql_row_cap
        )
