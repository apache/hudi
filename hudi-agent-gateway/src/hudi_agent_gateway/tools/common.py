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

"""Engine-agnostic pieces shared by the lakehouse tool backends.

Every SQL backend (Trino, Spark Thrift Server, ...) returns the same
:class:`QueryResult`, shapes results with the same byte/row-cap rules, and
validates externally supplied identifiers with the same conservative rule.
Keeping these here means a new engine only supplies a client (execute/ping)
and the engine-specific SQL for listing and describing tables.
"""

from __future__ import annotations

import difflib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from hudi_agent_gateway.tools.registry import ToolInputError


class LakehouseQueryError(Exception):
    """A query failed on the engine side; message is safe to surface to the model."""


class LakehouseTimeoutError(LakehouseQueryError):
    pass


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[Any]]
    row_count: int = field(init=False)

    def __post_init__(self) -> None:
        self.row_count = len(self.rows)


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


def error_payload(message: str, hint: str = "") -> str:
    payload = {"error": message}
    if hint:
        payload["hint"] = hint
    return json.dumps(payload)


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$-]*$")


def validate_identifier(value: str, kind: str) -> str:
    """Reject anything that could escape quoting when interpolated into SQL.

    catalog/schema/table names arrive over HTTP and MCP; only plain
    identifiers are accepted -- everything else is an input error.
    """
    if not IDENTIFIER_RE.match(value):
        raise ToolInputError(
            f"invalid {kind} name {value!r}",
            hint="Names may contain only letters, digits, '_', '$' or '-', "
            "and must not start with a digit.",
        )
    return value


# --- schema-aware error hints ------------------------------------------------
# Engine error fragments identifying "you referenced a name that does not
# exist". Table markers are checked first: Spark/Trino table errors often also
# contain the word "column"-adjacent phrasing, but never vice versa.
_TABLE_NOT_FOUND_MARKERS = (
    "table_or_view_not_found",
    "table or view not found",
    "table_not_found",
)
_COLUMN_NOT_FOUND_MARKERS = (
    "unresolved_column",
    "column_not_found",
    "cannot be resolved",
    "cannot resolve",
)

# `name`, 'name' or "name" -- how engines quote the offending identifier.
_QUOTED_IDENTIFIER_RE = re.compile(r"[`'\"]([A-Za-z0-9_$.\-]+)[`'\"]")

_MAX_HINT_CHARS = 700


def schema_error_hint(message: str, schema: dict[str, list[tuple[str, str]]]) -> str:
    """An actionable hint for a not-found query error, built from the cached
    schema: fuzzy "did you mean" plus the real names, so the model can correct
    itself in one hop instead of guessing again. Returns "" when the error is
    not a not-found error or the schema is unknown.
    """
    if not schema:
        return ""
    lowered = message.lower()
    is_table_error = any(m in lowered for m in _TABLE_NOT_FOUND_MARKERS) or (
        "table" in lowered and "does not exist" in lowered  # Trino TABLE_NOT_FOUND
    )
    is_column_error = not is_table_error and any(
        m in lowered for m in _COLUMN_NOT_FOUND_MARKERS
    )
    if not (is_table_error or is_column_error):
        return ""

    match = _QUOTED_IDENTIFIER_RE.search(message)
    # engines may qualify the name (`db.table` / `t.col`); compare the leaf
    name = match.group(1).split(".")[-1] if match else ""

    if is_table_error:
        tables = list(schema)
        suggestions = difflib.get_close_matches(name, tables, n=1, cutoff=0.5)
        did_you_mean = f"Did you mean `{suggestions[0]}`? " if suggestions else ""
        hint = f"{did_you_mean}Available tables: {', '.join(tables)}."
        return hint[:_MAX_HINT_CHARS]

    # column error: find the closest column anywhere, then show that table's
    # real column list -- models are far better at copying than guessing.
    all_columns: dict[str, str] = {}
    for table, columns in schema.items():
        for col, _ in columns:
            all_columns.setdefault(col, table)
    suggestions = difflib.get_close_matches(name, list(all_columns), n=1, cutoff=0.5)
    if suggestions:
        col = suggestions[0]
        table = all_columns[col]
        cols = ", ".join(c for c, _ in schema[table])
        hint = f"Did you mean `{col}`? Columns in `{table}`: {cols}."
    else:
        hint = (
            f"Available tables: {', '.join(schema)}. "
            "Call describe_table to see their columns."
        )
    return hint[:_MAX_HINT_CHARS]


def split_table_name(
    table: str, default_catalog: str, default_schema: str
) -> tuple[str, str, str]:
    parts = table.split(".")
    if len(parts) == 1:
        return default_catalog, default_schema, parts[0]
    if len(parts) == 2:
        return default_catalog, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise ToolInputError(
        f"invalid table name {table!r}", hint="Use table, schema.table, or catalog.schema.table."
    )
