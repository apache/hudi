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

"""SQL guardrails for model-written queries.

AST-level (sqlglot, selected backend dialect) rather than regex: read-only enforcement
survives comments, CTEs, and string literals; the row cap is injected as a
real ``LIMIT``. Fail-closed: anything that does not parse is rejected.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from hudi_agent_gateway.tools.registry import ToolInputError

_FORBIDDEN_NODES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Into,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Grant,
    exp.Set,
    exp.Command,  # also covers CALL and EXPLAIN, which parse as generic commands
    exp.Use,
)
_SPARK_FORBIDDEN_FUNCTIONS = frozenset({"java_method", "reflect", "try_reflect", "identifier"})
_SPARK_HUDI_TABLE_FUNCTIONS = frozenset({
    "hudi_query", "hudi_table_changes", "hudi_query_timeline", "hudi_filesystem_view",
    "hudi_metadata",
})
_SPARK_TABLE_FUNCTIONS = _SPARK_HUDI_TABLE_FUNCTIONS | frozenset({
    "range", "explode", "explode_outer", "posexplode", "posexplode_outer", "inline",
    "inline_outer", "stack", "json_tuple",
})


def enforce_guardrails(
    sql: str, row_cap: int, *, dialect: str = "trino", spark_catalog: str = "spark_catalog"
) -> str:
    """Validate ``sql`` as a single read-only SELECT and cap its LIMIT.

    Returns the (possibly rewritten) SQL to execute. Raises
    :class:`ToolInputError` on any violation.
    """
    hint = f"Provide exactly one read-only SELECT statement ({dialect} SQL)."
    try:
        statements = sqlglot.parse(sql, read=dialect)
    except sqlglot.errors.SqlglotError as e:
        raise ToolInputError(f"could not parse SQL: {e}", hint=hint) from e

    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise ToolInputError(
            f"exactly one statement is allowed, got {len(statements)}", hint=hint
        )
    stmt = statements[0]

    if not isinstance(stmt, (exp.Select, exp.SetOperation)):
        raise ToolInputError(
            f"read-only: only SELECT is allowed, got {type(stmt).__name__}", hint=hint
        )

    # Defense in depth: no write/DDL/command node anywhere in the tree
    # (catches e.g. a CTE wrapping an INSERT).
    for node in stmt.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            raise ToolInputError(
                f"read-only: {type(node).__name__} is not allowed", hint=hint
            )
        if dialect == "spark":
            _validate_spark_node(node, spark_catalog=spark_catalog, hint=hint)

    return _cap_limit(stmt, row_cap).sql(dialect=dialect)


def _validate_spark_node(node: object, *, spark_catalog: str, hint: str) -> None:
    if isinstance(node, exp.QueryTransform):
        raise ToolInputError("read-only: Spark script transforms are not allowed", hint=hint)
    if isinstance(node, exp.Table):
        _validate_spark_table(node, spark_catalog=spark_catalog, hint=hint)
    elif isinstance(node, exp.Lateral):
        # LATERAL functions are not necessarily wrapped in an exp.Table.
        if not isinstance(node.this, exp.Subquery):
            _validate_spark_table_function(node.this)
    elif isinstance(node, exp.Func):
        name = _spark_function_name(node)
        if name in _SPARK_FORBIDDEN_FUNCTIONS:
            # Reflection invokes JVM methods; IDENTIFIER hides names from AST checks.
            raise ToolInputError(f"read-only: Spark function {name} is not allowed", hint=hint)
        if name in _SPARK_HUDI_TABLE_FUNCTIONS:
            # Check arguments at the function node, regardless of its AST context.
            _validate_hudi_table_function(node, name=name)


def _validate_spark_table(node: exp.Table, *, spark_catalog: str, hint: str) -> None:
    if not isinstance(node.this, exp.Identifier):
        _validate_spark_table_function(node.this, qualified=bool(node.db or node.catalog))
        return
    if any(any(char in part.name for char in "/\\:") for part in node.parts):
        raise ToolInputError("read-only: Spark file paths are not allowed", hint=hint)
    if not node.catalog and node.db:
        # Any two-part name can be interpreted as a data source and path,
        # including custom providers. Force catalog table resolution.
        node.set("catalog", exp.to_identifier(spark_catalog, quoted=True))


def _spark_function_name(node: exp.Func) -> str:
    return (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()


def _validate_spark_table_function(node: exp.Expression, *, qualified: bool = False) -> None:
    # Unknown/custom TVFs may read files even with spark.sql.runSQLOnFiles=false.
    name = _spark_function_name(node) if isinstance(node, exp.Func) else node.sql(dialect="spark")
    if qualified or not isinstance(node, exp.Func) or name not in _SPARK_TABLE_FUNCTIONS:
        raise ToolInputError(
            f"read-only: Spark table function {name} is not allowed",
            hint="Use an unqualified table function: "
            + ", ".join(sorted(_SPARK_TABLE_FUNCTIONS)) + ".",
        )


def _validate_hudi_table_function(node: exp.Func, *, name: str) -> None:
    args = node.expressions
    hint = f"Pass a literal table name as the first argument to {name}, e.g. 'default.orders'."
    _validate_hudi_table_argument(args[0] if args else None, hint=hint)
    # The optional filesystem-view subpath is appended to the catalog location and
    # can traverse outside it. Only the whole-table form is exposed by the gateway.
    if name == "hudi_filesystem_view" and len(args) != 1:
        raise ToolInputError(
            "read-only: Hudi filesystem-view subpaths are not allowed",
            hint="Use hudi_filesystem_view('default.orders') without a subpath.",
        )


def _validate_hudi_table_argument(table: exp.Expression | None, *, hint: str) -> None:
    error = "read-only: Hudi table functions require a literal catalog table identifier"
    # Hudi evaluates expressions before deciding whether the first argument is a path.
    # Reject computed values, including concat/chr/casts, rather than trying to fold them.
    if not isinstance(table, exp.Literal) or not table.is_string:
        raise ToolInputError(error, hint=hint)
    if not table.this or any(char in table.this for char in "/\\:"):
        raise ToolInputError(error, hint=hint)
    try:
        identifier = sqlglot.parse_one(table.this, read="spark", into=exp.Table)
    except sqlglot.errors.SqlglotError as e:
        raise ToolInputError(error, hint=hint) from e
    if (not all(isinstance(part, exp.Identifier) for part in identifier.parts)
            or identifier.catalog
            or any(key not in {"this", "db", "catalog"} for key in identifier.args)):
        raise ToolInputError(error, hint=hint)


def _cap_limit(stmt: exp.Query, row_cap: int) -> exp.Query:
    limit = stmt.args.get("limit")
    if isinstance(limit, (exp.Limit, exp.Fetch)):
        count = limit.args.get("count") if isinstance(limit, exp.Fetch) else limit.expression
        if isinstance(limit, exp.Fetch) and count is None:
            count = exp.Literal.number(1)
            limit.set("count", count)
        if isinstance(count, exp.Neg):
            raise ToolInputError(
                "row limit must be non-negative",
                hint="Use LIMIT or FETCH with a non-negative integer.",
            )
        options = limit.args.get("limit_options")
        if options and (options.args.get("with_ties") or options.args.get("percent")):
            # A count with ties or a percentage is not a hard row bound. Keep the
            # original selection semantics, then cap the result in an outer query.
            return exp.select("*").from_(stmt.subquery("_guarded_query")).limit(row_cap)
        if isinstance(count, exp.Literal) and count.is_int:
            return stmt.limit(min(int(count.this), row_cap))
    return stmt.limit(row_cap)
