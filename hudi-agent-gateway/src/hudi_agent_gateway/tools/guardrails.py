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
_SPARK_REFLECTION_FUNCTIONS = frozenset({"java_method", "reflect", "try_reflect"})


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
        if dialect == "spark" and isinstance(node, exp.QueryTransform):
            raise ToolInputError("read-only: Spark script transforms are not allowed", hint=hint)
        if dialect == "spark" and isinstance(node, exp.Table):
            if any(any(char in part.name for char in "/\\:") for part in node.parts):
                raise ToolInputError("read-only: Spark file paths are not allowed", hint=hint)
            if not node.catalog and node.db and isinstance(node.this, exp.Identifier):
                # Any two-part name can be interpreted as a data source and path,
                # including custom providers. Force catalog table resolution.
                node.set("catalog", exp.to_identifier(spark_catalog, quoted=True))
        if dialect == "spark" and isinstance(node, exp.Func):
            name = node.name if isinstance(node, exp.Anonymous) else node.sql_name()
            if name.lower() in _SPARK_REFLECTION_FUNCTIONS:
                # Reflection can invoke JVM methods with side effects from a SELECT.
                raise ToolInputError(f"read-only: Spark function {name} is not allowed", hint=hint)

    return _cap_limit(stmt, row_cap).sql(dialect=dialect)


def _cap_limit(stmt: exp.Query, row_cap: int) -> exp.Query:
    limit = stmt.args.get("limit")
    if limit is not None:
        try:
            existing = int(limit.expression.name)
        except (TypeError, ValueError):
            existing = None
        if existing is not None and existing <= row_cap:
            return stmt
    return stmt.limit(row_cap)
