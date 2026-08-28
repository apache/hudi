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

AST-level (sqlglot, Trino dialect) rather than regex: read-only enforcement
survives comments, CTEs, and string literals; the row cap is injected as a
real ``LIMIT``. Fail-closed: anything that does not parse is rejected.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from hudi_agent_gateway.tools.registry import ToolInputError

_FORBIDDEN_NODES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
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

_HINT = "Provide exactly one read-only SELECT statement (Trino SQL)."


def enforce_guardrails(sql: str, row_cap: int) -> str:
    """Validate ``sql`` as a single read-only SELECT and cap its LIMIT.

    Returns the (possibly rewritten) SQL to execute. Raises
    :class:`ToolInputError` on any violation.
    """
    try:
        statements = sqlglot.parse(sql, read="trino")
    except sqlglot.errors.SqlglotError as e:
        raise ToolInputError(f"could not parse SQL: {e}", hint=_HINT) from e

    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise ToolInputError(
            f"exactly one statement is allowed, got {len(statements)}", hint=_HINT
        )
    stmt = statements[0]

    if not isinstance(stmt, (exp.Select, exp.SetOperation)):
        raise ToolInputError(
            f"read-only: only SELECT is allowed, got {type(stmt).__name__}", hint=_HINT
        )

    # Defense in depth: no write/DDL/command node anywhere in the tree
    # (catches e.g. a CTE wrapping an INSERT).
    for node in stmt.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            raise ToolInputError(
                f"read-only: {type(node).__name__} is not allowed", hint=_HINT
            )

    return _cap_limit(stmt, row_cap).sql(dialect="trino")


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
