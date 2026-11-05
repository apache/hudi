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

"""AST-level SQL guardrail behavior: read-only enforcement + LIMIT capping."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from hudi_agent_gateway.tools.guardrails import enforce_guardrails
from hudi_agent_gateway.tools.registry import ToolInputError

CAP = 100


def test_plain_select_gets_limit_injected() -> None:
    out = enforce_guardrails("SELECT a FROM t", row_cap=CAP)
    assert f"LIMIT {CAP}" in out


def test_existing_smaller_limit_is_preserved() -> None:
    out = enforce_guardrails("SELECT a FROM t LIMIT 10", row_cap=CAP)
    assert "LIMIT 10" in out and f"LIMIT {CAP}" not in out


def test_oversized_limit_is_lowered() -> None:
    out = enforce_guardrails("SELECT a FROM t LIMIT 100000", row_cap=CAP)
    assert f"LIMIT {CAP}" in out and "100000" not in out


@pytest.mark.parametrize("dialect", ["trino", "spark"])
@pytest.mark.parametrize("count", [0, 1, CAP, CAP + 1])
def test_limit_boundaries(dialect: str, count: int) -> None:
    out = enforce_guardrails(f"SELECT a FROM t LIMIT {count}", CAP, dialect=dialect)
    stmt = sqlglot.parse_one(out, read=dialect)
    assert int(stmt.args["limit"].expression.this) == min(count, CAP)


@pytest.mark.parametrize("clause, expected", [
    ("FETCH FIRST 10 ROWS ONLY", 10),
    ("FETCH NEXT 1000 ROWS ONLY", CAP),
    ("FETCH FIRST ROW ONLY", 1),
    ("FETCH FIRST 0 ROWS ONLY", 0),
])
def test_fetch_is_normalized_and_capped(clause: str, expected: int) -> None:
    out = enforce_guardrails(f"SELECT a FROM t ORDER BY a OFFSET 2 ROWS {clause}", CAP)
    stmt = sqlglot.parse_one(out, read="trino")
    assert isinstance(stmt.args["limit"], exp.Limit)
    assert int(stmt.args["limit"].expression.this) == expected
    assert stmt.args["offset"].expression.this == "2"
    assert stmt.args["order"] is not None


@pytest.mark.parametrize("count, expected", [("10", 10), ("1000", 1000), ("", 1)])
def test_fetch_with_ties_keeps_inner_selection_and_has_outer_cap(count: str, expected: int) -> None:
    sql = f"SELECT a FROM t ORDER BY a OFFSET 2 ROWS FETCH FIRST {count} ROWS WITH TIES"
    out = enforce_guardrails(sql, CAP)
    stmt = sqlglot.parse_one(out, read="trino")
    assert int(stmt.args["limit"].expression.this) == CAP
    inner = stmt.find(exp.Subquery).this
    assert isinstance(inner.args["limit"], exp.Fetch)
    assert int(inner.args["limit"].args["count"].this) == expected
    assert inner.args["limit"].args["limit_options"].args["with_ties"]
    assert inner.args["offset"].expression.this == "2"
    assert inner.args["order"] is not None


def test_union_fetch_with_ties_caps_the_whole_result() -> None:
    sql = "SELECT a FROM t UNION ALL SELECT a FROM u ORDER BY a FETCH FIRST 10 ROWS WITH TIES"
    stmt = sqlglot.parse_one(enforce_guardrails(sql, CAP), read="trino")
    assert int(stmt.args["limit"].expression.this) == CAP
    assert isinstance(stmt.find(exp.Subquery).this, exp.Union)


@pytest.mark.parametrize("dialect", ["trino", "spark"])
def test_negative_limit_is_rejected(dialect: str) -> None:
    with pytest.raises(ToolInputError, match="non-negative"):
        enforce_guardrails("SELECT a FROM t LIMIT -1", CAP, dialect=dialect)


def test_cte_select_is_allowed() -> None:
    out = enforce_guardrails("WITH x AS (SELECT a FROM t) SELECT * FROM x", row_cap=CAP)
    assert out.upper().startswith("WITH") and f"LIMIT {CAP}" in out


def test_union_is_allowed_and_capped() -> None:
    out = enforce_guardrails("SELECT a FROM t UNION ALL SELECT a FROM u", row_cap=CAP)
    assert "UNION" in out.upper() and f"LIMIT {CAP}" in out


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN UPDATE SET a = 1",
        "CREATE TABLE t2 AS SELECT * FROM t",
        "DROP TABLE t",
        "TRUNCATE TABLE t",
        "GRANT SELECT ON t TO alice",
        "SET SESSION query_max_run_time = '1m'",
        "EXPLAIN SELECT 1",
        "CALL system.runtime.kill_query('q')",
        "USE hudi.default",
    ],
)
def test_non_select_statements_are_rejected(sql: str) -> None:
    with pytest.raises(ToolInputError):
        enforce_guardrails(sql, row_cap=CAP)


def test_select_wrapping_dml_is_rejected() -> None:
    # SELECT-rooted, so only the defense-in-depth tree walk can catch it.
    with pytest.raises(ToolInputError, match="Insert"):
        enforce_guardrails("WITH t AS (INSERT INTO x VALUES (1)) SELECT * FROM t", row_cap=CAP)


def test_multiple_statements_are_rejected() -> None:
    with pytest.raises(ToolInputError, match="one statement"):
        enforce_guardrails("SELECT 1; SELECT 2", row_cap=CAP)


def test_garbage_is_rejected() -> None:
    with pytest.raises(ToolInputError):
        enforce_guardrails("please show me the data", row_cap=CAP)


def test_comment_smuggled_dml_is_rejected() -> None:
    with pytest.raises(ToolInputError):
        enforce_guardrails("/* harmless */ DELETE FROM t -- SELECT", row_cap=CAP)


def test_string_literal_containing_dml_is_fine() -> None:
    out = enforce_guardrails("SELECT 'DELETE FROM t' AS s", row_cap=CAP)
    assert "DELETE FROM t" in out  # inside the literal


def test_quoted_identifiers_round_trip() -> None:
    out = enforce_guardrails('SELECT "weird col" FROM "my table"', row_cap=CAP)
    assert '"weird col"' in out


@pytest.mark.parametrize("sql", [
    "SELECT TRANSFORM(a) USING 'sh -c id' AS (x) FROM t",
    "SELECT * FROM text.`file:///proc/self/environ`",
    "SELECT * FROM json.`/var/run/secrets/token`",
    "SELECT * FROM parquet.`s3a://other-bucket/path`",
    "WITH x AS (SELECT * FROM text.`file:///etc/passwd`) SELECT * FROM x",
    "SELECT * FROM (SELECT * FROM parquet.`s3a://bucket/path`) x",
    "SELECT * FROM hudi_table_changes('s3a://other-bucket/tbl', 'latest_state', 'earliest')",
    "SELECT * FROM hudi_query_timeline('s3a://other-bucket/tbl', 'false')",
    "SELECT * FROM IDENTIFIER('text.`/etc/passwd`')",
])
def test_spark_script_and_path_reads_are_rejected(sql: str) -> None:
    with pytest.raises(ToolInputError):
        enforce_guardrails(sql, row_cap=CAP, dialect="spark")


@pytest.mark.parametrize("sql", [
    "SELECT * FROM `2024`",
    "SELECT * FROM `schema with space`.`my-table`",
    "SELECT * FROM text.orders",
    "SELECT * FROM `json`.`orders`",
    "SELECT * FROM other.text.orders",
])
def test_spark_catalog_identifiers_are_allowed(sql: str) -> None:
    out = enforce_guardrails(sql, row_cap=CAP, dialect="spark", spark_catalog="my_catalog")
    assert "LIMIT 100" in out
    if sql in ("SELECT * FROM text.orders", "SELECT * FROM `json`.`orders`",
               "SELECT * FROM `schema with space`.`my-table`"):
        assert "`my_catalog`." in out


def test_spark_relative_datasource_path_cannot_use_file_fallback() -> None:
    out = enforce_guardrails("SELECT * FROM text.`relative-file`", CAP, dialect="spark")
    assert "`spark_catalog`.text." in out


@pytest.mark.parametrize("sql", [
    "SELECT * FROM xml.`secrets`",
    "SELECT * FROM libsvm.`secrets`",
    "SELECT * FROM `org.apache.spark.sql.execution.datasources.json`.`secrets`",
    "SELECT * FROM custom_provider.`*`",
    "WITH x AS (SELECT * FROM xml.`secrets`) SELECT * FROM x",
    "SELECT * FROM (SELECT * FROM custom_provider.`relative`) x",
])
def test_spark_all_two_part_tables_are_catalog_qualified(sql: str) -> None:
    out = enforce_guardrails(sql, CAP, dialect="spark", spark_catalog="my_catalog")
    tables = list(sqlglot.parse_one(out, read="spark").find_all(exp.Table))
    assert any(table.catalog == "my_catalog" for table in tables)
    assert all(not table.db or table.catalog for table in tables)


def test_spark_qualified_tables_and_allowed_table_functions_are_preserved() -> None:
    out = enforce_guardrails(
        "SELECT * FROM other.text.orders JOIN range(1) ON true", CAP, dialect="spark"
    )
    assert "other.text.orders" in out
    assert "RANGE(1)" in out


_HUDI_FUNCTION_CALLS = [
    "hudi_query({table}, 'snapshot')",
    "hudi_table_changes({table}, 'latest_state', 'earliest')",
    "hudi_query_timeline({table}, 'false')",
    "hudi_filesystem_view({table})",
    "hudi_metadata({table})",
]


@pytest.mark.parametrize("call", _HUDI_FUNCTION_CALLS)
@pytest.mark.parametrize("table", [
    "'s3a://other-bucket/tbl'", "'/etc/passwd'", "'../tbl'", "'file:tbl'",
    r"'C:\\data\\tbl'", "'`db`.`/tmp/tbl`'", "'db.tbl extra'", "'range(1)'",
    "concat('s3a:', '//other-bucket/tbl')", "'s3a:' || '//other-bucket/tbl'",
    "concat(chr(47), 'tmp', chr(47), 'tbl')", "CAST('/tmp/tbl' AS STRING)",
    "(SELECT '/tmp/tbl')", "NULL", "table_name", "''",
])
def test_spark_hudi_table_arguments_are_checked(call: str, table: str) -> None:
    with pytest.raises(ToolInputError, match="literal catalog table identifier"):
        enforce_guardrails(f"SELECT * FROM {call.format(table=table)}", CAP, dialect="spark")


@pytest.mark.parametrize("call", _HUDI_FUNCTION_CALLS)
@pytest.mark.parametrize("table", ["orders", "default.orders", "`my db`.`my``table`"])
def test_spark_hudi_catalog_tables_are_allowed(call: str, table: str) -> None:
    sql = "SELECT * FROM " + call.format(table=f"'{table}'")
    out = enforce_guardrails(sql, CAP, dialect="spark")
    assert table in out and "LIMIT 100" in out


@pytest.mark.parametrize("call", [
    "HuDi_Query_Timeline('/tmp/tbl', 'false')",
    "`hudi_metadata`('/tmp/tbl')",
    "hudi_filesystem_view('default.orders', '../other-table')",
    "hudi_filesystem_view('default.orders', concat('..', '/other-table'))",
    "hudi_metadata()",
    "IdEnTiFiEr(concat('text.', '`/etc/passwd`'))",
    "`identifier`('text.`/etc/passwd`')",
    "custom_reader(concat('s3a:', '//other-bucket/tbl'))",
    "schema.hudi_metadata('/tmp/tbl')",
    "schema.range(1)",
])
@pytest.mark.parametrize("query", [
    "SELECT * FROM {call}",
    "WITH x AS (SELECT * FROM {call}) SELECT * FROM x",
    "SELECT * FROM (SELECT * FROM {call}) x",
    "SELECT * FROM t JOIN {call} ON true",
    "SELECT * FROM t, LATERAL {call}",
    "SELECT * FROM t LATERAL VIEW {call} x AS y",
])
def test_spark_unsafe_table_functions_are_rejected_everywhere(call: str, query: str) -> None:
    with pytest.raises(ToolInputError):
        enforce_guardrails(query.format(call=call), CAP, dialect="spark")


@pytest.mark.parametrize("sql", [
    "SELECT IDENTIFIER('col') FROM t",
    "SELECT builtin.identifier('col') FROM t",
    "SELECT * FROM t WHERE IDENTIFIER('col') = 1",
])
def test_spark_dynamic_identifiers_are_rejected_outside_from(sql: str) -> None:
    with pytest.raises(ToolInputError, match="Spark function identifier"):
        enforce_guardrails(sql, CAP, dialect="spark")


@pytest.mark.parametrize("sql", [
    "SELECT '/etc/passwd', 's3a://bucket/tbl', 'IDENTIFIER(x)', `identifier` FROM t",
    "SELECT * FROM range(10)",
    "SELECT * FROM explode(array('a/b', 'file:/tmp'))",
    "SELECT * FROM explode_outer(array(1))",
    "SELECT * FROM posexplode(array(1))",
    "SELECT * FROM posexplode_outer(array(1))",
    "SELECT * FROM inline(array(struct(1)))",
    "SELECT * FROM inline_outer(array(struct(1)))",
    "SELECT * FROM stack(1, 2)",
    "SELECT * FROM json_tuple('{\"path\":\"a/b\"}', 'path')",
    "SELECT * FROM t LATERAL VIEW explode(array('a/b')) x AS y",
    "SELECT * FROM t, LATERAL hudi_query_timeline('default.orders', 'false')",
    "SELECT * FROM t, LATERAL (SELECT * FROM range(1)) x",
])
def test_spark_safe_functions_and_path_literals_are_allowed(sql: str) -> None:
    assert "LIMIT 100" in enforce_guardrails(sql, CAP, dialect="spark")


def test_trino_table_functions_are_unchanged() -> None:
    out = enforce_guardrails("SELECT * FROM schema.custom_reader('s3a://bucket/tbl')", CAP)
    assert "CUSTOM_READER" in out and "LIMIT 100" in out


@pytest.mark.parametrize("sql, name, hint", [
    ("SELECT * FROM custom_reader('x')", "custom_reader", "unqualified table function"),
    ("SELECT * FROM schema.range(1)", "range", "unqualified table function"),
    ("SELECT * FROM hudi_metadata(concat('db.', 't'))", "Hudi", "hudi_metadata"),
    ("SELECT * FROM hudi_filesystem_view('db.t', '../x')", "subpaths", "without a subpath"),
])
def test_spark_function_errors_explain_how_to_correct_the_query(
    sql: str, name: str, hint: str,
) -> None:
    with pytest.raises(ToolInputError, match=name) as error:
        enforce_guardrails(sql, CAP, dialect="spark")
    assert hint in error.value.hint
