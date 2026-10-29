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
import unittest
from unittest.mock import MagicMock

from sqlalchemy.engine.url import make_url
from sqlalchemy import types
from sqlalchemy import column, select
from hudi_spark_connect.dialect import SparkConnectDialect, spark_type


class DialectTest(unittest.TestCase):
    def setUp(self):
        self.dialect = SparkConnectDialect()

    def test_connect_url_and_schema(self):
        args, options = self.dialect.create_connect_args(make_url(
            "sparkconnect://localhost:15002/a%60b?tls=false&driver=spark"))
        self.assertEqual(args, [])
        self.assertEqual(options["db_kwargs"]["uri"],
                         "spark://localhost:15002?tls=false&api=connect&auth_type=none")
        self.dialect.dbapi = MagicMock()
        conn = self.dialect.dbapi.connect.return_value
        self.dialect.connect(*args, **options)
        conn.cursor.return_value.__enter__.return_value.execute.assert_called_once_with("USE `a``b`")
        self.dialect.dbapi.connect.assert_called_once_with(
            driver="spark", db_kwargs={"uri": "spark://localhost:15002?tls=false&api=connect&auth_type=none"}, autocommit=True)

    def test_credentials_and_protocol_rejected(self):
        for url in ("sparkconnect://u:p@host", "sparkconnect://host?api=thrift"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                self.dialect.create_connect_args(make_url(url))

    def test_failed_schema_selection_closes_connection(self):
        self.dialect.dbapi = MagicMock()
        conn = self.dialect.dbapi.connect.return_value
        conn.cursor.return_value.__enter__.return_value.execute.side_effect = RuntimeError("bad schema")
        with self.assertRaisesRegex(RuntimeError, "bad schema"):
            self.dialect.connect(_schema="missing")
        conn.close.assert_called_once()

    def test_execute_never_binds_or_retries(self):
        cursor = MagicMock()
        self.dialect.do_execute(cursor, "SELECT 1", ())
        cursor.execute.assert_called_once_with("SELECT 1")
        with self.assertRaises(NotImplementedError):
            self.dialect.do_execute(cursor, "SELECT ?", (1,))
        with self.assertRaises(NotImplementedError):
            self.dialect.do_executemany(cursor, "INSERT INTO t VALUES (?)", [(1,)])
        self.assertEqual(cursor.execute.call_count, 1)

    def test_offset_without_limit_does_not_add_negative_limit(self):
        stmt = select(column("id")).offset(5)
        sql = str(stmt.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True}))
        self.assertIn("OFFSET 5", sql)
        self.assertNotIn("LIMIT", sql)
        sql = str(stmt.limit(10).compile(
            dialect=self.dialect, compile_kwargs={"literal_binds": True}))
        self.assertIn("LIMIT 10", sql)
        self.assertIn("OFFSET 5", sql)

    def test_reflection_quoting_and_types(self):
        conn = MagicMock()
        conn.exec_driver_sql.side_effect = [
            [("db", "v", False)], [("db", "t", False), ("db", "v", False), ("db", "tmp", True)],
            [("id", "int", ""), ("amount", "decimal(18, 2)", ""), ("", "", ""), ("# Partition", "", "")],
        ]
        self.assertEqual(self.dialect.get_table_names(conn, 'a`b'), ["t"])
        columns = self.dialect.get_columns(conn, "table", 'a`b')
        self.assertEqual([x["name"] for x in columns], ["id", "amount"])
        self.assertIsInstance(columns[0]["type"], types.Integer)
        self.assertEqual((columns[1]["type"].precision, columns[1]["type"].scale), (18, 2))
        self.assertIn("`a``b`.`table`", conn.exec_driver_sql.call_args.args[0])
        self.assertIsInstance(spark_type("struct<a:int>"), types.NullType)


    def test_reflection_preserves_quoted_column_names(self):
        conn = MagicMock()
        conn.exec_driver_sql.return_value = [
            (" leading ", "int", ""), ("#metric", "string", ""),
            ("", "", ""), ("# Partition Information", "", ""),
        ]
        columns = self.dialect.get_columns(conn, "t")
        self.assertEqual([column["name"] for column in columns], [" leading ", "#metric"])

    def test_has_table_finds_session_temporary_view(self):
        conn = MagicMock()
        conn.exec_driver_sql.return_value = [("", "temporary_view", True)]
        self.assertTrue(self.dialect.has_table(conn, "temporary_view"))
        conn.exec_driver_sql.assert_called_once_with("SHOW TABLES")

    def test_has_table_does_not_bind_temporary_view_to_schema(self):
        conn = MagicMock()
        conn.exec_driver_sql.return_value = [("", "temporary_view", True)]
        self.assertFalse(self.dialect.has_table(conn, "temporary_view", schema="other"))


if __name__ == "__main__":
    unittest.main()
