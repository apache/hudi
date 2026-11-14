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
"""Opt-in live test: creates and removes a unique database on the target server."""
import os
from datetime import datetime, timezone
from decimal import Decimal
import unittest
import uuid

from sqlalchemy import MetaData, Table, column, create_engine, inspect, select, text
from sqlalchemy.dialects import registry

URI = os.environ.get("HUDI_SUPERSET_CONNECT_TEST_URI")


@unittest.skipUnless(URI, "set HUDI_SUPERSET_CONNECT_TEST_URI to a disposable endpoint")
class LiveDialectTest(unittest.TestCase):
    def test_offset_only_compiled_sql(self):
        registry.register("sparkconnect", "hudi_spark_connect.dialect", "SparkConnectDialect")
        engine = create_engine(URI)
        try:
            query = select(column("id")).select_from(text("range(10)"))\
                .order_by(column("id")).offset(5)
            sql = str(query.compile(dialect=engine.dialect, compile_kwargs={"literal_binds": True}))
            with engine.connect() as conn:
                self.assertEqual(conn.exec_driver_sql(sql).fetchall(), [(5,), (6,), (7,), (8,), (9,)])
        finally:
            engine.dispose()

    def test_epoch_seconds_and_milliseconds(self):
        registry.register("sparkconnect", "hudi_spark_connect.dialect", "SparkConnectDialect")
        engine = create_engine(URI)
        try:
            with engine.connect() as conn:
                row = conn.exec_driver_sql(
                    "SELECT unix_micros(timestamp_seconds(1700000000)), "
                    "unix_micros(timestamp_millis(1700000000123)), "
                    "unix_micros(timestamp_millis(-1))"
                ).fetchone()
                self.assertEqual(tuple(row), (1700000000000000, 1700000000123000, -1000))
        finally:
            engine.dispose()

    def test_hudi_read_write_and_reflection(self):
        registry.register("sparkconnect", "hudi_spark_connect.dialect", "SparkConnectDialect")
        engine = create_engine(URI)
        database = "connect_adapter_" + uuid.uuid4().hex
        table = database + ".orders"
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql("CREATE DATABASE " + database)
                try:
                    conn.exec_driver_sql(f"""CREATE TABLE {table} (id INT, region STRING, amount INT, ts BIGINT, day DATE)
                        USING hudi TBLPROPERTIES (type='cow', primaryKey='id', preCombineField='ts')""")
                    conn.exec_driver_sql(f"INSERT INTO {table} VALUES (1, 'East', 100, 1, DATE '2026-10-01'), (2, 'West', 200, 1, DATE '2026-10-01')")
                    self.assertIn(database, inspect(conn).get_schema_names())
                    self.assertIn('orders', inspect(conn).get_table_names(database))
                    reflected = Table('orders', MetaData(), schema=database, autoload_with=conn)
                    self.assertIn('amount', reflected.c)
                    columns = inspect(conn).get_columns('orders', database)
                    self.assertTrue({'id', 'region', 'amount', 'ts'} <= {c['name'] for c in columns})
                    self.assertEqual(conn.exec_driver_sql(f"SELECT SUM(amount) FROM {table}").scalar(), 300)
                    metadata = conn.exec_driver_sql(
                        f"SELECT _hoodie_commit_time, _hoodie_record_key FROM {table} ORDER BY id"
                    ).fetchall()
                    self.assertEqual([row[1] for row in metadata], ['1', '2'])
                    self.assertTrue(all(row[0] for row in metadata))
                    self.assertEqual(conn.exec_driver_sql(
                        f"SELECT region, SUM(amount) FROM {table} WHERE region='East' GROUP BY region"
                    ).fetchall(), [('East', 100)])
                    self.assertEqual(conn.exec_driver_sql(
                        f"SELECT CAST(date_trunc('day', day) AS STRING), SUM(amount) "
                        f"FROM {table} GROUP BY date_trunc('day', day)"
                    ).fetchall(), [('2026-10-01 00:00:00', 300)])
                    conn.exec_driver_sql(f"UPDATE {table} SET amount=150, ts=2 WHERE id=1")
                    self.assertEqual(conn.exec_driver_sql(f"SELECT id, amount FROM {table} ORDER BY id").fetchall(), [(1, 150), (2, 200)])
                    conn.exec_driver_sql(f"DELETE FROM {table} WHERE id=2")
                    self.assertEqual(conn.exec_driver_sql(f"SELECT id, amount FROM {table}").fetchall(), [(1, 150)])
                finally:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table} PURGE")
                    conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {database}")
        finally:
            engine.dispose()

    def test_session_view_and_quoted_columns(self):
        registry.register("sparkconnect", "hudi_spark_connect.dialect", "SparkConnectDialect")
        engine = create_engine(URI)
        name = "connect_view_" + uuid.uuid4().hex
        try:
            with engine.connect() as conn:
                try:
                    conn.exec_driver_sql(f"CREATE TEMP VIEW {name} AS SELECT 1 AS ` leading `, 2 AS `#metric`")
                    inspector = inspect(conn)
                    self.assertTrue(inspector.has_table(name))
                    self.assertFalse(inspector.has_table(name, schema="default"))
                    self.assertEqual([column["name"] for column in inspector.get_columns(name)],
                                     [" leading ", "#metric"])
                    self.assertEqual(conn.exec_driver_sql(f"SELECT ` leading `, `#metric` FROM {name}").fetchall(), [(1, 2)])
                finally:
                    conn.exec_driver_sql(f"DROP VIEW IF EXISTS {name}")
        finally:
            engine.dispose()

    def test_arrow_type_mapping(self):
        registry.register("sparkconnect", "hudi_spark_connect.dialect", "SparkConnectDialect")
        engine = create_engine(URI)
        try:
            with engine.connect() as conn:
                with conn.connection.cursor() as cursor:
                    cursor.execute("SET TIME ZONE 'UTC'")
                    cursor.execute("""SELECT CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP) AS ts,
                        CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP_NTZ) AS ntz,
                        CAST(123.456 AS DECIMAL(38,18)) AS d,
                        array(1, CAST(NULL AS INT)) AS a, named_struct('x', 7) AS s""")
                    arrow = cursor.fetch_arrow_table()
                    self.assertEqual(str(arrow.schema.field('ts').type), 'timestamp[us, tz=UTC]')
                    self.assertEqual(str(arrow.schema.field('ntz').type), 'timestamp[us]')
                    self.assertEqual(str(arrow.schema.field('d').type), 'decimal128(38, 18)')
                    self.assertEqual(arrow.to_pylist(), [{
                        'ts': datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc),
                        'ntz': datetime(2026, 1, 2, 3, 4, 5, 123456),
                        'd': Decimal('123.456000000000000000'),
                        'a': [1, None], 's': {'x': 7}}])
        finally:
            engine.dispose()
