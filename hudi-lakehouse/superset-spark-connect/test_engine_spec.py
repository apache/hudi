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
"""Run with Superset installed; exercises its actual filter handling path."""
import importlib.util
import unittest
from datetime import datetime


@unittest.skipUnless(importlib.util.find_spec('superset'), 'requires Superset')
class SupersetEngineSpecTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from superset.app import create_app
        cls.app = create_app()

    def test_schema_round_trip(self):
        from unittest.mock import MagicMock
        from sqlalchemy.engine.url import make_url
        from hudi_spark_connect.dialect import SparkConnectDialect
        from hudi_spark_connect.engine_spec import SparkConnectEngineSpec as spec

        for schema in ("a/b", "a`b", "a b", "a/b` c", "a%2Fb"):
            with self.subTest(schema=schema):
                uri, connect_args = spec.adjust_engine_params(
                    make_url("sparkconnect://localhost:15002/default?tls=false"), {}, schema=schema)
                uri = make_url(str(uri))
                self.assertEqual(spec.get_schema_from_engine_params(uri, connect_args), schema)
                dialect = SparkConnectDialect()
                args, options = dialect.create_connect_args(uri)
                self.assertEqual(options["_schema"], schema)
                dialect.dbapi = MagicMock()
                dialect.connect(*args, **options)
                cursor = dialect.dbapi.connect.return_value.cursor.return_value.__enter__.return_value
                cursor.execute.assert_called_once_with("USE `" + schema.replace("`", "``") + "`")

    def test_temporal_filter_values_compile(self):
        import sqlalchemy as sa
        from superset.models.helpers import ExploreMixin
        from superset.utils.core import FilterOperator, GenericDataType
        from hudi_spark_connect.engine_spec import SparkConnectEngineSpec

        for native, expected in [('DATE', "CAST('2023-11-14' AS DATE)"),
                                 ('DATETIME', "CAST('2023-11-14 22:13:20.000000' AS TIMESTAMP)"),
                                 ('TIMESTAMP', "CAST('2023-11-14 22:13:20.000000' AS TIMESTAMP)"),
                                 ('TIMESTAMP_NTZ', "CAST('2023-11-14 22:13:20.000000' AS TIMESTAMP_NTZ)")]:
            with self.subTest(native=native):
                value = ExploreMixin.filter_values_handler(
                    [1700000000000], FilterOperator.EQUALS, GenericDataType.TEMPORAL,
                    target_native_type=native, db_engine_spec=SparkConnectEngineSpec)
                sql = str(sa.select([sa.column('ts')]).where(sa.column('ts') == value).compile(
                    compile_kwargs={'literal_binds': True}))
                self.assertIn(expected, sql)

    def test_epoch_expressions_and_unknown_type(self):
        import sqlalchemy as sa
        from hudi_spark_connect.engine_spec import SparkConnectEngineSpec as spec
        self.assertEqual(spec.epoch_to_dttm(), 'TIMESTAMP_SECONDS({col})')
        self.assertEqual(spec.epoch_ms_to_dttm(), 'TIMESTAMP_MILLIS({col})')
        self.assertIsNone(spec.convert_dttm('STRING', datetime(2026, 1, 1)))
        with self.app.app_context():
            for fmt, function in [('epoch_s', 'TIMESTAMP_SECONDS'),
                                  ('epoch_ms', 'TIMESTAMP_MILLIS')]:
                with self.subTest(fmt=fmt):
                    expr = spec.get_timestamp_expr(sa.column('ts'), fmt, 'P1D')
                    sql = str(sa.select([expr]).compile(compile_kwargs={'literal_binds': True}))
                    self.assertIn("date_trunc('day', " + function + '(ts))', sql)


if __name__ == '__main__':
    unittest.main()
