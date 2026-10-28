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
import argparse
import contextlib
import importlib.util
import io
import pathlib
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / 'images/spark-connect/check-ready.py'


class ProbeTest(unittest.TestCase):
    def setUp(self):
        sql = types.ModuleType('pyspark.sql')
        sql.SparkSession = MagicMock()
        with patch.dict(sys.modules, {'pyspark': types.ModuleType('pyspark'), 'pyspark.sql': sql}):
            spec = importlib.util.spec_from_file_location('check_ready', SCRIPT)
            self.probe = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.probe)
        self.spark = sql.SparkSession.builder.remote.return_value.getOrCreate.return_value

    def run_probe(self, results):
        self.spark.sql.side_effect = [types.SimpleNamespace(collect=lambda rows=rows: rows) for rows in results]
        args = [str(SCRIPT), '--schema', 'probe', '--table', 'orders', '--column', 'id']
        with patch.object(sys, 'argv', args), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return self.probe.main()

    def test_three_checks_and_session_cleanup(self):
        self.assertEqual(self.run_probe([[(1,)], [('probe',)], [(42,)]]), 0)
        self.assertEqual(self.spark.sql.call_count, 3)
        self.spark.stop.assert_called_once()

    def test_missing_schema_or_no_rows_fail_and_close(self):
        for rows in ([[(1,)], [('other',)]], [[(1,)], [('probe',)], []]):
            with self.subTest(rows=rows):
                self.spark.reset_mock()
                self.assertEqual(self.run_probe(rows), 1)
                self.spark.stop.assert_called_once()

    def test_identifier_validation(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            self.probe.identifier('orders;DROP TABLE')


if __name__ == '__main__':
    unittest.main()
