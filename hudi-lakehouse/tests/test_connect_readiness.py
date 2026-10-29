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

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / 'local-dev/example/connect_readiness.py'


class ReadinessTest(unittest.TestCase):
    def setUp(self):
        self.dbapi = MagicMock()
        package = types.ModuleType('adbc_driver_manager')
        package.dbapi = self.dbapi
        with patch.dict(sys.modules, {'adbc_driver_manager': package}):
            spec = importlib.util.spec_from_file_location('connect_readiness', SCRIPT)
            self.module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.module)
        self.cursor = self.dbapi.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value

    def run_check(self):
        args = [str(SCRIPT), '--uri', 'spark://local', '--schema', 'probe', '--table', 'orders', '--column', 'id']
        with patch.object(sys, 'argv', args), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return self.module.main()

    def test_sql_catalog_and_data_must_each_pass(self):
        self.cursor.fetchall.side_effect = [[(1,)], [('probe',)], [(10,)]]
        self.assertEqual(self.run_check(), 0)
        self.assertEqual(self.cursor.execute.call_count, 3)
        self.assertIn('`probe`.`orders`', self.cursor.execute.call_args.args[0])

    def test_missing_catalog_or_empty_storage_fails(self):
        for values in ([(1,)], [('other',)], []), ([(1,)], [('probe',)], []):
            with self.subTest(values=values):
                self.cursor.reset_mock()
                self.cursor.fetchall.side_effect = list(values)
                self.assertEqual(self.run_check(), 1)

    def test_identifiers_reject_sql(self):
        for value in ('bad.name', 'bad`x', 'x;DROP', ''):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                self.module.identifier(value)


if __name__ == '__main__':
    unittest.main()
