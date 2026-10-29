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
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

EXAMPLES = Path(__file__).resolve().parents[1] / 'local-dev/example'


def load(name):
    spec = importlib.util.spec_from_file_location(name, EXAMPLES / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    package = types.ModuleType('adbc_driver_manager')
    package.dbapi = MagicMock()
    with patch.dict(sys.modules, {'adbc_driver_manager': package}):
        spec.loader.exec_module(module)
    return module


def connection_with_cursor(cursor):
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn


class FaultFailureTest(unittest.TestCase):
    def test_lost_response_retry_failure_stops_without_repeating_write(self):
        module = load('connect_lost_response')
        cursor = MagicMock()
        cursor.fetchall.return_value = [(2, 1)]
        def execute(sql):
            if 'COUNT(DISTINCT id)' in sql:
                raise RuntimeError('retry readback failed')
        cursor.execute.side_effect = execute
        process = MagicMock()
        process.stdout.readline.return_value = 'READY\n'
        process.poll.side_effect = [None, -15]
        proxy = MagicMock()
        with patch.object(module, 'connect', return_value=connection_with_cursor(cursor)), \
             patch.object(module, 'ResponseDropProxy', return_value=proxy), \
             patch.object(module.subprocess, 'Popen', return_value=process), \
             patch.object(module.select, 'select', return_value=([process.stdout], [], [])), \
             patch('sys.argv', ['test', '--port', '15002', '--rows', '2', '--retry']), \
             self.assertRaisesRegex(RuntimeError, 'retry readback failed'):
            module.main()
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertEqual(sum(sql.startswith('INSERT INTO') for sql in statements), 1)
        self.assertFalse(any(sql.startswith('DROP ') for sql in statements))

    def test_lost_response_retries_transient_observation_only(self):
        module = load('connect_lost_response')
        cursor = MagicMock()
        cursor.fetchall.return_value = [(2, 1)]
        failures = []

        def execute(sql):
            if sql.startswith('REFRESH TABLE') and not failures:
                failures.append(sql)
                raise RuntimeError('temporary read error')

        cursor.execute.side_effect = execute
        process = MagicMock()
        process.stdout.readline.return_value = 'READY\n'
        process.poll.side_effect = [None, None, -15]
        with patch.object(module, 'connect', return_value=connection_with_cursor(cursor)), \
             patch.object(module, 'ResponseDropProxy', return_value=MagicMock()), \
             patch.object(module.subprocess, 'Popen', return_value=process), \
             patch.object(module.select, 'select', return_value=([process.stdout], [], [])), \
             patch.object(module.time, 'sleep'), \
             patch('sys.argv', ['test', '--port', '15002', '--rows', '2']):
            self.assertEqual(module.main(), 0)
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertEqual(sum(sql.startswith('REFRESH TABLE') for sql in statements), 2)
        self.assertEqual(sum(sql.startswith('DROP DATABASE') for sql in statements), 1)

    def test_lost_response_cleanup_failure_is_not_readback_failure(self):
        module = load('connect_lost_response')
        cursor = MagicMock()
        cursor.fetchall.return_value = [(2, 1)]
        def execute(sql):
            if sql.startswith('DROP DATABASE'):
                raise RuntimeError('cleanup failed')
        cursor.execute.side_effect = execute
        process = MagicMock()
        process.stdout.readline.return_value = 'READY\n'
        process.poll.side_effect = [None, -15]
        with patch.object(module, 'connect', return_value=connection_with_cursor(cursor)), \
             patch.object(module, 'ResponseDropProxy', return_value=MagicMock()), \
             patch.object(module.subprocess, 'Popen', return_value=process), \
             patch.object(module.select, 'select', return_value=([process.stdout], [], [])), \
             patch('sys.argv', ['test', '--port', '15002', '--rows', '2']), \
             self.assertRaisesRegex(RuntimeError, 'cleanup failed'):
            module.main()
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertEqual(sum(sql.startswith('REFRESH TABLE') for sql in statements), 1)
        self.assertEqual(sum(sql.startswith('DROP DATABASE') for sql in statements), 1)

    def test_disconnect_cleanup_failure_is_not_retried(self):
        module = load('connect_disconnect')
        cursor = MagicMock()
        cursor.fetchall.return_value = [(2, 1)]
        def execute(sql):
            if sql.startswith('DROP DATABASE'):
                raise RuntimeError('cleanup failed')
        cursor.execute.side_effect = execute
        process = MagicMock()
        process.stdout.readline.return_value = 'READY\n'
        process.poll.return_value = None
        with patch.object(module, 'connect', return_value=connection_with_cursor(cursor)), \
             patch.object(module.subprocess, 'Popen', return_value=process), \
             patch.object(module.select, 'select', return_value=([process.stdout], [], [])), \
             patch.object(module.time, 'sleep'), \
             patch('sys.argv', ['test', '--uri', 'unused', '--rows', '2']), \
             self.assertRaisesRegex(RuntimeError, 'cleanup failed'):
            module.main()
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertEqual(sum(sql.startswith('REFRESH TABLE') for sql in statements), 1)
        self.assertEqual(sum(sql.startswith('DROP DATABASE') for sql in statements), 1)

    def test_cancel_cleanup_failure_is_not_retried(self):
        module = load('connect_cancel')
        read = MagicMock()
        read.fetchall.return_value = [(2, 1)]
        def execute(sql):
            if sql.startswith('DROP DATABASE'):
                raise RuntimeError('cleanup failed')
        read.execute.side_effect = execute
        writer = MagicMock()
        thread = MagicMock()
        thread.is_alive.side_effect = [True, False]
        with patch.object(module, 'connect', side_effect=[connection_with_cursor(read), connection_with_cursor(writer)]), \
             patch.object(module.threading, 'Thread', return_value=thread), \
             patch.object(module.threading, 'Event', return_value=MagicMock()), \
             patch.object(module.time, 'sleep'), \
             patch('sys.argv', ['test', '--port', '15002', '--rows', '2']), \
             self.assertRaisesRegex(RuntimeError, 'cleanup failed'):
            module.main()
        statements = [call.args[0] for call in read.execute.call_args_list]
        self.assertEqual(sum(sql.startswith('REFRESH TABLE') for sql in statements), 1)
        self.assertEqual(sum(sql.startswith('DROP DATABASE') for sql in statements), 1)


class ReconnectTest(unittest.TestCase):
    def test_failed_query_closes_cursor_then_connection(self):
        module = load('connect_restart_fault')
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.execute.side_effect = RuntimeError('not ready')
        with self.assertRaisesRegex(RuntimeError, 'not ready'):
            module.connect_ready(lambda port: conn, 15002)
        cursor.close.assert_called_once()
        conn.close.assert_called_once()

    def test_failed_cursor_creation_closes_connection(self):
        module = load('connect_restart_fault')
        conn = MagicMock()
        conn.cursor.side_effect = RuntimeError('cursor failed')
        with self.assertRaisesRegex(RuntimeError, 'cursor failed'):
            module.connect_ready(lambda port: conn, 15002)
        conn.close.assert_called_once()

    def test_success_transfers_ownership_to_caller(self):
        module = load('connect_restart_fault')
        conn = MagicMock()
        actual_conn, cursor = module.connect_ready(lambda port: conn, 15002)
        self.assertIs(actual_conn, conn)
        self.assertIs(cursor, conn.cursor.return_value)
        cursor.close.assert_not_called()
        conn.close.assert_not_called()


if __name__ == '__main__':
    unittest.main()
