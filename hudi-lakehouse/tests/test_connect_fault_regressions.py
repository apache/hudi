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
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import MagicMock, patch

EXAMPLES = Path(__file__).resolve().parents[1] / 'local-dev/example'
sys.path.insert(0, str(EXAMPLES))

import connect_fault_utils as utils


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


def mock_readback(cursor):
    def observe(uri, driver, table, deadline):
        cursor.execute(f'REFRESH TABLE {table}')
        cursor.execute(f'SELECT COUNT(*), SUM(id) FROM {table}')
        return cursor.fetchall()[0]
    return observe


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
        process.poll.return_value = None
        proxy = MagicMock()
        with patch.object(module, 'readback', side_effect=mock_readback(cursor)), \
             patch.object(module, 'connect', return_value=connection_with_cursor(cursor)), \
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
        process.poll.return_value = None
        with patch.object(module, 'readback', side_effect=mock_readback(cursor)), \
             patch.object(module, 'connect', return_value=connection_with_cursor(cursor)), \
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
        process.poll.return_value = None
        with patch.object(module, 'readback', side_effect=mock_readback(cursor)), \
             patch.object(module, 'connect', return_value=connection_with_cursor(cursor)), \
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
        with patch.object(module, 'readback', side_effect=mock_readback(cursor)), \
             patch.object(module, 'connect', return_value=connection_with_cursor(cursor)), \
             patch.object(module.subprocess, 'Popen', return_value=process), \
             patch.object(module, 'interrupt_before_response', return_value=True), \
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
        with patch.object(module, 'readback', side_effect=mock_readback(read)), \
             patch.object(module, 'connect', side_effect=[connection_with_cursor(read), connection_with_cursor(writer)]), \
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


class ProcessTest(unittest.TestCase):
    def test_response_and_interruption(self):
        module = load('connect_disconnect')
        scenarios = (
            ('response', 'import time; print("READY\\nRESPONSE", flush=True); time.sleep(60)', False),
            ('not_ready', 'import time; print("NOT_READY", flush=True); time.sleep(60)', False),
            ('unanswered', 'import time; print("READY", flush=True); time.sleep(60)', True),
            ('racing', 'import signal, time, sys; signal.signal(signal.SIGTERM, '
             'lambda *args: (print("RESPONSE", flush=True), sys.exit(0))); '
             'print("READY", flush=True); time.sleep(60)', False),
        )
        for name, code, expected in scenarios:
            with self.subTest(scenario=name):
                child = subprocess.Popen([sys.executable, '-c', code], stdout=subprocess.PIPE, text=True)
                try:
                    self.assertEqual(module.interrupt_before_response(child, 0.1), expected)
                    self.assertIsNotNone(child.poll())
                finally:
                    utils.stop_process(child)

    def test_client_processes_are_reaped(self):
        child = subprocess.Popen([sys.executable, '-c',
            'import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
            'print("READY", flush=True); time.sleep(60)'], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), 'READY')
            utils.stop_process(child)
            self.assertEqual(child.returncode, -9)
        finally:
            utils.stop_process(child)
            child.stdout.close()

        module = load('connect_disconnect')
        cursor = MagicMock()
        child = MagicMock()
        child.poll.return_value = None
        with patch.object(module, 'connect', return_value=connection_with_cursor(cursor)), \
             patch.object(module.subprocess, 'Popen', return_value=child), \
             patch.object(module, 'interrupt_before_response', side_effect=KeyboardInterrupt), \
             patch.object(sys, 'argv', ['test', '--uri', 'unused']), \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                module.main()
        child.terminate.assert_called_once()
        child.wait.assert_called_once_with(timeout=2)
        self.assertFalse(any(call.args[0].startswith('DROP ') for call in cursor.execute.call_args_list))


class ReadbackProcessTest(unittest.TestCase):
    def test_native_client_stalls_are_bounded_and_reaped(self):
        # A stand-in DBAPI runs in a real child, including connection and close.
        source = """import os, time
class Connection:
    def __init__(self): self.stall('connect')
    def stall(self, stage):
        if os.environ.get('STALL_STAGE') == stage: time.sleep(60)
    def __enter__(self): return self
    def __exit__(self, *args): self.stall('close')
    def cursor(self): return self
    def execute(self, sql): self.stall('execute')
    def fetchall(self):
        self.stall('fetch')
        return [(2, 1)]
class dbapi:
    @staticmethod
    def connect(**kwargs): return Connection()
"""
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'adbc_driver_manager.py').write_text(source)
            for stage in ('connect', 'execute', 'fetch', 'close', ''):
                with self.subTest(stage=stage), patch.dict(os.environ, {'PYTHONPATH': directory, 'STALL_STAGE': stage}):
                    processes = []
                    popen = subprocess.Popen
                    def start(*args, **kwargs):
                        process = popen(*args, **kwargs)
                        processes.append(process)
                        return process
                    started = time.monotonic()
                    with patch.object(utils.subprocess, 'Popen', side_effect=start):
                        args = ('unused', 'spark', 'connect_disconnect_' + 'a' * 32 + '.orders', started + 0.5)
                        if stage:
                            with self.assertRaises(TimeoutError):
                                utils.readback(*args)
                        else:
                            self.assertEqual(utils.readback(*args), (2, 1))
                    self.assertLess(time.monotonic() - started, 5)
                    self.assertIsNotNone(processes[0].poll())

    def test_result_after_deadline_is_rejected(self):
        process = MagicMock()
        process.communicate.return_value = ('[2, 1]', '')
        process.returncode = 0
        process.poll.return_value = 0
        with patch.object(utils.subprocess, 'Popen', return_value=process), \
             patch.object(utils.time, 'monotonic', side_effect=[0, 0, 100]):
            with self.assertRaises(TimeoutError):
                utils.readback('unused', 'spark', 'unused', 1)

    def test_deadline_failure_never_retries_writes_or_cleans_table(self):
        for name, argv in (
            ('connect_disconnect', ['--uri', 'unused']),
            ('connect_cancel', ['--port', '15002']),
            ('connect_lost_response', ['--port', '15002', '--retry']),
        ):
            with self.subTest(name=name):
                module = load(name)
                cursor = MagicMock()
                connection = connection_with_cursor(cursor)
                process = MagicMock()
                process.poll.return_value = None
                process.stdout.readline.return_value = 'READY\n'
                with contextlib.ExitStack() as stack:
                    stack.enter_context(patch.object(module, 'connect', return_value=connection))
                    observe = stack.enter_context(patch.object(module, 'readback', side_effect=TimeoutError))
                    stack.enter_context(patch.object(sys, 'argv', ['test', *argv]))
                    stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                    stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
                    if name == 'connect_cancel':
                        thread = MagicMock(); thread.is_alive.side_effect = [True, False]
                        stack.enter_context(patch.object(module.threading, 'Thread', return_value=thread))
                        stack.enter_context(patch.object(module.threading, 'Event', return_value=MagicMock()))
                        stack.enter_context(patch.object(module.time, 'sleep'))
                    else:
                        stack.enter_context(patch.object(module.subprocess, 'Popen', return_value=process))
                        if name == 'connect_disconnect':
                            stack.enter_context(patch.object(module, 'interrupt_before_response', return_value=True))
                        else:
                            stack.enter_context(patch.object(module, 'ResponseDropProxy', return_value=MagicMock()))
                            stack.enter_context(patch.object(module.select, 'select', return_value=([process.stdout], [], [])))
                    self.assertEqual(module.main(), 2)
                    observe.assert_called_once()
                self.assertFalse(any(call.args[0].startswith(('DROP ', 'INSERT '))
                                     for call in cursor.execute.call_args_list))


if __name__ == '__main__':
    unittest.main()
