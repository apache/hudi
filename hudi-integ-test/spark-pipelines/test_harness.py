# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Fast lifecycle regressions; no Spark installation is needed for these tests."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from harness import ConnectHarness, owned_process, stop_process_group, wait_for_process


class TestProcessLifecycle(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.log = self.root / "client.log"

    def process(self, code):
        return owned_process([sys.executable, "-u", "-c", code], self.log, self.root, os.environ.copy())

    def test_client_failure_keeps_diagnostics(self):
        with self.process("print('intentional client failure'); raise SystemExit(7)") as process:
            with self.assertRaisesRegex(RuntimeError, "exited with 7"):
                wait_for_process(process, 5, self.log)
        self.assertIn("intentional client failure", self.log.read_text())

    def test_timeout_terminates_owned_process_without_stopping_another_server(self):
        other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
        self.addCleanup(stop_process_group, other)
        with self.assertRaises(TimeoutError):
            with self.process("import time; time.sleep(60)") as process:
                wait_for_process(process, 0.2, self.log)
        self.assertIsNotNone(process.poll())
        self.assertIsNone(other.poll())

    def test_server_exit_interrupts_client(self):
        server = subprocess.Popen([sys.executable, "-c", "raise SystemExit(9)"])
        server.wait(timeout=5)
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "server exited with 9"):
            with self.process("import time; time.sleep(60)") as process:
                wait_for_process(process, 30, self.log, server)
        self.assertLess(time.monotonic() - started, 10)
        self.assertIsNotNone(process.poll())

    def test_kills_process_ignoring_term(self):
        command = [sys.executable, "-u", "-c",
                   "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready'); time.sleep(60)"]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True, start_new_session=True)
        self.addCleanup(stop_process_group, process, 0.2)
        self.addCleanup(process.stdout.close)
        self.assertEqual(process.stdout.readline().strip(), "ready")
        stop_process_group(process, grace=0.2)
        self.assertEqual(process.returncode, -9)

    def test_startup_failure_cleans_server_and_test_resources(self):
        spark_home = self.root / "pyspark"
        (spark_home / "bin").mkdir(parents=True)
        submit = spark_home / "bin/spark-submit"
        submit.write_text("#!/bin/sh\nexec sleep 60\n")
        submit.chmod(0o755)
        bundle = self.root / "bundle.jar"
        bundle.touch()
        fake_pyspark = SimpleNamespace(__version__="4.1.1", __file__=str(spark_home / "__init__.py"))
        with patch.dict(sys.modules, pyspark=fake_pyspark):
            harness = ConnectHarness(bundle, self.root / "logs", startup_timeout=0.2)
        with self.assertRaisesRegex(TimeoutError, "startup exceeded"):
            with harness:
                self.fail("A server which never listens must not become ready")
        self.assertFalse(harness.root.exists())
        self.assertIsNotNone(harness.server.poll())
        self.assertTrue((harness.logs / "server.log").is_file())


if __name__ == "__main__":
    unittest.main()
