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

"""Fixture isolation/cleanup and image entrypoint contracts; no live engines."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch

from test_spark_connect_chart import render

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "local-dev/example/connect_external_writer.py"


class ExternalWriterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("connect_external_writer", SCRIPT)
        cls.writer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.writer)
        cls.template = render()["Deployment"]["spec"]["template"]["spec"]

    def test_writer_uses_separate_jvm_and_shared_catalog_not_connect(self):
        pod = self.writer.writer_pod(self.template, "private-writer", "connect_smoke_test", "commit-schema")
        container = pod["spec"]["containers"][0]
        self.assertEqual(container["command"], ["/opt/spark/bin/spark-submit"])
        self.assertEqual(container["args"][:2], ["--master", "local[1]"])
        self.assertEqual(container["args"][-3:], ["write", "connect_smoke_test", "commit-schema"])
        self.assertEqual(pod["spec"]["restartPolicy"], "Never")
        self.assertFalse(pod["spec"]["automountServiceAccountToken"])
        self.assertNotIn("labels", pod["metadata"])
        self.assertNotIn("readinessProbe", container)
        env = {item["name"]: item for item in container["env"]}
        self.assertIn("HUDI_CONF_DIR", env)
        self.assertIn("valueFrom", env["AWS_ACCESS_KEY_ID"])
        self.assertNotIn("SPARK_REMOTE", env)
        self.assertEqual(container["image"], self.template["containers"][0]["image"])
        self.assertTrue(any(v["name"] == "config" for v in pod["spec"]["volumes"]))

    def run_launch(self, failure=None):
        calls = []

        def execute(args, **kwargs):
            calls.append(args)
            command = args[3:]
            if command[:2] == ["get", "deployment"]:
                stdout = json.dumps({"spec": {"template": {"spec": self.template}}})
            elif command[:2] == ["get", "pod"]:
                stdout = "Failed" if failure == "writer" else "Succeeded"
            elif command[0] == "logs":
                stdout = "EXTERNAL_WRITE_OK commit-schema"
            else:
                stdout = ""
            if (failure == "configmap" and command[:2] == ["create", "configmap"]) or (
                    failure == "pod" and command[:2] == ["create", "-f"]):
                raise subprocess.CalledProcessError(1, args)
            return types.SimpleNamespace(stdout=stdout)

        with patch.object(self.writer.subprocess, "run", side_effect=execute), \
                contextlib.redirect_stdout(io.StringIO()):
            if failure:
                with self.assertRaises((subprocess.CalledProcessError, RuntimeError)):
                    self.writer.launch("connect_smoke_test", "commit-schema")
            else:
                self.writer.launch("connect_smoke_test", "commit-schema")
        return [call[3:] for call in calls]

    def test_cleanup_only_deletes_resources_created_by_this_run(self):
        pod = ["delete", "pod", "connect-smoke-test-writer"]
        config = ["delete", "configmap", "connect-smoke-test-writer"]
        for failure, expected in (("configmap", []), ("pod", [config]),
                                  (None, [pod, config]), ("writer", [pod, config])):
            with self.subTest(failure=failure):
                deletes = [call[:3] for call in self.run_launch(failure) if call[0] == "delete"]
                self.assertEqual(deletes, expected)


class ConnectImageEntrypointTest(unittest.TestCase):
    def invoke(self, *args):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("submit", "executor"):
                stub = root / name
                stub.write_text('#!/bin/sh\nprintf "%s\\n" "' + name + '" "$@"\n')
                stub.chmod(0o755)
            # Substitute only the executable paths; exercise the actual script logic.
            script = (ROOT / "images/spark-connect/start-connect.sh").read_text()
            script = script.replace("/opt/spark/bin/spark-submit", str(root / "submit"))
            script = script.replace("/opt/entrypoint.sh", str(root / "executor"))
            entrypoint = root / "entrypoint.sh"
            entrypoint.write_text(script)
            return subprocess.run(["bash", str(entrypoint), *args], text=True,
                                  capture_output=True, check=True).stdout.splitlines()

    def test_server_and_executor_entrypoints(self):
        submit = ["submit", "--class", "org.apache.spark.sql.connect.service.SparkConnectServer"]
        jar = "/opt/spark/jars/spark-connect_2.13-4.1.3.jar"
        options = ["--master", "local[1]", "--conf", "spark.driver.host=10.0.0.2"]
        for args, expected in (([], [*submit, jar]), (options, [*submit, *options, jar]),
                               (["executor"], ["executor", "executor"])):
            with self.subTest(args=args):
                self.assertEqual(self.invoke(*args), expected)


if __name__ == "__main__":
    unittest.main()
