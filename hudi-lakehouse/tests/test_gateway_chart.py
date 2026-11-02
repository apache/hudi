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

"""Gateway backend and Secret chart contracts."""

import pathlib
import subprocess
import unittest

import yaml

CHART = pathlib.Path(__file__).resolve().parents[1] / "charts/hudi-agent-gateway"


def render(*args):
    result = subprocess.run(["helm", "template", "test", str(CHART), *args],
                            capture_output=True, text=True, check=True)
    deployment = next(d for d in yaml.safe_load_all(result.stdout) if d and d["kind"] == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    return {e["name"]: e for e in container["env"]}


class GatewayChartTest(unittest.TestCase):
    def test_default_trino(self):
        env = render()
        self.assertEqual(env["GATEWAY_SQL_BACKEND"]["value"], "trino")
        self.assertNotIn("GATEWAY_SPARK_CONNECT_URI", env)

    def test_connect_overlay(self):
        env = render("-f", str(CHART / "values-spark-connect.yaml"))
        self.assertEqual(env["GATEWAY_SQL_BACKEND"]["value"], "spark-connect")
        self.assertEqual(env["GATEWAY_SPARK_CONNECT_URI"]["value"],
                         "sc://hudi-spark-connect.hudi-lakehouse.svc:15002")
        self.assertEqual(env["GATEWAY_SPARK_CONNECT_CATALOG"]["value"], "spark_catalog")
        self.assertNotIn("GATEWAY_SPARK_CONNECT_DRIVER", env)

    def test_connect_uri_secret(self):
        env = render("--set", "sqlBackend=spark-connect", "--set", "sparkConnect.existingSecret=connect-auth",
                     "--set", "sparkConnect.uriSecretKey=uri")
        self.assertNotIn("value", env["GATEWAY_SPARK_CONNECT_URI"])
        self.assertEqual(env["GATEWAY_SPARK_CONNECT_URI"]["valueFrom"]["secretKeyRef"],
                         {"name": "connect-auth", "key": "uri"})

    def test_invalid_backend_rejected(self):
        with self.assertRaises(subprocess.CalledProcessError):
            render("--set", "sqlBackend=typo")


if __name__ == "__main__":
    unittest.main()
