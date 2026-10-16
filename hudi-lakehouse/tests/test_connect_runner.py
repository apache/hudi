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

"""Exercise Connect shell entrypoints without a cluster or driver."""
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

RUNNER = pathlib.Path(__file__).resolve().parents[1] / "local-dev/scripts/test-spark-connect.sh"


class ConnectRunnerTest(unittest.TestCase):
    def run_fixture(self, failure, cleanup_failure=False):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            kubectl = root / "kubectl"
            kubectl.write_text(f"#!{sys.executable}\n" + """
import signal
import sys
if 'port-forward' in sys.argv:
    print('Forwarding from 127.0.0.1:15002 -> 15002', flush=True)
    signal.pause()
""")
            client = root / "client"
            client.write_text(f"#!{sys.executable}\n" + """
import os
import pathlib
import sys
args = sys.argv[1:]
phase = args[1]
with open(os.environ['TEST_CALLS'], 'a') as calls:
    calls.write(phase + '\\n')
if phase == 'prepare':
    if os.environ['TEST_FAILURE'] == 'create':
        sys.exit(17)
    pathlib.Path(args[args.index('--created-marker') + 1]).write_text(
        args[args.index('--database') + 1])
    if os.environ['TEST_FAILURE'] == 'write':
        sys.exit(23)
if phase == 'cleanup' and os.environ['TEST_CLEANUP_FAILURE'] == '1':
    sys.exit(31)
""")
            kubectl.chmod(0o755)
            client.chmod(0o755)
            calls = root / "calls"
            env = dict(os.environ, PATH=f"{root}{os.pathsep}{os.environ['PATH']}",
                       TMPDIR=str(root), PYTHON=str(client), PORT="15002", SERVICE_PORT="15002",
                       TEST_CALLS=str(calls), TEST_FAILURE=failure,
                       TEST_CLEANUP_FAILURE="1" if cleanup_failure else "0")
            result = subprocess.run(["bash", str(RUNNER), "--restart-endpoint"], env=env,
                                    capture_output=True, text=True, timeout=20)
            return result, calls.read_text().splitlines()

    def test_failed_create_does_not_attempt_cleanup(self):
        result, calls = self.run_fixture("create")
        self.assertEqual(result.returncode, 17)
        self.assertEqual(calls, ["prepare"])

    def test_failed_write_attempts_cleanup_and_preserves_error(self):
        result, calls = self.run_fixture("write")
        self.assertEqual(result.returncode, 23)
        self.assertEqual(calls, ["prepare", "cleanup"])

    def test_failed_cleanup_keeps_original_error_and_prints_instructions(self):
        result, calls = self.run_fixture("write", cleanup_failure=True)
        self.assertEqual(result.returncode, 23)
        self.assertEqual(calls, ["prepare", "cleanup"])
        self.assertIn("Cleanup failed; retry", result.stderr)

    def test_success_cleans_up_once(self):
        result, calls = self.run_fixture("")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["prepare", "verify", "cleanup"])


class ConnectQuickstartTest(unittest.TestCase):
    def run_fixture(self, *args, deploy_only=False, image_exists=False, cluster_running=False,
                    fail_build=False, fail_example=False, fail_writer=False):
        root = RUNNER.parents[2]
        script = root / ("local-dev/scripts/up.sh" if deploy_only else "scripts/quickstart.sh")
        with tempfile.TemporaryDirectory() as directory:
            tools = pathlib.Path(directory)
            calls_file = tools / "calls.jsonl"
            # A restricted PATH also checks that Connect needs no host Maven/JDK/curl.
            for name in ("bash", "dirname", "basename", "grep", "head", "seq", "sleep", "cat"):
                (tools / name).symlink_to(shutil.which(name))
            legacy_tools = ("mvn", "java", "curl", "ls", "cp", "rm", "mkdir") if (
                "--spark-connect" not in args or "--run-example" in args) else ()
            for name in ("docker", "minikube", "helm", "kubectl", *legacy_tools):
                tool = tools / name
                tool.write_text(f"#!{sys.executable}\n" + """
import json
import os
import pathlib
import sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['TEST_CALLS'], 'a') as calls:
    calls.write(json.dumps([name, *args]) + '\\n')
if name == 'minikube':
    if args == ['status']:
        sys.exit(0 if os.environ['TEST_CLUSTER_RUNNING'] == '1' else 1)
    if args == ['docker-env', '--shell', 'bash']:
        print('export CONNECT_TEST_DOCKER_ENV=1')
if name == 'docker':
    if os.environ.get('CONNECT_TEST_DOCKER_ENV') != '1':
        sys.exit(71)
    if args[:2] == ['image', 'inspect']:
        sys.exit(0 if os.environ['TEST_IMAGE_EXISTS'] == '1' else 1)
    if args[:1] == ['build'] and os.environ['TEST_FAIL_BUILD'] == '1':
        sys.exit(42)
if name == 'ls':
    print('/fixture/hudi-spark3.5-bundle_2.12-test.jar' if 'hudi-spark-bundle' in ' '.join(args)
          else '/fixture/trino-hudi-test')
if name == 'kubectl' and 'get' in args and 'sparkapplication' in args:
    print('Failed' if os.environ['TEST_FAIL_WRITER'] == '1' else 'Succeeded')
if name == 'kubectl' and 'logs' in args:
    print('DEMO_OK rows=100 table=default.trips')
if name == 'kubectl' and 'apply' in args and args[-1] == '-':
    sys.stdin.read()
if name == 'kubectl' and 'exec' in args:
    if 'deploy/hudi-spark-connect' in args:
        assert args[-2:] == ['python3', '-']
        query = sys.stdin.read()
        assert 'from pyspark.sql.connect.session import SparkSession' in query
        assert 'FROM spark_catalog.default.trips' in query
        assert 'spark.stop()' in query
    if os.environ['TEST_FAIL_EXAMPLE'] == '1':
        sys.exit(29)
""")
                tool.chmod(0o755)
            env = dict(os.environ, PATH=str(tools), TEST_CALLS=str(calls_file),
                       TEST_IMAGE_EXISTS="1" if image_exists else "0",
                       TEST_CLUSTER_RUNNING="1" if cluster_running else "0",
                       TEST_FAIL_BUILD="1" if fail_build else "0",
                       TEST_FAIL_EXAMPLE="1" if fail_example else "0",
                       TEST_FAIL_WRITER="1" if fail_writer else "0",
                       INSTALL_AGENT_GATEWAY="1", INSTALL_VLLM="1")
            env.pop("BASH_ENV", None)
            env.pop("CONNECT_TEST_DOCKER_ENV", None)
            result = subprocess.run([str(tools / "bash"), str(script), *args], env=env,
                                    capture_output=True, text=True, timeout=20)
            calls = [json.loads(line) for line in calls_file.read_text().splitlines()] if calls_file.exists() else []
            return result, calls

    def assert_connect_deployment(self, calls):
        helm_calls = [call for call in calls if call[0] == "helm"]
        self.assertEqual(len(helm_calls), 1)
        self.assertEqual(helm_calls[0][:4], ["helm", "upgrade", "--install", "hudi-spark-connect"])
        manifests = [pathlib.Path(call[-1]).name for call in calls if call[:3] == ["kubectl", "apply", "-f"]]
        self.assertEqual(manifests, ["namespace.yaml", "minio.yaml", "hive-metastore.yaml",
                                     "minio-bucket-job.yaml", "spark-connect-secret.yaml"])
        for workload in ("deploy/minio", "deploy/hive-metastore", "job/minio-make-bucket"):
            wait = next(call for call in calls if workload in call)
            self.assertLess(calls.index(wait), calls.index(helm_calls[0]))
        self.assertFalse(any(call[1:3] == ["delete", "namespace"] for call in calls))

    def test_connect_only_builds_and_deploys_without_legacy_tools(self):
        result, calls = self.run_fixture("--spark-connect")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(["minikube", "start", "--cpus", "4", "--memory", "10g"], calls)
        builds = [call for call in calls if call[:2] == ["docker", "build"]]
        self.assertEqual(len(builds), 1)
        self.assertEqual(builds[0][2:4], ["-t", "hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0"])
        self.assert_connect_deployment(calls)
        self.assertNotIn("run-example.sh", result.stdout)
        self.assertFalse(any("exec" in call for call in calls))

    def test_cached_image_and_running_cluster_are_reused(self):
        result, calls = self.run_fixture("--spark-connect", image_exists=True, cluster_running=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(call[:2] in (["docker", "build"], ["minikube", "start"]) for call in calls))
        self.assert_connect_deployment(calls)

    def test_rebuild_images_rebuilds_only_connect(self):
        result, calls = self.run_fixture("--spark-connect", "--rebuild-images", image_exists=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        builds = [call for call in calls if call[:2] == ["docker", "build"]]
        self.assertEqual(len(builds), 1)
        self.assertIn("hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0", builds[0])
        self.assert_connect_deployment(calls)

    def test_build_failure_does_not_start_deployment(self):
        result, calls = self.run_fixture("--spark-connect", fail_build=True)
        self.assertEqual(result.returncode, 42, result.stderr)
        self.assertFalse(any(call[0] in ("kubectl", "helm") for call in calls))

    def test_example_failure_is_not_reported_as_quickstart_success(self):
        result, calls = self.run_fixture("--spark-connect", "--run-example", image_exists=True, fail_example=True)
        self.assertEqual(result.returncode, 29, result.stderr)
        self.assertNotIn("==> Done", result.stdout)

    def test_rebuild_jars_requires_a_writer_in_connect_mode(self):
        result, calls = self.run_fixture("--spark-connect", "--rebuild-jars")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires --run-example", result.stderr)
        self.assertEqual(calls, [])

    def test_examples_share_writer_and_select_query_engine(self):
        for flags in (("--run-example",), ("--run-example", "--spark-connect"),
                      ("--spark-connect", "--run-example")):
            with self.subTest(flags=flags):
                result, calls = self.run_fixture(*flags, image_exists=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                writer = next(call for call in calls if call[:3] == ["kubectl", "apply", "-f"]
                              and call[-1].endswith("example/spark-app.yaml"))
                query = next(call for call in calls if "exec" in call)
                self.assertLess(calls.index(writer), calls.index(query))
                connect = "--spark-connect" in flags
                self.assertIn("deploy/hudi-spark-connect" if connect else "deploy/hudi-trino", query)
                releases = [call[3] for call in calls if call[:3] == ["helm", "upgrade", "--install"]]
                self.assertIn("spark-kubernetes-operator", releases)
                if connect:
                    self.assertEqual(releases, ["spark-kubernetes-operator", "hudi-spark-connect"])
                    self.assertFalse(any("hudi-trino-plugin" in " ".join(call) for call in calls))

    def test_connect_example_builds_only_writer_and_connect_images(self):
        result, calls = self.run_fixture("--spark-connect", "--run-example", "--rebuild-jars")
        self.assertEqual(result.returncode, 0, result.stderr)
        images = [call[call.index("-t") + 1] for call in calls if call[:2] == ["docker", "build"]]
        self.assertEqual(images, ["hudi-lakehouse-spark:3.5", "hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0"])
        self.assertTrue(any(call[0] == "mvn" and "packaging/hudi-spark-bundle" in call for call in calls))
        self.assertFalse(any(call[0] == "mvn" and "packaging/hudi-spark-bundle" not in call for call in calls))

    def test_writer_failure_does_not_query(self):
        result, calls = self.run_fixture("--spark-connect", "--run-example", image_exists=True, fail_writer=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any("exec" in call for call in calls))
        self.assertNotIn("==> Done", result.stdout)

    def test_direct_deployment_skips_other_components(self):
        result, calls = self.run_fixture("--spark-connect", deploy_only=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_connect_deployment(calls)
        self.assertFalse(any(call[0] in ("docker", "minikube") for call in calls))
        self.assertFalse(any("exec" in call for call in calls))

    def test_default_deployment_still_installs_full_stack(self):
        result, calls = self.run_fixture(deploy_only=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        releases = [call[3] for call in calls if call[:3] == ["helm", "upgrade", "--install"]]
        self.assertEqual(releases, ["spark-kubernetes-operator", "hudi-trino", "vllm", "hudi-agent-gateway"])
        self.assertIn("run-example.sh", result.stdout)


if __name__ == "__main__":
    unittest.main()
