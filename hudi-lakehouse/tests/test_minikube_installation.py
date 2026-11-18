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

"""Installer command contracts and real Helm rendering; no running cluster required."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / 'local-dev'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class InstallerTest(unittest.TestCase):
    def invoke(self, *args, failure='', backend=''):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            fake = temp / 'fake'
            fake.write_text(f'#!{sys.executable}\n' + '''
import json, os, sys
name = os.path.basename(sys.argv[0])
a = sys.argv[1:]
body = sys.stdin.read() if (name == 'kubectl' and ('-i' in a or a[-1:] == ['-'])) else ''
with open(os.environ['CALLS'], 'a') as f:
    f.write(json.dumps([name, a, body]) + '\\n')
fail = os.environ['FAILURE']
if name == 'minikube' and a[-1:] == ['status']: sys.exit(0 if fail == 'running-profile' else 1)
if name == 'docker' and 'inspect' in a:
    if fail in ('missing-image', 'cached-image'): sys.exit(1)
    print(json.dumps([{'Id': 'sha256:local-id'}]))
if name == 'minikube' and 'ls' in a and '--format=json' in a:
    entries = []
    if fail in ('cached-image', 'unchanged-image'):
        entries = [{'id': 'local-id', 'repoTags': ['docker.io/library/' + image]} for image in (
            'hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0', 'hudi-lakehouse-agent-gateway:0.1.0')]
    print(json.dumps(entries))
if name == 'kubectl' and 'get' in a and 'namespace' in a: print('Active')
if name == 'kubectl' and 'get' in a and 'deployment' in a and '-o' in a:
    if a[-1] == 'json' and os.environ['BACKEND']:
        print(json.dumps({'spec': {'template': {'spec': {'containers': [{'env': [
            {'name': 'GATEWAY_SQL_BACKEND', 'value': os.environ['BACKEND']}]}]}}}}))
    elif a[-1] == 'name': print('deployment/test')
if name == 'kubectl' and 'rollout' in a and fail == 'rollout': sys.exit(42)
if name == 'kubectl' and 'exec' in a:
    if 'real MCP ->' in body and fail == 'mcp': sys.exit(43)
    if 'Check chat readiness' in body and fail == 'llm': sys.exit(44)
    if 'Verify actual built-in chat' in body and fail == 'chat': sys.exit(45)
if name == 'kubectl' and 'scale' in a and fail == 'scale': sys.exit(46)
''')
            fake.chmod(0o755)
            for name in ('docker', 'minikube', 'helm', 'kubectl'):
                (temp / name).symlink_to(fake)
            env = {k: v for k, v in os.environ.items() if not k.startswith((
                'HUDI_', 'SPARK_CONNECT_', 'GATEWAY_', 'MINIO_', 'HMS_', 'LOCAL_DEV_', 'INSTALL_'))}
            env.update(PATH=str(temp) + os.pathsep + env['PATH'], CALLS=str(temp / 'calls'),
                       FAILURE=failure, BACKEND=backend)
            result = subprocess.run(['bash', str(LOCAL / 'lakehouse.sh'), *args],
                                    env=env, text=True, capture_output=True, timeout=30)
            calls = [json.loads(line) for line in (temp / 'calls').read_text().splitlines()] if (temp / 'calls').exists() else []
            return result, calls

    def test_help_and_bad_arguments_do_not_contact_cluster(self):
        for args, status in (([], 0), (['--help'], 0), (['down', '--build'], 2),
                             (['up', '--build', '--pull-images'], 2), (['up', '--profile', '../bad'], 2)):
            result, calls = self.invoke(*args)
            self.assertEqual(result.returncode, status, result.stderr)
            self.assertEqual(calls, [])

    def test_up_uses_explicit_profile_and_preserves_current_context(self):
        result, calls = self.invoke('up', '--profile', 'isolated')
        self.assertEqual(result.returncode, 0, result.stderr)
        start = next(a for name, a, _ in calls if name == 'minikube' and 'start' in a)
        self.assertIn('--keep-context', start)
        for name, args, _ in calls:
            if name == 'kubectl':
                self.assertEqual(args[:2], ['--context', 'isolated'])
            if name == 'helm' and 'upgrade' in args:
                self.assertEqual(args[:2], ['--kube-context', 'isolated'])
                self.assertNotIn('hudi-trino', args)
                self.assertNotIn('spark-kubernetes-operator', args)
        self.assertFalse(any('build' in a for _, a, _ in calls))
        scripts = [body for name, a, body in calls if name == 'kubectl' and 'exec' in a]
        self.assertIn('Initialize missing demo tables', scripts[0])
        self.assertIn('Probe SQL and HMS', scripts[1])
        self.assertIn('real MCP ->', scripts[2])
        self.assertIn('Check chat readiness', scripts[3])
        self.assertIn('Verify actual built-in chat', scripts[4])

    def test_running_profile_is_reused_without_restarting_minikube(self):
        result, calls = self.invoke('up', failure='running-profile')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Reusing running minikube profile', result.stdout)
        self.assertFalse(any('start' in a for name, a, _ in calls if name == 'minikube'))

    def test_existing_trino_gateway_is_not_changed(self):
        result, calls = self.invoke('up', backend='trino')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Use a different --profile', result.stderr)
        self.assertFalse(any('apply' in a or 'upgrade' in a or 'load' in a for _, a, _ in calls))

    def test_missing_images_fail_before_workload_mutation(self):
        result, calls = self.invoke('up', failure='missing-image')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('not published releases', result.stderr)
        self.assertFalse(any('apply' in a or 'upgrade' in a for _, a, _ in calls))
        result, _ = self.invoke('up', failure='cached-image')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unchanged_image_ids_skip_large_application_transfers(self):
        result, calls = self.invoke('up', failure='unchanged-image')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Reusing unchanged cluster image', result.stdout)
        self.assertFalse(any(name == 'minikube' and 'load' in a and a[-1].startswith('hudi-lakehouse-')
                             for name, a, _ in calls))

    def test_build_loads_custom_tags_and_restarts_application_pods(self):
        result, calls = self.invoke('up', '--build', '--connect-image', 'local/connect:dev',
                                    '--gateway-image', 'local/gateway:dev')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any(a[:2] == ['tag', 'hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0']
                            for name, a, _ in calls if name == 'docker'))
        restarts = [a[-1] for name, a, _ in calls if name == 'kubectl' and 'restart' in a]
        self.assertEqual(restarts, ['deploy/hudi-spark-connect', 'deploy/hudi-agent-gateway'])
        self.assertTrue(any(a[-1] == 'local/connect:dev' for name, a, _ in calls
                            if name == 'minikube' and 'load' in a))

    def test_registry_mode_never_builds_or_loads_images(self):
        result, calls = self.invoke('up', '--pull-images', '--connect-image', 'localhost:5000/connect:dev',
                                    '--gateway-image', 'example.com/gateway:dev')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(name == 'docker' or 'load' in args for name, args, _ in calls))

    def test_llm_failure_is_distinct_from_mcp_failure(self):
        for failure, code in (('mcp', 1), ('llm', 0), ('chat', 0), ('rollout', 1)):
            result, calls = self.invoke('up', failure=failure)
            self.assertEqual(result.returncode, code, result.stderr)
            if failure == 'mcp':
                self.assertIn('MCP verification FAILED', result.stderr)
                self.assertFalse(any('Check chat readiness' in body for _, _, body in calls))
            elif failure == 'llm':
                self.assertIn('Chat unavailable', result.stderr)
                self.assertFalse(any('Verify actual built-in chat' in body for _, _, body in calls))
            elif failure == 'chat':
                self.assertIn('Chat inference/tool verification failed', result.stderr)
        result, _ = self.invoke('chat', failure='llm')
        self.assertEqual(result.returncode, 1)

    def test_test_is_read_only_and_never_reseeds(self):
        result, calls = self.invoke('test')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any('apply' in a or 'start' in a or 'upgrade' in a for _, a, _ in calls))
        scripts = [(a, b) for _, a, b in calls if 'exec' in a]
        self.assertIn('--verify-only', scripts[0][0])
        self.assertIn('--verify-sample', scripts[1][0])

    def test_down_preserves_namespace_pvcs_and_releases_and_propagates_failures(self):
        result, calls = self.invoke('down')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any('scale' in a for _, a, _ in calls))
        for _, args, _ in calls:
            self.assertNotIn('uninstall', args)
            if 'delete' in args:
                self.assertIn('job', args)
        result, _ = self.invoke('down', failure='scale')
        self.assertNotEqual(result.returncode, 0)

    def test_forward_is_localhost_only_and_foreground(self):
        result, calls = self.invoke('forward', '--port', '18000')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls[-1][1][-5:], ['port-forward', '--address', '127.0.0.1',
                                          'svc/hudi-agent-gateway', '18000:8000'])


class ChartAndDataTest(unittest.TestCase):
    def test_gateway_local_readiness_keeps_mcp_routable_without_llm(self):
        chart = ROOT / 'charts/hudi-agent-gateway'
        for extra, expected in (([], '/ready'), (['--set', 'readinessPath=/health'], '/health')):
            result = subprocess.run(['helm', 'template', 'test', str(chart), '-f',
                                     str(chart / 'values-local-dev.yaml'), '-f',
                                     str(chart / 'values-spark-connect.yaml'), *extra],
                                    text=True, capture_output=True, check=True)
            deployment = next(d for d in yaml.safe_load_all(result.stdout) if d and d['kind'] == 'Deployment')
            container = deployment['spec']['template']['spec']['containers'][0]
            self.assertEqual(container['readinessProbe']['httpGet']['path'], expected)
        result = subprocess.run(['helm', 'template', 'test', str(chart), '--set', 'readinessPath=/typo'],
                                capture_output=True)
        self.assertNotEqual(result.returncode, 0)

    def test_persistent_manifests_and_shared_hms_bootstrap(self):
        for filename, claim in (('minio.yaml', 'minio-data'), ('hive-metastore.yaml', 'hive-metastore-data')):
            docs = list(yaml.safe_load_all((LOCAL / 'manifests' / filename).read_text()))
            self.assertIn(claim, [d['metadata']['name'] for d in docs if d['kind'] == 'PersistentVolumeClaim'])
            deploy = next(d for d in docs if d['kind'] == 'Deployment')
            self.assertEqual(deploy['spec']['strategy']['type'], 'Recreate')
            if filename == 'minio.yaml':
                self.assertEqual(deploy['spec']['template']['spec']['securityContext']['fsGroup'], 65532)
            if filename == 'hive-metastore.yaml':
                container = deploy['spec']['template']['spec']['containers'][0]
                self.assertEqual(container['command'], ['/bin/bash', '/lakehouse/start-metastore.sh'])
                self.assertEqual(deploy['spec']['template']['spec']['volumes'][0]['configMap']['name'],
                                 'hive-metastore-bootstrap')

    def test_seed_preserves_existing_including_empty_tables(self):
        seed = load('seed', ROOT / 'docker/scripts/seed.py')
        spark = Mock()
        spark.catalog.tableExists.return_value = True
        with patch.object(seed, 'verify') as verify:
            seed.seed(spark)
        spark.sql.assert_not_called()
        verify.assert_not_called()
        self.assertEqual(spark.catalog.tableExists.call_count, 2)

    def test_seed_validates_only_new_tables_and_propagates_creation_failure(self):
        seed = load('seed', ROOT / 'docker/scripts/seed.py')
        spark = Mock()
        spark.catalog.tableExists.side_effect = [False, True]
        with patch.object(seed, 'verify') as verify:
            seed.seed(spark)
        spark.sql.assert_called_once_with(seed.DEMO_TABLES['drivers'])
        verify.assert_called_once_with(spark, ('drivers',))
        spark.catalog.tableExists.side_effect = None
        spark.catalog.tableExists.return_value = False
        spark.sql.side_effect = RuntimeError('existing data location')
        with self.assertRaisesRegex(RuntimeError, 'existing data location'):
            seed.seed(spark)
        self.assertFalse(any('DROP' in call.args[0] or 'TRUNCATE' in call.args[0]
                             for call in spark.sql.call_args_list))

    def test_chat_requires_successful_real_tool_result(self):
        chat = load('check_chat', LOCAL / 'scripts/check-chat.py')
        chat.verify({'message': '1', 'tool_trace': [{'name': 'query_lakehouse', 'result_preview': '{"rows": [[1]]}'}]})
        for response in ({'message': '1'}, {'message': '1', 'tool_trace': [{'name': 'query_lakehouse',
                         'is_error': True, 'result_preview': '{"rows": [[1]]}'}]}):
            with self.assertRaises(AssertionError):
                chat.verify(response)


if __name__ == '__main__':
    unittest.main()
