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

"""Compose contracts and installer failures; no daemon or Python dependencies needed."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
DOCKER = ROOT / 'docker'


def clean_env():
    return {k: v for k, v in os.environ.items() if not k.startswith(
        ('GATEWAY_', 'COMPOSE_', 'OPENAI_', 'ANTHROPIC_', 'MINIO_', 'SPARK_CONNECT_', 'OLLAMA_'))}


@unittest.skipUnless(shutil.which('docker'), 'Docker Compose CLI required (no daemon needed)')
class ComposeContractTest(unittest.TestCase):
    def config(self, *args):
        result = subprocess.run(['docker', 'compose', '--env-file', '/dev/null',
                                 '-f', str(DOCKER / 'compose.yaml'), *args,
                                 '--profile', 'local-llm', 'config', '--format', 'json'],
                                env=clean_env(), text=True, capture_output=True, check=True)
        return json.loads(result.stdout)['services']

    def test_startup_orders_seed_before_gateway_and_gates_hms(self):
        services = self.config()
        for service, dependency, condition in (
            ('gateway', 'seed', 'service_completed_successfully'),
            ('seed', 'spark-connect', 'service_healthy'),
            ('spark-connect', 'hive-metastore', 'service_healthy'),
            ('hive-metastore', 'minio-init', 'service_completed_successfully'),
        ):
            self.assertEqual(services[service]['depends_on'][dependency]['condition'], condition)
        for name, service in services.items():
            with self.subTest(service=name):
                self.assertNotIn('build', service)
                for port in service.get('ports', []):
                    self.assertEqual(port['host_ip'], '127.0.0.1')
        env = services['gateway']['environment']
        self.assertEqual(env['GATEWAY_SQL_BACKEND'], 'spark-connect')
        self.assertEqual(env['GATEWAY_SPARK_CONNECT_CATALOG'], 'spark_catalog')
        self.assertEqual(services['seed']['image'], services['spark-connect']['image'])

    def test_images_match_existing_charts_and_build_contexts_exist(self):
        services = self.config('-f', str(DOCKER / 'compose.build.yaml'))
        for service, chart in [('gateway', 'hudi-agent-gateway'), ('spark-connect', 'hudi-spark-connect')]:
            values = (ROOT / 'charts' / chart / 'values.yaml').read_text()
            repo = re.search(r'^  repository: (.+)$', values, re.M)[1]
            tag = re.search(r'^  tag: "(.+)"$', values, re.M)[1]
            self.assertEqual(services[service]['image'], f'{repo}:{tag}')
            self.assertTrue((Path(services[service]['build']['context']) / 'Dockerfile').is_file())

    def test_persistent_hms_and_storage(self):
        services = self.config()
        for service, target in [('minio', '/data'), ('hive-metastore', '/opt/hive/data')]:
            self.assertTrue(any(v['type'] == 'volume' and v['target'] == target
                                for v in services[service]['volumes']))
        tree = ET.parse(DOCKER / 'conf/hive-site.xml')
        properties = {p.findtext('name'): p.findtext('value') for p in tree.findall('property')}
        self.assertIn('/opt/hive/data/metastore_db', properties['javax.jdo.option.ConnectionURL'])
        self.assertEqual(services['ollama']['profiles'], ['local-llm'])


class InstallerTest(unittest.TestCase):
    def run_installer(self, args=('up',), *, failure='', memory='10000000000', environment=None,
                      env_file=None, caller_env=None, compose_version='2.31.0-desktop.2'):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            # Isolate from a developer's real .env, including in shell-path tests.
            shutil.copytree(DOCKER, temp / 'installation', ignore=shutil.ignore_patterns('.env'))
            if env_file is not None:
                (temp / 'installation/.env').write_text(env_file)
            if caller_env is not None:
                (temp / '.env').write_text(caller_env)
            fake = temp / 'docker'
            fake.write_text(f'#!{sys.executable}\n' + '''
import json
import os
import subprocess
import sys
args = sys.argv[1:]
with open(os.environ['CALLS'], 'a') as stream:
    stream.write(json.dumps(args) + '\\n')
failure = os.environ['FAILURE']
if args == ['compose', 'version', '--short']:
    if failure == 'compose': sys.exit(1)
    print(os.environ['TEST_COMPOSE_VERSION'])
if 'config' in args and '--environment' in args:
    if os.environ['REAL_DOCKER']:
        # Use the real resolver, but never run containers from this unit test.
        resolved = json.loads(subprocess.run(
            [os.environ['REAL_DOCKER'], *args[:-1], '--format', 'json'],
            capture_output=True, text=True, check=True).stdout)
        gateway = resolved['services']['gateway']
        with open(os.environ['CALLS'], 'a') as stream:
            stream.write(json.dumps(['resolved-config', resolved['name'],
                gateway['ports'][0]['published'],
                gateway['environment']['GATEWAY_LLM_PROVIDER']]) + '\\n')
        sys.exit(subprocess.run([os.environ['REAL_DOCKER'], *args]).returncode)
    for key in ('GATEWAY_LLM_PROVIDER', 'GATEWAY_LLM_MODEL'):
        if key in os.environ: print(key + '=' + os.environ[key])
if 'show' in args:
    sys.exit(0 if failure == 'cached-ollama' else 1)
if 'ollama' in args and 'pull' in args and failure in ('cached-ollama', 'pull'):
    sys.exit(32)
if args[0] == 'info':
    if failure == 'daemon': sys.exit(1)
    print(os.environ['MEMORY'])
if 'up' in args and failure == 'up': sys.exit(23)
if '/lakehouse/smoke-test.py' in args and failure == 'mcp': sys.exit(29)
if 'exec' in args and args[-1] == '-' and failure == 'llm': sys.exit(31)
if 'port' in args:
    print('127.0.0.1:' + args[-1])
''')
            fake.chmod(0o755)
            env = dict(clean_env(), PATH=str(temp) + os.pathsep + os.environ['PATH'],
                       CALLS=str(temp / 'calls'), FAILURE=failure, MEMORY=memory,
                       REAL_DOCKER=shutil.which('docker') or '', TEST_COMPOSE_VERSION=compose_version)
            env.update(environment or {})
            result = subprocess.run(['bash', str(temp / 'installation/lakehouse.sh'), *args],
                                    cwd=temp, env=env, capture_output=True, text=True, timeout=20)
            calls = ([json.loads(line) for line in (temp / 'calls').read_text().splitlines()]
                     if (temp / 'calls').exists() else [])
            return result, calls

    def test_help_does_not_contact_docker(self):
        for args in ([], ['--help'], ['-h']):
            with self.subTest(args=args):
                result, calls = self.run_installer(args, failure='daemon')
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('Usage: lakehouse.sh', result.stdout)
                self.assertEqual(calls, [])

    def test_invalid_commands_do_not_contact_docker(self):
        for args in (['up', '--mcp-only'], ['--build', '--local-llm'], ['up', 'down'], ['unknown']):
            with self.subTest(args=args):
                result, calls = self.run_installer(args)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn('Usage: lakehouse.sh', result.stderr)
                self.assertEqual(calls, [])

    def test_compose_version_gate(self):
        for version, supported in (
            ('2.20.0', False), ('2.27.1', False), ('unknown', False),
            ('2.27.2', True), ('v2.27.2', True), ('2.28.0', True),
            ('2.31.0-desktop.2', True), ('3.0.0', True),
        ):
            with self.subTest(version=version):
                result, calls = self.run_installer(compose_version=version)
                self.assertEqual(result.returncode == 0, supported, result.stderr)
                if not supported:
                    self.assertEqual(calls, [['compose', 'version', '--short']])
                    self.assertIn('2.27.2 or newer is required', result.stderr)
                    self.assertIn(version, result.stderr)
        result, calls = self.run_installer(failure='compose')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, [['compose', 'version', '--short']])
        self.assertIn('2.27.2 or newer is required', result.stderr)

    def test_daemon_and_memory_fail_before_starting_services(self):
        for failure, memory, message in [('daemon', '0', 'Docker is not running'),
                                         ('', '4000000000', 'at least 8 GB')]:
            result, calls = self.run_installer(failure=failure, memory=memory)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(message, result.stderr)
            self.assertFalse(any('up' in call for call in calls))

    def test_startup_failure_does_not_print_success_or_run_checks(self):
        result, calls = self.run_installer(failure='up')
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn('Chat UI:', result.stdout)
        self.assertFalse(any('exec' in call for call in calls))

    def test_mcp_failure_is_not_reported_as_success(self):
        result, calls = self.run_installer(failure='mcp')
        self.assertEqual(result.returncode, 29)
        self.assertNotIn('Chat UI:', result.stdout)
        self.assertNotIn('Lakehouse and MCP are ready.', result.stdout)
        self.assertFalse(any(call[-1:] == ['-'] for call in calls))

    def test_llm_failure_preserves_running_stack_and_reports_mcp_success(self):
        result, calls = self.run_installer(failure='llm')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Chat is unavailable', result.stderr)
        self.assertIn('Lakehouse and MCP are ready.', result.stdout)
        self.assertIn('Chat UI:', result.stdout)
        self.assertIn('Connect Claude:', result.stdout)
        self.assertFalse(any('down' in call for call in calls))

    def test_up_checks_mcp_then_chat_without_repeating_seed(self):
        result, calls = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('Chat is unavailable', result.stderr)
        mcp = next(i for i, call in enumerate(calls) if '/lakehouse/smoke-test.py' in call)
        chat = next(i for i, call in enumerate(calls) if 'exec' in call and call[-1] == '-')
        self.assertLess(mcp, chat)
        self.assertFalse(any('--verify-only' in call for call in calls))
        self.assertFalse(any('pull' in call or 'build' in call for call in calls))

    def test_down_preserves_volumes_including_optional_profile(self):
        result, calls = self.run_installer(['down'], memory='0')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('local-llm', calls[-1])
        self.assertIn('down', calls[-1])
        self.assertNotIn('--volumes', calls[-1])

    def test_api_autodetection_warns_about_missing_model_without_exposing_keys(self):
        result, calls = self.run_installer(environment={'OPENAI_API_KEY': 'test-secret'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('GATEWAY_LLM_MODEL', result.stderr)
        self.assertNotIn('test-secret', result.stdout + result.stderr)
        self.assertTrue(any('up' in call for call in calls))
        self.assertTrue(any('/lakehouse/smoke-test.py' in call for call in calls))
        self.assertFalse(any(call[-1:] == ['-'] for call in calls))

    def test_local_llm_pulls_model_before_starting_gateway(self):
        result, calls = self.run_installer(['up', '--local-llm'])
        self.assertEqual(result.returncode, 0, result.stderr)
        pull = next(i for i, call in enumerate(calls) if 'ollama' in call and 'pull' in call)
        start = next(i for i, call in enumerate(calls) if 'up' in call and 'gateway' in call)
        self.assertLess(pull, start)
        self.assertIn('local-llm', calls[start])

    @unittest.skipUnless(shutil.which('docker'), 'Real Compose resolver required')
    def test_missing_api_model_from_shell_or_env_file_does_not_block_startup(self):
        for provider in ('openai', 'anthropic', 'openai-compatible'):
            settings = {'GATEWAY_LLM_PROVIDER': provider, 'OPENAI_API_KEY': 'test-secret',
                        'ANTHROPIC_API_KEY': 'test-secret', 'GATEWAY_OPENAI_BASE_URL': 'http://api/v1'}
            for source in ('shell', 'env-file'):
                with self.subTest(provider=provider, source=source):
                    config = ({'environment': settings} if source == 'shell' else
                              {'env_file': '\n'.join(f'{key}={value}' for key, value in settings.items())})
                    result, calls = self.run_installer(['up', '--build'], **config)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn('Set GATEWAY_LLM_MODEL', result.stderr)
                    self.assertIn('Chat is unavailable', result.stderr)
                    self.assertNotIn('test-secret', result.stdout + result.stderr)
                    self.assertTrue(any('up' in call for call in calls))
                    self.assertTrue(any('build' in call for call in calls))
                    self.assertTrue(any('/lakehouse/smoke-test.py' in call for call in calls))
                    self.assertFalse(any(call[-1:] == ['-'] for call in calls))

    @unittest.skipUnless(shutil.which('docker'), 'Real Compose resolver required')
    def test_model_resolution_honors_env_interpolation_and_shell_override(self):
        config = 'GATEWAY_LLM_PROVIDER=openai\nGATEWAY_LLM_MODEL=${SELECTED_MODEL}\n'
        result, _ = self.run_installer(env_file=config, environment={'SELECTED_MODEL': 'api-model'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('Chat is unavailable', result.stderr)
        result, _ = self.run_installer(env_file=config, environment={
            'SELECTED_MODEL': '', 'GATEWAY_LLM_MODEL': 'shell-model',
        })
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('Chat is unavailable', result.stderr)
        result, _ = self.run_installer(env_file=config, environment={
            'SELECTED_MODEL': 'api-model', 'GATEWAY_LLM_MODEL': '',
        })
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Set GATEWAY_LLM_MODEL', result.stderr)

    @unittest.skipUnless(shutil.which('docker'), 'Real Compose resolver required')
    def test_unrelated_caller_env_is_ignored(self):
        result, calls = self.run_installer(caller_env=(
            'GATEWAY_LLM_PROVIDER=openai\nCOMPOSE_PROJECT_NAME=unrelated\nGATEWAY_PORT=19000\n'
        ))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(['resolved-config', 'hudi-lakehouse', '8000', 'ollama'], calls)

    @unittest.skipUnless(shutil.which('docker'), 'Real Compose resolver required')
    def test_cached_ollama_model_skips_pull_using_resolved_name(self):
        for model, config in (
            ('qwen3:8b', {}),
            ('from-env', {'env_file': 'GATEWAY_LLM_MODEL=from-env\n'}),
            ('from-shell', {'env_file': 'GATEWAY_LLM_MODEL=from-env\n',
                            'environment': {'GATEWAY_LLM_MODEL': 'from-shell'}}),
        ):
            with self.subTest(model=model):
                result, calls = self.run_installer(['up', '--local-llm'], failure='cached-ollama', **config)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertTrue(any(call[-3:] == ['ollama', 'show', model] for call in calls))
                self.assertFalse(any('ollama' in call and 'pull' in call for call in calls))
                self.assertTrue(any('up' in call and 'gateway' in call for call in calls))

    def test_missing_ollama_model_pull_failure_stops_startup(self):
        result, calls = self.run_installer(['up', '--local-llm'], failure='pull')
        self.assertEqual(result.returncode, 32)
        self.assertFalse(any('up' in call and 'gateway' in call for call in calls))
        self.assertNotIn('Chat UI:', result.stdout)

    def test_explicit_local_llm_takes_precedence_over_ambient_api_credentials(self):
        result, calls = self.run_installer(['up', '--local-llm'],
                                          environment={'OPENAI_API_KEY': 'test-secret'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('Set GATEWAY_LLM_MODEL', result.stderr)
        self.assertTrue(any(call[-3:] == ['ollama', 'pull', 'qwen3:8b'] for call in calls))

    def test_test_command_does_not_start_or_seed(self):
        result, calls = self.run_installer(['test'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any('up' in call or 'run' in call for call in calls))
        self.assertTrue(any('--verify-only' in call for call in calls))
        self.assertFalse(any(call[-1:] == ['-'] for call in calls))
        self.assertIn('LLM readiness was not checked', result.stdout)


if __name__ == '__main__':
    unittest.main()
