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
if name == 'kubectl' and 'apply' in a and os.path.basename(a[-1]) in ('minio.yaml', 'hive-metastore.yaml'):
    with open(a[-1]) as manifest:
        body = manifest.read()
if os.environ['ACTION'] in ('down', 'stop') or (name == 'kubectl' and 'view' in a):
    body = os.environ['KUBECONFIG']
if name == 'helm' and 'upgrade' in a:
    last_file = max(i for i, arg in enumerate(a) if arg == '-f') + 1
    with open(a[last_file]) as values:
        body = values.read()
with open(os.environ['CALLS'], 'a') as f:
    f.write(json.dumps([name, a, body]) + '\\n')
fail = os.environ['FAILURE']
infra = ('cgr.dev/chainguard/minio:latest', 'apache/hive:3.1.3')
host = os.environ['HOST_CONFIG']
if name == 'minikube' and ('start' in a or 'update-context' in a):
    assert os.environ['KUBECONFIG'] != host, a
    if fail == 'unavailable-profile': sys.exit(7)
    if fail == 'assert-private-config':
        with open(os.environ['PROFILE_CONFIG'], 'w') as config:
            config.write(os.environ['KUBECONFIG'])
if fail == 'assert-private-config' and name == 'helm':
    assert 'HELM_KUBEAPISERVER' not in os.environ
if fail == 'assert-private-config' and (
        name == 'helm' or (name == 'kubectl' and 'config' not in a) or (name == 'minikube' and 'image' in a)):
    with open(os.environ['PROFILE_CONFIG']) as config:
        expected = config.read()
    assert expected != os.environ['HOST_CONFIG'] and os.environ['KUBECONFIG'] == expected, a
if name == 'kubectl' and 'set-context' in a:
    assert '-n' not in a, a  # This edits a kubeconfig context, independent of the workload namespace.
if name == 'kubectl' and 'view' in a:
    profile = a[a.index('--context') + 1]
    if a[-1] == 'json':
        if body == host:
            empty = fail in ('new-profile', 'occupied-user', 'missing-context')
            print(json.dumps({'contexts': [] if empty else [{'name': profile, 'context': {}}],
                              'users': [{'name': profile, 'user': {'token': 'other'}}] if fail == 'occupied-user' else []}))
        else:
            print(json.dumps({'clusters': [{'cluster': {'server': 'https://localhost:54321', 'certificate-authority': '/ca'}}],
                              'users': [{'user': {'client-key': '/key', 'client-certificate': os.path.join(os.path.dirname(host), 'cert')}}]}))
    else:
        if body == host and fail == 'missing-context': sys.exit(1)
        foreign = body == host and fail in ('foreign-context', 'stopped-foreign')
        print('https://other-cluster:6443' if foreign else 'https://localhost:54321')
if name == 'minikube' and a[-1:] == ['status']:
    if fail == 'stale-kubeconfig': sys.exit(4)
    sys.exit(0 if fail in ('running-profile', 'foreign-context', 'missing-context') or
             (os.environ['ACTION'] in ('status', 'doctor') and fail != 'stopped-profile') else 1)
if name == 'docker' and 'inspect' in a:
    if fail in ('missing-image', 'cached-image'): sys.exit(1)
    if a[-1] in infra and fail.startswith('infra-') and fail != 'infra-rebuilt': sys.exit(1)
    changed = fail == 'rebuilt-gateway' and a[-1].startswith('hudi-lakehouse-agent-gateway:')
    changed = changed or (fail == 'infra-rebuilt' and a[-1] == infra[0])
    print(json.dumps([{'Id': 'sha256:rebuilt-id' if changed else 'sha256:local-id'}]))
if name == 'minikube' and 'ls' in a and '--format=json' in a:
    entries = []
    if fail in ('cached-image', 'unchanged-image', 'rebuilt-gateway'):
        entries = [{'id': 'local-id', 'repoTags': ['docker.io/library/' + image]} for image in (
            'hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0', 'hudi-lakehouse-agent-gateway:0.1.0')]
    if fail in ('infra-warm', 'infra-tracked', 'infra-rebuilt'):
        entries = [{'id': 'local-id', 'repoTags': [image]} for image in infra]
    print(json.dumps(entries))
if name == 'helm' and 'template' in a and fail == 'real-preflight':
    os.execv(os.environ['REAL_HELM'], [os.environ['REAL_HELM'], *a])
if name == 'kubectl' and 'create' in a and 'configmap' in a:
    data = {}
    for flag in a:
        if flag.startswith('--from-file='):
            _, key, path = flag.split('=', 2)
            with open(path) as source:
                data[key] = source.read()
            if fail == 'change-' + key:
                data[key] += ' '
    print(json.dumps({'kind': 'ConfigMap', 'data': data}))
if name == 'kubectl' and 'get' in a and 'service' in a:
    if a[-1] == 'jsonpath={.spec.ports[?(@.name=="http")].port}':
        print(8080)
if name == 'kubectl' and 'api-resources' in a:
    if fail == 'discovery': sys.exit(47)
    print('sparkapplications.spark.apache.org')
if name == 'kubectl' and 'get' in a and 'namespace' in a: print('Active')
if name == 'kubectl' and 'get' in a and 'deployment' in a and '-o' in a:
    if 'local-image-id' in a[-1] and fail == 'infra-tracked': print('local-id')
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
if name == 'kubectl' and '--for=delete' in a and fail == 'pod-timeout': sys.exit(1)
''')
            fake.chmod(0o755)
            for name in ('docker', 'minikube', 'helm', 'kubectl'):
                (temp / name).symlink_to(fake)
            env = {k: v for k, v in os.environ.items() if not k.startswith((
                'HUDI_', 'SPARK_CONNECT_', 'GATEWAY_', 'MINIO_', 'HMS_', 'LOCAL_DEV_', 'INSTALL_'))}
            env.update(PATH=str(temp) + os.pathsep + env['PATH'], CALLS=str(temp / 'calls'),
                       REAL_HELM=shutil.which('helm') or '', FAILURE=failure, BACKEND=backend, ACTION=args[0] if args else '',
                       KUBECONFIG=str(temp / 'host-config'), HOST_CONFIG=str(temp / 'host-config'),
                       PROFILE_CONFIG=str(temp / 'verified-path'))
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
        with patch.dict(os.environ, HELM_KUBEAPISERVER='https://other-cluster:6443'):
            result, calls = self.invoke('up', '--profile', 'isolated', failure='assert-private-config')
        self.assertEqual(result.returncode, 0, result.stderr)
        start = next(a for name, a, _ in calls if name == 'minikube' and 'start' in a)
        self.assertIn('--keep-context', start)
        self.assertNotIn('Next: kubectl -n', result.stdout)
        for name, args, _ in calls:
            if name == 'kubectl':
                self.assertEqual(args[:2], ['--context', 'isolated'])
            if name == 'helm' and 'upgrade' in args:
                self.assertEqual(args[:2], ['--kube-context', 'isolated'])
                self.assertNotIn('hudi-trino', args)
                self.assertNotIn('spark-kubernetes-operator', args)
        self.assertFalse(any('build' in a for _, a, _ in calls))
        scripts = [body for name, a, body in calls if name == 'kubectl' and 'exec' in a]
        self.assertIn('Probe SQL and HMS', scripts[0])
        self.assertIn('Initialize missing demo tables', scripts[1])
        self.assertIn('real MCP ->', scripts[2])
        self.assertIn('Check chat readiness', scripts[3])
        self.assertIn('Verify actual built-in chat', scripts[4])

    def test_catalog_wait_retries_only_the_read_only_probe(self):
        installer = load('installer', LOCAL / 'scripts/install.py')
        installation = installer.Installation(Mock(profile='local'))
        error = subprocess.CalledProcessError(1, 'kubectl')
        with patch.object(installer.time, 'sleep'), patch.object(installation, 'exec_script') as probe:
            probe.side_effect = [subprocess.TimeoutExpired('kubectl', 35), error, None]
            installation.wait_for_catalog()
            self.assertEqual(probe.call_count, 3)
            probe.reset_mock()
            probe.side_effect = error
            with self.assertRaisesRegex(ValueError, 'before seeding'):
                installation.wait_for_catalog()
            self.assertEqual(probe.call_count, 6)
            for call in probe.call_args_list:
                self.assertEqual(call.args[1].name, 'check-connect.py')

    def test_gateway_preflight_rejects_invalid_spark_overlay_before_apply(self):
        with tempfile.TemporaryDirectory() as directory:
            values = Path(directory) / 'gateway overrides.yaml'
            values.write_text('sparkConnect: null\n')
            result, calls = self.invoke('up', '--gateway-values', str(values), failure='real-preflight')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('sparkConnect', result.stderr)
        self.assertFalse(any('apply' in args or 'upgrade' in args for _, args, _ in calls))

    def test_hms_rollout_annotation_tracks_both_configmap_inputs(self):
        def checksum(scenario):
            result, calls = self.invoke('up', failure=scenario)
            self.assertEqual(result.returncode, 0, result.stderr)
            body = next(body for name, args, body in calls
                        if name == 'kubectl' and 'apply' in args and 'hudi.apache.org/bootstrap-checksum' in body)
            deploy = next(doc for doc in yaml.safe_load_all(body) if doc['kind'] == 'Deployment')
            return deploy['spec']['template']['metadata']['annotations']['hudi.apache.org/bootstrap-checksum']

        original = checksum('')
        self.assertEqual(original, checksum(''))
        for key in ('hive-site.xml', 'start-metastore.sh'):
            self.assertNotEqual(original, checksum('change-' + key))

    def test_running_profile_is_reused_without_restarting_minikube(self):
        for scenario in ('running-profile', 'stale-kubeconfig'):
            with self.subTest(scenario=scenario):
                result, calls = self.invoke('up', failure=scenario)
                self.assertEqual(result.returncode, 0, result.stderr)
                commands = [args for name, args, _ in calls if name == 'minikube']
                self.assertTrue(any('update-context' in args for args in commands))
                self.assertFalse(any('start' in args for args in commands))

    def test_up_rejects_foreign_or_missing_context_before_install(self):
        for failure in ('foreign-context', 'missing-context'):
            with self.subTest(failure=failure):
                result, calls = self.invoke('up', '--build', '--profile', 'staging', failure=failure)
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(all(name == 'minikube' or (name == 'kubectl' and 'view' in args)
                                    for name, args, _ in calls))
                self.assertFalse(any('image' in args for _, args, _ in calls))
                if failure == 'foreign-context':
                    self.assertIn("context 'staging' does not point", result.stderr)

    def test_start_preserves_host_contexts_and_registers_only_new_names(self):
        for failure in ('stopped-foreign', 'occupied-user', 'new-profile'):
            with self.subTest(failure=failure):
                result, calls = self.invoke('up', '--profile', 'staging', failure=failure)
                self.assertTrue(any(name == 'minikube' and 'start' in a for name, a, _ in calls))
                writes = [a[a.index('config') + 1] for name, a, _ in calls
                          if name == 'kubectl' and 'config' in a and 'view' not in a]
                if failure == 'new-profile':
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(writes, ['set-cluster', 'set-credentials', 'set-context'])
                else:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(writes, [])
                    self.assertFalse(any('apply' in a or 'upgrade' in a for _, a, _ in calls))

    def test_workload_commands_use_verified_profile(self):
        for action in ('forward', 'chat', 'test', 'status', 'doctor'):
            with self.subTest(action=action):
                result, calls = self.invoke(action, failure='foreign-context')
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(all('view' in a for name, a, _ in calls if name == 'kubectl'))
                result, calls = self.invoke(action, failure='assert-private-config')
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertTrue(any(name == 'kubectl' and 'config' not in a for name, a, _ in calls))

    def test_stopped_profile_diagnostics_skip_workload_queries(self):
        for action in ('status', 'doctor'):
            result, calls = self.invoke(action, failure='stopped-profile')
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual([(name, a) for name, a, _ in calls], [('minikube', ['-p', 'hudi-lakehouse', 'status'])])

    def test_stop_uses_private_config_even_for_an_unavailable_profile(self):
        result, calls = self.invoke('stop', failure='unavailable-profile')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([a[-1] for _, a, _ in calls], ['update-context', 'stop'])
        name, args, config = calls[-1]
        self.assertEqual((name, args), ('minikube', ['-p', 'hudi-lakehouse', 'stop']))
        self.assertNotEqual(Path(config).name, 'host-config')

    @unittest.skipUnless(shutil.which('kubectl'), 'requires kubectl for local kubeconfig edits')
    def test_refresh_only_the_previously_registered_endpoint(self):
        installer = load('installer', LOCAL / 'scripts/install.py')
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            profile = 'staging'
            installation = installer.Installation(Mock(profile=profile))
            installation.env['KUBECONFIG'] = str(temp / 'host')
            last_server = temp / 'last-server'
            last_server.write_text('https://localhost:50001')
            user = {'client-certificate': str(temp / 'client.crt'), 'client-key': str(temp / 'client.key')}
            cluster = {'certificate-authority': str(temp / 'ca.crt'), 'server': 'https://localhost:50002'}
            for address in (last_server.read_text(), 'https://localhost:50003'):
                with self.subTest(address=address):
                    config = {'apiVersion': 'v1', 'kind': 'Config', 'current-context': 'other',
                              'contexts': [{'name': name, 'context': {'cluster': name, 'user': name}}
                                           for name in ('other', profile)],
                              'users': [{'name': profile, 'user': user}],
                              'clusters': [{'name': profile, 'cluster': dict(cluster, server=address)}]}
                    host = temp / 'host'
                    host.write_text(json.dumps(config))
                    before = host.read_bytes()
                    installation.sync_profile_context(cluster, user, last_server, create=True)
                    if address == last_server.read_text():
                        view = json.loads(installation.kubectl('config', 'view', '-o', 'json', capture=True).stdout)
                        self.assertEqual(view['clusters'][0]['cluster']['server'], cluster['server'])
                        self.assertEqual(view['current-context'], 'other')
                    else:
                        self.assertEqual(host.read_bytes(), before)

    @unittest.skipUnless(shutil.which('kubectl'), 'requires kubectl for local kubeconfig edits')
    def test_running_profile_completes_partial_registration(self):
        installer = load('installer', LOCAL / 'scripts/install.py')
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            profile = 'staging'
            installation = installer.Installation(Mock(profile=profile))
            host = temp / 'host'
            cluster_dir = temp / 'clusters'
            cluster_dir.mkdir()
            cluster_file = cluster_dir / 'config'
            installation.env['KUBECONFIG'] = os.pathsep.join(map(str, (host, cluster_file)))
            user = {'client-certificate': str(temp / 'client.crt'), 'client-key': str(temp / 'client.key')}
            cluster = {'certificate-authority': str(cluster_dir / 'ca.crt'), 'server': 'https://localhost:50001'}
            for path in (*user.values(), cluster['certificate-authority']):
                Path(path).touch()
            cluster_file.write_text(json.dumps({
                'apiVersion': 'v1', 'kind': 'Config', 'clusters': [
                    {'name': profile, 'cluster': {**cluster, 'certificate-authority': 'ca.crt'}}],
            }))
            relative_user = {key: Path(value).name for key, value in user.items()}
            for registered_user in (None, relative_user, {'token': 'other'}):
                with self.subTest(registered_user=registered_user):
                    host.write_text(json.dumps({
                        'apiVersion': 'v1', 'kind': 'Config', 'current-context': 'other',
                        'contexts': [{'name': 'other', 'context': {'cluster': 'other', 'user': 'other'}}],
                        'users': [{'name': profile, 'user': registered_user}] if registered_user else [],
                    }))
                    if registered_user and registered_user != relative_user:
                        before = host.read_bytes()
                        with self.assertRaisesRegex(ValueError, 'already exists'):
                            installation.sync_profile_context(cluster, user, temp / 'last', create=False)
                        self.assertEqual(host.read_bytes(), before)
                        continue
                    installation.sync_profile_context(cluster, user, temp / 'last', create=False)
                    view = json.loads(installation.kubectl('config', 'view', '--minify', '-o', 'json', capture=True).stdout)
                    self.assertEqual(view['contexts'][0]['context'], {'cluster': profile, 'user': profile})
                    self.assertEqual(view['users'][0]['user'], relative_user)
                    self.assertEqual(installation.kubectl('config', 'current-context', capture=True).stdout.strip(), 'other')

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

    def test_rebuilt_gateway_changes_only_its_pod_annotation(self):
        def reconcile(scenario):
            result, calls = self.invoke('up', failure=scenario)
            self.assertEqual(result.returncode, 0, result.stderr)
            annotations = {a[a.index('--install') + 1]: json.loads(body)['podAnnotations']
                           for name, a, body in calls if name == 'helm' and 'upgrade' in a}
            loads = [a[-1] for name, a, _ in calls if name == 'minikube' and 'load' in a
                     and a[-1].startswith('hudi-lakehouse-')]
            return annotations, loads

        before, _ = reconcile('unchanged-image')
        after, loads = reconcile('rebuilt-gateway')
        self.assertEqual(loads, ['hudi-lakehouse-agent-gateway:0.1.0'])
        self.assertEqual(before['hudi-spark-connect'], after['hudi-spark-connect'])
        self.assertEqual(before['hudi-agent-gateway'], {'hudi.apache.org/local-image-id': 'local-id'})
        self.assertEqual(after['hudi-agent-gateway'], {'hudi.apache.org/local-image-id': 'rebuilt-id'})

    def test_infrastructure_rollouts_follow_local_changes_not_kubelet_pulls(self):
        def reconcile(scenario):
            result, calls = self.invoke('up', failure=scenario)
            self.assertEqual(result.returncode, 0, result.stderr)
            templates = {}
            for name, args, body in calls:
                if name == 'kubectl' and 'apply' in args and 'kind: Deployment' in body:
                    deploy = next(doc for doc in yaml.safe_load_all(body) if doc['kind'] == 'Deployment')
                    templates[deploy['metadata']['name']] = deploy['spec']['template']
            self.assertEqual(set(templates), {'minio', 'hive-metastore'})
            return templates

        # After the first Kubernetes pull, an identical up must apply identical Pod templates.
        annotation = 'hudi.apache.org/local-image-id'
        cold = reconcile('infra-cold')
        for template in cold.values():
            self.assertEqual(template['metadata']['annotations'][annotation], '')
        self.assertEqual(cold, reconcile('infra-warm'))
        # Previously tracked images retain their IDs even if the host copy was removed.
        tracked = reconcile('infra-tracked')
        rebuilt = reconcile('infra-rebuilt')
        self.assertEqual(tracked['hive-metastore'], rebuilt['hive-metastore'])
        self.assertEqual(tracked['minio']['metadata']['annotations'][annotation], 'local-id')
        self.assertEqual(rebuilt['minio']['metadata']['annotations'][annotation], 'rebuilt-id')

    def test_build_loads_custom_tags(self):
        result, calls = self.invoke('up', '--build', '--connect-image', 'local/connect:dev',
                                    '--gateway-image', 'local/gateway:dev')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any(a[:2] == ['tag', 'hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0']
                            for name, a, _ in calls if name == 'docker'))
        self.assertTrue(any(a[-1] == 'local/connect:dev' for name, a, _ in calls
                            if name == 'minikube' and 'load' in a))

    def test_registry_mode_never_builds_or_loads_images(self):
        result, calls = self.invoke('up', '--pull-images', '--connect-image', 'localhost:5000/connect:dev',
                                    '--gateway-image', 'example.com/gateway:dev')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(name == 'docker' or 'load' in args for name, args, _ in calls))

    def test_registry_digest_is_used_by_both_charts_and_spark_executors(self):
        digest = 'sha256:' + 'ab' * 32
        result, calls = self.invoke('up', '--pull-images', '--connect-image', 'localhost:5000/connect@' + digest,
                                    '--gateway-image', 'example.com/gateway@' + digest)
        self.assertEqual(result.returncode, 0, result.stderr)
        for name, args, body in calls:
            if name != 'helm' or 'upgrade' not in args:
                continue
            release = args[args.index('--install') + 1]
            chart = ROOT / 'charts' / release
            extra = []
            if release == 'hudi-spark-connect':
                extra = ['--set', 'spark.master=k8s://https://kubernetes.default.svc',
                         '--set', 'serviceAccount.name=connect,serviceAccount.automountToken=true']
                expected = 'localhost:5000/connect@' + digest
            else:
                expected = 'example.com/gateway@' + digest
            manifests = subprocess.check_output(['helm', 'template', release, chart, '-f',
                                                 chart / 'values-local-dev.yaml', '-f', '-', *extra],
                                                text=True, input=body)
            deploy = next(d for d in yaml.safe_load_all(manifests) if d and d['kind'] == 'Deployment')
            self.assertEqual(deploy['spec']['template']['spec']['containers'][0]['image'], expected)
            if extra:
                self.assertRegex(manifests, r'spark\.kubernetes\.executor\.container\.image\s+' + expected)

        for flags, image in ((['--pull-images'], 'connect@sha256:bad'), ([], 'connect@' + digest)):
            result, calls = self.invoke('up', *flags, '--connect-image', image)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(calls, [])

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
        self.assertIn('http://hudi-agent-gateway.hudi-lakehouse.svc:8080', scripts[1][0])

    def test_down_preserves_namespace_pvcs_and_releases_and_propagates_failures(self):
        result, calls = self.invoke('down')
        self.assertEqual(result.returncode, 0, result.stderr)
        _, profile_command, private_config = calls[0]
        self.assertEqual(profile_command, ['-p', 'hudi-lakehouse', 'update-context'])
        self.assertNotEqual(Path(private_config).name, 'host-config')
        self.assertTrue(all(config == private_config for name, args, config in calls
                            if name == 'kubectl' and 'view' not in args))
        self.assertTrue(any('scale' in a for _, a, _ in calls))
        for _, args, _ in calls:
            self.assertNotIn('uninstall', args)
            if 'delete' in args:
                self.assertTrue('job' in args or 'sparkapplications.spark.apache.org' in args)
        operations = [a for _, a, _ in calls]
        self.assertLess(next(i for i, a in enumerate(operations) if 'sparkapplications.spark.apache.org' in a),
                        next(i for i, a in enumerate(operations) if 'scale' in a))
        result, calls = self.invoke('down', failure='discovery')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any('scale' in a for _, a, _ in calls))
        wait = next(a for a in operations if '--for=delete' in a)
        self.assertIn('pod', wait)
        self.assertEqual(wait[wait.index('-l') + 1], 'app.kubernetes.io/name in (minio,hive-metastore)')
        for storage in ('minio', 'hive-metastore'):
            scale = next(a for a in operations if 'scale' in a and storage in a)
            self.assertLess(operations.index(scale), operations.index(wait))
        for failure in ('scale', 'pod-timeout'):
            result, _ = self.invoke('down', failure=failure)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn('Workloads stopped', result.stdout)

    def test_down_rejects_unavailable_profile_or_wrong_context_before_cleanup(self):
        for failure in ('unavailable-profile', 'missing-context', 'foreign-context'):
            with self.subTest(failure=failure):
                result, calls = self.invoke('down', '--profile', 'staging', failure=failure)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls[0][1], ['-p', 'staging', 'update-context'])
                self.assertTrue(all('view' in args for name, args, _ in calls if name == 'kubectl'))
                if failure == 'foreign-context':
                    self.assertIn("context 'staging' does not point", result.stderr)

    def test_forward_is_localhost_only_and_foreground(self):
        result, calls = self.invoke('forward', '--port', '18000')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls[-1][1][-5:], ['port-forward', '--address', '127.0.0.1',
                                          'svc/hudi-agent-gateway', '18000:http'])


class ChartAndDataTest(unittest.TestCase):
    def test_numeric_image_tags(self):
        for name in ('hudi-agent-gateway', 'hudi-spark-connect'):
            chart = ROOT / 'charts' / name
            rendered = subprocess.check_output(['helm', 'template', 'test', str(chart), '-f',
                                                str(chart / 'values-local-dev.yaml'), '--set', 'image.tag=12345'],
                                               text=True)
            deploy = next(d for d in yaml.safe_load_all(rendered) if d and d['kind'] == 'Deployment')
            self.assertTrue(deploy['spec']['template']['spec']['containers'][0]['image'].endswith(':12345'))

    def test_gateway_local_readiness_keeps_mcp_routable_without_llm(self):
        chart = ROOT / 'charts/hudi-agent-gateway'
        for extra, expected in (([], '/ready'), (['--set', 'readinessProbePath=null'], '/ready'),
                                (['--set', 'readinessProbePath=/health'], '/health')):
            result = subprocess.run(['helm', 'template', 'test', str(chart), '-f',
                                     str(chart / 'values-local-dev.yaml'), '-f',
                                     str(chart / 'values-spark-connect.yaml'), *extra],
                                    text=True, capture_output=True, check=True)
            deployment = next(d for d in yaml.safe_load_all(result.stdout) if d and d['kind'] == 'Deployment')
            container = deployment['spec']['template']['spec']['containers'][0]
            self.assertEqual(container['readinessProbe']['httpGet']['path'], expected)
        result = subprocess.run(['helm', 'template', 'test', str(chart), '--set', 'readinessProbePath=/typo'],
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
