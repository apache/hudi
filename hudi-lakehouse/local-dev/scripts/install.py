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

"""Local minikube installation; Python standard library only on the host."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent
NS = 'hudi-lakehouse'
CONNECT_IMAGE = 'hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0'
GATEWAY_IMAGE = 'hudi-lakehouse-agent-gateway:0.1.0'


def run(args, *, capture=False, check=True, **kwargs):
    return subprocess.run([str(a) for a in args], text=True, check=check,
                          stdout=subprocess.PIPE if capture else None, **kwargs)


def image_values(image, policy):
    if '@' in image or not re.fullmatch(r'[\w./:-]+:[\w.-]+', image) or ':' not in image.rsplit('/', 1)[-1]:
        raise ValueError('Application images require an explicit repository:tag: ' + image)
    repository, tag = image.rsplit(':', 1)
    return {'repository': repository, 'tag': tag, 'pullPolicy': policy}


class Installation:
    def __init__(self, args):
        self.args = args
        self.profile = args.profile
        self.kube = ['kubectl', '--context', self.profile, '-n', NS]
        self.mini = ['minikube', '-p', self.profile]
        self.env = dict(os.environ, HUDI_KUBE_CONTEXT=self.profile)

    def kubectl(self, *args, **kwargs):
        return run([*self.kube, *args], **kwargs)

    def exec_script(self, deployment, script, *args, check=True):
        return self.kubectl('exec', '-i', 'deploy/' + deployment, '--', 'python3', '-', *args,
                            input=script.read_text(), check=check)

    def assert_gateway_backend(self):
        # Do not repoint an existing Trino gateway when selecting an old profile.
        result = self.kubectl('get', 'deployment', 'hudi-agent-gateway',
                              '--ignore-not-found', '-o', 'json', capture=True)
        if result.stdout.strip():
            containers = json.loads(result.stdout)['spec']['template']['spec']['containers']
            env = {e['name']: e.get('value') for c in containers for e in c.get('env', [])}
            if env.get('GATEWAY_SQL_BACKEND') != 'spark-connect':
                raise ValueError('Existing gateway does not use Spark Connect. Use a different --profile to preserve it.')

    def images(self):
        args = self.args
        if args.build:
            run([ROOT / 'scripts/build-connect-image.sh'])
            if args.connect_image != CONNECT_IMAGE:
                run(['docker', 'tag', CONNECT_IMAGE, args.connect_image])
            run(['docker', 'build', '-t', args.gateway_image, ROOT.parent / 'hudi-agent-gateway'])
        if args.pull_images:
            print('Using registry application images (IfNotPresent)', flush=True)
            return
        cached = json.loads(run([*self.mini, 'image', 'ls', '--format=json'], capture=True).stdout)

        def normalize(name):
            return re.sub(r'^(docker.io/)?library/|^docker.io/', '', name)

        def load_image(image, required):
            entries = [entry for entry in cached if any(normalize(tag) == normalize(image)
                                                        for tag in entry.get('repoTags', []))]
            local = run(['docker', 'image', 'inspect', image], capture=True,
                        stderr=subprocess.DEVNULL, check=False)
            if local.returncode == 0:
                identity = json.loads(local.stdout)[0]['Id'].split(':')[-1]
                if any(entry['id'].split(':')[-1] == identity for entry in entries):
                    print(f'Reusing unchanged cluster image {image}', flush=True)
                    return
                print(f'Loading local image {image}', flush=True)
                run([*self.mini, 'image', 'load', '--daemon', '--overwrite=true', image])
            elif entries:
                print(f'Reusing cluster image {image}', flush=True)
            elif required:
                raise ValueError(f'Image {image} is absent locally and in this profile. '
                                 'Use up --build, or explicit registry images with --pull-images. '
                                 'Default application tags are not published releases.')

        for image in (args.connect_image, args.gateway_image):
            load_image(image, required=True)
        # Infrastructure can be pulled by Kubernetes if not present locally.
        for image in (args.minio_image, args.mc_image, args.hms_image):
            load_image(image, required=False)

    def up(self):
        args = self.args
        # Validate files/images before starting any workloads.
        policy = 'IfNotPresent' if args.pull_images else 'Never'
        connect = {'image': image_values(args.connect_image, policy)}
        gateway = {'image': image_values(args.gateway_image, policy)}
        for path in (args.connect_values, args.gateway_values):
            if path and not Path(path).is_file():
                raise ValueError('Values file does not exist: ' + path)
        status = run([*self.mini, 'status'], capture=True, check=False, stderr=subprocess.DEVNULL)
        if status.returncode:
            run([*self.mini, 'start', '--driver', args.driver, '--cpus', args.cpus,
                 '--memory', args.memory, '--keep-context'])
        else:
            print(f'Reusing running minikube profile {self.profile}', flush=True)
        self.assert_gateway_backend()
        self.images()
        with tempfile.TemporaryDirectory(prefix='hudi-minikube-') as directory:
            temp = Path(directory)
            for name, values in (('connect', connect), ('gateway', gateway)):
                # Helm merges the user's file before installer-owned image selection.
                source = getattr(args, name + '_values')
                files = ['-f', str(Path(source).resolve())] if source else []
                image_file = temp / (name + '-image.json')
                image_file.write_text(json.dumps(values))
                chart = ROOT / 'charts' / ('hudi-spark-connect' if name == 'connect' else 'hudi-agent-gateway')
                image_file = temp / (name + '-image.json')
                image_file.write_text(json.dumps(values))
                chart = ROOT / 'charts' / ('hudi-spark-connect' if name == 'connect' else 'hudi-agent-gateway')
                # Reject malformed values before applying Kubernetes resources.
                run(['helm', 'template', 'preflight', chart,
                     '-f', chart / 'values-local-dev.yaml', *files, '-f', image_file], capture=True)
                prefix = 'SPARK_CONNECT' if name == 'connect' else 'GATEWAY'
                self.env[prefix + '_VALUES'] = str(Path(source).resolve()) if source else ''
                self.env[prefix + '_IMAGE_VALUES'] = str(image_file)
            manifests = temp / 'manifests'
            shutil.copytree(HERE / 'manifests', manifests)
            for name, old, new in (
                ('minio.yaml', 'cgr.dev/chainguard/minio:latest', args.minio_image),
                ('minio-bucket-job.yaml', 'cgr.dev/chainguard/minio-client:latest-dev', args.mc_image),
                ('hive-metastore.yaml', 'apache/hive:3.1.3', args.hms_image),
            ):
                if not re.fullmatch(r'[\w./:@-]+', new):
                    raise ValueError('Invalid infrastructure image: ' + new)
                path = manifests / name
                path.write_text(path.read_text().replace('image: ' + old,
                                                         'image: ' + new))
            self.env['LOCAL_DEV_MANIFEST_DIR'] = str(manifests)
            run(['bash', HERE / 'scripts/up.sh', '--spark-connect', '--with-agent-gateway'], env=self.env)
        if args.build:
            for deployment in ('hudi-spark-connect', 'hudi-agent-gateway'):
                self.kubectl('rollout', 'restart', 'deploy/' + deployment)
                self.kubectl('rollout', 'status', 'deploy/' + deployment, '--timeout=600s')
        print('Initializing only missing sample tables', flush=True)
        self.exec_script('hudi-spark-connect', ROOT / 'docker/scripts/seed.py',
                         '--connect-uri', 'sc://127.0.0.1:15002')
        self.verify(strict=False)
        self.chat(required=False)
        self.endpoints()

    def verify(self, strict):
        if strict:
            self.exec_script('hudi-spark-connect', ROOT / 'docker/scripts/seed.py',
                             '--connect-uri', 'sc://127.0.0.1:15002', '--verify-only')
        else:
            self.exec_script('hudi-spark-connect', ROOT / 'docker/scripts/check-connect.py')
        try:
            self.exec_script('hudi-agent-gateway', ROOT / 'docker/scripts/smoke-test.py',
                             '--gateway-url', f'http://hudi-agent-gateway.{NS}.svc:8000',
                             *(['--verify-sample'] if strict else []))
        except subprocess.CalledProcessError:
            print('MCP verification FAILED; this is independent of LLM availability.', file=sys.stderr)
            raise
        print('PASS: Spark Connect and MCP data path.', flush=True)

    def chat(self, required):
        ready = self.exec_script('hudi-agent-gateway', ROOT / 'docker/scripts/check-llm.py', check=False)
        if ready.returncode:
            print('Chat unavailable: LLM readiness/model check failed. MCP is independent.', file=sys.stderr)
            if required:
                raise ValueError('Chat check failed.')
            return
        result = self.exec_script('hudi-agent-gateway', HERE / 'scripts/check-chat.py', check=False)
        if result.returncode:
            print('Chat inference/tool verification failed; inspect provider/model and gateway logs. '
                  'This does not invalidate the preceding MCP check.', file=sys.stderr)
            if required:
                raise ValueError('Chat check failed.')

    def endpoints(self):
        print(f'''Run in a separate terminal (localhost only; Ctrl-C closes the tunnel):
  {HERE / 'lakehouse.sh'} forward --profile {self.profile}
Chat UI: http://127.0.0.1:8000/ui/
MCP: http://127.0.0.1:8000/mcp/
Spark Connect: kubectl --context {self.profile} -n {NS} port-forward --address 127.0.0.1 svc/hudi-spark-connect 15002:15002
MinIO console: kubectl --context {self.profile} -n {NS} port-forward --address 127.0.0.1 svc/minio 9001:9001''', flush=True)

    def inspect(self, diagnostics=False):
        run([*self.mini, 'status'], check=False)
        self.kubectl('get', 'deploy,pods,svc,pvc,jobs', '-o', 'wide')
        if diagnostics:
            self.kubectl('get', 'events', '--sort-by=.lastTimestamp', check=False)
            for deployment in ('minio', 'hive-metastore', 'hudi-spark-connect', 'hudi-agent-gateway'):
                self.kubectl('logs', 'deploy/' + deployment, '--tail=80', check=False)
            self.exec_script('hudi-agent-gateway', ROOT / 'docker/scripts/check-llm.py', check=False)
        self.endpoints()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, epilog=(
        'Default application images are local build tags. down preserves PVCs; '
        'stop preserves the profile. No command deletes data. See local-dev/INSTALL.md.'))
    parser.add_argument('action', nargs='?', choices=('up', 'status', 'doctor', 'down', 'stop', 'test', 'chat', 'forward'))
    parser.add_argument('--profile', default=os.environ.get('HUDI_MINIKUBE_PROFILE', 'hudi-lakehouse'))
    parser.add_argument('--build', action='store_true', help='build application images from this checkout (up only)')
    parser.add_argument('--pull-images', action='store_true', help='use registry images instead of loading Docker images (up only)')
    parser.add_argument('--connect-image', default=os.environ.get('SPARK_CONNECT_IMAGE', CONNECT_IMAGE))
    parser.add_argument('--gateway-image', default=os.environ.get('GATEWAY_IMAGE', GATEWAY_IMAGE))
    parser.add_argument('--minio-image', default=os.environ.get('MINIO_IMAGE', 'cgr.dev/chainguard/minio:latest'))
    parser.add_argument('--mc-image', default=os.environ.get('MINIO_CLIENT_IMAGE', 'cgr.dev/chainguard/minio-client:latest-dev'))
    parser.add_argument('--hms-image', default=os.environ.get('HMS_IMAGE', 'apache/hive:3.1.3'))
    parser.add_argument('--connect-values', help='additional Spark Connect Helm values')
    parser.add_argument('--gateway-values', help='additional Gateway Helm values, including LLM configuration')
    parser.add_argument('--driver', default='docker')
    parser.add_argument('--cpus', default='4')
    parser.add_argument('--memory', default='8192')
    parser.add_argument('--port', type=int, default=8000, help='localhost Gateway port for forward')
    args = parser.parse_args(argv)
    if not args.action:
        parser.print_help()
        return 0
    if not re.fullmatch(r'[a-z][a-z0-9-]*', args.profile):
        parser.error('profile must start with a lowercase letter and contain only lowercase letters, digits and hyphens')
    if (args.build or args.pull_images) and args.action != 'up':
        parser.error('--build and --pull-images require up')
    if args.build and args.pull_images:
        parser.error('--build and --pull-images are mutually exclusive')
    if not 1 <= args.port <= 65535:
        parser.error('--port must be between 1 and 65535')
    needed = ['minikube', 'kubectl', 'helm']
    if args.action == 'up' and not args.pull_images:
        needed.append('docker')
    missing = [tool for tool in needed if not shutil.which(tool)]
    if missing:
        print('Install required tools: ' + ', '.join(missing), file=sys.stderr)
        return 1
    installation = Installation(args)
    try:
        if args.action == 'up':
            installation.up()
        elif args.action in ('status', 'doctor'):
            installation.inspect(diagnostics=args.action == 'doctor')
        elif args.action == 'down':
            run(['bash', HERE / 'scripts/down.sh'], env=installation.env)
        elif args.action == 'stop':
            run([*installation.mini, 'stop'])
        elif args.action == 'test':
            installation.verify(strict=True)
        elif args.action == 'chat':
            installation.verify(strict=False)
            installation.chat(required=True)
        elif args.action == 'forward':
            installation.kubectl('port-forward', '--address', '127.0.0.1',
                                 'svc/hudi-agent-gateway', f'{args.port}:8000')
        return 0
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f'FAILED: {error}\nInspect with lakehouse.sh doctor --profile {args.profile}. '
              'No automatic cleanup or data deletion was performed.', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    sys.exit(main())
