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

"""Execute the installer readiness probe against controlled provider responses."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error

SCRIPT = Path(__file__).resolve().parents[1] / 'docker/scripts/check-llm.py'
spec = importlib.util.spec_from_file_location('docker_check_llm', SCRIPT)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class ChatReadinessTest(unittest.TestCase):
    def run_probe(self, *, provider='ollama', model='demo:latest',
                  provider_error=None, ready=True, key=''):
        requests = []

        def urlopen(request, timeout):
            requests.append(request)
            url = request.full_url
            if url.endswith('/ready'):
                payload = {'ready': ready}
                if not ready:
                    raise urllib.error.HTTPError(url, 503, 'Not ready', {},
                                                 io.BytesIO(json.dumps(payload).encode()))
            elif url.endswith('/v1/info'):
                payload = {'provider': provider, 'model': model}
            elif url == 'http://provider/api/tags' or url == 'http://provider/v1/models':
                if provider_error:
                    raise provider_error
                payload = ({'models': [{'name': 'demo:latest'}]} if provider == 'ollama'
                           else {'data': [{'id': 'demo:latest'}]})
            else:
                raise AssertionError('Unexpected request (UI fallback must not be used): ' + url)
            return io.BytesIO(json.dumps(payload).encode())

        output, errors = io.StringIO(), io.StringIO()
        with patch.dict(probe.os.environ, {
            'GATEWAY_OLLAMA_BASE_URL': 'http://provider/',
            'GATEWAY_OPENAI_BASE_URL': 'http://provider/v1/', 'OPENAI_API_KEY': key,
        }, clear=True), patch.object(probe.urllib.request, 'urlopen', side_effect=urlopen), \
                contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = probe.main()
        return code, requests, output.getvalue() + errors.getvalue()

    def test_available_models_and_discovery_credentials(self):
        for provider, model, key in (
            ('ollama', 'demo:latest', ''), ('ollama', 'demo', ''),
            ('openai-compatible', 'demo:latest', ''),
            ('openai-compatible', 'demo:latest', 'test-secret'),
        ):
            with self.subTest(provider=provider, model=model, authenticated=bool(key)):
                code, requests, output = self.run_probe(provider=provider, model=model, key=key)
                self.assertEqual(code, 0, output)
                endpoint = '/api/tags' if provider == 'ollama' else '/v1/models'
                self.assertEqual(requests[-1].full_url, 'http://provider' + endpoint)
                self.assertEqual(requests[-1].get_header('Authorization'),
                                 'Bearer ' + key if key else None)
                self.assertNotIn('test-secret', output)

    def test_missing_model_fails_for_both_local_providers(self):
        for provider in ('ollama', 'openai-compatible'):
            with self.subTest(provider=provider):
                code, _, output = self.run_probe(provider=provider, model='not-installed')
                self.assertEqual(code, 1)
                self.assertIn('Configured model is not available', output)

    def test_provider_errors_fail_instead_of_falling_back(self):
        for provider in ('ollama', 'openai-compatible'):
            for error in (urllib.error.URLError('unreachable'),
                          urllib.error.HTTPError('http://provider', 401, 'Unauthorized', {}, None),
                          urllib.error.HTTPError('http://provider', 500, 'Unavailable', {}, None),
                          ValueError('invalid JSON')):
                with self.subTest(provider=provider, error=str(error)):
                    code, _, output = self.run_probe(provider=provider, provider_error=error)
                    self.assertEqual(code, 1)
                    self.assertIn('Chat readiness failed', output)

    def test_unready_gateway_stops_before_model_discovery(self):
        code, requests, output = self.run_probe(ready=False)
        self.assertEqual(code, 1)
        self.assertEqual(len(requests), 1)
        self.assertIn('Gateway dependencies are not ready', output)

    def test_hosted_providers_do_not_make_inference_or_discovery_requests(self):
        for provider in ('openai', 'anthropic'):
            with self.subTest(provider=provider):
                code, requests, output = self.run_probe(provider=provider)
                self.assertEqual(code, 0, output)
                self.assertEqual(len(requests), 2)
                self.assertIn('authentication and inference were not tested', output)


if __name__ == '__main__':
    unittest.main()
