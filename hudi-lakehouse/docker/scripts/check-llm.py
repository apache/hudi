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

"""Check chat readiness inside the gateway without the UI's model-list fallback."""
import json
import os
import sys
import urllib.error
import urllib.request


def read_json(url, *, headers=None, timeout=30):
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def ollama_model_name(name):
    # Ollama accepts an omitted tag as :latest; /api/tags returns explicit tags.
    return name if ':' in name.rsplit('/', 1)[-1] else name + ':latest'


def check():
    try:
        ready = read_json('http://127.0.0.1:8000/ready', timeout=150)
    except urllib.error.HTTPError as error:
        if error.code != 503:
            raise
        ready = json.load(error)
    print(json.dumps(ready, indent=2))
    if not ready['ready']:
        raise ValueError('Gateway dependencies are not ready')
    info = read_json('http://127.0.0.1:8000/v1/info')
    provider, model = info['provider'], info['model']
    if provider == 'ollama':
        url = os.environ['GATEWAY_OLLAMA_BASE_URL'].rstrip('/') + '/api/tags'
        models = read_json(url)['models']
        available = ollama_model_name(model) in {
            ollama_model_name(entry['name']) for entry in models
        }
    elif provider == 'openai-compatible':
        url = os.environ['GATEWAY_OPENAI_BASE_URL'].rstrip('/') + '/models'
        key = os.environ.get('OPENAI_API_KEY', '')
        headers = {'Authorization': 'Bearer ' + key} if key else {}
        models = read_json(url, headers=headers)['data']
        available = model in {entry['id'] for entry in models}
    else:
        print('API key configured; hosted-provider authentication and inference were not tested.')
        return
    if not available:
        raise ValueError('Configured model is not available: ' + model)
    print('PASS: configured model is available from ' + provider)


def main():
    try:
        check()
    except (urllib.error.URLError, ValueError, KeyError, TypeError, OSError) as error:
        print('Chat readiness failed: ' + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
