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

"""Verify actual built-in chat inference and a successful SQL tool call."""
import json
import signal
import urllib.request
import uuid


def verify(response):
    assert response.get('message'), 'Chat returned no answer'
    for entry in response.get('tool_trace', []):
        if entry['name'] == 'query_lakehouse' and not entry.get('is_error'):
            try:
                result = json.loads(entry['result_preview'])
            except (ValueError, KeyError):
                continue
            if result.get('rows') == [[1]] and 'error' not in result:
                return
    raise AssertionError('No successful SQL probe in chat tool trace; model may lack tool support')


def main():
    signal.alarm(330)
    request = urllib.request.Request('http://127.0.0.1:8000/v1/chat',
        data=json.dumps({'session_id': 'install-' + uuid.uuid4().hex,
                         'stream': False,
                         'message': 'Use query_lakehouse to execute exactly SELECT 1 AS installation_probe. '
                                    'Report the returned value. Do not answer without using the tool.'}).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=320) as response:
        result = json.load(response)
    verify(result)
    print('PASS: built-in chat inference and SQL tool execution')


if __name__ == '__main__':
    main()
