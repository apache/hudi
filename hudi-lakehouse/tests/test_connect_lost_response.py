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
import importlib.util
from pathlib import Path
import socket
import threading
import unittest

spec = importlib.util.spec_from_file_location('lost_response', Path(__file__).resolve().parents[1] / 'local-dev/example/connect_lost_response.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class LostResponseProxyTest(unittest.TestCase):
    def test_forwards_requests_but_drops_gated_responses(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        received = []
        done = threading.Event()

        def serve():
            conn, _ = listener.accept()
            with conn:
                for _ in range(2):
                    data = conn.recv(1024)
                    received.append(data)
                    conn.sendall(data.upper())
                done.set()

        server = threading.Thread(target=serve, daemon=True)
        server.start()
        proxy = module.ResponseDropProxy(listener.getsockname()[1])
        proxy.start()
        try:
            with socket.create_connection(('127.0.0.1', proxy.port), timeout=2) as client:
                client.sendall(b'first')
                self.assertEqual(client.recv(1024), b'FIRST')
                proxy.drop.set()
                client.sendall(b'second')
                self.assertTrue(done.wait(2))
                client.settimeout(0.25)
                self.assertEqual(client.recv(1024), b'')
                self.assertEqual(received, [b'first', b'second'])
        finally:
            proxy.close()
            listener.close()
            server.join(timeout=2)

    def test_close_before_start(self):
        proxy = module.ResponseDropProxy(1)
        proxy.close()


if __name__ == '__main__':
    unittest.main()
