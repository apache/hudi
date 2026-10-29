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
"""Discard responses to one local test client while independently observing a commit.

Only use with a disposable, unauthenticated endpoint exposed on localhost.
An unobserved write leaves its unique test database for later manual cleanup.
"""
import argparse
import select
import socket
import subprocess
import sys
import threading
import time
import uuid


def connect(port, driver):
    from adbc_driver_manager import dbapi
    uri = f'spark://127.0.0.1:{port}?api=connect&auth_type=none&tls=false'
    return dbapi.connect(driver=driver, db_kwargs={'uri': uri}, autocommit=True)


class ResponseDropProxy:
    """One-connection TCP proxy; drop server bytes once the gate is closed."""
    def __init__(self, target_port):
        self.target_port = target_port
        self.drop = threading.Event()
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(1)
        self.port = self.listener.getsockname()[1]
        self.sockets = []
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.started = False

    def start(self):
        self.thread.start()
        self.started = True

    def _serve(self):
        try:
            client, _ = self.listener.accept()
            self.sockets.append(client)
            upstream = socket.create_connection(('127.0.0.1', self.target_port), timeout=10)
            upstream.settimeout(None)
            self.sockets.append(upstream)
            while True:
                readable, _, _ = select.select([client, upstream], [], [], 1)
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    if source is upstream and self.drop.is_set():
                        continue
                    (upstream if source is client else client).sendall(data)
        except (OSError, ValueError):
            pass
        finally:
            for sock in self.sockets:
                sock.close()

    def close(self):
        self.listener.close()
        for sock in self.sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        if self.started:
            self.thread.join(timeout=5)


def child(port, driver, table, rows):
    with connect(port, driver) as conn, conn.cursor() as cursor:
        cursor.execute('SELECT 1')
        cursor.fetchall()
        print('READY', flush=True)
        if sys.stdin.readline().strip() != 'WRITE':
            return 1
        cursor.execute(f'INSERT INTO {table} SELECT CAST(id AS INT) AS id, CAST(id AS BIGINT) AS ts FROM range({rows})')
        print('RESPONSE', flush=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, required=True, help='localhost port of disposable Spark Connect endpoint')
    parser.add_argument('--driver', default='spark')
    parser.add_argument('--rows', type=int, default=1000)
    parser.add_argument('--wait', type=int, default=120)
    parser.add_argument('--retry', action='store_true', help='repeat the same INSERT after the lost response')
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or args.rows < 1 or args.wait < 1:
        parser.error('port must be valid and rows and wait must be positive')
    database = 'connect_lost_response_' + uuid.uuid4().hex
    table = database + '.orders'
    print(f'Test database: {database}', flush=True)
    created = False
    process = None
    proxy = ResponseDropProxy(args.port)
    try:
        with connect(args.port, args.driver) as conn, conn.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE {database}')
            created = True
            cursor.execute(f"CREATE TABLE {table} (id INT, ts BIGINT) USING hudi "
                           "TBLPROPERTIES(type='cow', primaryKey='id', preCombineField='ts')")
            proxy.start()
            # The child gets the generated table name through a pipe, not user input.
            process = subprocess.Popen([sys.executable, __file__, '--port', str(args.port),
                                        '--driver', args.driver, '--rows', str(args.rows),
                                        '--child-port', str(proxy.port), '--child-table', table],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.DEVNULL, text=True)
            if not select.select([process.stdout], [], [], 60)[0] or process.stdout.readline().strip() != 'READY':
                print('INCONCLUSIVE: client did not become ready')
                return 2
            proxy.drop.set()
            process.stdin.write('WRITE\n')
            process.stdin.flush()
            print('Response gate closed; write submitted to client', flush=True)
            deadline = time.monotonic() + args.wait
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    print('INCONCLUSIVE: client exited before commit was observed')
                    return 2
                try:
                    cursor.execute(f'REFRESH TABLE {table}')
                    cursor.execute(f'SELECT COUNT(*), SUM(id) FROM {table}')
                    count, total = cursor.fetchall()[0]
                except Exception as exc:
                    print(f'Readback not yet available: {type(exc).__name__}', flush=True)
                    time.sleep(2)
                    continue
                if count == args.rows and total == args.rows * (args.rows - 1) // 2:
                    process.terminate()
                    process.wait(timeout=30)
                    print(f'OBSERVED: {count} rows committed while responses were discarded; client was terminated')
                    if args.retry:
                        cursor.execute(f'INSERT INTO {table} SELECT CAST(id AS INT) AS id, CAST(id AS BIGINT) AS ts FROM range({args.rows})')
                        cursor.execute(f'REFRESH TABLE {table}')
                        cursor.execute(f'SELECT COUNT(*), COUNT(DISTINCT id), SUM(id) FROM {table}')
                        count, distinct, total = cursor.fetchall()[0]
                        expected_sum = args.rows * (args.rows - 1) // 2
                        if distinct != args.rows or count not in (args.rows, 2 * args.rows) or total != expected_sum * (count // args.rows):
                            raise RuntimeError(f'Unexpected retry result: count={count}, distinct={distinct}, sum={total}')
                        print(f'OBSERVED: retry produced {count} rows with {distinct} distinct IDs', flush=True)
                    cursor.execute(f'DROP TABLE IF EXISTS {table} PURGE')
                    cursor.execute(f'DROP DATABASE IF EXISTS {database}')
                    created = False
                    print('PASS: owned test data cleaned')
                    return 0
                time.sleep(2)
            print('INCONCLUSIVE: commit was not observed; outcome may still change')
            return 2
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            process.wait(timeout=30)
        proxy.close()
        if created:
            print(f'Leave {database} untouched until the server write has stopped; then remove its table and database.', file=sys.stderr)


if __name__ == '__main__':
    # Child flags are internal and generated by the parent; reject arbitrary SQL identifiers.
    if '--child-table' in sys.argv:
        import re
        p = argparse.ArgumentParser()
        p.add_argument('--port', type=int, required=True)
        p.add_argument('--driver', required=True)
        p.add_argument('--rows', type=int, required=True)
        p.add_argument('--child-port', type=int, required=True)
        p.add_argument('--child-table', required=True)
        a = p.parse_args()
        if not re.fullmatch(r'connect_lost_response_[0-9a-f]{32}\.orders', a.child_table) or a.rows < 1:
            p.error('invalid internal child arguments')
        raise SystemExit(child(a.child_port, a.driver, a.child_table, a.rows))
    raise SystemExit(main())
