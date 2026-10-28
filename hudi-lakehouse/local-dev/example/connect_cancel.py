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
"""Opt-in ADBC cancellation experiment against a disposable localhost endpoint."""
import argparse
import os
import threading
import time
import uuid


def connect(port, driver):
    from adbc_driver_manager import dbapi
    uri = f'spark://127.0.0.1:{port}?api=connect&auth_type=none&tls=false'
    return dbapi.connect(driver=driver, db_kwargs={'uri': uri}, autocommit=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--port', type=int, required=True)
    p.add_argument('--driver', default='spark')
    p.add_argument('--rows', type=int, default=20000)
    p.add_argument('--delay', type=float, default=1)
    p.add_argument('--wait', type=int, default=90)
    args = p.parse_args()
    if not 1 <= args.port <= 65535 or args.rows < 1 or args.delay < 0 or args.wait < 1:
        p.error('invalid port, row count, delay or wait')
    database = 'connect_cancel_' + uuid.uuid4().hex
    table = database + '.orders'
    print(f'Test database: {database}', flush=True)
    created = False
    with connect(args.port, args.driver) as observer, observer.cursor() as read, \
            connect(args.port, args.driver) as writer, writer.cursor() as write:
        try:
            read.execute(f'CREATE DATABASE {database}')
            created = True
            read.execute(f"CREATE TABLE {table} (id INT, ts BIGINT) USING hudi "
                         "TBLPROPERTIES(type='cow', primaryKey='id', preCombineField='ts')")
            outcome = []
            started = threading.Event()

            def submit():
                started.set()
                try:
                    write.execute(f'INSERT INTO {table} SELECT CAST(id AS INT) AS id, CAST(id AS BIGINT) AS ts FROM range({args.rows})')
                    outcome.append('success')
                except Exception as exc:
                    outcome.append(f'{type(exc).__name__}: {exc}')

            worker = threading.Thread(target=submit, daemon=True)
            worker.start()
            started.wait()
            time.sleep(args.delay)
            if not worker.is_alive():
                print('INCONCLUSIVE: write finished before cancellation', flush=True)
                return 2
            try:
                write.adbc_cancel()
                print('Cancellation request returned', flush=True)
            except Exception as exc:
                print(f'Cancellation request failed: {type(exc).__name__}: {exc}', flush=True)
            worker.join(30)
            if worker.is_alive():
                # Do not close a cursor concurrently with an active operation.
                print(f'INCONCLUSIVE: client did not stop; leave {database} for inspection', flush=True)
                os._exit(2)
            print(f'Client write result: {outcome}', flush=True)
            deadline = time.monotonic() + args.wait
            while time.monotonic() < deadline:
                try:
                    read.execute(f'REFRESH TABLE {table}')
                    read.execute(f'SELECT COUNT(*), SUM(id) FROM {table}')
                    count, total = read.fetchall()[0]
                except Exception as exc:
                    print(f'Readback not yet available: {type(exc).__name__}', flush=True)
                    time.sleep(2)
                    continue
                if count == args.rows and total == args.rows * (args.rows - 1) // 2:
                    print(f'OBSERVED: all {count} rows committed after cancellation', flush=True)
                    read.execute(f'DROP TABLE IF EXISTS {table} PURGE')
                    read.execute(f'DROP DATABASE IF EXISTS {database}')
                    created = False
                    print('PASS: owned test data cleaned', flush=True)
                    return 0
                time.sleep(2)
            print('INCONCLUSIVE: complete commit was not observed; outcome may still change', flush=True)
            return 2
        finally:
            if created:
                print(f'Leave {database} untouched until the server write has stopped; then remove its table and database.', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
