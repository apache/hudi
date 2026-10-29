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
"""Opt-in client-disconnect experiment on a disposable Connect endpoint.

An indeterminate result leaves its unique test database for manual cleanup.
This is not a retry, transaction, cancellation, or concurrency guarantee.
"""
import argparse
import re
import select
import subprocess
import sys
import time
import uuid

from adbc_driver_manager import dbapi


def connect(uri, driver):
    return dbapi.connect(driver=driver, db_kwargs={'uri': uri}, autocommit=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--uri', required=True)
    parser.add_argument('--driver', default='spark')
    parser.add_argument('--rows', type=int, default=10000)
    parser.add_argument('--delay', type=float, default=1.0)
    parser.add_argument('--wait', type=int, default=90)
    parser.add_argument('--child-table', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.rows < 1 or args.delay < 0 or args.wait < 1:
        parser.error('rows and wait must be positive and delay nonnegative')
    if args.child_table:
        if not re.fullmatch(r"connect_disconnect_[0-9a-f]{32}\.orders", args.child_table):
            parser.error("invalid internal test table")
        # The table name comes only from this script's generated parent argument.
        with connect(args.uri, args.driver) as conn, conn.cursor() as cursor:
            print('READY', flush=True)
            cursor.execute(f'INSERT INTO {args.child_table} SELECT CAST(id AS INT) AS id, CAST(id AS BIGINT) AS ts FROM range({args.rows})')
            print('RESPONSE', flush=True)
        return 0

    database = 'connect_disconnect_' + uuid.uuid4().hex
    table = database + '.orders'
    print(f'Test database: {database}', flush=True)
    created = False
    with connect(args.uri, args.driver) as conn, conn.cursor() as cursor:
        cursor.execute(f'CREATE DATABASE {database}')
        created = True
        try:
            cursor.execute(f"CREATE TABLE {table} (id INT, ts BIGINT) USING hudi "
                           "TBLPROPERTIES(type='cow', primaryKey='id', preCombineField='ts')")
            child = subprocess.Popen([sys.executable, __file__, '--uri', args.uri, '--driver', args.driver,
                                      '--rows', str(args.rows), '--child-table', table],
                                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            # If the child exits before READY, do not treat the write as submitted.
            if not select.select([child.stdout], [], [], 60)[0]:
                child.terminate()
                child.wait(timeout=30)
                print('FAIL: client did not become ready; inspect test database before cleanup', file=sys.stderr)
                return 1
            if child.stdout.readline().strip() != 'READY':
                child.wait(timeout=10)
                print('FAIL: client did not reach the write; inspect test database before cleanup', file=sys.stderr)
                return 1
            time.sleep(args.delay)
            if child.poll() is not None:
                print('INCONCLUSIVE: client exited before disconnect; inspect test database before cleanup')
                return 2
            child.terminate()
            child.wait(timeout=30)
            print(f'Client disconnected after {args.delay}s; waiting up to {args.wait}s for a completed write', flush=True)
            deadline = time.monotonic() + args.wait
            while time.monotonic() < deadline:
                try:
                    cursor.execute(f'REFRESH TABLE {table}')
                    cursor.execute(f'SELECT COUNT(*), SUM(id) FROM {table}')
                    count, total = cursor.fetchall()[0]
                except Exception as exc:
                    print(f'Readback not yet available: {type(exc).__name__}', flush=True)
                    time.sleep(2)
                    continue
                if count == args.rows and total == args.rows * (args.rows - 1) // 2:
                    print(f'OBSERVED: all {count} rows committed after client disconnect')
                    cursor.execute(f'DROP TABLE IF EXISTS {table} PURGE')
                    cursor.execute(f'DROP DATABASE IF EXISTS {database}')
                    created = False
                    print('PASS: owned test data cleaned')
                    return 0
                time.sleep(2)
            print('INCONCLUSIVE: full write was not observed; outcome may still change')
            return 2
        finally:
            if created:
                print(f'Leave {database} untouched until the server write has stopped; then remove its table and database.',
                      file=sys.stderr, flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
