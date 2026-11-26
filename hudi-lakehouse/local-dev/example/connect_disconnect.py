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
import queue
import re
import subprocess
import sys
import threading
import time
import uuid

from adbc_driver_manager import dbapi
from connect_fault_utils import readback, stop_process


def connect(uri, driver):
    return dbapi.connect(driver=driver, db_kwargs={'uri': uri}, autocommit=True)


def interrupt_before_response(child, delay):
    """Only count the experiment when no successful response was observed."""
    messages = queue.Queue()

    def consume():
        try:
            for line in child.stdout:
                messages.put(line.strip())
        finally:
            messages.put(None)

    reader = threading.Thread(target=consume, daemon=True)
    reader.start()
    ready = False
    try:
        try:
            if messages.get(timeout=60) != 'READY':
                return False
            ready = True
            # Consume output continuously, including TextIOWrapper buffering.
            messages.get(timeout=delay)
            return False  # RESPONSE, EOF, or unexpected output before interruption.
        except queue.Empty:
            # A missing READY is not evidence that the write was submitted.
            if child.poll() is not None or not ready:
                return False
        stop_process(child)
        reader.join(timeout=5)
        if reader.is_alive():
            return False
        while not messages.empty():
            if messages.get_nowait() == 'RESPONSE':
                return False
        return True
    finally:
        stop_process(child)
        reader.join(timeout=5)
        if not reader.is_alive():
            child.stdout.close()


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
    child = None
    with connect(args.uri, args.driver) as conn, conn.cursor() as cursor:
        cursor.execute(f'CREATE DATABASE {database}')
        created = True
        try:
            cursor.execute(f"CREATE TABLE {table} (id INT, ts BIGINT) USING hudi "
                           "TBLPROPERTIES(type='cow', primaryKey='id', preCombineField='ts')")
            child = subprocess.Popen([sys.executable, __file__, '--uri', args.uri, '--driver', args.driver,
                                      '--rows', str(args.rows), '--child-table', table],
                                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            if not interrupt_before_response(child, args.delay):
                print('INCONCLUSIVE: client was not interrupted before its response; preserve test data')
                return 2
            print(f'Client interrupted without an observed response; waiting up to {args.wait}s for readback', flush=True)
            deadline = time.monotonic() + args.wait
            while time.monotonic() < deadline:
                try:
                    count, total = readback(args.uri, args.driver, table, deadline)
                except TimeoutError:
                    print('INCONCLUSIVE: readback deadline expired; preserve test data', flush=True)
                    return 2
                except Exception as exc:
                    print(f'Readback not yet available: {type(exc).__name__}', flush=True)
                    time.sleep(min(2, max(0, deadline - time.monotonic())))
                    continue
                if count == args.rows and total == args.rows * (args.rows - 1) // 2:
                    print(f'OBSERVED: all {count} rows visible after client disconnect; commit timing is unknown')
                    cursor.execute(f'DROP TABLE IF EXISTS {table} PURGE')
                    cursor.execute(f'DROP DATABASE IF EXISTS {database}')
                    created = False
                    print('PASS: owned test data cleaned')
                    return 0
                time.sleep(min(2, max(0, deadline - time.monotonic())))
            print('INCONCLUSIVE: full write was not observed; outcome may still change')
            return 2
        finally:
            stop_process(child)
            if created:
                print(f'Leave {database} untouched until the server write has stopped; then remove its table and database.',
                      file=sys.stderr, flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
