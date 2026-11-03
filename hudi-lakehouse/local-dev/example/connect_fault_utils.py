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
"""Bounded client-process helpers for disposable fault experiments."""
import json
from pathlib import Path
import re
import subprocess
import sys
import time


def stop_process(process):
    """Reap an owned client, escalating when graceful termination stalls."""
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
    else:
        process.wait(timeout=2)


def readback(uri, driver, table, deadline):
    """Bound connection, query, fetch and close by the observation deadline.

    A separate process owns the native ADBC handles. Killing it does not prove
    that its read-only Spark query stopped; never use this helper for writes.
    """
    return _observe(uri, driver, table, deadline, 'rows')


def read_timeline(uri, driver, table, deadline):
    """Read the test table's active timeline within the same deadline."""
    return _observe(uri, driver, table, deadline, 'timeline')


def _observe(uri, driver, table, deadline, operation):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('readback deadline expired')
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve())],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('readback deadline expired')
        try:
            output, error = process.communicate(
                json.dumps({'uri': uri, 'driver': driver, 'table': table, 'operation': operation}),
                timeout=remaining,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError('readback deadline expired') from exc
        if time.monotonic() >= deadline:
            raise TimeoutError('readback completed after deadline')
        if process.returncode:
            raise RuntimeError('readback client failed: ' + error.strip())
        return tuple(json.loads(output))
    finally:
        try:
            stop_process(process)
        finally:
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()


def main():
    from adbc_driver_manager import dbapi
    args = json.load(sys.stdin)
    table = args['table']
    if not re.fullmatch(r'connect_(?:cancel|disconnect|lost_response)_[0-9a-f]{32}\.orders', table):
        raise ValueError('invalid fault experiment table')
    with dbapi.connect(driver=args['driver'], db_kwargs={'uri': args['uri']}, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f'REFRESH TABLE {table}')
            if args['operation'] == 'rows':
                cursor.execute(f'SELECT COUNT(*), SUM(id) FROM {table}')
                result = cursor.fetchall()[0]
            elif args['operation'] == 'timeline':
                cursor.execute(f"SELECT timestamp, action, state FROM hudi_query_timeline('{table}', 'false')")
                timeline = cursor.fetchall()
                cursor.execute(f"CALL show_rollbacks(table => '{table}')")
                result = (timeline, cursor.fetchall())
            else:
                raise ValueError('invalid observation operation')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
