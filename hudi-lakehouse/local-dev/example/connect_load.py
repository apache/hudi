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
"""Opt-in result-size experiment; no tables or persistent data are created."""
import argparse
import platform
import resource
import time

from adbc_driver_manager import dbapi


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--uri', required=True)
    parser.add_argument('--driver', default='spark')
    parser.add_argument('--rows', type=int, required=True)
    parser.add_argument('--payload-bytes', type=int, default=64)
    parser.add_argument('--fetch-size', type=int, default=10000)
    args = parser.parse_args()
    if args.rows < 1 or args.payload_bytes < 0 or args.fetch_size < 1:
        parser.error('rows and fetch size must be positive; payload bytes must be nonnegative')
    start = time.monotonic()
    count = total = 0
    print(f'rows={args.rows} payload_bytes={args.payload_bytes} fetchmany={args.fetch_size}', flush=True)
    with dbapi.connect(driver=args.driver, db_kwargs={'uri': args.uri}, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT id, repeat('x', {args.payload_bytes}) AS payload FROM range({args.rows})")
            while True:
                rows = cursor.fetchmany(args.fetch_size)
                if not rows:
                    break
                count += len(rows)
                total += sum(row[0] for row in rows)
                if any(len(row[1]) != args.payload_bytes for row in rows):
                    raise AssertionError('unexpected payload width')
    expected = args.rows * (args.rows - 1) // 2
    if count != args.rows or total != expected:
        raise AssertionError(f'count={count} sum={total}, expected={args.rows}/{expected}')
    print(f'PASS count={count} checksum={total} seconds={time.monotonic() - start:.1f} '
          f'client_peak_rss={resource.getrusage(resource.RUSAGE_SELF).ru_maxrss} '
          f'platform={platform.system()} (RSS bytes on macOS, KiB on Linux)', flush=True)


if __name__ == '__main__':
    main()
