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
"""Read-only SQL, catalog and storage checks for an existing Connect endpoint."""
import argparse
import re
import sys

from adbc_driver_manager import dbapi


def identifier(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise argparse.ArgumentTypeError('identifier must contain only letters, numbers and underscores')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--uri', required=True)
    parser.add_argument('--driver', default='spark')
    parser.add_argument('--schema', required=True, type=identifier)
    parser.add_argument('--table', required=True, type=identifier)
    parser.add_argument('--column', required=True, type=identifier,
                        help='non-null data column in an existing non-empty storage-backed table')
    args = parser.parse_args()
    checks = (
        ('sql', 'SELECT 1', lambda rows: rows == [(1,)]),
        ('catalog', 'SHOW DATABASES', lambda rows: any(row[0] == args.schema for row in rows)),
        ('storage', f'SELECT `{args.column}` FROM `{args.schema}`.`{args.table}` '
                    f'WHERE `{args.column}` IS NOT NULL LIMIT 1', lambda rows: bool(rows)),
    )
    try:
        with dbapi.connect(driver=args.driver, db_kwargs={'uri': args.uri}, autocommit=True) as conn:
            with conn.cursor() as cursor:
                for name, sql, valid in checks:
                    try:
                        cursor.execute(sql)
                        if not valid(cursor.fetchall()):
                            raise ValueError('expected result missing')
                        print(f'PASS: {name}', flush=True)
                    except Exception as exc:
                        print(f'FAIL: {name}: {exc}', file=sys.stderr, flush=True)
                        return 1
    except Exception as exc:
        print(f'FAIL: connection: {exc}', file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
