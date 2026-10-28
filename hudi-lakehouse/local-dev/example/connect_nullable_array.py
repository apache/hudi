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
"""Check Hudi nullable-array write/read on a disposable localhost endpoint."""
import argparse
import uuid

from adbc_driver_manager import dbapi


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--driver', default='spark')
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error('invalid port')
    uri = f'spark://127.0.0.1:{args.port}?api=connect&auth_type=none&tls=false'
    database = 'connect_nullable_' + uuid.uuid4().hex
    print(f'Test database: {database}', flush=True)
    created = False
    with dbapi.connect(driver=args.driver, db_kwargs={'uri': uri}, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            cursor.execute(f'CREATE DATABASE {database}')
            created = True
            cursor.execute(f"CREATE TABLE {database}.orders (id INT, arr ARRAY<INT>, ts BIGINT) USING hudi "
                           "TBLPROPERTIES (type='cow', primaryKey='id', preCombineField='ts')")
            cursor.execute(f'INSERT INTO {database}.orders VALUES (1, array(1, CAST(NULL AS INT), 3), 1)')
            cursor.execute(f'SELECT id, arr FROM {database}.orders')
            rows = cursor.fetchall()
            if rows != [(1, [1, None, 3])]:
                raise AssertionError(f'Unexpected rows: {rows}')
            print(f'PASS: nullable array readback: {rows}', flush=True)
            cursor.execute(f'DROP TABLE IF EXISTS {database}.orders PURGE')
            cursor.execute(f'DROP DATABASE IF EXISTS {database}')
            created = False
            print('PASS: owned test data cleaned', flush=True)
        finally:
            if created:
                print(f'Leave {database} untouched until the server write has stopped; then remove its table and database.', flush=True)


if __name__ == '__main__':
    main()
