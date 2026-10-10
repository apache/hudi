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

"""ADBC-only CRUD smoke test; each invocation creates a new Connect session.

Use prepare -> restart the endpoint -> verify -> cleanup to check that both
catalog metadata and data survive. Only a uniquely named test database is used.
"""
import argparse
import re
from pathlib import Path

from adbc_driver_manager import dbapi


def identifier(value):
    # This CLI interpolates only its own test identifiers, never arbitrary SQL.
    if not re.fullmatch(r"connect_smoke_[a-z0-9_]+", value):
        raise argparse.ArgumentTypeError("database must match connect_smoke_[a-z0-9_]+")
    return value


def run(cursor, sql, expected=None):
    print(sql, flush=True)
    cursor.execute(sql)
    # DDL/DML do not necessarily have result sets; never fetch unconditionally.
    if expected is not None:
        actual = [tuple(row) for row in cursor.fetchall()]
        if actual != expected:
            raise AssertionError(f"Expected {expected!r}, got {actual!r}")
        print(f"PASS: {actual!r}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["prepare", "verify", "cleanup"])
    parser.add_argument("--database", required=True, type=identifier)
    parser.add_argument("--uri", default="spark://localhost:15002?api=connect&auth_type=none&tls=false")
    parser.add_argument("--driver", default="spark", help="Foundry driver name or native library path")
    parser.add_argument("--created-marker", type=Path,
                        help="prepare only: record database ownership for the shell runner's cleanup")
    args = parser.parse_args()
    if args.created_marker:
        if args.phase != "prepare":
            parser.error("--created-marker is only valid for prepare")
        if args.created_marker.exists():
            parser.error("--created-marker must be a new file")
    database = args.database
    table = f"{database}.orders"

    with dbapi.connect(driver=args.driver, db_kwargs={"uri": args.uri}, autocommit=True) as conn:
        with conn.cursor() as cursor:
            if args.phase == "prepare":
                # No IF NOT EXISTS: refuse to reuse another run's database.
                run(cursor, f"CREATE DATABASE {database}")
                # A failed/ambiguous CREATE must not authorize automatic deletion.
                if args.created_marker:
                    with args.created_marker.open("x") as marker:
                        marker.write(database)
                run(cursor, f"""CREATE TABLE {table} (
                    id INT, region STRING, amount INT, ts BIGINT
                ) USING hudi
                TBLPROPERTIES (
                    type = 'cow', primaryKey = 'id', preCombineField = 'ts',
                    'hoodie.write.table.version' = '8',
                    'hoodie.write.auto.upgrade' = 'false'
                )""")
                run(cursor, f"INSERT INTO {table} VALUES (1, 'East', 100, 1), (2, 'West', 200, 1)")
                run(cursor, f"SELECT id, amount FROM {table} ORDER BY id", [(1, 100), (2, 200)])
                run(cursor, f"UPDATE {table} SET amount = 150, ts = 2 WHERE id = 1")
                run(cursor, f"SELECT id, amount FROM {table} ORDER BY id", [(1, 150), (2, 200)])
                run(cursor, f"DELETE FROM {table} WHERE id = 2")
                run(cursor, f"SELECT id, amount FROM {table} ORDER BY id", [(1, 150)])
            elif args.phase == "verify":
                run(cursor, f"SELECT id, region, amount, ts FROM {table}", [(1, "East", 150, 2)])
            else:
                # Never CASCADE: fail if unrelated tables appeared in this DB.
                run(cursor, f"DROP TABLE IF EXISTS {table}")
                run(cursor, f"DROP DATABASE IF EXISTS {database}")
    print(f"PASS: {args.phase}", flush=True)


if __name__ == "__main__":
    main()
