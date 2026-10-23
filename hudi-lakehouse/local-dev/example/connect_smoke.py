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

"""ADBC SQL acceptance tests against an already deployed endpoint.

Use prepare -> restart the endpoint -> verify -> cleanup to check that both
catalog metadata and data survive. Only a uniquely named test database is used.
"""
import argparse
import json
import os
import re
import subprocess
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


def query(cursor, sql):
    print(sql, flush=True)
    cursor.execute(sql)
    return [tuple(row) for row in cursor.fetchall()]


def sql_literal(value):
    # Values come from server metadata, not user SQL. Reject escapes as well.
    if any(char in value for char in "'\\\r\n"):
        raise ValueError(f"Unsafe SQL literal: {value!r}")
    return f"'{value}'"


def sql_matrix(cursor, database, expected_version):
    """Extra SQL surface; no DataFrame APIs and no per-table version overrides."""
    table = f"{database}.orders"
    commits = query(cursor, f"CALL show_commits(table => '{table}', limit => 100)")
    assert commits, "No completed commits"
    first_commit = min(row[0] for row in commits)
    run(cursor, f"SELECT id, amount FROM {table} TIMESTAMP AS OF {sql_literal(first_commit)} ORDER BY id",
        [(1, 100), (2, 200)])
    run(cursor, f"SELECT id, amount FROM hudi_table_changes('{table}', 'latest_state', 'earliest') ORDER BY id",
        [(1, 175), (3, 300)])

    # HMS sync must register the partitions, not just the Spark catalog table.
    hms = query(cursor, "SET spark.hadoop.hive.metastore.uris")[0][1]
    partitioned = f"{database}.partitioned_orders"
    run(cursor, f"""CREATE TABLE {partitioned} (id INT, amount INT, ts BIGINT, region STRING)
        USING hudi PARTITIONED BY (region) TBLPROPERTIES (
        type = 'cow', primaryKey = 'id', preCombineField = 'ts',
        'hoodie.datasource.write.hive_style_partitioning' = 'true',
        'hoodie.datasource.meta.sync.enable' = 'true',
        'hoodie.datasource.hive_sync.enable' = 'true',
        'hoodie.datasource.hive_sync.mode' = 'hms',
        'hoodie.datasource.hive_sync.metastore.uris' = {sql_literal(hms)},
        'hoodie.datasource.hive_sync.database' = '{database}',
        'hoodie.datasource.hive_sync.table' = 'partitioned_orders',
        'hoodie.datasource.hive_sync.partition_fields' = 'region',
        'hoodie.datasource.hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
        )""")
    run(cursor, f"INSERT INTO {partitioned} VALUES (1, 100, 1, 'East'), (2, 200, 1, 'West')")
    run(cursor, f"SELECT region, SUM(amount) FROM {partitioned} GROUP BY region ORDER BY region",
        [("East", 100), ("West", 200)])

    mor = f"{database}.mor_orders"
    run(cursor, f"""CREATE TABLE {mor} (id INT, amount INT, ts BIGINT) USING hudi
        TBLPROPERTIES (type = 'mor', primaryKey = 'id', preCombineField = 'ts',
        'hoodie.compact.inline' = 'true', 'hoodie.compact.inline.max.delta.commits' = '2')""")
    run(cursor, f"INSERT INTO {mor} VALUES (1, 100, 1)")
    run(cursor, f"SELECT id, amount FROM hudi_query('{mor}', 'read_optimized')", [(1, 100)])
    run(cursor, f"UPDATE {mor} SET amount = 150, ts = 2 WHERE id = 1")
    run(cursor, f"SELECT id, amount FROM {mor}", [(1, 150)])
    # The second delta commit triggers the explicitly configured inline compaction.
    compactions = query(cursor, f"CALL show_compaction(table => '{mor}', limit => 10)")
    assert any(row[2] == "COMPLETED" for row in compactions), compactions
    run(cursor, f"SELECT id, amount FROM hudi_query('{mor}', 'read_optimized')", [(1, 150)])
    if expected_version is not None:
        for name in (table, partitioned, mor):
            properties = dict(query(cursor, f"CALL show_table_properties(table => '{name}', limit => 1000)"))
            assert properties.get("hoodie.table.version") == str(expected_version), properties


def trino_readback(database, deployment):
    # JSON CLI output makes comparisons independent of table formatting/padding.
    for table, columns, expected in (
        ("orders", "id, amount", [{"id": 1, "amount": 175}, {"id": 3, "amount": 300}]),
        ("partitioned_orders", "id, region, amount", [
            {"id": 1, "region": "East", "amount": 100}, {"id": 2, "region": "West", "amount": 200}]),
    ):
        sql = f"SELECT {columns} FROM hudi.{database}.{table} ORDER BY id"
        print(sql, flush=True)
        result = subprocess.run([
            "kubectl", "-n", os.environ.get("NAMESPACE", "hudi-lakehouse"),
            "exec", f"deploy/{deployment}", "--", "trino", "--output-format", "JSON", "--execute", sql,
        ], check=True, capture_output=True, text=True, timeout=120)
        actual = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        assert actual == expected, (actual, expected)
        print(f"PASS: Trino {actual!r}", flush=True)


def freshness(cursor, database, writer, refresh):
    """Keep this cursor/session open while a separate Spark application writes."""
    table = f"{database}.freshness_orders"
    run(cursor, f"""CREATE TABLE {table} (id INT, amount INT, ts BIGINT) USING hudi
        TBLPROPERTIES (type = 'cow', primaryKey = 'id', preCombineField = 'ts')""")
    run(cursor, f"INSERT INTO {table} VALUES (1, 100, 1)")
    run(cursor, f"SELECT id, amount FROM {table}", [(1, 100)])
    before = query(cursor, f"SELECT MAX(_hoodie_commit_time) FROM {table}")[0][0]
    subprocess.run([*writer, database, "commit-schema"], check=True, timeout=600)
    if refresh:
        run(cursor, f"REFRESH TABLE {table}")
    run(cursor, f"SELECT id, amount, note FROM {table} ORDER BY id",
        [(1, 100, None), (2, 200, "external")])
    after = query(cursor, f"SELECT MAX(_hoodie_commit_time) FROM {table}")[0][0]
    assert after != before, "No external commit observed"
    subprocess.run([*writer, database, "recreate"], check=True, timeout=600)
    if refresh:
        run(cursor, f"REFRESH TABLE {table}")
    run(cursor, f"SELECT id, replacement FROM {table}", [(9, "recreated")])
    print(f"PASS: same-session external commit/schema/recreation; refresh={refresh}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["prepare", "sql", "freshness", "trino", "verify", "cleanup"])
    parser.add_argument("--database", required=True, type=identifier)
    parser.add_argument("--uri", default="spark://localhost:15002?api=connect&auth_type=none&tls=false")
    parser.add_argument("--driver", default="spark", help="Foundry driver name or native library path")
    parser.add_argument("--expected-table-version", type=int, choices=[8, 9],
                        help="sql phase: check stored versions without per-statement overrides")
    parser.add_argument("--trino-deployment", default="hudi-trino")
    parser.add_argument("--external-writer", nargs="+",
                        help="freshness: command argv; database and action are appended (no shell)")
    parser.add_argument("--refresh", action="store_true",
                        help="explicitly refresh after external writes; default tests automatic visibility")
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
    if args.phase == "freshness" and not args.external_writer:
        parser.error("freshness requires --external-writer")
    if args.phase == "trino":
        trino_readback(database, args.trino_deployment)
        return

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
                    type = 'cow', primaryKey = 'id', preCombineField = 'ts'
                )""")
                run(cursor, f"INSERT INTO {table} VALUES (1, 'East', 100, 1), (2, 'West', 200, 1)")
                run(cursor, f"SELECT id, amount FROM {table} ORDER BY id", [(1, 100), (2, 200)])
                run(cursor, f"UPDATE {table} SET amount = 150, ts = 2 WHERE id = 1")
                run(cursor, f"SELECT id, amount FROM {table} ORDER BY id", [(1, 150), (2, 200)])
                run(cursor, f"DELETE FROM {table} WHERE id = 2")
                run(cursor, f"SELECT id, amount FROM {table} ORDER BY id", [(1, 150)])
                run(cursor, f"""MERGE INTO {table} t USING
                    (SELECT 1 AS id, 'East' AS region, 175 AS amount, CAST(3 AS BIGINT) AS ts
                     UNION ALL SELECT 3, 'North', 300, CAST(3 AS BIGINT)) s ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
                run(cursor, f"SELECT id, amount FROM {table} ORDER BY id", [(1, 175), (3, 300)])
            elif args.phase == "sql":
                sql_matrix(cursor, database, args.expected_table_version)
            elif args.phase == "freshness":
                freshness(cursor, database, args.external_writer, args.refresh)
            elif args.phase == "verify":
                run(cursor, f"SELECT id, region, amount, ts FROM {table} ORDER BY id",
                    [(1, "East", 175, 3), (3, "North", 300, 3)])
            else:
                # Never CASCADE: fail if unrelated tables appeared in this DB.
                for name in ("freshness_orders", "mor_orders", "partitioned_orders", "orders"):
                    run(cursor, f"DROP TABLE IF EXISTS {database}.{name}")
                run(cursor, f"DROP DATABASE IF EXISTS {database}")
    print(f"PASS: {args.phase}", flush=True)


if __name__ == "__main__":
    main()
