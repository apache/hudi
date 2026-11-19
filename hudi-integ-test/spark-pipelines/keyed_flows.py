# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Check keyed Hudi SDP flows across ordering, deletion, reinsertion and restart."""
import argparse
import json
import signal
from pathlib import Path
import sys
import tempfile

SOURCE = 'from pyspark import pipelines as dp\nfrom pyspark.sql import SparkSession\nspark = SparkSession.active()\n@dp.temporary_view(name="input_events")\ndef input_events():\n    return spark.readStream.schema("id INT, ts LONG, part STRING, value STRING, _hoodie_is_deleted BOOLEAN").json(spark.conf.get("hudi.test.input"))\n'
PROPERTIES = "'primaryKey'='id', 'preCombineField'='ts', 'hoodie.record.merge.mode'='EVENT_TIME_ORDERING'"
TABLES = ("keyed_new", "keyed_existing")


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def interrupted(signum, _frame):
    raise KeyboardInterrupt(f"Received signal {signum}")


def client(mode, remote, root):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote(remote).getOrCreate()
    try:
        require(spark.conf.get("spark.hudi.test.run.id") == root.name, "Connected to the wrong test server")
        table_type = (root / "table-type").read_text()
        properties = f"'type'='{table_type}', {PROPERTIES}"
        if mode == "prepare":
            spark.sql(f"CREATE TABLE sdp_test.keyed_existing (id INT, ts BIGINT, part STRING, value STRING, "
                      f"_hoodie_is_deleted BOOLEAN) USING hudi PARTITIONED BY (part) TBLPROPERTIES ({properties})").collect()
        else:
            expected = json.loads((root / "expected.json").read_text())
            for table in TABLES:
                actual = [list(row) for row in spark.table(f"sdp_test.{table}")
                          .select("id", "ts", "part", "value").orderBy("id").collect()]
                require(actual == expected, (table, actual, expected))
                print(f"PASS {mode} {table_type} {table}: {actual}")
                if table_type == "mor" and mode == "verify-2":
                    location = root / "warehouse" / "sdp_test.db" / table
                    require(any(location.rglob("*.log.*")), f"No MOR log files in {location}")
                    base_rows = [list(row) for row in spark.read.format("hudi")
                                 .option("hoodie.datasource.query.type", "read_optimized")
                                 .load(location.as_uri()).select("id", "ts", "part", "value")
                                 .orderBy("id").collect()]
                    expected_base = json.loads((root / "expected-base.json").read_text())
                    require(base_rows == expected_base, (table, "MOR base rows", base_rows, expected_base))
                    print(f"PASS MOR log merge {table}: base rows {base_rows}, snapshot {actual}")
    finally:
        spark.stop()


def scenario(h, table_type):
    (h.root / "table-type").write_text(table_type)
    properties = f"'type'='{table_type}', {PROPERTIES}"
    source = h.root / "input"
    source.mkdir()
    (h.root / "source.py").write_text(SOURCE)
    flows = "\n".join(
        f"CREATE STREAMING TABLE {table} USING hudi PARTITIONED BY (part) TBLPROPERTIES ({properties}) "
        "AS SELECT id, ts, part, value, _hoodie_is_deleted FROM STREAM input_events;" for table in TABLES)
    (h.root / "flows.sql").write_text(flows)
    spec = {"name": "keyed_flows", "storage": (h.root / "checkpoints").as_uri(),
            "catalog": "spark_catalog", "database": "sdp_test",
            "libraries": [{"glob": {"include": "source.py"}}, {"glob": {"include": "flows.sql"}}],
            "configuration": {"hudi.test.input": source.as_uri(), "spark.sql.shuffle.partitions": "2",
                              "spark.hoodie.metadata.enable": "false",
                              "spark.hoodie.compact.inline": "false",
                              "spark.hoodie.datasource.write.streaming.disable.compaction": "true",
                              "spark.sql.pipelines.maxFlowRetryAttempts": "0"}}
    path = h.root / "spark-pipeline.yml"
    path.write_text(json.dumps(spec, indent=2))
    for name in ("source.py", "flows.sql", path.name):
        (h.logs / name).write_text((h.root / name).read_text())
    h.client(Path(__file__).resolve(), "prepare")
    stages = (
        ([(1, 2, "a", "new", False), (1, 1, "a", "old", False), (2, 1, "b", "keep", False)],
         [[1, 2, "a", "new"], [2, 1, "b", "keep"]]),
        ([(1, 3, "a", "updated", False)], [[1, 3, "a", "updated"], [2, 1, "b", "keep"]]),
        ([(1, 2, "a", "late", False)], [[1, 3, "a", "updated"], [2, 1, "b", "keep"]]),
        ([(1, 2, "a", "late-delete", True)], [[1, 3, "a", "updated"], [2, 1, "b", "keep"]]),
        ([(1, 4, "a", "deleted", True)], [[2, 1, "b", "keep"]]),
        ([(1, 5, "a", "reinserted", False)], [[1, 5, "a", "reinserted"], [2, 1, "b", "keep"]]))
    (h.root / "expected-base.json").write_text(json.dumps(stages[0][1]))
    for i, (rows, expected) in enumerate(stages, 1):
        (source / f"{i}.json").write_text("".join(json.dumps(dict(zip(
            ("id", "ts", "part", "value", "_hoodie_is_deleted"), row))) + "\n" for row in rows))
        (h.root / "expected.json").write_text(json.dumps(expected))
        h.run_pipeline(path, f"stage-{i}")
        h.client(Path(__file__).resolve(), f"verify-{i}")
    def completed_commits():
        return {table: sorted(p.name for p in
                (h.root / "warehouse" / "sdp_test.db" / table / ".hoodie" / "timeline").glob("*")
                if p.suffix in (".commit", ".deltacommit", ".replacecommit")) for table in TABLES}

    before_restart = completed_commits()
    require(all(before_restart.values()), "Expected completed commits before restart")
    h.run_pipeline(path, "restart")
    after_restart = completed_commits()
    require(after_restart == before_restart,
            ("Unchanged restart wrote an extra commit", before_restart, after_restart))
    h.client(Path(__file__).resolve(), "verify-restart")
    invalid = (
        ("missing_key", properties.replace("'primaryKey'='id'", "'primaryKey'='missing_key'"), "part", SOURCE, "Can't find primaryKey"),
        ("missing_order", properties.replace("'preCombineField'='ts'", "'preCombineField'='missing_order'"), "part", SOURCE, "Can't find ordering fields"),
        ("missing_partition", properties, "missing_partition", SOURCE, "Partition column 'missing_partition'"),
        ("invalid_marker", properties, "part", SOURCE.replace("_hoodie_is_deleted BOOLEAN", "_hoodie_is_deleted STRING"), "has to be BOOLEAN"),
        ("changed_identity", properties.replace("'primaryKey'='id'", "'primaryKey'='value'"), "part", SOURCE, "Can't set primaryKey to 'value' on Hudi table"))
    for name, props, partition, definition, error_text in invalid:
        target = "keyed_existing" if name == "changed_identity" else f"bad_{name}"
        (h.root / "source.py").write_text(definition)
        statement = (f"CREATE STREAMING TABLE {target} USING hudi PARTITIONED BY ({partition}) "
                     f"TBLPROPERTIES ({props}) AS SELECT id, ts, part, value, _hoodie_is_deleted FROM STREAM input_events;")
        (h.root / "flows.sql").write_text(statement)
        spec["name"] = name
        spec["storage"] = (h.root / f"checkpoint-{name}").as_uri()
        path.write_text(json.dumps(spec, indent=2))
        (h.logs / f"{name}.sql").write_text(statement)
        try:
            h.run_pipeline(path, f"reject-{name}")
        except RuntimeError:
            log = (h.logs / f"reject-{name}.log").read_text()
            require(error_text.lower() in log.lower(), (name, log[-5000:]))
            require(h.server.poll() is None, name)
        else:
            raise AssertionError(f"Invalid configuration unexpectedly succeeded: {name}")
        if name != "changed_identity":
            timeline = h.root / "warehouse" / "sdp_test.db" / target / ".hoodie" / "timeline"
            require(not [p for p in timeline.glob("*") if p.suffix in (".commit", ".deltacommit", ".replacecommit")], name)
        h.client(Path(__file__).resolve(), f"verify-after-{name}")
    print("PASS: keyed deduplication, updates, late values, delete, reinsertion, restart and invalid configurations")


def main():
    if len(sys.argv) == 4 and (sys.argv[1] == "prepare" or sys.argv[1].startswith("verify-")):
        client(sys.argv[1], sys.argv[2], Path(sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", required=True, type=Path)
    parser.add_argument("--table-type", choices=("cow", "mor"), default="cow")
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--logs", type=Path, default=Path("target/keyed-acceptance-logs"))
    args = parser.parse_args()
    sys.path.insert(0, str(args.harness_dir.resolve()))
    from harness import ConnectHarness
    args.logs.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run-", dir=args.logs.resolve()))
    print(f"Keyed logs: {logs}", flush=True)
    signal.signal(signal.SIGTERM, interrupted)
    with ConnectHarness(args.bundle, logs) as harness:
        scenario(harness, args.table_type)


if __name__ == "__main__":
    main()
