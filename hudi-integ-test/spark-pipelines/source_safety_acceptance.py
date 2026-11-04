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

"""Run guarded Hudi source reset and coordinated rebuild acceptance using the #20453 Spark CLI/Connect harness."""

import argparse
import json
from pathlib import Path
import signal
import sys
import tempfile


SOURCE = '''from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.active()

@dp.temporary_view(name="input_events")
def input_events():
    return spark.readStream.schema("id INT, ts LONG, value STRING").json(spark.conf.get("hudi.test.input"))

@dp.temporary_view(name="guarded_upstream")
def guarded_upstream():
    # SDP discards explicit options on graph-internal reads; the pipeline SQLConf enables safety.
    return (spark.readStream
            .table("sdp_test.upsert_events")
            .select("id", "ts", "value"))
'''

# The downstream insert operation makes replay visible as duplicate business rows.
# Explicit projection excludes the upstream table's reserved Hudi metadata fields.
FLOWS = '''SET spark.hoodie.datasource.write.operation=upsert;
CREATE STREAMING TABLE upsert_events USING hudi
TBLPROPERTIES ('type'='{table_type}', 'primaryKey'='id', 'preCombineField'='ts')
AS SELECT id, ts, value FROM STREAM input_events;

SET spark.hoodie.datasource.write.operation=insert;
CREATE STREAMING TABLE downstream_events USING hudi
TBLPROPERTIES ('type'='{table_type}', 'primaryKey'='id', 'preCombineField'='ts')
AS SELECT id, ts, value FROM STREAM guarded_upstream;
'''


def verify(mode, remote, root):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(remote).getOrCreate()
    try:
        assert spark.conf.get("spark.hudi.test.run.id") == root.name
        if mode == "truncate-upstream":
            spark.sql("TRUNCATE TABLE sdp_test.upsert_events")
            return
        if mode == "truncate-graph":
            for table in ("upsert_events", "downstream_events"):
                spark.sql(f"TRUNCATE TABLE sdp_test.{table}")
            return
        if mode == "rebuilt":
            for table in ("upsert_events", "downstream_events"):
                actual = [tuple(row) for row in spark.table(f"sdp_test.{table}")
                          .select("id", "ts", "value").collect()]
                assert actual == [(1, 3, "reset")], (table, actual)
            return
        old = (1, 1, "old")
        new = (1, 2, "new")
        for table, expected in (
            ("upsert_events", [old] if mode == "first" else [new]),
            ("downstream_events", [old] if mode == "first" else [old, new]),
        ):
            if mode == "failed-reset" and table == "upsert_events":
                continue
            actual = [tuple(row) for row in spark.table(f"sdp_test.{table}")
                      .select("id", "ts", "value").orderBy("ts").collect()]
            assert actual == expected, (table, actual, expected)
            properties = dict(spark.sql(f"SHOW TBLPROPERTIES sdp_test.{table}").collect())
            assert not any("write.operation" in key for key in properties), properties
        print(f"Verified {mode}: incremental upstream-to-downstream rows through Spark Connect")
    finally:
        spark.stop()


def scenario(harness, table_type, metadata):
    flows = FLOWS.format(table_type=table_type)
    input_dir = harness.root / "input"
    input_dir.mkdir()
    (harness.root / "source.py").write_text(SOURCE)
    (harness.root / "flows.sql").write_text(flows)
    spec = {
        "name": "hudi_chaining",
        "storage": (harness.root / "checkpoints").as_uri(),
        "catalog": "spark_catalog",
        "database": "sdp_test",
        "libraries": [{"glob": {"include": "source.py"}}, {"glob": {"include": "flows.sql"}}],
        "configuration": {
            "hudi.test.input": input_dir.as_uri(),
            "spark.hoodie.datasource.read.streaming.fail.on.data.loss": "true",
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.pipelines.maxFlowRetryAttempts": "0",
            "spark.hoodie.metadata.enable": str(metadata).lower(),
            "spark.hoodie.datasource.compaction.async.enable": str(table_type == "mor").lower(),
            "spark.hoodie.compact.inline.max.delta.commits": "1",
        },
    }
    spec_path = harness.root / "spark-pipeline.yml"
    spec_path.write_text(json.dumps(spec, indent=2))
    for name in ("source.py", "flows.sql", "spark-pipeline.yml"):
        (harness.logs / name).write_text((harness.root / name).read_text())
    for name, timestamp, value in (("first", 1, "old"), ("second", 2, "new")):
        row = json.dumps({"id": 1, "ts": timestamp, "value": value}) + "\n"
        (input_dir / f"{name}.json").write_text(row * (2 if name == "first" else 1))
        harness.run_pipeline(spec_path, name=f"pipeline-{name}")
        harness.client(Path(__file__).resolve(), name)
    def commits():
        result = {}
        for table in ("upsert_events", "downstream_events"):
            timeline = harness.root / "warehouse" / "sdp_test.db" / table / ".hoodie" / "timeline"
            result[table] = sorted(p.name for p in timeline.iterdir()
                                   if p.suffix in (".commit", ".deltacommit", ".replacecommit"))
            assert result[table], f"No completed commits found for {table}"
        return result

    before = commits()
    if table_type == "mor":
        # The upsert update creates a log file and must actually exercise async compaction.
        assert any(name.endswith(".commit") for name in before["upsert_events"]), before
        # AvailableNow may capture its end offset before async compaction completes.
        # Drain that maintenance instant before asserting a quiescent restart.
        harness.run_pipeline(spec_path, name="pipeline-compaction-catchup")
        harness.client(Path(__file__).resolve(), "restart")
        caught_up = commits()
        assert before["upsert_events"] == caught_up["upsert_events"], (before, caught_up)
        (harness.logs / "compaction-catchup.json").write_text(
            json.dumps({"before": before, "after": caught_up}, indent=2))
        before = caught_up

    # Restart with no new input must leave both outputs and the completed timeline unchanged.
    harness.run_pipeline(spec_path, name="pipeline-restart")
    harness.client(Path(__file__).resolve(), "restart")
    after = commits()
    assert before == after, (before, after)
    (harness.logs / "restart-commits.json").write_text(json.dumps({"before": before, "after": after}, indent=2))
    downstream_before = commits()["downstream_events"]
    harness.client(Path(__file__).resolve(), "truncate-upstream")
    (input_dir / "third.json").write_text('{"id": 1, "ts": 3, "value": "reset"}\n')
    try:
        harness.run_pipeline(spec_path, name="pipeline-expected-reset-failure")
    except RuntimeError:
        log = (harness.logs / "pipeline-expected-reset-failure.log").read_text()
        assert "rebuild required" in log, log[-4000:]
        assert harness.server.poll() is None, "Source failure killed the Connect server"
    else:
        raise AssertionError("A recreated upstream table was accepted with the old checkpoint")
    assert commits()["downstream_events"] == downstream_before
    harness.client(Path(__file__).resolve(), "failed-reset")
    # All flows are stopped. Rebuild every affected table and start with fresh checkpoints.
    # The full-refresh CLI path separately depends on catalog DeleteColumn support (#20492).
    harness.client(Path(__file__).resolve(), "truncate-graph")
    spec["storage"] = (harness.root / "rebuild-checkpoints").as_uri()
    spec_path.write_text(json.dumps(spec, indent=2))
    (harness.logs / "rebuild-spec.json").write_text(spec_path.read_text())
    harness.run_pipeline(spec_path, name="pipeline-rebuild")
    harness.client(Path(__file__).resolve(), "rebuilt")
    print("PASS: guarded restart, reset rejection before downstream commit, and coordinated rebuild")


def main():
    if len(sys.argv) == 4 and sys.argv[1] in ("first", "second", "restart", "truncate-upstream", "truncate-graph", "rebuilt", "failed-reset"):
        verify(sys.argv[1], sys.argv[2], Path(sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--logs", type=Path, default=Path("target/source-safety-acceptance-logs"))
    parser.add_argument("--table-type", choices=("cow", "mor"), default="cow")
    parser.add_argument("--metadata", choices=("enabled", "disabled"), default="disabled")
    args = parser.parse_args()
    if not (args.harness_dir / "harness.py").is_file():
        parser.error("The #20453 harness is required; pass its spark-pipelines directory with --harness-dir")
    sys.path.insert(0, str(args.harness_dir.resolve()))
    from harness import ConnectHarness

    args.logs.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run-", dir=args.logs.resolve()))
    print(f"Source safety acceptance logs: {logs}", flush=True)

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    with ConnectHarness(args.bundle, logs) as harness:
        scenario(harness, args.table_type, args.metadata == "enabled")


if __name__ == "__main__":
    main()
