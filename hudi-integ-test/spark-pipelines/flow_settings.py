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

"""Run the #20403 SQL SET scenario using the #20402 Spark CLI/Connect harness."""

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
'''

# Only immutable identity belongs in the table properties. The operation is captured per flow.
FLOWS = '''SET spark.hoodie.datasource.write.operation=insert;
CREATE STREAMING TABLE insert_events USING hudi
TBLPROPERTIES ('type'='cow', 'primaryKey'='id', 'preCombineField'='ts')
AS SELECT id, ts, value FROM STREAM input_events;

SET spark.hoodie.datasource.write.operation=upsert;
CREATE STREAMING TABLE upsert_events USING hudi
TBLPROPERTIES ('type'='cow', 'primaryKey'='id', 'preCombineField'='ts')
AS SELECT id, ts, value FROM STREAM input_events;
'''


def verify(mode, remote, root):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(remote).getOrCreate()
    try:
        assert spark.conf.get("spark.hudi.test.run.id") == root.name
        old = (1, 1, "old")
        new = (1, 2, "new")
        for table, expected in (
            ("insert_events", [old] if mode == "first" else [old, new]),
            ("upsert_events", [old] if mode == "first" else [new]),
        ):
            actual = [tuple(row) for row in spark.table(f"sdp_test.{table}")
                      .select("id", "ts", "value").orderBy("ts").collect()]
            assert actual == expected, (table, actual, expected)
            properties = dict(spark.sql(f"SHOW TBLPROPERTIES sdp_test.{table}").collect())
            assert not any("write.operation" in key for key in properties), properties
        print(f"Verified {mode}: independent insert/upsert flows through SQL SET and Spark Connect")
    finally:
        spark.stop()


def scenario(harness):
    input_dir = harness.root / "input"
    input_dir.mkdir()
    (harness.root / "source.py").write_text(SOURCE)
    (harness.root / "flows.sql").write_text(FLOWS)
    spec = {
        "name": "hudi_flow_settings",
        "storage": (harness.root / "checkpoints").as_uri(),
        "catalog": "spark_catalog",
        "database": "sdp_test",
        "libraries": [{"glob": {"include": "source.py"}}, {"glob": {"include": "flows.sql"}}],
        "configuration": {
            "hudi.test.input": input_dir.as_uri(),
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.pipelines.maxFlowRetryAttempts": "0",
            "spark.hoodie.metadata.enable": "false",
        },
    }
    spec_path = harness.root / "spark-pipeline.yml"
    spec_path.write_text(json.dumps(spec, indent=2))
    for name in ("source.py", "flows.sql", "spark-pipeline.yml"):
        (harness.logs / name).write_text((harness.root / name).read_text())
    for name, timestamp, value in (("first", 1, "old"), ("second", 2, "new")):
        (input_dir / f"{name}.json").write_text(json.dumps({"id": 1, "ts": timestamp, "value": value}) + "\n")
        harness.run_pipeline(spec_path, name=f"pipeline-{name}")
        harness.client(Path(__file__).resolve(), name)
    # Restart with no new input must leave both outputs unchanged.
    harness.run_pipeline(spec_path, name="pipeline-restart")
    harness.client(Path(__file__).resolve(), "restart")


def main():
    if len(sys.argv) == 4 and sys.argv[1] in ("first", "second", "restart"):
        verify(sys.argv[1], sys.argv[2], Path(sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--logs", type=Path, default=Path("target/flow-settings-logs"))
    args = parser.parse_args()
    if not (args.harness_dir / "harness.py").is_file():
        parser.error("The #20402 harness is required; pass its spark-pipelines directory with --harness-dir")
    sys.path.insert(0, str(args.harness_dir.resolve()))
    from harness import ConnectHarness

    args.logs.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run-", dir=args.logs.resolve()))
    print(f"Flow settings logs: {logs}", flush=True)

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    with ConnectHarness(args.bundle, logs) as harness:
        scenario(harness)


if __name__ == "__main__":
    main()
