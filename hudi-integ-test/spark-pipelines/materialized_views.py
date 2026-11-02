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

"""Refresh Hudi materialized views three times through the real SDP CLI."""
import argparse
import json
from pathlib import Path
import sys
import tempfile

SOURCE = 'from pyspark import pipelines as dp\nfrom pyspark.sql import SparkSession\nspark = SparkSession.active()\n\n@dp.materialized_view(name="json_mv", format="hudi", table_properties={"type": "cow", "primaryKey": "id"})\ndef json_mv():\n    return spark.read.schema("id INT, value STRING").json(spark.conf.get("hudi.test.input"))\n\n@dp.materialized_view(name="hudi_mv", format="hudi", table_properties={"type": "cow", "primaryKey": "id"})\ndef hudi_mv():\n    return spark.read.format("hudi").load(spark.conf.get("hudi.test.source")).select("id", "value")\n'

def client(mode, remote, root):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote(remote).getOrCreate()
    try:
        assert spark.conf.get("spark.hudi.test.run.id") == root.name
        expected = json.loads((root / "expected.json").read_text())
        frame = spark.createDataFrame([tuple(row) for row in expected], "id INT, value STRING")
        if mode.startswith("prepare"):
            (frame.write.format("hudi").mode("overwrite")
             .option("hoodie.table.name", "mv_source")
             .option("hoodie.datasource.write.recordkey.field", "id")
             .option("hoodie.datasource.write.partitionpath.field", "")
             .option("hoodie.datasource.write.operation", "bulk_insert")
             .option("hoodie.metadata.enable", "false").save((root / "hudi-source").as_uri()))
        else:
            for name in ("json_mv", "hudi_mv"):
                actual = [list(row) for row in spark.table(f"sdp_test.{name}")
                          .select("id", "value").orderBy("id").collect()]
                assert actual == expected, (name, actual, expected)
                print(f"PASS {mode} {name}: {actual}")
    finally:
        spark.stop()


def scenario(h):
    source = h.root / "input"
    source.mkdir()
    (h.root / "mv.py").write_text(SOURCE)
    spec = {"name": "hudi_mv_refresh", "storage": (h.root / "checkpoints").as_uri(),
            "catalog": "spark_catalog", "database": "sdp_test",
            "libraries": [{"glob": {"include": "mv.py"}}],
            "configuration": {"hudi.test.input": source.as_uri(),
                              "hudi.test.source": (h.root / "hudi-source").as_uri(),
                              "spark.sql.shuffle.partitions": "2",
                              "spark.hoodie.metadata.enable": "false",
                              "spark.sql.pipelines.maxFlowRetryAttempts": "0"}}
    path = h.root / "spark-pipeline.yml"
    path.write_text(json.dumps(spec, indent=2))
    (h.logs / "mv.py").write_text(SOURCE)
    (h.logs / path.name).write_text(path.read_text())
    # Remove and change rows as well as adding them: stale append output must fail.
    for i, expected in enumerate(([[1, "one"], [2, "two"]],
                                   [[2, "updated"], [3, "three"]], [[4, "four"]]), 1):
        (h.root / "expected.json").write_text(json.dumps(expected))
        (source / "rows.json").write_text("".join(json.dumps(dict(zip(("id", "value"), row))) + "\n"
                                                   for row in expected))
        h.client(Path(__file__).resolve(), f"prepare-{i}")
        h.run_pipeline(path, f"refresh-{i}")
        h.client(Path(__file__).resolve(), f"verify-{i}")
    print("PASS: three exact MV refreshes over JSON and Hudi sources")


def main():
    if len(sys.argv) == 4 and sys.argv[1].startswith(("prepare-", "verify-")):
        client(sys.argv[1], sys.argv[2], Path(sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", required=True, type=Path)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--logs", type=Path, default=Path("target/mv-acceptance-logs"))
    args = parser.parse_args()
    sys.path.insert(0, str(args.harness_dir.resolve()))
    from harness import ConnectHarness
    args.logs.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run-", dir=args.logs.resolve()))
    print(f"MV logs: {logs}", flush=True)
    with ConnectHarness(args.bundle, logs) as harness:
        scenario(harness)


if __name__ == "__main__":
    main()
