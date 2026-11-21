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

"""Verify a flow-scoped commit cap through the Spark pipelines CLI and Connect."""
import argparse
import json
from pathlib import Path
import signal
import sys
import tempfile


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def client(mode, remote, root):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote(remote).getOrCreate()
    try:
        require(spark.conf.get("spark.hudi.test.run.id") == root.name, "Wrong Connect server")
        if mode == "prepare":
            table_type = (root / "table-type").read_text()
            spark.sql(f"CREATE TABLE sdp_test.upstream (id INT, ts BIGINT, value STRING) USING hudi "
                      f"TBLPROPERTIES ('type'='{table_type}', 'primaryKey'='id', 'preCombineField'='ts')").collect()
            for i in range(1, 4):
                spark.sql(f"INSERT INTO sdp_test.upstream VALUES ({i}, {i}L, 'value{i}')").collect()
        elif mode == "append":
            spark.sql("INSERT INTO sdp_test.upstream VALUES (4, 4L, 'value4')").collect()
        else:
            count = 4 if mode == "verify-next" else 3
            actual = [tuple(row) for row in spark.table("sdp_test.downstream")
                      .select("id", "ts", "value").orderBy("id").collect()]
            require(actual == [(i, i, f"value{i}") for i in range(1, count + 1)], actual)
            print(f"PASS {mode}: {actual}")
    finally:
        spark.stop()


def scenario(h, table_type):
    (h.root / "table-type").write_text(table_type)
    h.client(Path(__file__).resolve(), "prepare")
    (h.root / "source.py").write_text('''from pyspark import pipelines as dp
from pyspark.sql import SparkSession
spark = SparkSession.active()
@dp.temporary_view(name="input_events")
def input_events():
    return spark.readStream.table("sdp_test.upstream").select("id", "ts", "value")
''')
    (h.root / "flows.sql").write_text(
        "SET spark.hoodie.datasource.write.operation=insert;\n"
        f"CREATE STREAMING TABLE downstream USING hudi TBLPROPERTIES ('type'='{table_type}', "
        "'primaryKey'='id', 'preCombineField'='ts') AS SELECT * FROM STREAM input_events;")
    spec = {"name": "capped_source", "storage": (h.root / "checkpoints").as_uri(),
            "catalog": "spark_catalog", "database": "sdp_test",
            "libraries": [{"glob": {"include": "source.py"}}, {"glob": {"include": "flows.sql"}}],
            "configuration": {"spark.hoodie.datasource.read.streaming.max.commits.per.trigger": "1",
                              "spark.sql.shuffle.partitions": "2", "spark.hoodie.metadata.enable": "true",
                              "spark.hoodie.datasource.write.streaming.disable.compaction": "true"}}
    path = h.root / "spark-pipeline.yml"
    path.write_text(json.dumps(spec, indent=2))
    for name in ("source.py", "flows.sql", path.name):
        (h.logs / name).write_text((h.root / name).read_text())
    def commits():
        timeline = h.root / "warehouse" / "sdp_test.db" / "downstream" / ".hoodie" / "timeline"
        return sorted(p.name for p in timeline.iterdir() if p.suffix in (".commit", ".deltacommit"))
    h.run_pipeline(path, "capped-backlog")
    h.client(Path(__file__).resolve(), "verify-backlog")
    before = commits()
    require(len(before) == 3, ("Three source commits must produce three capped batches", before))
    h.run_pipeline(path, "restart")
    h.client(Path(__file__).resolve(), "verify-restart")
    require(commits() == before, "Restart replayed committed input")
    h.client(Path(__file__).resolve(), "append")
    h.run_pipeline(path, "next")
    h.client(Path(__file__).resolve(), "verify-next")
    after = commits()
    require(len(after) == 4 and set(before).issubset(after), (before, after))
    (h.logs / "commits.json").write_text(json.dumps({"backlog": before, "after_next": after}, indent=2))
    print("PASS: SQLConf cap drains backlog as three batches, restart is idle, new input advances once")


def main():
    if len(sys.argv) == 4 and sys.argv[1] in ("prepare", "append", "verify-backlog", "verify-restart", "verify-next"):
        client(sys.argv[1], sys.argv[2], Path(sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--table-type", choices=("cow", "mor"), default="cow")
    parser.add_argument("--logs", type=Path, default=Path("target/source-throttling-logs"))
    args = parser.parse_args()
    from harness import ConnectHarness
    args.logs.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run-", dir=args.logs.resolve()))
    print(f"Throttling logs: {logs}", flush=True)
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Received signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    with ConnectHarness(args.bundle, logs) as h:
        scenario(h, args.table_type)


if __name__ == "__main__":
    main()
