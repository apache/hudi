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

"""Check MV refresh, schema evolution and failure recovery through the real SDP CLI."""
import argparse
import json
import signal
from pathlib import Path
import sys
import tempfile
from urllib.parse import unquote, urlparse

SOURCE = """from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.active()

@dp.materialized_view(name="json_mv", format="hudi", partition_cols=["value"],
                      table_properties={"type": "cow", "primaryKey": "id"})
def json_mv():
    return spark.read.schema("id INT, value STRING").json(spark.conf.get("hudi.test.input"))

@dp.materialized_view(name="hudi_mv", format="hudi", table_properties={"type": "cow", "primaryKey": "id"})
def hudi_mv():
    return spark.read.format("hudi").load(spark.conf.get("hudi.test.source")).select("id", "value")

@dp.materialized_view(name="aggregate_mv", format="hudi", table_properties={"type": "cow"})
def aggregate_mv():
    return (spark.read.schema("id INT, value STRING").json(spark.conf.get("hudi.test.input"))
            .groupBy("value").agg(F.sum("id").alias("total"), F.count("*").alias("rows")))
"""


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
        if mode.startswith("contract-"):
            contract_client(spark, mode, root)
            return
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
                require(actual == expected, (name, actual, expected))
                print(f"PASS {mode} {name}: {actual}")
            expected_aggregate = json.loads((root / "expected-aggregate.json").read_text())
            actual = [list(row) for row in spark.table("sdp_test.aggregate_mv")
                      .select("value", "total", "rows").orderBy("value").collect()]
            require(actual == expected_aggregate, ("aggregate_mv", actual, expected_aggregate))
            print(f"PASS {mode} aggregate_mv: {actual}")
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
    stages = (
        ([[1, "one"], [2, "one"], [3, "two"]], [["one", 3, 2], ["two", 3, 1]]),
        ([[2, "one"], [3, "three"], [4, "one"]], [["one", 6, 2], ["three", 3, 1]]),
        ([[4, "four"]], [["four", 4, 1]]),
        ([], []))
    for i, (expected, aggregate) in enumerate(stages, 1):
        (h.root / "expected-aggregate.json").write_text(json.dumps(aggregate))
        (h.root / "expected.json").write_text(json.dumps(expected))
        (source / "rows.json").write_text("".join(json.dumps(dict(zip(("id", "value"), row))) + "\n"
                                                   for row in expected))
        h.client(Path(__file__).resolve(), f"prepare-{i}")
        h.run_pipeline(path, f"refresh-{i}")
        h.client(Path(__file__).resolve(), f"verify-{i}")
    print("PASS: four exact keyed and keyless aggregate MV refreshes")


def contract_client(spark, mode, root):
    check = json.loads((root / "contract-check.json").read_text())
    location = Path(check["location"])
    if mode == "contract-prepare":
        if check["external"]:
            spark.sql("CREATE TABLE sdp_test.contract_mv (id INT, value STRING) USING hudi "
                      "PARTITIONED BY (value) TBLPROPERTIES ('type'='cow', 'primaryKey'='id') "
                      f"LOCATION '{location.as_uri()}'").collect()
        return
    details = {row.col_name.strip(): row.data_type for row in
               spark.sql("DESCRIBE TABLE EXTENDED sdp_test.contract_mv").collect()}
    require(details["Type"] == ("EXTERNAL" if check["external"] else "MANAGED"), details)
    require(Path(unquote(urlparse(details["Location"]).path)).resolve() == location.resolve(), details)
    frame = spark.table("sdp_test.contract_mv")
    actual = [list(row) for row in frame.select(*check["columns"]).orderBy("id").collect()]
    require(actual == check["rows"], (mode, actual, check["rows"]))
    properties = (location / ".hoodie" / "hoodie.properties").read_text().splitlines()
    require("hoodie.table.recordkey.fields=id" in properties, properties)
    require("hoodie.table.partition.fields=value" in properties, properties)
    timeline = sorted(p.name for p in (location / ".hoodie" / "timeline").glob("*"))
    print(f"STATE {mode}: location={location}, timeline={timeline}, schema={frame.schema.json()}")
    if check["rows"]:
        # Check both catalog and persisted schema, not only projected query output.
        persisted = spark.read.format("hudi").load(location.as_uri())
        expected_schema = {name: "int" if name == "id" else "string" for name in check["columns"]}
        for schema in (frame.schema, persisted.schema):
            business = {field.name: field.dataType.simpleString() for field in schema
                        if not field.name.startswith("_hoodie_")}
            require(business == expected_schema, (mode, business, expected_schema))
    print(f"PASS {mode}: {details['Type']}, rows={actual}, columns={check['columns']}")


def contract_scenario(h, external):
    """Exercise schema reconciliation and real failures on one isolated MV."""
    source = h.root / "contract-input"
    source.mkdir()
    definition = ("from pyspark import pipelines as dp\nfrom pyspark.sql import SparkSession\n"
                  "spark = SparkSession.active()\n@dp.temporary_view(name='contract_input')\n"
                  "def contract_input():\n"
                  "    return spark.read.schema('id INT, value STRING, extra STRING')"
                  ".json(spark.conf.get('hudi.test.contract.input'))\n")
    (h.root / "contract.py").write_text(definition)
    (h.logs / "contract.py").write_text(definition)
    spec = {"name": "hudi_mv_contract", "storage": (h.root / "contract-checkpoints").as_uri(),
            "catalog": "spark_catalog", "database": "sdp_test",
            "libraries": [{"glob": {"include": "contract.py"}}, {"glob": {"include": "contract.sql"}}],
            "configuration": {"hudi.test.contract.input": source.as_uri(),
                              "spark.sql.shuffle.partitions": "2", "spark.hoodie.metadata.enable": "false",
                              "spark.sql.pipelines.maxFlowRetryAttempts": "0"}}
    spec_path = h.root / "contract.yml"
    spec_path.write_text(json.dumps(spec, indent=2))
    (h.logs / spec_path.name).write_text(spec_path.read_text())
    location = h.root / "external-mv" if external else h.root / "warehouse" / "sdp_test.db" / "contract_mv"
    check = {"external": external, "location": str(location), "columns": ["id", "value"], "rows": []}

    def verify(name, rows, columns):
        check.update(rows=rows, columns=columns)
        (h.root / "contract-check.json").write_text(json.dumps(check))
        (h.logs / f"{name}-expected.json").write_text(json.dumps(check, indent=2))
        h.client(Path(__file__).resolve(), name)

    def flow(selection="id, value, extra", key="id", partition="value"):
        return (f"CREATE MATERIALIZED VIEW contract_mv USING hudi PARTITIONED BY ({partition}) "
                f"TBLPROPERTIES ('type'='cow', 'primaryKey'='{key}') "
                f"AS SELECT {selection} FROM contract_input;")

    def run(name, sql, error=None):
        (h.root / "contract.sql").write_text(sql)
        (h.logs / f"{name}.sql").write_text(sql)
        try:
            h.run_pipeline(spec_path, name)
        except RuntimeError:
            if error is None:
                raise
            log = (h.logs / f"{name}.log").read_text()
            require(all(marker in log for marker in error), (name, "Unexpected failure", log[-4000:]))
            require(h.server.poll() is None, "Expected failure killed the Connect server")
        else:
            require(error is None, f"{name} unexpectedly succeeded")

    def commits():
        return sorted(p.name for p in (location / ".hoodie" / "timeline").glob("*")
                      if p.suffix in (".commit", ".deltacommit", ".replacecommit"))

    def input_rows(rows):
        (source / "rows.json").write_text("".join(json.dumps(dict(zip(
            ("id", "value", "extra"), row))) + "\n" for row in rows))

    verify("contract-prepare", [], ["id", "value"])
    input_rows([[1, "one", "old"], [2, "two", "old"]])
    run("contract-initial", flow("id, value"))
    verify("contract-verify-initial", [[1, "one"], [2, "two"]], ["id", "value"])
    rows = [[1, "one", "new"], [3, "three", "added"]]
    columns = ["id", "value", "extra"]
    input_rows(rows)
    run("contract-add-column", flow())
    verify("contract-verify-add-column", rows, columns)
    run("contract-unchanged", flow())
    verify("contract-verify-unchanged", rows, columns)

    # Resolution and partition validation happen before destructive refresh.
    for name, sql, error in (
            ("resolution", flow("missing_mv_column, value, extra"), ("UNRESOLVED_COLUMN", "missing_mv_column")),
            ("partition", flow(partition="id"), ("CANNOT_UPDATE_PARTITION_COLUMNS",))):
        before = commits()
        require(before, "Expected committed data before failure")
        run(f"contract-fail-{name}", sql, error)
        require(commits() == before, (name, "Failure changed completed timeline", before, commits()))
        verify(f"contract-verify-{name}", rows, columns)
        run(f"contract-retry-{name}", flow())
        verify(f"contract-verify-retry-{name}", rows, columns)

    # SDP truncates before catalog reconciliation. A rejected identity change
    # therefore exposes the empty window even though the old identity survives.
    run("contract-fail-reconciliation", flow(key="extra"), ("hoodie.table.recordkey.fields", "can't be changed"))
    require(not commits(), ("Reconciliation failure left completed writes", commits()))
    verify("contract-verify-reconciliation", [], columns)
    run("contract-retry-reconciliation", flow())
    verify("contract-verify-retry-reconciliation", rows, columns)

    # Keep the fault dependent on input so it occurs during execution, not folding.
    selection = ("id, value, CASE WHEN id = 1 THEN "
                 "CAST(raise_error('MV_WRITE_FAILURE') AS STRING) ELSE extra END AS extra")
    run("contract-fail-write", flow(selection), ("USER_RAISED_EXCEPTION", "MV_WRITE_FAILURE"))
    require(not commits(), ("Failed result write was committed", commits()))
    verify("contract-verify-write", [], columns)
    input_rows([])
    run("contract-retry-empty", flow())
    verify("contract-verify-retry-empty", [], columns)
    input_rows(rows)
    run("contract-after-empty", flow())
    verify("contract-verify-after-empty", rows, columns)
    print("PASS: MV schema, identity, failure boundaries and retries")


def main():
    if len(sys.argv) == 4 and sys.argv[1].startswith(("prepare-", "verify-", "contract-")):
        client(sys.argv[1], sys.argv[2], Path(sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", required=True, type=Path)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--logs", type=Path, default=Path("target/mv-acceptance-logs"))
    parser.add_argument("--external", action="store_true",
                        help="Manually run the contract matrix against a pre-created external target")
    args = parser.parse_args()
    sys.path.insert(0, str(args.harness_dir.resolve()))
    from harness import ConnectHarness
    args.logs.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run-", dir=args.logs.resolve()))
    print(f"MV logs: {logs}", flush=True)
    signal.signal(signal.SIGTERM, interrupted)
    with ConnectHarness(args.bundle, logs) as harness:
        scenario(harness)
        contract_scenario(harness, args.external)


if __name__ == "__main__":
    main()
