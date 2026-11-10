<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Spark Connect / Declarative Pipelines integration harness

The nightly harness runs the **real `spark-pipelines` CLI** against a separately launched
Spark 4.1.1 Connect JVM with the candidate Hudi bundle. It supports Linux and
macOS, JDK 17, and Python 3.10 or newer (CI uses Python 3.11).
The server and clients share a local filesystem; no external storage or metastore
is required. Use a dedicated Python virtual environment.

## Two test layers

Layer 2 invokes `python -m pyspark.pipelines.cli` with `SPARK_REMOTE`; layer 3
also exercises the `spark-pipelines` launcher. Both use PySpark 4.1.1. Neither substitutes
in-JVM `DataflowGraph` calls for the client/server protocol.

| Layer | Server | Coverage | Execution |
| --- | --- | --- | --- |
| 2: `TestHoodieSparkConnectPipelines` | Connect service in the ScalaTest JVM, Hudi on the Maven test classpath | MV/ST exact rows, independent SQL flow insert/upsert settings, null-key failure and a fresh remote query afterward | Focused Maven job on PRs, pushes and merge groups |
| 3: `harness.py` | Separate `spark-submit` JVM with the candidate bundle | Bundle packaging, static server configuration, remote writes/reads, process ownership and cleanup | Nightly at 03:23 UTC and `workflow_dispatch` |

Spark 4.1 remains disabled in the broad Java CI matrices. The existing focused
`test-spark-connect-pipelines` job now builds only the Spark Maven test dependency
closure and runs the three layer-2 tests, without producing a shaded bundle.
It is included in the existing `Java CI result` aggregate. This still costs a
Spark 4.1 reactor build and a cached PySpark installation; it does not restore all
Spark 4.1 test shards or make those costs disappear.

The separate `Spark Connect nightly` workflow retries the remote smoke test once
with fresh storage/processes and retains both attempts' logs. Build/install
failures are reported directly. Scheduled runs are restricted to `apache/hudi`;
manual runs also work on forks. There is no global merge gate, issue bot, or
label-triggered PR bypass in this change. Whether a failed nightly should block
unrelated merges remains a separate PMC decision.

### Run the layer-2 suite locally

With JDK 17, install the pinned `requirements.txt` in a Python environment, then:

```sh
mvn install -T 2 -Pjava17 -Dspark4.1 -Dscala-2.13 -DskipTests \
  -Dgpg.skip -Djacoco.skip -am -pl hudi-spark-datasource/hudi-spark
HUDI_SDP_PYTHON=/path/to/venv/bin/python HUDI_SDP_REQUIRE_PYTHON=true \
  mvn test -Punit-tests -Pjava17 -Dspark4.1 -Dscala-2.13 \
  -Dtest=skipJavaTests -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsuites=org.apache.spark.sql.connect.hudi.TestHoodieSparkConnectPipelines \
  -Dgpg.skip -Djacoco.skip -pl hudi-spark-datasource/hudi-spark
```

`HUDI_SDP_PYTHON` defaults to `python3`. Without usable Python dependencies the
suite cancels its tests locally; `HUDI_SDP_REQUIRE_PYTHON=true` makes that a failure
and is always set in CI. Only the Spark 4.1 profile compiles the suite or resolves
the test-scoped Connect server dependency. Other Spark profiles are unaffected.
The server binds an ephemeral loopback port and is stopped with the test session.
Each CLI has a 180-second timeout; dependency and post-failure probes have shorter
bounds. Generated pipelines and CLI logs remain under
`hudi-spark-datasource/hudi-spark/target/sdp-layer2`, including after failures.

## Run the layer-3 harness locally

From the repository root, with `JAVA_HOME` pointing to JDK 17:

```sh
mvn package -T 2 -Pjava17 -Dspark4.1 -Dscala-2.13 \
  -DskipTests -Dgpg.skip -Djacoco.skip -am -pl packaging/hudi-spark-bundle

python3 -m venv /tmp/hudi-sdp-venv
/tmp/hudi-sdp-venv/bin/python -m pip install \
  -r hudi-integ-test/spark-pipelines/requirements.txt

# Set this to the non-sources/non-javadoc jar produced by the build above.
HUDI_BUNDLE="$PWD/packaging/hudi-spark-bundle/target/hudi-spark4.1-bundle_2.13-1.3.0-SNAPSHOT.jar"
/tmp/hudi-sdp-venv/bin/python hudi-integ-test/spark-pipelines/harness.py \
  --bundle "$HUDI_BUNDLE"
```

The last command starts the server, probes it with a remote query, runs the
pipeline, reads the Hudi output, and stops the server. It uses the Spark server
jars and executables installed by the pinned PySpark wheel; there is no separate
Spark distribution to download and no released Hudi bundle is substituted.
An inherited `SPARK_HOME` or Spark defaults file is not used.

`pipeline.py` declares one Hudi streaming table over a deterministic JSON input
with three rows. SDP controls the AvailableNow execution and checkpoint location.
A second Connect client checks all business columns by both catalog name and Hudi
path, verifies `.hoodie/hoodie.properties`, and the harness checks that SDP wrote
a streaming offset under its checkpoint directory. Every run starts with new
table, warehouse, input, Spark scratch, and checkpoint directories.

This is infrastructure smoke coverage for [#20402](https://github.com/apache/hudi/issues/20402).
It does not establish the full RFC-112 contract: repeat materialized-view refresh,
streaming configuration forwarding, restart/full-refresh semantics, chaining,
and keyed update/delete acceptance belong to the sibling tasks under
[#20401](https://github.com/apache/hudi/issues/20401).

## Configuration ownership

| Configuration | Owner / location |
| --- | --- |
| Candidate bundle, Hudi session extension and `spark_catalog`, Kryo serializer/registrator | `ConnectHarness.server_configuration()` and server `spark-submit` arguments, before JVM startup |
| Warehouse, in-memory catalog, local Hadoop filesystem, driver binding, Connect port, scratch storage | Server startup configuration |
| Pipeline storage/checkpoint root, catalog and database | Generated `spark-pipeline.yml` |
| Session shuffle partitions, SDP retry limit, input path | Pipeline YAML `configuration` |
| Flow-specific shuffle partitions | `@dp.table(spark_conf=...)` in `pipeline.py` |
| Hudi table type, record key, precombine field | Dataset `table_properties` in `pipeline.py` |

Static server settings must not be passed through the pipeline YAML or CLI
`--conf`. The readiness client verifies the startup extension/catalog and that
the extension is not session-modifiable. The CLI always receives
`--remote sc://127.0.0.1:<port>`; it must not start its own local server.
All data paths are absolute URIs accessible to the server and its local executors.
Tests for other storage backends must install connectors and configure storage
access on the server at startup, before creating client sessions.

## Failures and cleanup

`--startup-timeout` (default 90 seconds) bounds the port wait and the readiness
client individually. `--client-timeout` (default 180 seconds) bounds each CLI or
verification process, including Spark Connect retries. SDP flow retries are
disabled in this smoke test to report the first failure. A server exit interrupts
a waiting client. All failures produce a nonzero harness exit code.

Each subprocess starts in its own process group. On success, exceptions,
timeouts, Ctrl-C, or SIGTERM, the harness sends SIGTERM and then SIGKILL after a
five-second grace period if necessary, reaps the launcher, and removes the test
directories. It does not use `pkill`, Spark's shared PID files, or the global
`stop-connect-server.sh`. A force-killed harness cannot run cleanup; the CI job
has an outer timeout and runs on a disposable runner.

Logs survive cleanup in a unique run directory under `target/spark-pipelines-logs`
(override with `--logs`). `server.log`, `probe.log`, `pipeline.log`, `verify.log`,
the server configuration, and the generated pipeline specification provide the
execution context. On failure, the final 60 lines of each available process log
are printed. CI uploads the log directory on both success and failure.

## Extend the suite

Use `ConnectHarness` as a context manager. Its `root` is isolated local storage;
`remote` is the explicit Connect URL. Write a scenario's definitions and spec
under `root`, then call `run_pipeline(spec, name)` and `client(script, name)`.
Each client script receives `name`, `remote`, and `root` as positional arguments.
Use distinct log names for successive calls. Spark calls belong in those bounded
client subprocesses, never in the supervising harness. Keep assertions in the
scenario's verification client and let failures propagate.

Fast lifecycle regression tests require only the Python standard library:

```sh
python3 -B -m unittest discover -s hudi-integ-test/spark-pipelines -p 'test_*.py' -v
```

The `.github/workflows/spark_connect_pipelines.yml` nightly/manual workflow runs
these lifecycle tests, builds the Spark 4.1 / Scala 2.13 bundle from the checked-out
revision, and executes the separate-server harness with the pinned client. To
validate a candidate before merge, run the documented command locally or manually
dispatch the workflow on a branch containing it. The per-PR Maven suite is in
`.github/workflows/bot.yml`.
