<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Hudi Spark Connect endpoint

Deploys Spark Connect 4.1.3 with Hudi 1.2.0, Scala 2.13, and Java 17.
Tables use an external Hive Metastore and S3-compatible storage.
See the [Dockerfile](../../images/spark-connect/Dockerfile) for dependency versions.
The delivery scope is interactive SQL reads and writes, not general Spark job
submission or a required DataFrame-write integration.

## Local-dev quickstart

Requires Docker, minikube, Helm, and kubectl. Run from the repository root:

```bash
# Start MinIO, Hive Metastore, and Spark Connect
./hudi-lakehouse/scripts/quickstart.sh --spark-connect

# Also write example data and query it through Spark Connect (requires Maven and a JDK)
./hudi-lakehouse/scripts/quickstart.sh --run-example --spark-connect
```

The example uses the existing Spark writer to populate and register `default.trips`,
then prints city counts: `chennai = 33`, `san_francisco = 34`, `sao_paulo = 33`.
The query runs through PySpark inside the endpoint pod; no host client is needed.
Omit `--spark-connect` to query through Trino instead.

To rebuild the image and reload the running endpoint:

```bash
./hudi-lakehouse/scripts/quickstart.sh --spark-connect --rebuild-images
kubectl -n hudi-lakehouse rollout restart deployment/hudi-spark-connect
kubectl -n hudi-lakehouse rollout status deployment/hudi-spark-connect
```

## Install against existing infrastructure

Build an image and supply your metastore, warehouse, and storage credentials:

```bash
./hudi-lakehouse/scripts/build-connect-image.sh --registry my.registry/team --push
helm upgrade --install hudi-spark-connect hudi-lakehouse/charts/hudi-spark-connect \
  --namespace hudi-lakehouse --create-namespace \
  --set image.repository=my.registry/team/hudi-lakehouse-spark-connect \
  --set catalog.metastoreUri=thrift://my-hms:9083 \
  --set catalog.warehouse=s3a://my-bucket/connect \
  --set storage.s3.existingSecret=my-s3-credentials \
  --wait --timeout 10m
```

The Secret must be in the same namespace and contain `accessKey` and `secretKey`.
For temporary credentials, also set `storage.s3.sessionTokenKey`.
Without a Secret, Hadoop uses its default credential provider chain.
Local-dev credentials are for development only.

Other settings in [values.yaml](values.yaml):

| Setting | Purpose |
|---|---|
| `storage.s3.endpoint`, `region`, `pathStyleAccess` | MinIO or custom S3 endpoints |
| `spark.cores` | Local worker threads |
| `spark.master` | Empty means `local[cores]`; opt in to a remote master explicitly |
| `spark.driverMemory` | JVM heap; keep the pod memory limit higher |
| `resources` | Driver pod CPU, memory, and optional ephemeral-storage requests/limits |
| `scratch.sizeLimit` | Optional disk-backed `/tmp` volume limit, e.g. `20Gi`; empty by default |
| `spark.extraConf` | Additional Spark properties; no credentials or overrides of chart-owned settings |
| `hudi.defaults` | Server-side `hoodie.*` defaults loaded from `HUDI_CONF_DIR/hudi-defaults.conf` |
| `serviceAccount.name`, `automountToken` | Existing driver account and token mounting; required for Kubernetes executors |

By default the endpoint runs one shared driver in local mode, without separate executors.
Keep one replica. Restarting it ends active sessions; tables remain in the
external metastore and object store.

Size `/tmp` for peak concurrent shuffle/spill and temporary write data, with headroom.
For example, `--set scratch.sizeLimit=20Gi` caps the driver's scratch volume; it does
not reserve disk. Set `resources.requests.ephemeral-storage` for scheduling and
`resources.limits.ephemeral-storage` for the pod budget, allowing for logs and the
writable container layer as well. Node disk pressure can evict the pod before the
volume reaches its limit. Scratch data is lost with the pod; this is not table storage.
In cluster mode, size executor scratch separately; this setting applies only to the driver.

The service has no authentication or TLS. Keep it on a trusted network:
any client that can connect can read and write using the server's storage credentials.
In Kubernetes executor mode, this trust also extends to the driver's ServiceAccount
permissions, including creating executor pods through the Kubernetes API.

## Hudi writer defaults and table services

The generic chart does not pin a table version. The local-dev values set:

```yaml
hudi:
  defaults:
    hoodie.write.table.version: "8"
    hoodie.write.auto.upgrade: "false"
```

This keeps new tables compatible with the local-dev Trino reader. Hudi 1.2.0
otherwise defaults to table version 9. These are defaults, not enforcement:
explicit session/table settings can override them. Changing them does not downgrade
existing tables. Verify the versions supported by every reader before changing the pin.
The smoke test deliberately does not repeat the pin in `CREATE TABLE`.

Use `hudi.defaults` for deployment-wide defaults, `TBLPROPERTIES` for table-specific
settings, and session `SET` for a writer operation. For example:

```sql
CREATE TABLE orders (id INT, amount INT, ts BIGINT) USING hudi
TBLPROPERTIES (type = 'cow', primaryKey = 'id', preCombineField = 'ts',
               'hoodie.clustering.inline' = 'false');

SET hoodie.clean.automatic = true;
INSERT INTO orders VALUES (1, 100, 1);
RESET hoodie.clean.automatic;
```

Trino needs partition metadata in HMS as well as the Spark catalog entry. Ensure
the writer syncs partitions to HMS. To configure sync explicitly, add these
`TBLPROPERTIES` when creating a table (use your metastore URI):

```sql
'hoodie.datasource.meta.sync.enable' = 'true',
'hoodie.datasource.hive_sync.enable' = 'true',
'hoodie.datasource.hive_sync.mode' = 'hms',
'hoodie.datasource.hive_sync.metastore.uris' = 'thrift://hive-metastore.hudi-lakehouse.svc.cluster.local:9083'
```

The SQL matrix includes partition sync and Trino readback. These properties are not
enabled by the generic chart or local-dev defaults.

Table properties and session settings take precedence over global defaults in
the Spark SQL write configuration; consult the relevant Hudi operation's settings
when overriding them. Quote Helm property values when exact numeric text matters.
Do not store credentials in `spark.extraConf` or `hudi.defaults`: both are ConfigMaps.
Configuration changes roll the driver and end existing sessions.
Hudi's defaults-file parser keeps backslashes literally but normalizes whitespace;
the chart rejects leading/trailing or repeated whitespace rather than silently
changing such values. This differs from Spark's Java-properties escaping.

The endpoint does not schedule separate table-service jobs. Services enabled on a
SQL writer run according to Hudi's configuration in the shared Spark application.
In Hudi 1.2.0 inline compaction and inline clustering default to **off**; automatic
cleaning is enabled by default. Enabling inline compaction/clustering adds work to
the write path and can affect interactive-query latency. For example, a MoR writer
can set `hoodie.compact.inline=true` and `hoodie.compact.inline.max.delta.commits=2`.
For externally managed compaction/clustering, the operator must arrange both
scheduling and execution; this chart creates neither a scheduler nor a worker job.

The caller/operator owns writer configuration, concurrency mode and lock provider
across **all** writers, including external Spark/Flink jobs. Separate Connect sessions
do not provide multi-writer coordination. This endpoint adds no locking policy,
workload isolation, or custom recovery guarantees. Embedded timeline-server reuse
(`hoodie.embed.timeline.server.reuse.enabled`) is not enabled here and needs separate
evaluation before changing its lifecycle in a long-running driver.

## Kubernetes executor configuration (opt-in)

The chart exposes the configuration needed for a shared driver with separate
executors. Local mode remains the tested default; rendering this configuration is
**not** evidence of a validated cluster deployment.

```yaml
spark:
  master: k8s://https://kubernetes.default.svc:443
  extraConf:
    spark.executor.instances: "2"
    spark.executor.cores: "1"
    spark.executor.memory: 1g
serviceAccount:
  name: spark-connect-driver
  automountToken: true
```

Provision this ServiceAccount and its executor-management RBAC in the release
namespace first. The chart does not grant permissions. In Kubernetes client mode,
it advertises the driver's Pod IP and Pod name, selects the release namespace,
and defaults the executor image to the same Hudi-enabled image. The image delegates
executor startup to Spark's base entrypoint. Rebuild it when updating the entrypoint;
an older image will not provide that behavior. Executors receive the configured S3
Secret references, including a session token when present, without embedding values
in Spark properties. The Hudi defaults file is loaded by the SQL driver; the resolved
write configuration is passed by Hudi to tasks.
Set `spark.kubernetes.authenticate.executor.serviceAccountName` to a dedicated,
minimal-permission executor ServiceAccount; otherwise executors use the namespace's
default account. Disable token automount on that account unless the workload needs it.

Before using this mode, validate driver/executor networking, resource capacity,
image availability/pull credentials, and storage access from executor pods. Use
`spark.kubernetes.executor.container.image` for a different compatible executor image,
and Spark's executor settings for placement, workload identity, or
credentials other than the chart's S3 Secret. Driver pod settings do not automatically
apply to executors. Keep executors in the release namespace when using its Secrets.
The chart defaults executor pull secrets from `imagePullSecrets`; override
`spark.kubernetes.container.image.pullSecrets` for a different comma-separated list.
Dynamic allocation/autoscaling is not enabled by this example and needs separate
configuration and validation. Keep one driver replica; this is not a tenant-isolated service.
Standalone (`spark://`) masters are not supported by this chart.

## Connect and validate with ADBC

Forward the endpoint for local clients:

```bash
kubectl -n hudi-lakehouse port-forward svc/hudi-spark-connect 15002:15002
```

| Client | URI |
|---|---|
| PySpark | `sc://localhost:15002` |
| Foundry ADBC | `spark://localhost:15002?api=connect&auth_type=none&tls=false` |

ADBC requires `api=connect`; its default is Thrift. In-cluster clients can use
`hudi-spark-connect.hudi-lakehouse.svc:15002` without port-forwarding.
The ADBC driver is installed on the client, not the server.

For the CRUD and restart test, prepare a Python 3.11 environment:

```bash
python3.11 -m venv .venv-connect
. .venv-connect/bin/activate
pip install -r hudi-lakehouse/local-dev/example/connect-requirements.txt dbc==0.3.1
dbc install 'spark=0.2.1'
```

Stop any manual port-forward before running the test; it manages its own:

```bash
PYTHON="$VIRTUAL_ENV/bin/python" \
  ./hudi-lakehouse/local-dev/scripts/test-spark-connect.sh --restart-endpoint
```

The test creates a temporary table, checks INSERT/UPDATE/DELETE/MERGE, restarts the
endpoint, verifies persistence, and cleans up. It interrupts active sessions.
On failure, follow the printed cleanup instructions.

Additional checks against a local-dev deployment:

```bash
PYTHON="$VIRTUAL_ENV/bin/python" \
  ./hudi-lakehouse/local-dev/scripts/test-spark-connect.sh \
  --restart-endpoint --sql-matrix --freshness --trino
```

- `--sql-matrix`: CoW time travel and incremental TVF, partitioned CoW with HMS
  sync, MoR snapshot/read-optimized reads and a completed inline compaction. Checks
  stored table version 8 without statement-level version overrides; set
  `EXPECTED_TABLE_VERSION=9` when deliberately testing a v9 deployment.
- `--trino`: implies `--sql-matrix`; compares CoW and partitioned-table rows through
  an **already running** `hudi-trino` deployment (override `TRINO_DEPLOYMENT` if needed).
  Connect-only quickstart does not install Trino. An unavailable/incompatible Trino
  fails the check rather than skipping it. This tests Connect-created tables, not
  just external-writer tables queried by both engines.
- `--freshness`: keeps one ADBC connection open while a separate Spark JVM commits
  data, adds a column, and then recreates its test table. The supplied fixture creates
  short-lived writer pods using the same pinned image and shared catalog/storage;
  it is **not** another Connect session and does not test Spark 3.5/Flink compatibility.
  Allow an additional 1 GiB memory request (1.5 GiB limit), CPU, and permissions to
  create/delete pods and ConfigMaps. Only uniquely named test resources are touched.

Freshness checks do not automatically refresh or reconnect. If they fail on stale
metadata, rerun with `--freshness --refresh` to test explicit `REFRESH TABLE` after
each external change. Record which mode passes for your version combination;
do not claim automatic visibility based on a refresh-assisted run. The fixture
does not alter shared example tables, and cleanup never uses `DROP DATABASE CASCADE`.
Other external writers can use the Python CLI's `--external-writer <command argv>`
hook; it receives the test database and `commit-schema` / `recreate` action.

These acceptance paths require live Spark/ADBC (and optionally Trino). Offline
tests validate their assertions, configuration and cleanup, not engine behavior.
With Spark 4.1.3, Hudi 1.2.0 and Foundry 0.2.1, the live SQL matrix, Trino 472
readback of CoW and partitioned tables, and endpoint restart checks passed.
An existing session did not see an externally added column until
`REFRESH TABLE`; with explicit refresh, external writes, schema changes and table
recreation passed. Further end-to-end validation is tracked in
[#20233](https://github.com/apache/hudi/issues/20233).

<a id="spark-connect-client-tutorials"></a>

## BI clients

Gateway integration and BI adapters/examples for Superset and other tools are tracked
in [#20232](https://github.com/apache/hudi/issues/20232) and will be delivered in follow-up PRs.
The initial SQLAlchemy adapter is planned under `hudi-lakehouse`; upstreaming can
be evaluated separately after the integration stabilizes.

- **Superset 5.0:** requires a custom SQLAlchemy dialect and engine spec for ADBC;
  neither is included here. The built-in PyHive connector uses Thrift, not Connect.
  See [Superset engine specs](https://github.com/apache/superset/blob/5.0.0/superset/db_engine_specs/README.md).
- **Zeppelin 0.12:** use ADBC from the `%python` interpreter, with the client
  packages above installed in its Python environment. See the
  [Python interpreter guide](https://zeppelin.apache.org/docs/0.12.0/interpreter/python.html).

[Foundry Spark ADBC 0.2.1](https://adbc-drivers.org/drivers/spark/) uses autocommit.

With Spark 4.1.3, Hudi 1.2.0 and Foundry 0.2.1, local fault tests observed that a
write could commit after client disconnect, and retrying `INSERT` after a lost
response produced duplicate rows. Check the data or timeline before retrying.
After a driver restart at the requested-instant stage, the next write rolled back
the unfinished instant and committed. These observations depend on the write
configuration and failure stage; they are not a safe-retry guarantee.
In a local write test, `adbc_cancel()` returned `context canceled` to the client,
but Spark jobs continued and all 200 inserted rows committed, confirmed by a
separate connection and the Hudi timeline. Do not treat a client cancellation
error as proof that a write was stopped or rolled back.

Use `LIMIT` and size client/driver memory for the expected results. Foundry 0.2.1
rejected Arrow responses exceeding its 4 MiB gRPC receive limit in the 100k-row test.
Local-dev sets `spark.connect.grpc.arrow.maxBatchSize=1m`, which allowed that test
to complete. Set the same property in `spark.extraConf` for other deployments using
this driver. It controls batch size, not total result size or memory; oversized
individual rows can still exceed the receive limit.

## Troubleshooting

| Symptom | Check |
|---|---|
| Pod stays Pending | Pod events and available cluster memory |
| ImagePullBackOff | Image was built/loaded or pushed; registry is reachable |
| SQL fails despite readiness | Spark logs, metastore connectivity, S3 credentials and bucket; probes check only the TCP listener |
| ADBC driver not found | Install Foundry in the client environment or set `ADBC_DRIVER` to its library path |
| Session fails after restart | Open a new connection |

## Chart checks

Requires Helm, Python with PyYAML, and JDK 11+:

```bash
helm lint hudi-lakehouse/charts/hudi-spark-connect \
  -f hudi-lakehouse/charts/hudi-spark-connect/values-local-dev.yaml
python -m unittest discover -s hudi-lakehouse/tests -v
```

## Teardown

Remove only the endpoint; external table data is retained:

```bash
helm uninstall hudi-spark-connect -n hudi-lakehouse
```

To delete the entire local-dev namespace, including its PVCs and local data:

```bash
./hudi-lakehouse/local-dev/scripts/down.sh
```
