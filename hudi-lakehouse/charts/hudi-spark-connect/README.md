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
| `spark.driverMemory` | JVM heap; keep the pod memory limit higher |
| `resources` | Pod CPU and memory requests/limits |
| `spark.extraConf` | Additional Spark properties; no credentials or overrides of chart-owned settings |

The endpoint runs one shared driver in local mode, without separate executors.
Keep one replica. Restarting it ends active sessions; tables remain in the
external metastore and object store.

The service has no authentication or TLS. Keep it on a trusted network:
any client that can connect can read and write using the server's storage credentials.

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

The test creates a temporary table, checks INSERT/UPDATE/DELETE, restarts the
endpoint, verifies persistence, and cleans up. It interrupts active sessions.
On failure, follow the printed cleanup instructions.

<a id="spark-connect-client-tutorials"></a>

## BI clients

- **Superset 5.0:** requires a custom SQLAlchemy dialect and engine spec for ADBC;
  neither is included here. The built-in PyHive connector uses Thrift, not Connect.
  See [Superset engine specs](https://github.com/apache/superset/blob/5.0.0/superset/db_engine_specs/README.md).
- **Zeppelin 0.12:** use ADBC from the `%python` interpreter, with the client
  packages above installed in its Python environment. See the
  [Python interpreter guide](https://zeppelin.apache.org/docs/0.12.0/interpreter/python.html).

[Foundry Spark ADBC 0.2.1](https://adbc-drivers.org/drivers/spark/) uses autocommit,
buffers results before fetching, and does not support statement cancellation.
Use `LIMIT` for interactive queries and check table state before retrying writes.

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
