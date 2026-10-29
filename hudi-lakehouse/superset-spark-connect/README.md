<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Spark Connect adapter for Superset

This experimental package provides a SQLAlchemy dialect and a Superset database
engine spec for the Foundry Spark ADBC driver. It uses Spark Connect, not the
Spark Thrift Server. It is independent of Hudi-specific SQL and can query other
Spark tables. The repository's Connect deployment instructions are in
[the chart README](../charts/hudi-spark-connect/README.md).

Install the package and native driver in **every** Superset web and worker
runtime. The driver must be accessible to the user running Superset, not just
installed in the build user's home. The initial target is Superset 6.1.0,
Python 3.11, SQLAlchemy 1.4.54, ADBC manager 1.12.0, PyArrow 18.1.0 and
Foundry Spark ADBC 0.2.1 (the official Superset 6.1.0 container used
Python 3.10 in the SQL Lab API check):

```bash
pip install ./hudi-lakehouse/superset-spark-connect 'dbc==0.3.1'
dbc install 'spark=0.2.1'
```

The supplied Dockerfile extends the official Superset 6.1.0 image, installs the
adapter into its existing Python environment and installs the native driver at
system level for the Superset user:

```bash
docker build -t hudi-superset-connect:local hudi-lakehouse/superset-spark-connect
```

Use that image for Superset web and worker processes. Configure a Superset secret
and metadata database using the normal Superset deployment instructions. The image
is a local integration example, not a hardened production deployment. It replaces
the base image's PyArrow 16.1.0 with 18.1.0; compatibility with unrelated Superset
features and connectors has not been established.

Restart Superset after installation. Configure the database with a URI such as:

```text
sparkconnect://localhost:15002/default?api=connect&auth_type=none&tls=false
```

`localhost` is relative to the Superset process. For an in-cluster client use
`hudi-spark-connect.hudi-lakehouse.svc:15002`; for a host client, use the
port-forward in the chart README. This URI is only for a trusted local network.
The adapter currently rejects credentials in the URL; authenticated endpoints and
catalog selection have not been validated. Its URI path selects the Spark schema.
The default driver name is `spark`; use the URI query parameter `driver` to
supply a driver manifest name or library path. Do not put credentials in query
parameters, which can appear in logs.

Use `superset test-db -c '{}'` with the URI to check dialect compatibility. In SQL Lab,
select the schema and table, run a grouped query, then create a dataset and chart.
To test direct writes, enable **Allow DML** on a disposable database connection,
execute INSERT, UPDATE and DELETE against a uniquely named test table, and run a
SELECT after each mutation. Refresh the chart after the writes. Disable Allow DML
when finished. The agent gateway remains read-only; these direct writes bypass it.

Parameterized execution and executemany are deliberately rejected: the tested
Foundry 0.2.1 combination did not support parameter binding. Do not interpolate
untrusted values into SQL. Driver errors or disconnects can occur after a write
commits; inspect the table and timeline before retrying. Unsupported complex Spark
types are reported as unknown SQLAlchemy types. Query cancellation, large results,
authentication, concurrency and dashboard behaviour require separate validation.

Offline contract tests and an opt-in live test:

```bash
python -m unittest discover -s hudi-lakehouse/superset-spark-connect -p 'test_dialect.py' -v
HUDI_SUPERSET_CONNECT_TEST_URI='sparkconnect://localhost:15002/default?tls=false' \
  python -m unittest discover -s hudi-lakehouse/superset-spark-connect -p 'test_live.py' -v
```

The live test creates a uniquely named database and Hudi table, checks discovery,
queries and CRUD, and drops only its own table and database. It needs a disposable
server with Hudi extensions and write permissions. A failed cleanup may leave the
named resources behind. Offline tests do not prove Superset UI or Spark behaviour.

To exercise Superset's SQL Lab API, install `requests` in the test environment,
configure a disposable Superset database connection with Allow DML enabled, and
set `HUDI_SUPERSET_URL`, `HUDI_SUPERSET_USER`, `HUDI_SUPERSET_PASSWORD` and
`HUDI_SUPERSET_DATABASE_ID`. Optionally set `HUDI_SUPERSET_SCHEMA` (default
`default`). Then run:

```bash
python -m unittest discover -s hudi-lakehouse/superset-spark-connect -p 'test_superset_live.py' -v
```

The SQL Lab test creates a unique table and Superset dataset, checks MERGE updates
and inserts with readback, verifies chart data
before and after a forced refresh following an update, then removes both resources;
it reports their names if cleanup fails. Use a disposable environment and
verify database permissions before enabling DML. The test covers the application
API; it does not automate browser rendering or dashboard refresh.

A local functional run used Superset 6.1.0, Spark Connect 4.1.3, Hudi 1.2.0
and Foundry ADBC 0.2.1 against both a local container and the Helm-deployed
endpoint. The SQL Lab API test passed queries, filtering and
INSERT/UPDATE/DELETE with readback; the chart data API returned updated aggregation
results after a forced refresh. The SQL Lab interface displayed a simple
query result. In a separate local-container run, a Superset dashboard table showed
East 100 and West 200; after an independent ADBC writer changed East to 150,
clicking **Refresh dashboard** displayed East 150 and West 200.

These checks used a single local-mode Spark driver and do not establish distributed
execution or production readiness.
