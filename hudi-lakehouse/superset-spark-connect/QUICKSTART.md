<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
associated with this work for additional information
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

# Try Superset and Zeppelin with Spark Connect

This local example uses a disposable Hudi table. It targets Docker Desktop on
macOS, Superset 6.1.0 and Zeppelin 0.12.0. Run shell commands from the root of
an Apache Hudi checkout containing this adapter. You need Docker Desktop,
minikube, kubectl, Helm and network access for images and dependencies. Allocate
at least 4 CPUs and 10 GiB of memory to minikube, plus resources for the BI
containers. First startup can take several minutes. These settings are for a
trusted local environment, not production.

## 1. Start Spark Connect

Start Docker Desktop, then run:

```bash
./hudi-lakehouse/scripts/quickstart.sh --spark-connect
kubectl -n hudi-lakehouse rollout status deployment/hudi-spark-connect --timeout=300s
```

In a separate terminal, leave the port-forward running:

```bash
kubectl -n hudi-lakehouse port-forward svc/hudi-spark-connect 15002:15002
```

If minikube reports a missing `minikube` Docker container while the script says
it is running, the profile may be stale. Check `minikube status` and Docker
Desktop's current context; repair or recreate the profile only after checking
whether its existing data is needed. See the [endpoint guide](../charts/hudi-spark-connect/README.md)
for other deployment options.

## 2. Start Superset and connect

Build and initialize a disposable local Superset instance:

```bash
docker build -t hudi-superset-connect:local hudi-lakehouse/superset-spark-connect
docker run -d --name hudi-bi-superset -p 127.0.0.1:8088:8088 \
  -e SUPERSET_SECRET_KEY=local-demo-secret-change-for-real-use \
  -e SUPERSET_HOME=/tmp/superset-home \
  --entrypoint /bin/sh hudi-superset-connect:local \
  -c 'mkdir -p "$SUPERSET_HOME" &&
      superset db upgrade &&
      superset fab create-admin --username admin --firstname Local --lastname Demo \
        --email admin@example.invalid --password admin &&
      superset init &&
      superset run -h 0.0.0.0 -p 8088'
docker logs -f hudi-bi-superset
```

Once startup completes, press Ctrl-C to stop following the logs; this does not
stop the container. Open <http://localhost:8088> and sign in with `admin` / `admin`.
Under **Settings → Database Connections**, click **+ Database**, and select
**Apache Spark Connect (ADBC)**. On the **Basic** tab, enter a display name and
the SQLAlchemy URI below. Before clicking **Connect**, open **Advanced → SQL Lab**
and enable **Allow DDL and DML** so the example can create and write its table.
Use a disposable connection.

```text
sparkconnect://host.docker.internal:15002/default?api=connect&auth_type=none&tls=false
```

`host.docker.internal` routes from a Docker Desktop container to the Mac; it is
not the Kubernetes service name. Test the connection and open **SQL Lab**.
Run the following statements one at a time, selecting this connection and the
`default` schema. The table name is reserved for this walkthrough: use a unique
name if it already exists.

```sql
CREATE TABLE default.bi_connect_demo (id INT, region STRING, amount INT, ts BIGINT)
USING hudi TBLPROPERTIES (type='cow', primaryKey='id', preCombineField='ts');
```

```sql
INSERT INTO default.bi_connect_demo VALUES (1, 'East', 100, 1), (2, 'West', 200, 1);
```

```sql
MERGE INTO default.bi_connect_demo t
USING (SELECT 1 AS id, 'East' AS region, 175 AS amount, CAST(3 AS BIGINT) AS ts
       UNION ALL SELECT 3, 'North', 300, CAST(3 AS BIGINT)) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

```sql
SELECT id, region, amount, ts FROM default.bi_connect_demo ORDER BY id;
```

Expected rows: `(1, East, 175, 3)`, `(2, West, 200, 1)` and
`(3, North, 300, 3)`. Create a dataset from `bi_connect_demo` and a chart grouped
by `region` to explore the data. If a write times out or disconnects, check the
server and table before retrying: the write may still commit.

## 3. Try Zeppelin

Start Zeppelin and install the client in its Python environment:

```bash
docker run -d --name hudi-bi-zeppelin -p 127.0.0.1:8080:8080 apache/zeppelin:0.12.0
docker exec --user root hudi-bi-zeppelin bash -c \
  "python3 -m pip install adbc-driver-manager==1.8.0 pyarrow==18.1.0 dbc==0.3.1 && dbc install --level system 'spark=0.2.1' && chown -R 1000:0 /opt/zeppelin/.config"
```

The final `chown` makes the configuration directory created by `dbc` accessible
to Zeppelin's user in the official image. If the driver registry download fails,
check network access and rerun the `docker exec` command before opening a notebook.

Open <http://localhost:8080>, create a notebook and run this `%python` paragraph.
It reads the same table created in Superset and renders the three rows:

```python
%python
from adbc_driver_manager import dbapi
import pandas as pd

uri = 'spark://host.docker.internal:15002?api=connect&auth_type=none&tls=false'
with dbapi.connect(driver='spark', db_kwargs={'uri': uri}, autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT id, region, amount, ts FROM default.bi_connect_demo ORDER BY id')
        z.show(pd.DataFrame(cur.fetchall(), columns=['id', 'region', 'amount', 'ts']))
```

The adapter tests also exercise Zeppelin writes and MERGE; see the
[client guide](README.md) and [Zeppelin test instructions](../charts/hudi-spark-connect/README.md#bi-clients).
Zeppelin uses ADBC from `%python`; this example does not use `%jdbc`.

## 4. Clean up

In Superset SQL Lab, after all queries have completed, run:

```sql
DROP TABLE default.bi_connect_demo PURGE;
```

Remove only the two containers created above, and stop the port-forward with
Ctrl-C in its terminal:

```bash
docker rm -f hudi-bi-superset hudi-bi-zeppelin
```

The minikube deployment remains available. See the [local-dev teardown guide](../local-dev/README.md)
before removing the cluster or its data.
