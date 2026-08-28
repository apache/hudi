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
# Example: submitting a Hudi Spark job

This folder is a **copy-me template** for running Spark jobs against the
local-dev lakehouse (and, with adjusted endpoints, any cluster):

- `hudi_table_writer.py` — a PySpark job that writes a partitioned Hudi
  Copy-on-Write table to object storage and registers it in the Hive
  Metastore via hive-sync, making it queryable from Trino.
- `spark-app.yaml` — the `SparkApplication` resource for the
  [Apache Spark Kubernetes Operator](https://github.com/apache/spark-kubernetes-operator)
  that runs it.

Run it with:

```bash
../scripts/run-example.sh
```

which uploads the script to `s3a://warehouse/jobs/` and applies the
`SparkApplication`.

## Running your own job

1. Copy `hudi_table_writer.py` and `spark-app.yaml` under new names.
2. Edit the script; keep the two compatibility pins if Trino must read the
   table (`hoodie.write.table.version=8`, `hoodie.write.auto.upgrade=false`)
   and the `hoodie.datasource.hive_sync.*` options if it should appear in the
   metastore.
3. In the yaml: point `pyFiles` at your uploaded script (or use
   `mainClass`+`jars` for JVM jobs), and adjust the
   `spark.kubernetes.driverEnv.*` parameters.
4. Upload + apply (mirroring what `run-example.sh` does):

```bash
kubectl -n hudi-lakehouse create configmap my-job-script --from-file=my_job.py
# ... run an mc upload Job, or use `mc cp` against a port-forwarded MinIO ...
kubectl apply -f my-spark-app.yaml
```
