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
# local-dev: a laptop Hudi lakehouse

This directory stands up a complete, self-contained Hudi lakehouse on a local
Kubernetes cluster (minikube) for **development only**:

| Piece | What | Why it's here and not in a chart |
|---|---|---|
| `manifests/minio.yaml` | S3-compatible object storage (fixed creds `admin`/`password`) | in real deployments you bring your own S3/GCS |
| `manifests/hive-metastore.yaml` | Derby-backed HMS on thrift 9083 | in real deployments you bring your own HMS/Glue |
| Apache Spark Kubernetes Operator | runs `SparkApplication` jobs | installed from its upstream helm chart by `up.sh` |
| `charts/hudi-trino` (product chart) | Trino + the Hudi connector | the same chart used against real infrastructure, installed with `values-local-dev.yaml` |
| `example/` | copy-me template to submit any Spark job | see its README |

## Quickstart

```bash
# 1. build jars + images (from the repo root)
./hudi-lakehouse/scripts/build-jars.sh              # spark bundle (JDK 11/17) + trino plugin (JDK 23)
minikube start --cpus 4 --memory 10g
eval "$(minikube docker-env)"
./hudi-lakehouse/scripts/build-images.sh

# 2. bring up the stack
./hudi-lakehouse/local-dev/scripts/up.sh

# 3. write a Hudi table (Spark) + register it in HMS
./hudi-lakehouse/local-dev/scripts/run-example.sh

# 4. query it from Trino
kubectl -n hudi-lakehouse exec deploy/hudi-trino -- \
  trino --execute "SELECT city, count(*) FROM hudi.default.trips GROUP BY city"

# or run 2-4 plus assertions in one shot:
./hudi-lakehouse/local-dev/scripts/smoke-test.sh

# tear down
./hudi-lakehouse/local-dev/scripts/down.sh
```

Consoles (via `kubectl -n hudi-lakehouse port-forward`):
MinIO `svc/minio 9001`, Trino UI `svc/hudi-trino 8080`.

## Optional: vLLM instead of Ollama

If your cluster has a GPU node, `INSTALL_VLLM=1 ./scripts/up.sh` also installs
the `vllm` chart (default model `Qwen/Qwen3-8B`, override with `VLLM_MODEL=`)
and points the gateway at it instead of host Ollama.

## Notes

- `apache/hive:3.1.3` is amd64-only; on Apple-silicon minikube it runs under
  emulation (slower HMS startup, functionally fine).
- Tables that Trino must read are written with `hoodie.write.table.version=8`
  and `hoodie.write.auto.upgrade=false` — see the note in the top-level README.
