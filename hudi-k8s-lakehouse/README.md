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
# hudi-k8s-lakehouse

Kubernetes packaging for running a Hudi lakehouse: deployable Helm charts for
the components, plus a self-contained laptop environment to develop against.

## Quickstart (one command)

Prereqs: `docker`, `minikube`, `helm`, `kubectl`, `mvn`, a JDK (11/17 for the
Hudi build; a JDK 23 for the Trino connector is auto-provisioned if missing).

```bash
./hudi-k8s-lakehouse/scripts/quickstart.sh --run-example
```

The script is **idempotent** — run it as many times as you like; every step
(minikube, maven artifacts, container images, Kubernetes releases) checks
whether its work is already done and converges. It brings up MinIO, a Hive
Metastore, the Apache Spark Kubernetes Operator, and Trino with the Hudi
connector; `--run-example` additionally runs a Spark job that writes a Hudi
table, registers it in the metastore, and queries it back through Trino:

```
     city      | trips
---------------+-------
 chennai       |    33
 san_francisco |    34
 sao_paulo     |    33
```

Tear down with `./hudi-k8s-lakehouse/local-dev/scripts/down.sh`.

## Layout: two separated concerns

A lakehouse is a decoupled architecture — writers, object storage, the
catalog, and query engines are independent. This directory keeps the
*deployable product* separate from the *development scaffolding*:

1. **`charts/` — product charts.** Components you install against **real**
   infrastructure. Each chart deploys one component and points at the others
   via values.

   | Chart | What it deploys | Points at |
   |---|---|---|
   | `hudi-trino` | Trino (server 472) with the Hudi connector built from this repository | your Hive Metastore or AWS Glue; your S3/GCS |
   | `hudi-ai-gateway` | the [Hudi AI gateway](../hudi-ai-gateway): agent chat API (sessions + SSE), MCP server, and chat UI over guarded lakehouse tools | your `hudi-trino`; your LLM (Anthropic/OpenAI keys, Ollama, or any OpenAI-compatible endpoint like vLLM) |

   (Planned siblings as the stack grows: vLLM serving.)

2. **`local-dev/` — a laptop environment.** Minikube scaffolding that stands
   up everything the charts point at (MinIO, a Derby-backed Hive Metastore,
   the Spark operator), installs the same `hudi-trino` chart with local
   values, and ships a copy-me [`example/`](local-dev/example) for submitting
   Spark jobs. Details: [`local-dev/README.md`](local-dev/README.md).

## Scripts

| Script | Purpose |
|---|---|
| `scripts/quickstart.sh` | one-command idempotent bring-up (everything below, in order) |
| `scripts/build-jars.sh` | maven builds: hudi-spark bundle (JDK 11/17) + hudi-trino-plugin (JDK 23) |
| `scripts/build-images.sh` | stages jars and builds the two container images; `--registry`/`--push` for clusters |
| `local-dev/scripts/up.sh` | manifests + spark-operator + hudi-trino chart onto the current kube context |
| `local-dev/scripts/run-example.sh` | uploads the example job and submits it as a `SparkApplication` |
| `local-dev/scripts/smoke-test.sh` | end-to-end assert: up → example → Trino row counts |
| `local-dev/scripts/down.sh` | full teardown |

## Deploying `hudi-trino` against real infrastructure

```bash
# build + push the image, then:
./hudi-k8s-lakehouse/scripts/build-jars.sh
./hudi-k8s-lakehouse/scripts/build-images.sh --registry my.registry/team --push

helm install hudi-trino hudi-k8s-lakehouse/charts/hudi-trino \
  --set image.repository=my.registry/team/hudi-lakehouse-trino \
  --set catalog.metastore.uri=thrift://my-hms.example.internal:9083 \
  --set catalog.filesystem.s3.existingSecret=my-s3-creds
# or for AWS Glue + IAM-based S3 access:
helm install hudi-trino hudi-k8s-lakehouse/charts/hudi-trino \
  --set image.repository=my.registry/team/hudi-lakehouse-trino \
  --set catalog.metastore.type=glue
```

`charts/hudi-trino/values-local-dev.yaml` is a fully worked example of the
values surface. No binaries are ever committed: image builds stage locally
built jars into gitignored `images/*/target/` directories.

## Version compatibility notes

- **Trino server is pinned to 472**: the `hudi-trino-plugin` performs a strict
  SPI version match at load time. The chart deliberately has no Trino version
  knob; it tracks the plugin.
- **Table version pin**: the plugin currently reads with a Hudi 1.0.x runtime,
  which understands table versions 6 and 8. Writers on this repository's
  master default to a newer table version, so any table Trino must read has to
  be written with `hoodie.write.table.version=8` and
  `hoodie.write.auto.upgrade=false` (the local-dev example does this). This
  pin goes away once the plugin's Hudi dependency is upgraded to a release
  that reads the current table version.
- The Spark image's Scala version must match the bundle:
  `apache/spark:3.5.x`/Scala 2.12 ↔ `-Pspark3.5` (default);
  `apache/spark:4.1.x`/Scala 2.13 ↔ `-Pspark4.1`
  (`quickstart.sh --spark-version 4.1`, experimental).
