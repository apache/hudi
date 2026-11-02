<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# RFC-112 Phase 1 acceptance

`phase1_acceptance.py` runs the real Spark 4.1.1 `spark-pipelines` CLI
against a separate Connect server using the harness from PR #20453.
It can run before that harness lands: pass its checkout's directory explicitly.
Install that harness's pinned `requirements.txt` in a dedicated virtual environment
and build the candidate Spark 4.1 bundle using JDK 17:

```sh
mvn package -Pjava17 -Dspark4.1 -Dscala-2.13 -DskipTests -Dgpg.skip \
  -Djacoco.skip -am -pl packaging/hudi-spark-bundle
python hudi-integ-test/spark-pipelines/phase1_acceptance.py \
  --harness-dir /path/to/harness-checkout/hudi-integ-test/spark-pipelines \
  --bundle "$PWD/packaging/hudi-spark-bundle/target/hudi-spark4.1-bundle_2.13-1.3.0-SNAPSHOT.jar"
```

The deterministic JSON source supplies duplicate keys, then new versions in a
second file. Independent insert and upsert flows capture `spark.hoodie.*`
operation settings through SQL SET. Exact rows distinguish their behavior after
each run. An unchanged restart checks exact rows and equality of completed Hudi
commit filenames; `restart-commits.json` preserves the before/after evidence.
Finally, a null-key flow must fail while the same Connect server remains available
for a new remote query. Every client and CLI invocation is bounded by the harness.
Logs remain under `target/phase1-acceptance-logs` after temporary data is removed.

This scenario uses one writer per target and disables metadata indexing to isolate
MVP behavior. It does not claim coverage of materialized views, full refresh,
metadata-writer lifetime, fan-in, chaining, or commit/checkpoint failure injection.
The generated source, SQL and pipeline specification are retained in the logs as
runnable examples within the harness's isolated filesystem layout.
