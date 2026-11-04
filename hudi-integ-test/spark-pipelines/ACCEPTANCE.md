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

Use `--table-type cow` (default) or `--table-type mor` to run the same assertions
against either table type. MOR enables async compaction with a one-delta-commit
threshold and requires a completed compaction commit on the upsert table before
the unchanged restart. Each invocation uses an isolated server and filesystem.

Use `--metadata enabled` to exercise the default metadata-enabled configuration.
The default `--metadata disabled` isolates the sink assertions from the known
metadata-writer lifecycle caveat tracked by #20491; a passing disabled run does
not establish default-configuration coverage. Run both explicitly, for example:

```sh
# Append these options to the command above for separate runs:
--table-type mor --metadata disabled
--table-type cow --metadata enabled
--table-type mor --metadata enabled
```

Validated with Spark 4.1.1 / Scala 2.13 / JDK 17 and the candidate bundle built
from master `b0d16f94b3b`: all four COW/MOR and metadata disabled/enabled
combinations passed. Both MOR runs completed upsert compaction, and all four
restart timelines remained unchanged. These bounded runs do not resolve #20491.

This scenario uses one writer per target. It does not claim coverage of materialized views, full refresh,
metadata-writer lifetime, fan-in, chaining, or commit/checkpoint failure injection.
The generated source, SQL and pipeline specification are retained in the logs as
runnable examples within the harness's isolated filesystem layout.

## Incremental Hudi table chaining

`chaining_acceptance.py` adds a second Hudi streaming table that reads the
upstream keyed table through SQL `FROM STREAM upsert_events`. Run it using
the same `--harness-dir`, `--bundle`, `--table-type` and `--metadata` options
as the Phase 1 scenario:

```sh
python hudi-integ-test/spark-pipelines/chaining_acceptance.py \
  --harness-dir /path/to/harness-checkout/hudi-integ-test/spark-pipelines \
  --bundle /path/to/candidate-hudi-spark4.1-bundle.jar
```

The first input contains duplicate keys that the upstream upsert deduplicates.
The downstream uses insert deliberately: it must contain one old record after
the first run and exactly the old and new versions after the second run.
Replaying an earlier increment would produce extra rows and fail the assertion.
A quiescent restart verifies exact rows and identical completed commit filenames
for both tables. MOR first drains the completed async compaction instant: if
compaction finished after AvailableNow captured its end offset, this catch-up
can create an empty downstream commit. The test checks that it does not replay
business records, records that timeline change separately, and then asserts
that the next restart creates no commits. Explicit business-column projection prevents
upstream Hudi metadata fields from becoming downstream input columns.

This tests the default earliest starting offset and checkpoint-based resume.
The source emits latest-state incremental records, not every intermediate
change or hard-delete events. Its AvailableNow fallback has no bounded
admission control; size the input backlog to fit one micro-batch.

This scenario covers only the incremental portion of #20407. It does not
establish safety across expired history, physical table recreation, or logical
truncate/overwrite, nor reader-option conflict handling. Stop downstream
consumers before resetting upstream tables; rebuild affected downstream tables
and checkpoints in dependency order. Automated reset detection and coordinated
rebuild acceptance remain required before declaring #20407 complete.
