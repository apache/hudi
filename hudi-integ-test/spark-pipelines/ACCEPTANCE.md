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
The Phase 1 script retains `--metadata disabled` as its default; the chaining
script below defaults to `enabled`. Run both configurations explicitly when
checking the Phase 1 scenario, for example:

```sh
# Append these options to the command above for separate runs:
--table-type mor --metadata disabled
--table-type cow --metadata enabled
--table-type mor --metadata enabled
```

Validated with Spark 4.1.1 / Scala 2.13 / JDK 17 and the candidate bundle built
from master `b0d16f94b3b`: all four COW/MOR and metadata disabled/enabled
combinations passed. Both MOR runs completed upsert compaction, and all four
restart timelines remained unchanged.

This scenario uses one writer per target. It does not claim coverage of materialized views, full refresh,
fan-in, chaining, or commit/checkpoint failure injection.
The generated source, SQL and pipeline specification are retained in the logs as
runnable examples within the harness's isolated filesystem layout.

## Incremental Hudi table chaining

`chaining_acceptance.py` adds a second partitioned Hudi streaming table that
reads the upstream keyed table through SQL `FROM STREAM upsert_events`.
**Metadata is enabled by default**, matching Hudi's default configuration.
The scenario verifies that both tables initialize their metadata tables and
produce completed metadata writes. Run COW and MOR separately:

```sh
PYTHONOPTIMIZE=1 python hudi-integ-test/spark-pipelines/chaining_acceptance.py \
  --harness-dir /path/to/harness-checkout/hudi-integ-test/spark-pipelines \
  --bundle /path/to/candidate-hudi-spark4.1-bundle.jar \
  --table-type cow
# Repeat with --table-type mor. Use --metadata disabled only for comparison.
```

All checks use `require()` and remain active under Python optimization, including
verification subprocesses when `PYTHONOPTIMIZE=1` is set. SIGTERM unwinds the
harness to stop owned processes and remove temporary data.

The first input contains a duplicate key 1 in partition `a`, plus unchanged
keys 2 and 3 in partitions `b` and `a`. The upstream upsert deduplicates key 1.
The second input updates key 1 and adds key 4 in partition `b`; it leaves keys
2 and 3 untouched. Exact business rows are ordered by `id` and `ts`. After the
second run, the upstream has four rows and the downstream insert table has five
(the old and new versions of key 1 plus keys 2, 3 and 4). Re-reading checkpointed
history would replay an untouched key and create an extra downstream row, even
if the updated key is collapsed to its latest state. Both tables must expose
exactly `part=a` and `part=b`.

A quiescent restart checks exact rows and identical completed data-commit
filenames for both tables. MOR first drains the completed async compaction
instant: if compaction finishes after AvailableNow captures its end offset,
catch-up can create an empty downstream commit. The test records that change,
requires no business-record replay, then verifies the next restart creates no
commits. Skipping empty sink batches is proposed follow-up work for #20406;
this scenario preserves the current sink behavior.

Explicit projection of `id, ts, value, part` excludes reserved upstream metadata
columns from the downstream input. **`SELECT * FROM STREAM upstream` is not
validated by this scenario.** Its metadata-column handling remains acceptance
work under #20407: add a real CLI case and either establish safe stripping or
verify a clear rejection. Do not infer that wildcard chaining is supported from
these passing projection-based runs. `_hoodie_is_deleted` is a separate supported
business input marker, not a reason to drop every `_hoodie_*` field blindly.

This tests the default earliest starting offset and checkpoint-based resume.
The source emits latest-state incremental records, not every intermediate
change or hard-delete events. Its AvailableNow fallback has no bounded admission
control; size the input backlog to fit one micro-batch.

This scenario covers the incremental portion and **leaves #20407 open**.
Reset/recreation, expired-history recovery and reader-option behavior are
covered separately by #20516. Stop downstream consumers before resetting
upstream tables; rebuild affected tables and checkpoints in dependency order.

Validated with Spark 4.1.1 / Scala 2.13 / JDK 17, using a freshly built candidate
bundle from this branch (production base `c58c54e068b`) and the #20453 harness.
Both COW and MOR passed with `PYTHONOPTIMIZE=1` and no `--metadata` override,
exercising the enabled default. Both tables passed metadata initialization/write,
exact-row, two-partition, and unchanged-restart checks. MOR completed upstream
compaction; in this run its catch-up needed no additional downstream commit.
The bundle package build skipped unit-test execution.
