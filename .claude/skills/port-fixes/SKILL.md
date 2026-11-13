---
name: port-fixes
description: Use when one Hudi fix has to land on both apache/hudi `master` (1.x) and `branch-0.x`, or when a PR fails the `validate-pr-and-source` co-author or PR-template check; when a metadata-table test passes locally but reproduces nothing; or when a cherry-pick compiles cleanly and then tests nothing on the other branch.
---

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

# port-fixes — one defect, two bases, two red runs

## Overview

One defect, two PRs, two different environments. The bases diverge in Java target, timeline
API, and metadata-table instant scheme, so a straight cherry-pick compiles and then silently tests
nothing. Verify red on **each** base separately.

## The two bases

Both are branches of the one `hudi` clone. Do the actual work in **sibling** git worktrees
(`hudi-wt-1`, `hudi-wt-2`, …), siblings of the clone, never nested inside it.

| base | line | notes |
|---|---|---|
| `apache/hudi` **master** | 1.x | `-Dscala-2.12 -Dspark3.5` (default Flink profile) |
| `apache/hudi` **branch-0.x** | 0.x | `-Dscala-2.12 -Dspark3.5`; Flink profiles stop at `flink1.18` |

**`build-hudi` is authoritative for each base's `<java.version>`, local JDK, and CI JDK** — look
them up there rather than here, so the two cannot drift.

*Troubleshooting:* a local Java 8 run can die with `NoSuchMethodError: java.nio.ByteBuffer.flip()`
when `~/.m2` holds a `spark-core_2.12-3.5.x` jar that was installed from a JDK 17 build. The fix is
to clear and reinstall that artifact. Running the same build on Temurin 11 also makes it go away
(Java 8-target bytecode runs there), but that is a workaround, not the default.

## Order of operations

1. Fix on **master** first, with its regression test.
2. Backport to **branch-0.x** only when the defect exists there. Open it as its own PR and name the
   master PR in its body.
3. Each PR gets its own red/green run on its own base (below).

## Traps that cost real time

**Metadata-table init instants differ by table version.** 0.x always appends a three-digit suffix to
the data instant. 1.x `HoodieBackedTableMetadataWriter#generateUniqueInstantTime` derives them from
`SOLO_COMMIT_TIMESTAMP` (sorts *below* every data instant); only
`HoodieBackedTableMetadataWriterTableVersionSix` still appends the suffix. A test needing the suffixed
scheme on a 1.x base must set `hoodie.write.table.version=6` **and** `withWriteTableVersion(6)`,
**and** construct the writer through `SparkMetadataWriterFactory.create(..., tableConfig)` — calling
`SparkHoodieBackedTableMetadataWriter.create(...)` directly always yields the v8 writer, so the test
passes while reproducing nothing. Assert `HoodieTableVersion.SIX` as a precondition.

**A bypass flag passed as `-DargLine` never reaches the fork.** Hudi's pom sets surefire's argLine to
`@{argLine}` for jacoco, so `-DargLine=-Dfoo=true` is discarded and the "red" run passes. Use a plain
`-Dfoo=true`. Prove the flag is live by first making the bypass *throw*, then switch it to the
pass-through behaviour and re-run.

**apache/hudi rejects Claude co-author trailers.** The `validate-pr-and-source` job
(`.github/workflows/validate_source.yml`) fails any commit matching `co-authored-by: claude ...` or
`<noreply@anthropic.com>`. Rewrite the offending commit and force-push; this is the one case where
`create-pr`'s no-force-push rule gives way.

**apache/hudi enforces PR template section order.** The same job requires
`### Describe the issue this Pull Request addresses` first, then Summary and Changelog / Impact /
Risk Level / Documentation Update / Contributor's checklist. Titles are conventional-commit
(`fix(scope): ...`); no JIRA.

## API drift, 0.x → 1.x

| 0.x | 1.x |
|---|---|
| `instant.getTimestamp()` | `instant.requestedTime()` |
| `metaClient.getBasePathV2()` | `metaClient.getBasePath()` |
| `HoodieTimeline.compareTimestamps/LESSER_THAN_OR_EQUALS` | `InstantComparison.*` |
| `HoodieActiveTimeline.parseDateFromInstantTime/formatDate` | `TimelineUtils.*` |
| `HoodieMetadataConfig.Builder.withEnableRecordIndex` | `withEnableGlobalRecordLevelIndex` |
| `create(Configuration, ...)` | `create(StorageConfiguration<?>, ...)` |
| `LOG` field | Lombok `@Slf4j` `log` on master |

Do not swap `parseDateFromInstantTime` for `parseDateFromInstantTimeSafely` — the latter maps
`NOT_PARSABLE_TIMESTAMPS` to `new Date(Integer.parseInt(...))`, an epoch-era value.

Checkstyle import ordering is stricter on master than on 0.x; run `mvn checkstyle:check -pl <module>`
per base before pushing.

## Per-base verification loop

```
mvn -q clean package -pl hudi-utilities -am -DskipTests <profiles>
mvn test -pl hudi-utilities -am -Punit-tests -Dtest="<Class>#<newTests>" -DfailIfNoTests=false <profiles>   # expect GREEN
# add temporary bypass in the fixed method, then:
mvn test ... -Dbypass=true                                                                                  # expect RED, both paths
# remove bypass, grep to confirm it is gone, then:
mvn test -pl hudi-utilities -am -Punit-tests -Dtest="<Class>" -DfailIfNoTests=false <profiles>              # full class, no regressions
```

Compare the full-class count against the unmodified base — on 0.x this caught three genuine
regressions that a single-test run had hidden.

## Common mistakes

- Trusting a green new test without a red run on that same base.
- Reusing the 0.x test shape on a 1.x base (no table-version pinning) — it passes vacuously.
- Running only the new test, not the whole class.
- Assuming a check that "should" be red is the only red one.

## Related

- `build-hudi` — the authoritative branch / JDK / profile facts, and build and single-test
  invocation for each base.
- `author-changes` — designing the change and its tests.
- `review-pr` — the reviewer bar to self-check against before pushing.
- `create-pr` — PR mechanics (branch, commit, push, review replies).
