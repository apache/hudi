---
name: hudi-rc-validation
description: Validate an Apache Hudi release candidate before voting on its [VOTE] thread. Picks a validation scale (minimal ~5-10 min, decent ~15-30 min, large ~2-4 h) from the user's intent, runs scripts/release/validate_rc.sh (source release checks, quickstart on the staged bundles, HoodieStreamer continuous end-to-end with async compaction and cleaning, build from source, existing test suites), triages failures into RC problems vs local-environment problems, and drafts the vote reply. Use when asked to certify, validate, verify, test or vote on a Hudi RC.
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

# Hudi release-candidate validation

The mechanics live in `scripts/release/validate_rc.sh` and `scripts/release/rc_validation/`. This
skill decides what to run, runs it, explains the result, and drafts the vote. Never send the vote
email or post to the mailing list on the user's behalf.

## 1. Collect inputs

From the vote email (ask the user to paste it if it is not in the conversation):

| Input | Where in the email | Flag |
|---|---|---|
| version, RC number | subject / tag `release-X.Y.Z-rcN` | `--release`, `--rc_num` |
| staging repository | "artifacts to be deployed to the Maven Central Repository" link | `--staging_repo` |
| signing key | "signed with the key with fingerprint …" | `--signing_key` |

Locally: a Spark distribution matching the bundle's Spark version (`--spark_home`; a `spark-submit`
on PATH is often a stale wrapper, so check `$SPARK_HOME/jars/spark-core_*.jar`), `gpg`, `svn`,
`curl`, Maven, and for `decent`/`large` the JDK the release builds with (the script reads
`<java.version>` from the source pom and resolves `JAVA<N>_HOME` or macOS `java_home`).

## 2. Pick the scale from intent

| Intent | Scale | Runs | Time |
|---|---|---|---|
| "quick sanity", "smoke test", "does the RC basically work", short on time | `minimal` | source release checks (checksum, signature, key match, no binaries, LICENSE/NOTICE, RAT); quickstart on COW and MOR against the staged spark bundle; HoodieStreamer continuous MOR run, 12 small batches | 5-10 min |
| "certify", "validate for my vote", no qualifier | `decent` | minimal + build from the voted source tarball + HoodieStreamer continuous-mode tests + a HoodieStreamer run with 50k-row initial load, 24 batches of 30k | 15-30 min |
| "thorough", "deep", major release, release manager's own check, regression suspected | `large` | decent + `TestCOWDataSource`, `TestMORDataSource`, `TestInsertTable`, `TestMergeIntoTable` + HoodieStreamer runs on MOR and COW with a 1M-row initial load, 60 batches of 400k | 2-4 h |

Measured on a laptop for 1.1.2-rc1: minimal ~8.5 min (release 0.5, quickstart 3, e2e 5), decent
~28 min (adds e2e 10, build 6, tests 8). State the chosen scale and its time budget before starting. To re-run only part, pass
`--steps=release,quickstart,e2e,build,tests` (any subset). For a different Spark/Scala bundle use
`--spark_version` / `--scala_version` (and a matching `--spark_home`).

## 3. Run

```bash
scripts/release/validate_rc.sh --release=X.Y.Z --rc_num=N --scale=<scale> \
  --staging_repo=<url> --signing_key=<key id> --spark_home=<spark dir> --work_dir=<dir>
```

Run it in the background and wait for completion rather than polling. Each step logs to
`<work_dir>/<step>.log`; the e2e run keeps `streamer.log`, `datagen.log` and `validate.log` under
`<work_dir>/streamer_e2e_<table type>/`; test logs are under `<work_dir>/tests/`. The final summary
is `<work_dir>/summary.txt`. Every check prints a `QS|E2E|TESTS PASS|FAIL <check> (<detail>)` line.

## 4. Triage failures before calling anything an RC problem

Read the step log, find the root exception, and classify it. These have all been seen and are
**environment** problems, not RC problems:

- `ClassCastException: …AppClassLoader cannot be cast to …URLClassLoader` in HoodieStreamer test
  setup: tests ran on a newer JDK than the release targets (embedded Hive 2.3 server).
- `NoSuchMethodError: java.nio.ByteBuffer.limit(I)Ljava/nio/ByteBuffer;`: classes compiled on JDK 9+
  executed on JDK 8 — build and test must use the same JDK.
- `ZipFile invalid LOC header`: two builds wrote the same `~/.m2` artifact concurrently.
- Tests reported as run 0, or `ClassNotFoundException` for a suite name: a selector matched nothing.
- spark-shell never exits: the script threw before `System.exit` without stdin redirected (the
  scripts already pass `< /dev/null`; keep it if you hand-run them).

Things that look wrong but are correct Hudi behavior (the scripts already account for them; keep that
in mind when writing extra checks):

- Incremental queries in 1.x bound on **completion** time, so a requested instant time as
  `begin.instanttime` includes that commit.
- An upsert or MERGE INTO with a lower ordering-field value than the stored row keeps the stored row.
- On a small MOR table without `hoodie.parquet.small.file.limit=0`, inserts are bin-packed into small
  base files, no log files are written and compaction plans are empty.
- In 1.x the `.requested`/`.inflight` files stay next to the completed `<requested>_<completed>.<action>`
  file; they do not mean the instant is pending.

Known pre-existing issues (present in earlier releases and on master, so not RC blockers):

- `CALL show_cleans(..., showArchived => true)` fails with `unable to read commit metadata for
  instant [...clean__COMPLETED]` / `Not an Avro data file` once a clean has been archived to the
  1.x LSM timeline. The validator reads active-timeline cleans only.

If a failure reproduces after fixing the environment, check whether it also fails on the previous
release or on master before reporting it as an RC blocker, and give the user the exact command and
stack trace to post on the vote thread.

## 5. Report and draft the vote

Show the summary as a table (step, result, duration, key detail), list environment-only issues
separately from RC findings, then draft the reply for the user to send:

```
+1 (binding|non-binding)

- Verified checksums and signature (key <id>)
- Verified source release: no binary files, LICENSE/NOTICE, RAT
- [decent/large] Built from the source release (Spark <v> / Scala <v>)
- [decent/large] Ran <test classes>: all passed
- Quickstart on COW and MOR with the staged hudi-spark<v>-bundle (datasource + Spark SQL): passed
- HoodieStreamer continuous mode with ParquetDFSSource + FilebasedSchemaProvider on <MOR[/COW]>:
  <N> commits, async compaction and cleaning succeeded, record counts validated
```

Ask whether the vote is binding (PMC) before finalizing that line. If anything is a real RC problem,
draft a -1 or a question instead, with the reproduction.
