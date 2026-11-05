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
# Test corpus

Regression cases drawn from public `apache/hudi` issues. Each case is a stacktrace excerpt, the
catalog row that should match it, and the category that should be reported. This is what keeps the
catalog honest: change a signature regex, re-run these, and see what you broke.

Every issue number here was fetched from the public tracker. **Do not add a case without a public
issue number you have verified.** Cases invented to exercise a regex belong in a unit-test fixture,
not here — the value of this file is that every row is a failure a real user actually reported.

## How to use it

For each case, give the agent **only the excerpt** — not the expected columns — and check three things:

1. Did it walk to the **innermost** `Caused by` rather than classifying off the wrapper?
2. Did it return the expected pattern ID, or correctly return `unclassified`?
3. Did it return the expected category, and state which evidence rung it was on?

A case that expects `unclassified` is **passing when the agent declines to attribute**. Those cases
(16, 17, and the no-stacktrace rows) exist specifically to catch an agent that guesses confidently.
Guessing on them is a regression even if the guess happens to be right.

## Cases

| # | Issue | Stacktrace excerpt | Expected pattern | Expected category |
|---|---|---|---|---|
| 1 | 11335 | `Caused by: org.apache.avro.AvroTypeException: Cannot encode decimal with precision 14 as max precision 13`<br>`  at org.apache.avro.Conversions$DecimalConversion.validate(Conversions.java:140)`<br>`  at org.apache.hudi.avro.HoodieAvroUtils.rewritePrimaryTypeWithDiffSchemaType(HoodieAvroUtils.java:1077)`<br>`  at org.apache.hudi.avro.HoodieAvroUtils.rewriteRecordWithNewSchema(HoodieAvroUtils.java:873)` | HUDI-RCA-001 | `input/config` |
| 2 | 8040 | `org.apache.parquet.io.InvalidRecordException: Parquet/Avro schema mismatch: Avro field 'deleted_column_name' not found`<br>`  at org.apache.parquet.avro.AvroRecordConverter.getAvroField(AvroRecordConverter.java:221)` | HUDI-RCA-003 (column dropped by source) | `input/config` |
| 3 | 8040 | `org.apache.hudi.exception.HoodieException: Exception when reading log file`<br>`  at org.apache.hudi.common.table.log.AbstractHoodieLogRecordReader.scanInternal(AbstractHoodieLogRecordReader.java:352)`<br>`  at org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner.performScan(HoodieMergedLogRecordScanner.java:110)`<br>`  at org.apache.hudi.table.action.compact.HoodieCompactor.compact(HoodieCompactor.java:198)` | HUDI-RCA-001, **MOR deferred variant** — agent must say the bad write happened earlier, in a different job | `input/config` |
| 4 | 8020 | `org.apache.avro.AvroTypeException: Cannot encode decimal with precision 4 as max precision 2` | HUDI-RCA-001 | `input/config` |
| 5 | 3759 | `Caused by: org.apache.hudi.exception.HoodieKeyException: recordKey value: "null" for field: "my_primary_key_column_here" cannot be null or empty.` | HUDI-RCA-002 | `input/config` |
| 6 | 11776 | `org.apache.hudi.exception.HoodieKeyException: recordKey value: "null" for field: "ID" cannot be null or empty.` | HUDI-RCA-002, **case-sensitivity variant** — agent should raise case before nulls | `input/config` |
| 7 | 10233 | `org.apache.hudi.exception.HoodieException: ts(Part -ts) field not found in record. Acceptable fields were :[c1, c2, c3, c4, c5]`<br>`  at org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldVal(HoodieAvroUtils.java:601)` | HUDI-RCA-002, **Streamer `ts` default** — agent must note the user never configured `ts` | `input/config` |
| 8 | 2408 | `: java.lang.OutOfMemoryError`<br>`  at java.io.ByteArrayOutputStream.hugeCapacity(ByteArrayOutputStream.java:123)`<br>`  at org.apache.hudi.common.table.log.block.HoodieAvroDataBlock.serializeRecords(HoodieAvroDataBlock.java:114)`<br>`  at org.apache.hudi.table.HoodieTimelineArchiveLog.writeToFile(HoodieTimelineArchiveLog.java:309)`<br>`  at org.apache.hudi.table.HoodieTimelineArchiveLog.archiveIfRequired(HoodieTimelineArchiveLog.java:133)` | HUDI-RCA-015 **archival variant (b)** — NOT the upsert side; more executor memory is the wrong answer | `sizing/scale` |
| 9 | 1491 | `- java.lang.OutOfMemoryError: Java heap space`<br>`- The java.lang.OutOfMemoryError: GC overhead limit exceeded error`<br>`- java.util.concurrent.TimeoutException: Cannot receive any reply from <host>:41498 in 10000 milliseconds` | HUDI-RCA-015 **merge variant (a)** | `sizing/scale` |
| 10 | 1977 | `py4j.protocol.Py4JJavaError: An error occurred while calling o155.save.`<br>`: java.lang.NoSuchMethodError: scala.Some.value()Ljava/lang/Object;`<br>`  at org.apache.hudi.HoodieSparkSqlWriter$.write(HoodieSparkSqlWriter.scala:59)` | HUDI-RCA-035 | `environment` |
| 11 | 7653 | `Exception in thread "main" org.apache.hudi.exception.HoodieWriteConflictException: java.util.ConcurrentModificationException: Cannot resolve conflicts for overlapping writes`<br>`  at org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy.resolveConflict(SimpleConcurrentFileWritesConflictResolutionStrategy.java:102)`<br>`  at org.apache.hudi.client.utils.TransactionUtils.resolveWriteConflictIfAny(TransactionUtils.java:79)`<br>`  at org.apache.hudi.client.SparkRDDWriteClient.preCommit(SparkRDDWriteClient.java:475)` | HUDI-RCA-006 — **agent must ask for writer config before deciding healthy-OCC vs no-lock-provider** | `input/config` |
| 12 | 3731 | `: org.apache.hudi.exception.HoodieLockException: Unable to acquire lock, lock object LockResponse(lockid:255, state:WAITING)`<br>`  at org.apache.hudi.client.transaction.lock.LockManager.lock(LockManager.java:82)`<br>`  at org.apache.hudi.client.transaction.TransactionManager.beginTransaction(TransactionManager.java:64)` | HUDI-RCA-006, lock-acquisition variant | `input/config` |
| 13 | 3731 | `  at org.apache.hudi.common.table.timeline.HoodieActiveTimeline.createImmutableFileInPath(HoodieActiveTimeline.java:544)`<br>`Caused by: org.apache.hadoop.fs.FileAlreadyExistsException: File already exists: file:/tmp/test_table/.hoodie/20210921151138.commit.requested` | HUDI-RCA-006, timeline-collision variant | `input/config` |
| 14 | 9213 | `Caused by: org.apache.hudi.exception.HoodieRollbackException: Failed to rollback <base-path> commits 20230716231210452`<br>`  at org.apache.hudi.client.BaseHoodieWriteClient.rollback(BaseHoodieWriteClient.java:783)`<br>`  at org.apache.hudi.client.BaseHoodieWriteClient.rollbackFailedWrites(BaseHoodieWriteClient.java:1193)`<br>`  at org.apache.hudi.common.util.CleanerUtils.rollbackFailedWrites(CleanerUtils.java:151)`<br>`  at org.apache.hudi.client.BaseHoodieWriteClient.startCommitWithTime(BaseHoodieWriteClient.java:963)` | HUDI-RCA-027 | `defect` |
| 15 | 9213 | `java.lang.IllegalArgumentException: ALREADY_ACQUIRED`<br>`  at org.apache.hudi.common.util.ValidationUtils.checkArgument(ValidationUtils.java:40)`<br>`  at org.apache.hudi.hive.HiveMetastoreBasedLockProvider.acquireLock(HiveMetastoreBasedLockProvider.java:136)`<br>`  at org.apache.hudi.client.transaction.lock.LockManager.lock(...)` | HUDI-RCA-006, lock-provider variant | `environment` |
| 16 | 4230 | `java.lang.RuntimeException: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieRemoteException: Failed to create marker file /<fileId>-0_0-7-15_20211206094243657.parquet.marker.CREATE`<br>`  at org.apache.hudi.client.utils.LazyIterableIterator.next(LazyIterableIterator.java:121)` | HUDI-RCA-031 — **three nested wrappers; agent must reach `HoodieRemoteException`, not stop at `RuntimeException`** | `environment` |
| 17 | 5484 | `java.lang.RuntimeException: Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient`<br>`  at org.apache.hudi.hive.ddl.HMSDDLExecutor.<init>(HMSDDLExecutor.java:69)`<br>`  at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:73)`<br>`  at org.apache.hudi.hive.HiveSyncTool.initClient(HiveSyncTool.java:95)` | HUDI-RCA-007 | `input/config` |
| 18 | 11202 | `Caused by: java.util.NoSuchElementException: FileID 1b88b26f-94b1-4bd1-94ad-e919e17183ee-0 of partition path datatype=xxxx/year=2024/month=05/day=07 does not exist.`<br>`  at org.apache.hudi.io.HoodieMergeHandle.getLatestBaseFile(HoodieMergeHandle.java:161)`<br>`  at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.getUpdateHandle(BaseSparkCommitActionExecutor.java:400)` | HUDI-RCA-025 — **EXPLAIN-ONLY. Agent must not propose or script deletion. Must name L3 and require a human.** | `defect` |
| 19 | 2515 | `java.lang.NullPointerException: null of string of map of union in field extraMetadata of org.apache.hudi.avro.model.HoodieCommitMetadata of union in field hoodieCommitMetadata of org.apache.hudi.avro.model.HoodieArchivedMetaEntry`<br>`  at org.apache.avro.generic.GenericDatumWriter.npe(GenericDatumWriter.java:145)`<br>`  at org.apache.hudi.table.HoodieTimelineArchiveLog.writeToFile(HoodieTimelineArchiveLog.java:361)` | HUDI-RCA-015 archival variant, with HUDI-RCA-010 (null target schema) as the upstream cause | `input/config` |
| 20 | 5540 | `org.apache.hudi.exception.HoodieException: Commit 20220509105215 failed and rolled-back !`<br>`  at org.apache.hudi.utilities.deltastreamer.DeltaSync.writeToSink(DeltaSync.java:492)` | **`unclassified`** — content-free wrapper. Agent must return low confidence and request the driver log. See HUDI-RCA-010. | `unclassified` |
| 21 | 8519 | `Caused by: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 5.0 failed 4 times, most recent failure: Lost task 0.3 in stage 5.0 (TID 14) (<host> executor 1): java.lang.NullPointerException`<br>`  at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2304)` | **`unclassified`** — bare NPE, no Hudi frame. Agent should offer L13 and the explicit-exceptions diagnostic, not attribute. | `unclassified` |
| 22 | 2020 | `java.io.FileNotFoundException` during compaction retry, on a file named in the compaction plan | HUDI-RCA-025 (stale file-slice reference); check `hoodie.filesystem.view.incr.timeline.sync.enable` | `defect` |
| 23 | 7657 | `java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieRollbackException: Failed to rollback <base-path> commits 20230112132517252`<br>`  at org.apache.hudi.async.AsyncCleanerService.waitForCompletion(AsyncCleanerService.java:75)`<br>`  at org.apache.hudi.client.BaseHoodieWriteClient.autoCleanOnCommit(BaseHoodieWriteClient.java:605)`<br>plus `Invalid number of file groups for partition:column_stats` | HUDI-RCA-026 — **presents as a rollback failure; the `AsyncCleanerService` frame is the discriminator** | `defect` |
| 24 | 5777 | *(no stacktrace — duplicate rows for one primary key)* | HUDI-RCA-013 | `input/config` |
| 25 | 10508 | *(no stacktrace — `ComplexKeyGenerator` encoding changed in 0.14.1; upserts duplicate and deletes silently no-op)* | HUDI-RCA-008 (config/table drift), surfacing as HUDI-RCA-013 | `defect` |
| 26 | 4154 | *(no stacktrace — `INSERT OVERWRITE` writes parquet but `SELECT` returns stale or empty results)* | HUDI-RCA-009 | `input/config` |
| 27 | 2123 | *(no stacktrace — timestamps render as `+49134-01-07 05:30:00.000` in the query engine)* | HUDI-RCA-009, timestamp variant | `input/config` |

## What this corpus does not cover

Honest gaps, so nobody mistakes a green run for full coverage:

- **No cases for 18 of the 39 catalog rows.** The rows sourced from operational experience rather than
  the public tracker — including HUDI-RCA-019, 021, 022, 023, 028, 029, 030, 033, 034, 036 and all
  three Group E rows — have no public issue to cite, and inventing one would defeat the purpose of
  this file. They are exercised by reading the row, not by this corpus.
- **Group E is structurally hard to test this way.** "Do nothing" patterns produce no artifact to
  paste. The check that matters for them is behavioural: given a cleaner that appears idle, does the
  agent run the `numWrites == numInserts` check before recommending a config change?
- **No multi-artifact cases.** Every case here is rung 0 (a stacktrace alone). The rungs that matter
  most — `hoodie.properties` diffs, health-checker JSON, event logs — are not represented, and cases
  11 and 20 are the only ones that test whether the agent *asks* for them.
- **No Flink cases.** See the coverage note in `failure-catalog.md`.

Contributions that close any of these gaps with a verifiable public issue number are worth more than
additional cases for rows already covered.
