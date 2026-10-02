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

# Checks, properties, and remedies

## JSON envelope

```json
{
  "basePath": "...",
  "tableType": "MERGE_ON_READ",
  "overallStatus": "UNHEALTHY",
  "checks": [
    {
      "name": "mdt-sync",
      "status": "UNHEALTHY",
      "summary": "one line for the operator",
      "findings": ["actionable sentence", "..."],
      "effectiveConfigs": {
        "hoodie.metadata.enable": "true",
        "effective.partitions.sampled.max": "20",
        "observed.data.table.latest.completed.instant": "20260901123000000",
        "observed.mdt.synced.instant": "20260901120000000",
        "observed.data.table.writes.behind": "3",
        "observed.partitions.sampled": "20",
        "observed.partitions.inconsistent": "0"
      }
    }
  ],
  "errors": []
}
```

`effectiveConfigs` key convention: bare `hoodie.*` keys are writer properties as supplied;
`effective.*` are thresholds the check derived from them; `observed.*` are measurements from the
table. `overallStatus` is the most severe individual status (`UNHEALTHY` > `HEALTHY` > `SKIPPED`).

All three checks report `SKIPPED` when the table has no metadata table, or when the writer runs
with `hoodie.metadata.enable=false`. That is the end of the conversation for this Skill: nothing
below applies to a table without a metadata table.

## Per check

### mdt-sync

| | |
|---|---|
| Needs | `hoodie.metadata.enable` |
| Unhealthy when | any completed data-table write is newer than the metadata table's synced instant, **or** the latest base-file/log-file names differ between the metadata table and storage in any sampled partition (all partitions when 20 or fewer; otherwise the 20 newest in sorted order, where drift originates, plus the 2 oldest) |
| Finding "writes are missing from the metadata table" | Sync lag. If a writer is active, a write may still be finalizing: rerun after it completes. If it persists, look for incomplete metadata-table delta commits — `metadata timeline show incomplete` in `hudi-cli` — and for metadata-table errors in the writer logs. |
| Finding "Partition '...': N base files listed by the metadata table but absent on storage" (or the reverse, or the log-file variants) | Listing inconsistency. The metadata table has drifted from what is on storage; readers that list through it see the wrong files. The finding names the first few files on each side. |
| Remedy for lag | None beyond getting the writer's next commit through. **Do not rebuild the metadata table for lag.** |
| Remedy for inconsistency | First confirm the extent: `spark-submit --class org.apache.hudi.utilities.HoodieMetadataTableValidator <hudi-utilities-bundle> --base-path <path> --validate-latest-file-slices --validate-latest-base-files`. If it confirms drift, rebuild by running one write with `hoodie.metadata.enable=false` (which removes the metadata table) and then re-enabling it. That is **disruptive** — a full listing and re-indexing of the table, during which readers fall back to file-system listing — and should be scheduled with the table owner, not recommended casually. |

### mdt-compaction

| | |
|---|---|
| Needs | `hoodie.metadata.enable`, `hoodie.metadata.compact.max.delta.commits` |
| Unhealthy when | completed metadata-table delta commits since the last completed metadata-table compaction exceed 2x the configured trigger |
| Also reports | `observed.lag.duration.minutes` — how long compaction has been lagging, from the last compaction (or the first delta commit if there has never been one) to the latest delta commit; `observed.last.mdt.compaction.instant`; `observed.data.table.earliest.pending.instant` |
| Why it lags | Metadata-table compaction runs inline with the data-table writer. To keep the compacted base file consistent with the data table, the writer pins the compaction instant just below the earliest data-table instant that is still pending but already applied to the metadata table. Once a compaction at that pinned instant exists, later attempts are skipped until the pending instant completes or is rolled back. |
| Finding "Data-table instant ... is pending" | The first thing to check. If the instant belongs to a writer that is gone, rolling it back unblocks compaction. `timeline show incomplete` in `hudi-cli` lists pending data-table instants. |
| Finding "No data-table instant is pending" | Nothing is pinning compaction, so look at the writer: confirm it runs metadata-table table services after its commits, and search its logs for `Error in scheduling and executing compaction in metadata table`. |
| Finding "compaction(s) are scheduled on the metadata table but not complete" | `metadata timeline show incomplete` in `hudi-cli` lists them. The writer re-runs pending metadata-table compactions before its next write; repeated failures point at the writer logs. |
| Remedy | Clear whatever is pinning it — usually completing or rolling back the pending data-table instant — and let the writer's next commit compact. Compaction lag recovers on its own. **Do not rebuild the metadata table for compaction lag.** Rolling back a pending instant discards only that instant's uncommitted work. |

### record-index

| | |
|---|---|
| Applies to | tables whose `hoodie.table.metadata.partitions` lists `record_index`; `SKIPPED` otherwise |
| Needs | for a global index: `hoodie.metadata.global.record.level.index.min.filegroup.count` and `hoodie.metadata.global.record.level.index.max.filegroup.count`; for a partitioned index (`hoodie.metadata.record.level.index.enable=true`): `hoodie.metadata.record.level.index.min.filegroup.count` and `hoodie.metadata.record.level.index.max.filegroup.count`; plus, in both cases, `hoodie.metadata.record.index.max.filegroup.size` and `hoodie.metadata.record.index.growth.factor`. Config alternatives are honoured. |
| Unhealthy when | `observed.file.group.count` is below `effective.min.healthy.file.group.count`, which is ceil(0.5 x `effective.ideal.file.group.count`) — per data partition for a partitioned index, with the worst data partition reported — **or** `observed.largest.file.group.bytes` exceeds `effective.max.healthy.file.group.bytes`, which is 1.5 x `hoodie.metadata.record.index.max.filegroup.size` |
| How the ideal is derived | The record count is an **estimate**: index bytes divided by `effective.average.record.size.bytes` (48). The check labels it as such; treat `observed.estimated.record.count` as an order of magnitude, not a fact. |
| `effective.count.comparison` | Says what was compared: `whole index (global record index)`; `per data partition (partitioned record index); worst data partition reported`; or a `skipped: ...` string when the writer configuration says one kind of index but the index on storage is the other. A `skipped` comparison means the count rule did not run — say so rather than calling the count healthy. |
| Reports | `observed.file.group.count`, `observed.base.file.count`, `observed.log.file.count`, `observed.total.size.bytes`, `observed.largest.file.group.bytes`, `observed.largest.file.group.id`, `observed.estimated.record.count`; for partitioned indexes also `observed.data.partition.count`, `observed.worst.data.partition`, `observed.worst.data.partition.file.group.count` |
| Why it matters | The file-group count of the record-level index is fixed at initialization and cannot be changed in place; it does not grow with the table. An undersized index concentrates lookups and log files on too few file groups, and each file group grows without bound. |
| Remedy | Drop the index, correct the sizing configuration, and rebuild. Drop with `metadata delete-record-index` in `hudi-cli` (or `HoodieIndexer --mode dropindex --index-types RECORD_INDEX`); fix the file-group count and size properties above; then rebuild with `HoodieIndexer --mode scheduleAndExecute --index-types RECORD_INDEX`. This is **disruptive** — the index is unavailable during the rebuild, so lookups fall back to the slower path — and **not undoable in place**; schedule it with the table owner. Rebuilding without changing the sizing configuration reproduces the same undersized index. |
