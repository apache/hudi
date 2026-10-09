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
      "name": "compaction",
      "status": "UNHEALTHY",
      "summary": "one line for the operator",
      "findings": ["actionable sentence", "..."],
      "effectiveConfigs": {
        "hoodie.compact.inline.max.delta.commits": "5",
        "effective.unhealthy.threshold.delta.commits": "10",
        "observed.delta.commits.since.last.compaction": "27"
      }
    }
  ],
  "errors": []
}
```

`effectiveConfigs` key convention: bare `hoodie.*` keys are writer properties as supplied;
`effective.*` are thresholds the check derived from them; `observed.*` are measurements from the
table. `overallStatus` is the most severe individual status (`UNHEALTHY` > `HEALTHY` > `SKIPPED`).

## Per check

### compaction

| | |
|---|---|
| Applies to | Merge-on-Read only; `SKIPPED` on Copy-on-Write |
| Needs | `hoodie.compact.inline.trigger.strategy`, `hoodie.compact.inline.max.delta.commits` (and `hoodie.compact.inline.max.delta.seconds` for time-based strategies) |
| Unhealthy when | delta commits since the last completed compaction exceed 2x what the trigger allows |
| Finding "No compaction is scheduled" | The scheduler is not running or compaction is disabled. Check `hoodie.compact.inline` / `hoodie.compact.schedule.inline` on the writer, or the async compaction job. |
| Finding "scheduled but not executed" | Plans exist on the timeline but never complete. Look at the async compactor's logs for repeated failures; `compactions show all` in `hudi-cli` lists pending plans. |
| Remedy | Get the compactor running; if a plan is corrupt, `compaction validate` then `compaction unschedule` in `hudi-cli`. Running compaction is reversible in the sense that it does not lose data; unscheduling a plan discards only the plan. |

### cleaner

| | |
|---|---|
| Needs | `hoodie.clean.trigger.max.commits` (echoes `hoodie.clean.policy`, `hoodie.clean.commits.retained` if given) |
| Unhealthy when | write commits since the last completed clean exceed max(2x trigger, 5) |
| Finding "No clean is scheduled" | Cleaning is disabled or the job is not running. Check `hoodie.clean.automatic` and `hoodie.clean.async`. |
| Finding "has not completed" | A clean is requested or inflight and stuck. Under concurrent writers a clean that keeps losing the table lock is the usual cause; check the writer logs. |
| Remedy | `cleans run` in `hudi-cli` runs one clean now. Cleaning is not reversible — it deletes file versions — but it only deletes what the retention policy already permits. |

### savepoint

| | |
|---|---|
| Needs | `hoodie.keep.max.commits` |
| Unhealthy when | a savepoint sits further back than the archival retention window |
| Also flags | any savepoint older than 7 days, even when not yet blocking, as likely forgotten |
| Why it matters | Archival stops at the earliest savepoint. The active timeline cannot shrink past it until the savepoint is released. |
| Remedy | Once the operation the savepoint was taken for is complete: `savepoint delete --commit <instant>` in `hudi-cli`. **Not reversible.** Confirm with the owner before recommending it, and never suggest configuring archival to skip savepoints instead. |

### archival

| | |
|---|---|
| Needs | `hoodie.keep.max.commits` |
| Unhealthy when | write instants exceed 1.1x the configured maximum, or completed instants exceed 5000 regardless of configuration |
| First thing to check | The `savepoint` check — a blocking savepoint is the most common cause. |
| Otherwise | Confirm archival is enabled and look for repeated archival failures in the writer logs. `timeline show active` in `hudi-cli` shows the current size. |
| Remedy | Release the blocking savepoint if there is one; otherwise get the writer's archival running. Archival is reversible in effect — archived instants remain readable from the archived timeline. |
