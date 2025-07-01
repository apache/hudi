<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# RFC-97: Deprecate Hudi Payload Class Usage

## Proposers

*   Lin Liu

## Approvers

*   Ethan Guo
*   Sivabalan Narayanan
*   Vinoth Chandar

## Status

JIRA: HUDI-9560

# Motivation

During read, currently Hudi supports three paths to merge records during runtime:

* By APIs provided by `HoodieRecordPayload`
* By various merger instances inherited from `HoodieRecordMerger`
* Through merge mode, when the semantic is standard commit time or event time based

Unified APIs should be desired to simplify the Hudi repo. This RFC proposes to deprecate the usage of `HoodieRecordPayload` in Hudi.

---

# Requirements

* Standard merge logic is handled by setting `RecordMergeMode` (i.e., `COMMIT_TIME_ORDERING`, `EVENT_TIME_ORDERING`)
* Partial merge logic is natively handled within each of the merge modes – it is **not** a separate merge mode
* Custom merge logic (if absolutely needed) can be handled by merger classes inherited from `HoodieRecordMerger`
* Our goal is to deprecate legacy payloads as much as possible
* Several OSS users are using custom payloads, and these should continue to work

---

## Payload and Writer Usages Callout

* Not all payloads can be used across all different writers: Spark SQL, Spark DataSource, and HoodieStreamer
* `MySqlDebeziumAvroPayload` and `PostgresDebeziumAvroPayload` need a transformer and can only be used with HoodieStreamer
* `ExpressionPayload` is only used for Spark SQL writes

**TL;DR:** Some payloads require a transformer, which only works with HoodieStreamer

---

## Partial Update Mode

A new table config `hoodie.write.partial.update.mode=<value>` will control partial update behavior.

| Mode              | Description                                                             |
| ----------------- | ----------------------------------------------------------------------- |
| `KEEP_VALUES`     | (default) Use previous value if column is missing in the current record |
| `FILL_DEFAULTS`   | Use schema default if column is missing in current record               |
| `IGNORE_DEFAULTS` | Use previous value if current value equals schema default               |
| `IGNORE_MARKERS`  | Use previous value if current column equals a marker value              |

**Note**: Marker value is set via `hoodie.write.partial.custom.marker` table property.

---

# Payload Migration Table

| Payload Class                               | Merge Mode + Partial Update Mode           | Changes Proposed                                                                                   | Recommendations to User                                    | Behavior / Notes                                                                                                                         |
| ------------------------------------------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `OverwriteWithLatestAvroPayload`            | `COMMIT_TIME_ORDERING`                     | Set merge mode and remove payload class config                                                     | None                                                       | Custom delete markers will start working. Migration doesn't retain exact old behavior to reduce complexity.                              |
| `DefaultHoodieRecordPayload`                | `EVENT_TIME_ORDERING`                      | Set merge mode and remove payload class config                                                     | No action                                                  | No change in behavior                                                                                                                    |
| `EventTimeAvroPayload`                      | `EVENT_TIME_ORDERING`                      | Set `hoodie.write.enable.event.time.watermark.in.commit.metadata=true`                             | Set config true if this payload is detected                | Only writers are impacted (post table version 9). Event time metadata is written for this payload, not for `DefaultHoodieRecordPayload`. |
| `FirstValueAvroPayload`                     | N/A                                        | Stop support unless explicit merger class is defined                                               | Recommend users define their own merger class              | Deprecated. Previously returned old record if ordering values matched.                                                                   |
| `OverwriteNonDefaultsWithLatestAvroPayload` | `COMMIT_TIME_ORDERING` + `IGNORE_DEFAULTS` | Set partial update mode                                                                            | Add partial update mode support                            | Default values compared using schema. Writers require table version 9.                                                                   |
| `PartialUpdateAvroPayload`                  | `EVENT_TIME_ORDERING` + `KEEP_VALUES`      | Set partial update mode                                                                            | Add partial update mode support                            | Missing columns use values from the previous record                                                                                      |
| `AWSDmsAvroPayload`                         | `COMMIT_TIME_ORDERING`                     | Serialize delete marker configs into table property                                                | Upgrade readers before writers                             | Fixes for MoR readers; writer support requires table version 9                                                                           |
| `MySqlDebeziumAvroPayload`                  | `EVENT_TIME_ORDERING`                      | Create `LegacyMySqlDebeziumAvroMerger` for existing tables; new transformer for new tables         | Existing tables need no action; update transformer for new | Uses custom comparison logic for `_event_seq` like "002.3" vs "02.12" where string comparison fails but numeric parsing works            |
| `PostgresDebeziumAvroPayload`               | `EVENT_TIME_ORDERING` + `IGNORE_MARKERS`   | Set `hoodie.write.partial.update.mode=IGNORE_MARKERS` and marker as `__debezium_unavailable_value` | Full compaction and rollback of pending commits required   | Readers backward-compatible; payload logic: if column == marker → use old value, else → new value                                        |
| `ExpressionPayload`                         | N/A                                        | Leave unchanged                                                                                    | None                                                       | Under new workflow implementation; specific to Spark                                                                                     |
| `HoodieMetadataPayload`                     | N/A                                        | Create and configure specific merger class                                                         | No action                                                  | Highly custom logic; treated separately                                                                                                  |

---

# Q\&A

### How do we support old Hudi version readers?

Retain old table properties including payload class configs and **also** add new merge mode properties.
This ensures:

* Old readers (e.g., Trino/Presto) that read table version 8 will continue to work
* New readers will switch to merge mode-based reads (table version 9)

---

# Open Items

* Should writers fully abandon mergers for standard merge modes?

    * Only `CopyOnWrite` merge handle may need update
    * Other paths either don’t need merger or already use `FileGroupReader`

---
