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

---

# Motivation

During reads, Hudi currently supports three distinct mechanisms to merge records at runtime:

* **Via APIs provided by `HoodieRecordPayload`** – Legacy interface enabling users to plug in merge logic.
* **Through pluggable merger implementations inheriting from `HoodieRecordMerger`** – Newer and more composable approach introduced to separate merge semantics from payload definitions.
* **By configuring merge modes such as `COMMIT_TIME_ORDERING` or `EVENT_TIME_ORDERING`** – Recommended declarative approach for most standard use cases.

The `HoodieRecordPayload` abstraction was once necessary to encapsulate merge semantics, especially before Hudi had consistent event time handling, watermark metadata, and standard schema evolution support. However, over the years, the payload interface has become a limiting factor:

* It’s tightly coupled with the write path, making it hard to optimize read/write independently.
* Many of the behaviors (e.g., null handling, default values) are re-implemented inconsistently in various payloads.
* It breaks composability — writers like HoodieStreamer, DeltaStreamer, and Spark SQL have different expectations about payload behavior.
* It’s difficult to evolve and maintain, especially with increasing user needs (e.g., CDC ingestion, partial updates, deduplication).

This RFC proposes to **deprecate the usage of `HoodieRecordPayload`**, encourage standard declarative merge modes, and move towards cleanly defined, testable `HoodieRecordMerger` implementations for custom logic. This aligns Hudi with modern lakehouse expectations and simplifies the ecosystem significantly.

---

# Requirements

* **Declarative merge semantics** are enforced via `RecordMergeMode`, which cover 90%+ of industry use cases.
* **Partial update semantics** (null-handling, default-filling, etc.) are captured as part of merge mode behavior via `hoodie.write.partial.update.mode`.
* **Custom merge logic**, when required, is implemented through `HoodieRecordMerger` instances and configured via table properties.
* **Legacy payloads must still function**, especially for large installations using Hudi for multi-year tables (e.g., in fintech, retail, health tech).
* **All writers (SQL, HoodieStreamer, Flink, Java client)** should migrate toward payload-less workflows, even if they need a transition layer.
* **Minimal to no changes for readers** (Presto/Trino/Spark SQL) reading table version <9.

---

## Payload and Writer Usages Callout

Payload-based write paths today are highly fragmented:

* **`MySqlDebeziumAvroPayload` / `PostgresDebeziumAvroPayload`** are often used with HoodieStreamer + Avro transformer. They assume CDC structure and extract metadata from nested fields. These aren’t portable to Spark SQL or Java client directly.
* **`ExpressionPayload`** is used only within Spark SQL engine (e.g., `update(...) set ... where ...`). It doesn’t work in HoodieStreamer or bulk insert paths.
* **Some payloads like `AWSDmsAvroPayload`** have table-specific logic for delete markers and are only functional with certain MoR writers.

These inconsistencies lead to bugs, surprises during upgrades, and poor UX for new users. By eliminating the need for payloads, we can:

* Decouple writers from tightly-coupled logic embedded in payloads.
* Consolidate test coverage and semantics around well-defined `RecordMergeMode`s and `PartialUpdateMode`s.
* Improve future features like lakehouse-wide CDC ingestion, Iceberg interoperability, and schema-less streaming.

---

## Partial Update Mode

The new table property `hoodie.table.partial.update.mode=<value>` now controls how missing columns are interpreted in a record. This enables flexible logic without writing a custom payload or merger.

| Mode              | Description                                                     |
| ----------------- | --------------------------------------------------------------- |
| `KEEP_VALUES`     | (default) Use value from previous record if column is missing   |
| `FILL_DEFAULTS`   | Fill missing columns with default values from Avro schema       |
| `IGNORE_DEFAULTS` | Skip update if current record has schema default value          |
| `IGNORE_MARKERS`  | Skip update if current record matches a configured marker value |

This config supports:

* Use cases like **Debezium/CDC**, where marker values signify unknown/unavailable fields.
* **Sparse updates** from streaming systems like Kafka, Flink.
* **Backward-compatible upserts** during schema evolution.

This behavior is now decoupled from merge mode, and supports all ingestion sources uniformly.

---

# Payload Migration Table

*(Expanded with context on industry usage and reasoning)*

| Payload Class                               | Merge Mode + Partial Update Mode           | Changes Proposed                                                                                                                                                                                                                                                                                   | Recommendations to User                                                                | Behavior / Notes                                                                                                 |
| ------------------------------------------- | ------------------------------------------ |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `OverwriteWithLatestAvroPayload`            | `COMMIT_TIME_ORDERING`                     | Upgrade process sets right merge mode and add legacy payload class from table config.                                                                                                                                                                                                              | No action                                                                              | Most common for bulk ingest. Removing payload makes delete marker support consistent across COW/MOR.             |
| `DefaultHoodieRecordPayload`                | `EVENT_TIME_ORDERING`                      | Upgrade process sets right merge mode and remove payload class from table config. Set `hoodie.write.enable.event.time.watermark.in.commit.metadata=true` to produce event time watermarks commit metadata.                                                                                         | No action                                                                              | Default since Hudi 0.5.0; behavior unchanged.                                                                    |
| `EventTimeAvroPayload`                      | `EVENT_TIME_ORDERING`                      | Set merge mode and remove payload class config                                                                                                                                                                                                                                                     | No action                                                                              | Needed for out-of-order ingestion from Kafka, Pulsar, Flink. Deprecated in favor of `EVENT_TIME_ORDERING`.       |
| `FirstValueAvroPayload`                     | N/A                                        | Stop support unless explicit merger class is defined                                                                                                                                                                                                                                               | Users should implement explicit merger logic                                           | Rarely used in OSS. If multiple rows have same ordering value, older wins. Use case: dedup based on first-write. |
| `OverwriteNonDefaultsWithLatestAvroPayload` | `COMMIT_TIME_ORDERING` + `IGNORE_DEFAULTS` | Upgrade process automatically sets the partial update mode to table property. Add support for "Partial update mode" feature in general.                                                                                                                                                            | Upgrade Readers before writers. (Writer changes will only kick in for table version 9)                                    | Solves issues like empty strings treated as valid values. Cleaner than previous manual null checks.              |
| `PartialUpdateAvroPayload`                  | `EVENT_TIME_ORDERING` + `KEEP_VALUES`      | Upgrade process automatically sets the partial update mode in table property. Add support for "Partial update mode" feature in general.                                                                                                                                                            | Upgrade Readers before writers. (Writer changes will only kick in for table version 9)                                                                 | Common in streaming UPSERT. Used in streaming CDC pipelines.                                                     |
| `AWSDmsAvroPayload`                         | `COMMIT_TIME_ORDERING`                     | Upgrade process sets custom delete marker properties (hoodie.payload.delete.field = 'Op'  and hoodie.payload.delete.marker = 'D'  ) in table property                                                                                                                                              | Upgrade Readers before writers. (Writer changes will only kick in for table version 9) | Fixes delete handling in MoR read paths; used in AWS DMS-based ingestion.                                        |
| `MySqlDebeziumAvroPayload`                  | `EVENT_TIME_ORDERING`                      | Add support for multi-ordering values feature in general. Upgrade automatically sets the merge mode in table property.                                                                                                                                                                             | For existing tables: update `hoodie.table.precombine.field` config for multiple ordering fields. | Important in banking/transactional ingestion.                                                                    |
| `PostgresDebeziumAvroPayload`               | `EVENT_TIME_ORDERING` + `IGNORE_MARKERS`   | a.  Upgrade automatically sets `hoodie.table.partial.update.mode` to `IGNORE_MARKERS`  table property and b. Upgrade automatically sets `hoodie.table.partial.update.custom.marker`  as `__debezium_unavailable_value` c. Rollback any pending commits and trigger full compaction during upgrade. | No action                                                                              | CDC systems like Debezium mark unavailable fields. Full compaction is needed to migrate.                         |
| `ExpressionPayload`                         | N/A                                        | Leave unchanged                                                                                                                                                                                                                                                                                    | No action                                                                              | Used in `Merge into (...) where` logic in SQL. Will eventually be rewritten into a merger.                       |
| `HoodieMetadataPayload`                     | N/A                                        | An explicit merger class is provided during the upgrade                                                                                                                                                                                                                                            | No action                                                                              | Not impacted. Handles metadata table compactions. Merges handled explicitly for performance and correctness.     |

---

# Required Changes Highlighted

## Reader Side
* Create an enum class named `PartialUpdateMode` for partial update modes defined above as follows.
  ```java
  public enum PartialUpdateMode {
    NONE, KEEP_VALUES, FILL_DEFAULTS, IGNORE_DEFAULTS, IGNORE_MARKERS
  }
  ```
  The mode `NONE` represents no partial update should be attempted, which is introduced for lower table versions. 
* Introduce a new table configuration `hoodie.table.partial.update.mode` for partial update mode, whose default value is `NONE`.
* Introduce a new table configuration `hoodie.table.merge.properties` to collect a list of configurations that could be  
  used during merging for above mentioned payloads. E.g., for `PostgresDebeziumAvroPayload`, \
  `hoodie.table.merge.properties="hoodie.table.partial.update.custom.marker=__debezium_unavailable_value"`.
* `BufferedRecordMergerFactory` generates two more partial-update related mergers, **CommitTimeBufferedRecordPartialUpdateMerger**,
  **EventTimeBufferedRecordPartialUpdateMerger**, which are used for partial update modes for `COMMIT_TIME_ORDERING` and `EVENT_TIME_ORDERING` merge modes.
* Class `PartialUpdateStrategy` implements the detailed logics for all partial update modes, which is wrapped into above
  mergers. We can employ a branching to trigger a specific partial logic based on the input partial update mode due to
  simplicity of the implementation.

## Writer Side


## Upgrade/Downgrade Support

---

# Q\&A

### How do we support old Hudi version readers?

To maintain compatibility with older table versions and readers:

* Writers will retain the existing `hoodie.payload.class` property in metadata
* New properties (`hoodie.write.partial.update.mode`) will be added
* Table version will be bumped to 9 only when necessary (e.g., watermark handling, partial update logic)

This enables:

* **Backward compatibility** with Presto, Hive, Trino, Spark <3.2 etc.
* Smooth upgrade path for large enterprises with dozens of readers
* Incremental migration and testing at table-level granularity

---

# Open Items

* **Should writers fully abandon mergers for standard merge modes?**

  * Recommendation: Yes, for standard use cases like bulk insert, streaming UPSERT, and log compaction
  * Only fallback to `HoodieRecordMerger` when advanced use cases are needed (e.g., custom dedup logic, schema-aware merge with metadata)
  * `CopyOnWriteMergeHandle` may still need a light wrapper to resolve nulls/defaults during record rewrite

* **Do we need to retain payload interface for Flink integration?**

  * Flink support is evolving rapidly, and migrating to merger-based design aligns better with unified data plane
  * Current Flink connectors may require a thin compatibility layer

* **Should `ExpressionPayload` also be migrated?**

  * Likely in a separate proposal. Needs a compiler step to translate SQL expressions to merge plan, not inline payload code.

---
