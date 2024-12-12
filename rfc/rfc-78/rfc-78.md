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
# RFC-78: 1.0 Migration

## Proposers

- @nsivabalan
- @vbalaji

## Approvers
 - @yihua
 - @codope

## Status

JIRA: [HUDI-7856](https://issues.apache.org/jira/browse/HUDI-7856)

> Please keep the status updated in `rfc/README.md`.

## Abstract

[Hudi 1.x](https://github.com/apache/hudi/blob/ae1ee05ab8c2bd732e57bee11c8748926b05ec4b/rfc/rfc-69/rfc-69.md) is a powerful 
re-imagination of the transactional database layer in Hudi to power continued innovation across the community in the coming 
years. It introduces lot of differentiating features for Apache Hudi. Feel free to checkout the 
[release page](https://hudi.apache.org/releases/release-1.0.0-beta1) for more info. We had beta1 and beta2 releases which was meant for 
interested developers/users to give a spin on some of the  advanced features. But as we are working towards 1.0 GA, we are proposing 
in this RFC a strategy for smoother migration for existing Hudi users. 

## Objectives 

Goal is to have a smooth migration experience for the users from 0.x to 1.0. A typical organization might have a
medallion architecture deployed with 1000s of Hudi pipelines spread across bronze, silver and gold layer, where most
pipelines can be both readers and writers and a few reader only pipelines (gold). For this layout of pipelines, here are
some challenges faced during migration.\

1. Existing pipelines are in 0.15.x. (bronze, silver, gold)
2. Migrate gold pipelines to 1.x.\
   &emsp; We need to strictly migrate only gold pipelines to 1.x. Because, a 0.15.x reader may not be able to read 1.x
   Hudi tables. So, if we migrate any of silver pipelines to
   1.x before migrating entire gold layer, we might end up in a situation, where a 0.15.x reader (gold) might end up
   reading 1.x table (silver). This might lead to failures.
   So, we have to follow certain order in which we migrate pipelines.
3. Once all of gold is migrated to 1.x, we can move all of silver to 1.x.
4. Once all of gold and silver pipelines are migrated to 1.x, finally we can move all of bronze to 1.x.

As you could see, we need some coordination with which we need to migrate. And in a very large organization, sometimes
we may not have good control over downstream consumers.
Hence, coordinating entire migration workflow and orchestrating the same might be challenging. And here we just spoke
about happy paths. But there are failure scenarios which
could make it even more challenging. For instance, after upgrade, if users prefer to downgrade, then downgrade should
ensure to completely revert all changes
done in 1.x so that a 0.15.x reader and writer can continue without any issues.
Hence, the migration protocol is key to ensure a smooth migration experience for the users.

#### Here are the objectives with this migration strategy

- 1.x reader should be able to read 0.14.x or later tables (table version 6) w/o any loss in 0.x functionality and
  guaranteeing no data inconsistencies. By no data inconsistencies, we mean, the queries results ff
- 1.x writer should be able to write in both the table versions tables (table version 6 and the current version) w/o any
  loss in 0.x functionality and guaranteeing no data inconsistencies.
  But for new features that was introduced in 1.x, we may not be able to support all of them.
- In this case, we explicitly request users to not turn on these features untill all readers are completely migrated to
  1.x so as to not break any readers as applicable.

Connecting back to our example above, lets see how the migration might look like for an existing user. 

1. Stop any async table services in 0.x completely.
2. Upgrade writers to 1.x with table version 6 (tv=6, this won't auto-upgrade anything); 0.x readers will continue to
   work; writers can also be readers and will continue to read both tv=6.
3. Upgrade table services to 1.x with tv=6, and resume operations.
4. Upgrade all remaining readers to 1.x, with tv=6.
5. Redeploy writers with tv=8; table services and readers will adapt/pick up tv=8 on the fly.
6. Once all readers and writers are in 1.x, we are good to enable any new features (like NBCC) with 1.x tables.

### Considerations when choosing migration strategy

- While migration is happening, we want to allow readers to continue reading data. This means, we cannot employ a
  stop-the-world strategy when we are migrating.
  All the actions that we are performing as part of table upgrade should not have any side-effects of breaking snapshot
  isolation for readers.
- Also, users should have migrated to 0.14.x or later before upgrading to 1.x. We do not want to add read support for
  very old versions of hudi in 1.x (for eg 0.7.0).

The recommended guideline is to upgrade all readers and writers to 0.14.x or later. and then slowly start upgrading to
1.x(readers followed by writers). Support matrix for different query types is given below in another section. 

Before we dive in further, lets understand the format changes:

## Format changes in 1.x
### Table properties
- Payload class ➝ payload type.
- hoodie.record.merge.mode is introduced in 1.x. 
- New metadata partitions could be added (optionally enabled)

### MDT changes
- New MDT partitions are available in 1.x. MDT schema upgraded.
- RLI schema is upgraded to hold row positions.

### Timeline:
- [storage changes] Completed write commits have completed times in the file name(timeline commit files).
- [storage changes] Completed and inflight write commits are in avro format which were json in 0.x.
- We are switching the action type for pending clustering from “replace commit” to “cluster”.
- [storage changes] Timeline ➝ LSM timeline. There is an archived timeline in 1.x, but its been redesigned for scalable access.
- [In-memory changes] HoodieInstant changes due to presence of completion time for completed HoodieInstants.
- Timeline is under `.hoodie/timeline` and LSM timeline is under `.hoodie/timeline/history` in 1.x.
- Commit instants w/ completion time should be parsed and deduced.
- Commit metadata in avro instead of json should be readable.
    - More details on this under Implementation section.
- Pending Clustering commits using “cluster” action.
- HoodieDefaultTimeline should be able to support both 0.x timeline and 1.x timeline.
    - More details on this under Implementation section.

### Filegroup/FileSlice changes:

- Log file names contain delta commit time instead of base instant time.
- Log appends are disabled in 1.x. In other words, each log block is already appended to a new log file. There is 1-1
  mapping from a log block to log file.
- File Slice determination logic for log files changed. In 0.x, we have base instant time in log files and its straight
  forward.
  In 1.x, we find completion time for a log file and find the base instant time (parsed from base files) for a given
  HoodieFileGroup,
- which has the highest value lesser than the completion time of the log file) of interest.
- Log file ordering within a file slice. (in 0.x, we use base instant time ➝ log file versions ➝ write token) to order
  diff log files. in 1.x, we will be using completion time to order).
- Rollbacks in 0.x appends a new rollback block (new log file). While in 1.x, rollback will remove the partially failed
  log files.

### Log format changes:
- We have added a new header type, `IS_PARTIAL` in 1.x.

#### Completion time based read in FileGroup/FileSlice grouping and log block reads
- 1.0 reader will have the capability to read log files written in table version 6. But we may not need to port over the
  completion time based read support to 1.0.

### FileSystemView:
- 1.0 can do file slicing based on completion time as well as base instant time.

### Table properties
- payload type inference from payload class need to be ported.
- hoodie.record.merge.mode needs to be ported over. 

### MDT changes:
- MDT schema changes need to be ported.

### Log reader/format changes:
- New log header type need to be ported.
- For unsupported features, meaningful errors should be thrown.  

### Read optimized query:
- No explicit changes required (apart from timeline, and FSV) to support read optimized query.  

### Incremental reads and time travel query: 

- Incremental reads in 1.x is expected to have some changes and design is not fully out.
- Time travel query: There aren't notable changes in 1.x wrt time travel query. So, as per master, we are still using
  commit time (i.e not completed time) to serve time travel query.

### CDC reads:
- There has not been a lot of changes in 1.x wrt CDC. Only minor change was done in HoodieAppendHandle. but that affects
  only the writer side logic. So, we are good wrt CDC.

## Implementation details

### 1.0 Writer changes:

- Add new write config `hoodie.write.table.version` to specify the table version to be created and produced by the
  writer.
- Add new write config `hoodie.write.auto.upgrade` to control whether or not writer can auto-upgrade a table when
  attempting to write to a table with lower version.
- Wiring in the table version in all write paths and validate that write and table versions match, on all write
  operations and tooling.
- Only table versions 6,8 are supported as valid value for hoodie.write.table.version
- Handle rollback done in startCommit* methods and as part of upgrade, w.r.t table version mismatches

### Timeline changes

Support 0.x and 1.x implementation for timeline and related classes. Major changes (as part
of [HUDI-8076](https://issues.apache.org/jira/browse/HUDI-8076) proposal):

1. Create Factory Interface for instantiating HoodieInstant and Hoodie Timelines.
2. Use TimelineLayout to determine what version to use for Timeline and instant specific logic.
3. HoodieInstant has following new interfaces:
   (a) InstantFactory : Construct HoodieInstant
   (b)InstantFileNameFactory : Construct filenames from HoodieInstant
   (c) InstantFileNameParser : Encapsulates logic to extract timestamp
   (d) InstantComparator : Encapsulates logic to compare instants (e:g completion time based) - Each of these interfaces
   has implementation for 0.15 and 1.x release versions.
4. Make Timeline classes such as HoodieActiveTImeline, HoodieArchiveTImeline and DefaultTImeline as interfaces.
6. Port 0.x timeline and HoodieInstant logic as one implementations of the above factory and timeline interfaces.
7. Refactor existing timeline classes as 1.x implementation for the new timeline interfaces.
8. Handle 0.x commit and other metadata
9. Create new interface for ArchiveTimelineWriter and migrate 0.x code.

### Log file handling and FileSystemView changes

- Annotate log headers, blocks with table version (tv)
- Handle both tv=6, and tv=8 for log naming
- Eliminate notion of rolloverWriteToken
- Simplify/make efficient log writer building across code paths
- Handle both tv=6 & 8 for log version, handling
- Prevent tv=8 headers from being written with tv=6.
- Bring back tv=6 rollback behavior.

### Flink

Most of the timeline, FSV are shared b/w spark and flink. Only additional changes we did to Flink that is not in spark (
yet) is the incremental read. We have introduced completion time based incremental query capabilities in 1.x Flink
reads. 

### Upgrade Implementation

The following steps outline the upgrade process for transitioning an Apache Hudi table to a newer version. The upgrade
ensures that the table metadata, configurations, and timelines are updated to align with the new version requirements.

---

#### **Step-by-Step Upgrade Process**

1. **Check for Auto-Upgrade Configuration**:
    - If auto-upgrade is disabled:
        - Ensure that metadata tables are either disabled or already compatible.
        - Set the table version to the legacy version (e.g., version 6).
        - Exit the upgrade process without making further changes.

2. **Synchronize Metadata Table**:
    - If the table is a data table and metadata table functionality is enabled:
        - Check if the metadata table is behind the data table version.
        - If so, delete the outdated metadata table to ensure compatibility.

3. **Rollback and Compact Table**:
    - Perform rollback operations to address any failed writes.
    - Run a compaction operation to clean up the table and optimize file organization.
    - Ensure these operations align with the new table version's expectations.

4. **Upgrade Timeline Layout**:
    - Create a new table layout on storage, specifying the updated timeline layout version.

5. **Upgrade Table Properties**:
    - Add or modify key table properties to align with the new version:
        - Set the timeline path property to its default value.
        - Upgrade partition fields to match the new table schema.
        - Update merge mode configurations.
        - Set the initial version of the table.
        - Update key generator type for record generation.
        - Upgrade bootstrap index type for handling new table structures.

6. **Handle Active Timeline Instants**:
    - Retrieve all timeline instants from the active timeline.
    - Rewrite each instant into the new timeline format:
        - Generate the original instant file name.
        - Apply serialization updates using appropriate serializers for the old and new formats.
        - Update the active timeline with the new instant format.

7. **Upgrade Archived Timeline to LSM Timeline Format**:
    - Convert the archived timeline into an LSM (Log-Structured Merge) timeline format for efficient time-range queries
      and improved performance.

---

This process ensures that the table structure, metadata, and timelines are fully aligned with the newer version while
maintaining data integrity and compatibility. It also accounts for incremental upgrades, rollback safety, and
optimization of metadata management.

### Downgrade Implementation

The following steps outline the downgrade process for transitioning an Apache Hudi table to an earlier version. The
downgrade ensures that the table metadata, configurations, and timelines are reverted to match the requirements of the
older version.

---

#### **Step-by-Step Downgrade Process**

1. **Rollback and Compact Table**:
    - Address any failed writes through rollback operations.
    - Perform compaction to clean up the table and ensure it is in a stable state.
    - Align these operations with the expectations of the older table version.

2. **Handle Timeline Downgrade**:
    - Retrieve all timeline instants from the active timeline, including incomplete ones.
    - Rewrite each instant to match the format used in the older table version:
        - Use the appropriate serializers for the current and target table versions.
        - Update the active timeline with the downgraded instants.
    - Convert the LSM (Log-Structured Merge) timeline format back to the archived timeline format used in the older
      version.

3. **Downgrade Table Properties**:
    - Modify table properties to match the older version:
        - Downgrade partition fields to align with the schema requirements of the older version.
        - Remove the initial version setting added in the newer version.
        - Reset the record merge mode to its previous configuration.
        - Downgrade the key generator type to the format used in the older version.
        - Downgrade the bootstrap index type to the format supported in the target version.

4. **Handle Metadata Table**:
    - If a metadata table is available:
        - Remove any metadata partitions that are unsupported in the target version.
        - Update the metadata table version to match the target table version.

5. **Handle LSM Timeline Downgrade**:
    - Attempt to revert the LSM timeline format back to the archived timeline format.
    - Log warnings if any issues occur but continue the downgrade process.

---

This process ensures that the table structure, metadata, and timelines are reverted to a compatible state for the older
version while maintaining data integrity. It also accounts for handling metadata partitions, ensuring compatibility with
archived timelines, and rolling back any incompatible features.

## Support Matrix for different readers and writers

|                         | Reader 0.x | Reader 1.x tv=6/8 | Table Services 0.x | Table Services 1.x tv=6 | Table Services 1.x tv=8 |
|-------------------------|------------|-------------------|--------------------|-------------------------|-------------------------|
| Writer 1.x tv=6         | Y          | Y                 | N                  | Y                       | N                       |
| Writer 1.x tv=8         | N          | Y                 | N                  | N                       | Y                       |
| Table Services 1.x tv=6 | Y          | Y                 | N                  | Y                       | N                       |
| Table Services 1.x tv=8 | N          | Y                 | N                  | N                       | Y                       |

### Limitations

When performing an upgrade or downgrade between Apache Hudi versions, the following limitations must be taken into
account to ensure a smooth transition and maintain data integrity:

1. **Metadata Table Writes**:
    - Metadata table writes are **not allowed** during the upgrade or downgrade process.
    - Both writers and readers must have metadata explicitly disabled (`hoodie.metadata.enable=false`) until the upgrade
      or downgrade process is complete.
    - Metadata table will only synchronize with the data table once all writers have been upgraded to the final target
      version.

2. **Async Table Services**:
    - All asynchronous table services (e.g., compaction, clustering) must be stopped before initiating an upgrade or
      downgrade.
    - Failing to stop table services may result in inconsistencies or compatibility issues.

3. **Reader and Writer Compatibility**:
    - During the rolling upgrade or downgrade, readers and writers must operate with the same table version (`tv=6`).
    - Mixing readers or writers across incompatible table versions (e.g., `tv=6` and `tv=8`) is not supported and will
      lead to failures.

4. **Timeline Format Changes**:
    - Timeline format changes between versions require careful migration. Instants in the timeline are rewritten during
      the process, and incomplete migrations may cause data access issues.
    - Ensure the migration process is fully completed before resuming normal operations.

5. **Metadata Partitions**:
    - Metadata partitions that are unsupported in the target table version must be explicitly removed during downgrade.
    - Automatic handling of unsupported partitions is not provided, requiring manual intervention or pre-configured
      steps.

6. **Auto-Upgrade and Auto-Downgrade**:
    - Auto-upgrade or auto-downgrade is **not supported** during the rolling process.
    - Users must manually set `hoodie.write.auto.upgrade=false` to prevent unintended version transitions.

7. **Backward Compatibility for Table Services**:
    - Some features introduced in later versions may not be available in earlier versions (e.g., advanced clustering
      strategies).
    - Table services need to be reconfigured for compatibility when downgrading.

8. **Rollback and Compaction**:
    - Rollback and compaction operations are mandatory during both upgrade and downgrade to ensure table consistency.
    - These operations may introduce additional processing overhead and should be planned during low-traffic periods.

9. **New Features Activation**:
    - New features available in the upgraded version cannot be activated until all components (readers, writers, and
      table services) have been fully upgraded to the final target version.

10. **Safe Recovery from Failures**:

- The upgrade and downgrade processes are designed to be idempotent; however, users must ensure table locking mechanisms
  are in place to prevent concurrent writes or reads during incomplete migrations.
- Mid-process failures require restarting the operation from a stable state to avoid data corruption.

11. **CDC and Incremental Queries**:

- Change Data Capture (CDC) and incremental queries may be temporarily disrupted during the upgrade or downgrade.
- Ensure compatibility for such queries by verifying the data pipeline end-to-end after the process.

By adhering to these limitations, users can avoid common pitfalls and ensure a seamless transition between Apache Hudi
versions.

## Rollout/Adoption Plan

### 0.14.x (or later) ➝ 1.0 upgrade steps

1. Stop any async table services in 0.x completely.
2. Upgrade writers to 1.x with table version 6, `autoUpgrade` and metadata disabled (this won't auto-upgrade anything);
   0.x readers will continue to work; writers can also be readers and will continue to read both tv=6.
   a. Set `hoodie.write.auto.upgrade` to false.
   b. Set `hoodie.metadata.enable` to false.
3. Upgrade table services to 1.x with tv=6, and resume operations.
4. Upgrade all remaining readers to 1.x, with tv=6.
5. Redeploy writers with tv=8; table services and readers will adapt/pick up tv=8 on the fly.
6. Once all readers and writers are in 1.x, we are good to enable any new features, including metadata, with 1.x tables.

During the upgrade, metadata table will not be updated and it will be behind the data table. It is important to note
that metadata table will be updated only when the writer is upgraded to tv=8. So, even the readers should keep metadata
disabled during rolling upgrade until all writers are upgraded to tv=8.

### 1.0 ➝ 0.14.0 (or later) downgrade

The downgrade protocol is more or less similar to upgrade protocol with some differences.

1. Stop any async table services in 1.x completely.
    - Ensure no active table services (compaction, clustering) are running to avoid inconsistencies during the
      downgrade.

2. Downgrade writers to 1.x with table version 6, `autoUpgrade` and metadata disabled.
    - Ensure writers stop writing in table version 8 and switch to table version 6 for compatibility with older readers
      and table services.
    - Configure the following settings:
      a. Set `hoodie.write.auto.upgrade` to false.
      b. Set `hoodie.metadata.enable` to false.
    - Writers will now only write to table version 6, and metadata updates will remain paused.

3. Downgrade table services to 0.x and resume operations.
    - Replace the table services binary with the compatible 0.x version.
    - Resume compaction and clustering services after ensuring they align with table version 6.

4. Downgrade all readers to 0.x.
    - Replace reader binaries with the compatible 0.x version.
    - Verify that the readers are compatible with the downgraded table version and can read table version 6.

5. Redeploy writers with 0.x configuration.
    - Switch to 0.x writers to complete the downgrade.
    - Ensure that writers and table services operate with full compatibility in the 0.x environment.

6. Re-enable metadata for the 0.x environment, if required.
    - Once all writers and readers are downgraded to 0.x and operating normally, re-enable metadata updates if necessary
      for 0.x table version compatibility.

To recover from any failures, we will provide additional tooling through hudi-cli to rollback the table to a previous
state if necessary.

### Additional Considerations During Upgrade/Downgrade

- **Metadata Table Compatibility**:
    - Metadata table updates will remain paused during the downgrade to avoid inconsistencies. Metadata tables should
      only be re-enabled after all components are upgraded/downgraded and synchronized with corresponding table version.

- **Reader-Writer Compatibility**:
    - Ensure 1.x writers and 0.x readers do not operate concurrently, as this may result in compatibility issues.

- **Safe Upgrade/Downgrade Protocol**:
    - The upgrade/downgrade process should be idempotent. In case of failures, it should be safe to retry without
      corrupting the table state.

- **Testing and Validation**:
    - After each upgrade/downgrade step, test the functionality of the system to ensure no regressions or data
      inconsistencies.

## Test Plan

<Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.>