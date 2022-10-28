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

# [WIP] RFC-XX: New Hudi Table Spec API for Query Integrations

## Proposers

- @codope
- @alexeykudinkin

## Approvers

- @xiarixiao
- @danny0405
- @vinothchandar
- @prasanna
- @xushiyan

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-XXX

> Please keep the status updated in `rfc/README.md`.

## Abstract

In this RFC we propose new set of higher-level Table Spec APIs that would allow
us to up-level our current integration model with new Query Engines, enabling faster 
turnaround for such integrations.

## Background

TBA

## Implementation

```
////////////////////////////////////////////////////////////////////////
// Qs
//
//  1. Some of the models defined here do already exist and might be representing lower-level
//     components. Do we expose them in a higher level APIs or do we bifurcate and hide them
//     as internal impl details (for ex, `HoodieFileGroup` vs `HoodieInternalFileGroup`)?
//
////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////
// MODELs
////////////////////////////////////////////////////////////////////////

// File Slice holds
//  - For COW: a base file
//      - A single snapshot (version) of the batch of co-located records, persisted in a single base file)
//  - For MOR: a base file along w/ corresponding delta-log files
//      - A collection of snapshots (versions) comprised of
//          a) snapshot of the initial batch of the co-located records (persisted in a single base file), along with
//          b) subsequently updated versions of these records
class HoodieFileSlice {
// Id of the file-group this slice belongs to
HoodieFileGroupId fileGroupId;

    String baseFilePath;
    List<String> logFilesPaths;
}

// Represents logical grouping of the File Slices (or snapshots of the given batch of records)
//
// NOTE:
//      - In case of COW set of records w/in the File Group wouldn't change
class HoodieFileGroup {
// Id of the file-group
HoodieFileGroupId id;

    // Projection of the timeline of committed actions affecting file-slices in this group
    HoodieTimeline timeline

    // File-slices ordered by corresponding instant they've been committed at
    TreeMap<HoodieInstant, FileSlice> fileSlices
}

// Describes particular partition w/in the dataset
class PartitionDescriptor {
// Relative partition path w/in the dataset
String partitionPath

    // List of values for corresponding partition columns
    List<Object> partitionColumnValues
}

// Partition Snapshot represents a state of a single table's partition at any particular
// instant T, comprised of the file-slices that are the _latest_ (current) w/in their
// corresponding file-groups at the instant T
//
// NOTE: Non-partitioned tables assumed to have 1 partition enclosing the whole table
class PartitionSnapshot {
PartitionDescriptor descriptor

    // (Latest) Instant T as of which partition's state is represented
    HoodieInstant latestInstant

    // File-slices current at the instant T
    List<HoodieFileSlice> fileSlices
}

// Partition Incremental Snapshot represents a state of a single table's partition at a particular
// instant T_y, while only considering commits that occurred in the table after T_x (ie w/in the timeline T_x -> T_y)
// comprised of the file-slices that are the _latest_ (current) w/in their corresponding file-groups
// at instant T_y
//
// NOTE: Partition Snapshot is a special case of Partition Incremental Snapshot where T_x = 0
class PartitionIncrementalSnapshot {
PartitionDescriptor descriptor

    // Timeline starting at T_x and ending at T_y, completed actions thereof represent state of the partition
    HoodieTimeline timeline

    // File-slices current at the instant T_y
    List<HoodieFileSlice> fileSlices
}

enum ReadingMode {
// Representing state of the table at a particular instant T
SNAPSHOT,

    // (MOR only) Representing state of the table at a particular instant T,
    // reading exclusively base-files (ie, ignoring any delta-logs if present)
    SNAPSHOT_READ_OPTIMIZED,

    // Representing state of the table at a particular instant T_y assuming,
    // as an initial instant Tx (snapshot is a specification of this mode, where T_x = 0)
    INCREMENTAL,

    // Representing state of the table as a stream of CRUD operations on
    // individual records
    CDC
}

////////////////////////////////////////////////////////////////////////
// APIs
////////////////////////////////////////////////////////////////////////

class Timeline {
// Looks up latest commit instant, provide (temporal) instant T
//
// This method is required to resolve commit-instants by the timestamp
HoodieInstant findLatestCompletedActionAt(Instant)
}

class RecordIndex {
// Tags provided record-keys (PKs) w/ corresponding locations w/in the dataset;
// in case record-key is not currently present in the dataset, no location will be provided
HoodiePairData<HoodieKey, Option<HoodieRecordLocation>> tag(HoodieData<HoodieKey> records);
}

class FileIndex {
// Lists files visible/reachable at the instant T
// Equivalent to `listFilesBetween(null, instant, filters)`
List<PartitionSnapshot> listFilesAt(HoodieInstant, Filter[])

    // Lists files added visible/reachable at the instant `to`, that were
    // added no earlier than at the instant `from`
    List<PartitionIncrementalSnapshot> listFilesBetween(HoodieInstant from, HoodieInstant to, Filter[])

    // TODO add CDC-capable API
}

class FileSliceReader {
    // Projects output of this reader as a projection of the provided schema
    // NOTE: Provided schema could be an evolved schema
    FileSliceReader project(InternalSchema)

    // Pushes down filters to low-level file-format readers (if supported)
    FileSliceReader pushDownFilters(Filter[])

    // Specifies reading mode for this reader
    FileSliceReader readingMode(ReadingMode)

    // Produces an iterable sequence of records from the particular file-slice
    Iterator<HoodieRecord> open(HoodieFileSlice)

}
```

## Rollout/Adoption Plan

TBA

## Test Plan

TBA