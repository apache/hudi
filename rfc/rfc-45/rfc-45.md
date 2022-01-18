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

# RFC-45: Asynchronous Metadata Indexing

## Proposers

- @codope
- @manojpec

## Approvers

- @nsivabalan
- @vinothchandar

## Status

JIRA: [HUDI-2488](https://issues.apache.org/jira/browse/HUDI-2488)

## Abstract

Metadata indexing (aka metadata bootstrapping) is the process of creation of one
or more metadata-based indexes, e.g. data partitions to files index, that is
stored in Hudi metadata table. Currently, the metadata table (referred as MDT
hereafter) supports single partition which is created synchronously with the
corresponding data table, i.e. commits are first applied to metadata table
followed by data table. Our goal for MDT is to support multiple partitions to
boost the performance of existing index and records lookup. However, the
synchronous manner of metadata indexing is not very scalable as we add more
partitions to the MDT because the regular writers (writing to the data table)
have to wait until the MDT commit completes. In this RFC, we propose a design to
support asynchronous metadata indexing.

## Background

We can read more about the MDT design
in [RFC-15](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+Improvements)
. Here is a quick summary of the current state (Hudi v0.10.1). MDT is an
internal Merge-on-Read (MOR) table that has a single partition called `files`
which stores the data partitions to files index that is used in file listing.
MDT is co-located with the data table (inside `.hoodie/metadata` directory under
the basepath). In order to handle multi-writer scenario, users configure lock
provider and only one writer can access MDT in read-write mode. Hence, any write
to MDT is guarded by the data table lock. This ensures only one write is
committed to MDT at any point in time and thus guarantees serializability.
However, locking overhead adversely affects the write throughput and will reach
its scalability limits as we add more partitions to the MDT.

## Goals

- Support indexing one or more partitions in MDT while regular writers and table
  services (such as cleaning or compaction) are in progress.
- Locking to be as lightweight as possible.
- Keep required config changes to a minimum to simplify deployment / upgrade in
  production.
- Do not require specific ordering of how writers and table service pipelines
  need to be upgraded / restarted.
- If an external long-running process is being used to initialize the index, the
  process should be made idempotent so it can handle errors from previous runs.
- To re-initialize the index, make it as simple as running the external
  initialization process again without having to change configs.

## Implementation

### A new Hudi action: INDEX

We introduce a new action `index` which will denote the index building process,
the mechanics of which is as follows:

1. From an external process, users can issue a CREATE INDEX or similar statement
   to trigger indexing for an existing table.
    1. This will add a `<instant_time>.index.requested` to the timeline, which
       contains the indexing plan.
    2. From here on, the index building process will continue to build an index
       up to instant time `t`, where `t` is the latest completed instant time on
       the timeline without any
       "holes" i.e. no pending async operations prior to it.
    3. The indexing process will write these out as base files within the
       corresponding metadata partition. A metadata partition cannot be used if
       there is any pending indexing action against it.

2. Any inflight writers (i.e. with instant time `t'` > `t`)  will check for any
   new indexing request on the timeline prior to preparing to commit.
    1. Such writers will proceed to additionally add log entries corresponding
       to each such indexing request into the metadata partition.
    2. There is always a TOCTOU issue here, where the inflight writer may not
       see an indexing request that was just added and proceed to commit without
       that. We will correct this during indexing action completion. In the
       average case, this may not happen and the design has liveness.

3. When the indexing process is about to complete, it will check for all
   completed commit actions to ensure each of them added entries per its
   indexing plan, otherwise simply abort after a configurable timeout. Let's
   call this the **indexing check**.
    1. The corner case here would be that the indexing check does not factor in
       the inflight writer just about to commit. But given indexing would take
       some finite amount of time to go from requested to completion (or we can
       add some, configurable artificial delays here say 60 seconds), an
       inflight writer, that is just about to commit concurrently, has a very
       high chance of seeing the indexing plan and aborting itself.

We can just introduce a lock for adding events to the timeline and these races
would vanish completely, still providing great scalability and asynchrony for
these processes.

### Multi-writer scenario

![](./async_metadata_index.png)

Let us walkthrough a concrete mutli-writer scenario to understand the above
indexing mechanism. In this scenario, let instant `t0` be the last completed
instant on the timeline. Suppose user triggered index building from an external
process at `t3`. This will create `t3.index.requested` file with the indexing
plan. The plan contains the metadata partitions that need to be created and the
last completed instant, e.g.

```
[
  {MetadataPartitionType.FILES.partitionPath(), t0}, 
  {MetadataPartitionType.BLOOM_FILTER.partitionPath(), t0}, 
  {MetadataPartitionType.COLUMN_STATS.partitionPath(), t0}
]
```

Further, suppose there were two inflight writers Writer1 and Writer2 (with
inflight instants `t1` and `t2` respectively) while the indexing was requested
or inflight. In this case, the writers will check for pending index action and
find a pending instant `t3`. Now, if the metadata index creation is inflight and
a basefile is already being written under the metadata partition, then each
writer will create log files in the same filegroup for the metadata index
update. This will happen within the existing data table lock. However, if the
indexing has still not started and instant `t3` is still in requested state,
then writer will still continue to log entries but indexer will handle this
scenario and assign the same filegroup id when `t3` transitions to inflight.

The indexer runs in a loop until the metadata for data upto `t0` plus the data
written due to `t1` and `t2` has been indexed, or the indexing timed out. After
timeout, indexer will abort writing the instant upto which indexing was done
in `t3.index.inflight` file in the timeline. At this point, user can trigger the
index process again, however, this time `t2` will become the last completed
instant. This design ensures that the regular writers do not fail due to
indexing.

### Error Handling

**Case 1: Writer fails while indexer is inflight**

This means index update due to writer did not complete. Indexer continues to
build the index ignoring the failed instant due to writer. The next update by
the writer will trigger a rollback of the failed instant, which will also
rollback incomplete updates in metadata table.

**Case 2: Indexer fails while writer is inflight**

Writer will commit adding log entries to the metadata partition. Indexer will
fetch the last instant for which indexing was done from `.index.inflight` file.
It will start indexing again from the instant thereafter.

**Case 3: Race conditions**

a) Writer went inflight just after an indexing request was added but indexer has
not yet started executing.

In this case, writer will continue to log updates in metadata partition. At the
time of execution, indexer will see there are already some log files and handle
that in the indexing check.

b) Inflight writer about to commit, but indexing completed just before that.

In this case, since the indexer completed before the writer, so it has already
missed the index updates due to the writer. We can let async compaction on the
metadata table handle this scenario so that the log files written by the writer
are merged into a base file. But what if the async compaction has not even
completed and there is another indexing request? What will be the latest
completed instant then?

Or, we can introduce a lock for adding events to the metadata timeline.

**Case 4: Async table services**

The metadata partition cannot be used if there is any pending index action
against it. So, async compaction/cleaning/clustering will ignore the metadata
partition for which indexing is inflight.

## Rollout/Adoption Plan

- What impact (if any) will there be on existing users?

There can be two kinds of existing users:

a) Enabling metadata for the first time: There should not be any impact on such
users. When they enable metadata, they can trigger indexing process. b) Metadata
already enabled: Such users already have metadata table with at least one
partition. If they trigger indexing process, then the indexer should take into
account the existing metadata and ignore instants upto which MDT is in sync with
the data table.

- If we are changing behavior how will we phase out the older behavior?

The changes will be backward-compatible and if the async indexing is diabled
then the existing behavior of MDT creation and updates will be used.

- If we need special migration tools, describe them here.

Not required.

- When will we remove the existing behavior

Not required

## Test Plan

- Extensive unit tests to cover all scenarios including conflicts and
  error-handling.
- Run a long-running test on EMR cluster with async indexing enabled.
