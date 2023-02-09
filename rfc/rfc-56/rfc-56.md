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

# RFC-56: Early Conflict Detection For Multi-writer

## Proposers

- @zhangyue19921010

## Approvers

- @yihua
- @prasannarajaperumal

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-1575

## Abstract

At present, Hudi implements an OCC (Optimistic Concurrency Control) based on timeline to ensure data consistency,
integrity and correctness between multi-writers. OCC detects the conflict at Hudi's file group level, i.e., two
concurrent writers updating the same file group are detected as a conflict. Currently, the conflict detection is
performed before commit metadata and after the data writing is completed. If any conflict is detected, it leads to a
waste of cluster resources because computing and writing were finished already.

To solve this problem, this RFC proposes an early conflict detection mechanism to detect the conflict during the data
writing phase and abort the writing early if conflict is detected, using Hudi's marker mechanism. Before writing each
data file, the writer creates a corresponding marker to mark that the file is created, so that the writer can use the
markers to automatically clean up uncommitted data in failure and rollback scenarios. We propose to use the markers
identify the conflict at the file group level during writing data. There are some subtle differences in early conflict
detection work flow between different types of marker maintainers. For direct markers, hoodie lists necessary marker
files directly and does conflict checking before the writers creating markers and before starting to write corresponding
data file. For the timeline-server based markers, hoodie just gets the result of marker conflict checking before the
writers creating markers and before starting to write corresponding data files. The conflicts are asynchronously and
periodically checked so that the writing conflicts can be detected as early as possible. Both writers may still write
the data files of the same file slice, until the conflict is detected in the next round of checking.

What's more? Hoodie can stop writing earlier because of early conflict detection and release the resources to cluster,
improving resource utilization.

Note that, the early conflict detection proposed by this RFC operates within OCC. Any conflict detection outside the
scope of OCC is not handle. For example, current OCC for multiple writers cannot detect the conflict if two concurrent
writers perform INSERT operations for the same set of record keys, because the writers write to different file groups.
This RFC does not intend to address this problem.

## Background

As we know, transactions and multi-writers of data lakes are becoming the key characteristics of building Lakehouse
these days. Quoting this inspiring blog <strong>Lakehouse Concurrency Control: Are we too optimistic?</strong> directly:
https://hudi.apache.org/blog/2021/12/16/lakehouse-concurrency-control-are-we-too-optimistic/

> "Hudi implements a file level, log based concurrency control protocol on the Hudi timeline, which in-turn relies
> on bare minimum atomic puts to cloud storage. By building on an event log as the central piece for inter process
> coordination, Hudi is able to offer a few flexible deployment models that offer greater concurrency over pure OCC
> approaches that just track table snapshots."

In the multi-writer scenario, Hudi's existing conflict detection occurs after the writer finishing writing the data and
before committing the metadata. In other words, the writer just detects the occurrence of the conflict when it starts to
commit, although all calculations and data writing have been completed, which causes a waste of resources.

For example:

Now there are two writing jobs: job1 writes 10M data to the Hudi table, including updates to file group 1. Another job2
writes 100G to the Hudi table, and also updates the same file group 1.

Job1 finishes and commits to Hudi successfully. After a few hours, job2 finishes writing data files(100G) and starts to
commit metadata. At this time, a conflict with job1 is found, and the job2 has to be aborted and re-run after failure.
Obviously, a lot of computing resources and time are wasted for job2.

Hudi currently has two important mechanisms, marker mechanism and heartbeat mechanism:

1. Marker mechanism can track all the files that are part of an active write.
2. Heartbeat mechanism that can track all active writers to a Hudi table.

Based on marker and heartbeat, this RFC proposes a new conflict detection: Early Conflict Detection. Before the writer
creates the marker and before it starts to write the file, Hudi performs this new conflict detection, trying to detect
the writing conflict directly (for direct markers) or get the async conflict check result (for timeline-server-based
markers) as early as possible and abort the writer when the conflict occurs, so that we can release compute resource as
soon as possible and improve resource utilization.

## Implementation

Here is the high level workflow of early conflict detection as shown in Figure 1 below. The early conflict detection is
guarded by a new feature flag. As we can see, when both `supportsOptimisticConcurrencyControl`
and `isEarlyConflictDetectionEnable` (the new feature flag) are true, we could use this early conflict detection
feature. Else, we skip this check and create marker directly.

![](figure1.png)

The three important steps marked in red in Figure 1 are introduced one by one as follows:

### [1] Check Marker Conflict

As we know, Hudi has two ways to create and maintain markers:

1. DirectWriteMarkers: individual marker file corresponding to each data file is directly created by the writer.
2. TimelineServerBasedWriteMarkers: marker operations are all handled at the timeline service which serves as a proxy

Therefore, for different types of Marker, we must implement the corresponding conflict detection logic based on the
markers. Here we design a new interface `HoodieEarlyConflictDetectionStrategy` to ensure the extensibility of checking
marker conflict.

![](flow1.png)

In this design, we provide `SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy` and
`SimpleDirectMarkerBasedEarlyConflictDetectionStrategy` for DirectWriteMarkers to perform corresponding conflict
detection and conflict resolution. And we provide `AsyncTimelineMarkerEarlyConflictDetectionStrategy` for
TimelineServerBasedWriteMarkers to perform corresponding conflict detection and conflict resolution

#### DirectWriteMarkers related strategy

##### SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy

![](figure2.png)

As for current strategy shown in Figure 2, we need to pay attention to the following details:

First, for the same partitionPath/fileId, only one writer can check and create the corresponding marker file at the same
time to make sure no conflict is missed in race conditions. We propose to use the existing transaction mechanism to lock
at the <strong>PartitionPath + "/" + fileID</strong> level before checking conflicts and before creating marker files.
Take ZK locker as an example => `LockConfiguration.ZK_LOCK_KEY_PROP_KEY, partitionPath + "/" + fileId`.

In addition, during conflict detection, we do not need to list all marker files in all directories. Instead, we can
prune the markers to check based on the current partition Path and fileID to avoid unnecessary list operations. For
example (also shown in figure2), we are going to create maker file based on partition path 2022/07/01, and fileID
ff26cb9e-e034-4931-9c59-71bac578f7a0-0. During marker conflict detection, we do not need to list all the partitions
under ".temp". Listing marker files under $BasePath/.hoodie/.temp/instantTime/2022/07/01 and checking if fileID
ff26cb9e-e034-4931-9c59-71bac578f7a0-0 existed or not are sufficient.

##### SimpleDirectMarkerBasedEarlyConflictDetectionStrategy

Compared with `SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy`, this strategy drops the steps of
locking including new transaction, begin transaction and end transaction. The advantages are that the checking speed is
faster, and multiple writers do not affect each other. The downside is that the conflict detection may be delayed till
pre-commit stage.

For example, when two writers detect PartitionPath1 + fileID1 at the same time, both writers pass conflict detection and
successfully create a marker, in which conflict detection is delayed. If so, the conflict can only be found in the
pre-commit conflict detection and fail writing. This leads to waste of resources, but don't compromise the correctness
of the data.

As we can see, all these direct based early conflict detection strategy need extra FS calls.

##### BloomDirectMarkerBasedEarlyConflictDetectionStrategy
Same as `SimpleDirectMarkerBasedEarlyConflictDetectionStrategy` which has no transaction. The difference is that in this `BloomDirectMarkerBasedEarlyConflictDetectionStrategy`:
1. Marker handlers write bloom files related to corresponding markers.
2. Conflict detection read these bloom files firstly to pick out potentially conflicting marker files as quick as possible.

#### TimelineServerBasedWriteMarkers related strategy

##### AsyncTimelineMarkerEarlyConflictDetectionStrategy

This design expands the create marker API on timeline server.

![](figure3.png)

As shown in Figure 3, the client side calls create marker API, requesting to create markers.

On the timeline server side, there is a MarkerCheckerRunnable thread, which checks the conflicts between the markers in
memory created by the current writer and the markers persisted to the storage created by other active writers
periodically. If any conflict is detected, this thread updates the value of hasConflict, which is used to respond to the
marker creation requests.

We did a performance test for async conflict checker using local FS and two concurrency writers based on 1500+ inflight
markers located in MARKER0~MARKER19. It takes 179ms to finish checking conflict.

Here we need to pay attention to the following details:

Firstly, There may be a concurrency issue between reading and writing the same MARKER file causing EOFException. In
order to simplify the model, we return empty result of markers when catching any exception during reading specific
MARKER and let the subsequent checker to make up for it. For example, writer1 reads writer2's MARKER0 file, but at this
moment writer2 is overwriting MARKER0 file. Finally, we get empty result after writer1 reading writer2's MARKER0. In
extreme cases, the delay of early conflict detection will also occur. Fortunately, we can adjust the frequency of the
schedule according to the actual situation.

Secondly, the marker checker runnable only looks at the instants smaller than the current writer for conflict detection.
This is to avoid two writers discovering conflicts with each other at the same time and failing together. Such conflict
detection logic based on the instant time is consistent with the existing pre-commit conflict detection.

##### BloomAsyncTimelineMarkerEarlyConflictDetectionStrategy

Same as `AsyncTimelineMarkerEarlyConflictDetectionStrategy`. The difference is that in this `BloomAsyncTimelineMarkerEarlyConflictDetectionStrategy`:
1. Marker handlers write bloom files related to corresponding markers.
2. Conflict detection read these bloom files firstly to pick out potentially conflicting marker files as quick as possible.

### [2] Check Commit Conflict: Why we still need to check commit conflict here?

As we know, when a writer completes writing and commits, the corresponding markers are deleted immediately. Other
writers miss the conflict detection if solely based on the markers.

Let's take Figure 4 as an example

![](figure4.png)

Writer1 starts writing data at time t1 and finishes at time t3. Writer2 starts writing at time t2, and
at time t4 - writer2 tries to create marker for a file fileA which already updated by writer1 to fileAa(updated).
Since all markers of writer1 have been deleted at time t4, such conflict cannot
be found in the stage of marker conflict detection until starting to commit. In order to avoid such delay of early
conflict detection, it is necessary to add the steps of checking commit conflict during the detection.

```
1. Get and cache all instants before init write handler as committed_instants_before.
2. Create and reload a new active timeline after finishing marker conflict checking as committed_instants_after.
3. Get the diff instants between committed_instants_before and committed_instants_after as committed_instants_during.
4. Read all the changed fileIds contained in committed_instants_during as fileId_set_during
5. Check if current writing fileID is overlapped with fileId_set_during
```

### [3] Try to resolve conflict

For now, the default behavior is that the write handlers with conflict throw an `HoodieEarlyConflictDetectionException`
which are running on executors. These tasks then fail and the Hudi write transaction retries.

As for HoodieDeltaStreamer, when we detect marker conflicts, corresponding writing task fails and retries. If the retry
reaches a certain number of times we set, the current stage will fail. At this time, this behavior is consistent with
the existing OCC based conflict detection.

## Configuration

This RFC adds a feature flag and three new configs to control the behavior of early conflict detection

1. `hoodie.write.lock.early.conflict.detection.enable` default false. Enable early conflict detection based on markers. It will try to detect writing conflict before create markers and fast fail which will release cluster resources as soon as possible.
2. `hoodie.write.lock.early.conflict.async.checker.batch.interval` default 30000L. Used for timeline based marker AsyncTimelineMarkerEarlyConflictDetectionStrategy. The time to delay first async marker conflict checking.
3. `hoodie.write.lock.early.conflict.async.checker.period` default 30000L. Used for timeline based marker AsyncTimelineMarkerEarlyConflictDetectionStrategy. The period between each marker conflict checking.
4. `hoodie.write.lock.early.conflict.detection.strategy` default AsyncTimelineMarkerEarlyConflictDetectionStrategy. Early conflict detection class name, this should be subclass of oorg.apache.hudi.common.model.HoodieEarlyConflictDetectionStrategy
5. `hoodie.write.lock.early.conflict.check.commit` default false. Set true if users are sensitive to conflict detection. When set ture hoodie is able to check commit conflict during early conflict detection.


## Rollout/Adoption Plan

 - There is a feature flag named `isEarlyConflictDetectionEnable`, and have no impact on existing users.

## Test Plan
Performance test and benchmark between two concurrent writers.
Case 1 enable this early conflict detection.
Case 2 disable this early conflict detection.
Compare the execution time.