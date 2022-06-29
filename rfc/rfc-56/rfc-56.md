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

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-1575


## Abstract

At present, Hudi implements an optimized occ mechanism based on timeline to ensure data consistency, integrity and 
correctness between multi-writers. However, the related conflict detection is performed before commit metadata and 
after the data writing is completed. If this detection was failed, it would lead to a waste of cluster resources 
because computing and writing were finished already. To solve this problem, this RFC design an early conflict detection 
mechanism based on the existing Hudi marker mechanism. This new mechanism will do conflict checking before the writers 
creating markers and before starting to write corresponding data file. So that the writing conflicts can be detected as 
early as possible. What's more? We can stop writing earlier because of early conflict detection and release the 
resources to cluster, improving resource utilization.

## Background
As we know, Transactions and multi-writers of data lakes are becoming the key characteristics of building LakeHouse 
these days. Quoting this inspiring blog <strong>Lakehouse Concurrency Control: Are we too optimistic?</strong> directly: 
https://hudi.apache.org/blog/2021/12/16/lakehouse-concurrency-control-are-we-too-optimistic/

>"Hudi implements a file level, log based concurrency control protocol on the Hudi timeline, which in-turn relies 
> on bare minimum atomic puts to cloud storage. By building on an event log as the central piece for inter process 
> coordination, Hudi is able to offer a few flexible deployment models that offer greater concurrency over pure OCC 
> approaches that just track table snapshots."
  
In the multi-writer scenario, Hudi's existing conflict detection occurs after the writer finished writing the data 
and before committing the metadata. In other words, the writer will only detect the occurrence of the conflict
when it starts to commit, although all calculations and data writing have been completed, which will cause a waste 
of resources.

For example:

Now there are two writing jobs: job1 will write 10M data to the Hudi table, including update file group 1. 
Another job2 will write 100G to the Hudi table, and will also update the same file group 1. 

Job1 finished and committed to Hudi successfully. After a few hours, job2 finished writing data files(100G) and start 
to commit metadata. At this time, a conflict compared with job1 was found, and the job2 had to be aborted and re-run 
after failure. Obviously, a lot of computing resources and time are wasted for job2.


Hudi currently has two important mechanisms, marker mechanism and heartbeat mechanism:
1. Marker mechanism can track all the files that are part of an active write.
2. Heartbeat mechanism that can track all active writers to a Hudi table.


Based on marker and heartbeat, this RFC design a new conflict detection: Early Conflict Detection. 
Before the writer creates the marker and before it starts to write the file, Hudi will perform this new conflict 
detection, trying to detect the writing conflict as early as possible and abort the writer when the conflict occurs, 
so that we can release resources as soon as possible and improve resource utilization.


## Implementation
Here is the high level workflow of early conflict detection as figure1 showed. 
As we can see, only both `supportsOptimisticConcurrencyControl` and `isEarlyConflictDetectionEnable` are true, we 
could use this early conflict detection feature. Else, we will skip this check and create marker directly.

![](figure1.png)

The three important steps marked in red in Figure 1 are introduced one by one as follows:

### [1] Check Marker Conflict

As we know, we have two ways to create and maintain markers:
1. DirectWriteMarkers: individual marker file corresponding to each data file is directly created by the writer.
2. TimelineServerBasedWriteMarkers: marker operations are all handled at the timeline service which serves as a proxy

Therefore, for different types of Marker we have, we must implement the corresponding check marker conflict logic. 
Here we expand the existing `ConflictResolutionStrategy` interface to ensure the scalability of checking marker conflict.

![](flow1.png)

In this design, we provide `DirectMarkerWithTransactionConflictResolutionStrategy` and 
`SimpleDirectMarkerConflictResolutionStrategy` for DirectWriteMarkers to perform corresponding conflict detection and 
conflict resolution. And we provide `AsyncTimelineMarkerConflictResolutionStrategy` for TimelineServerBasedWriteMarkers 
to perform corresponding conflict detection and conflict resolution

#### DirectWriteMarkers related strategy

##### DirectMarkerWithTransactionConflictResolutionStrategy

![](figure2.png)

As for current strategy showed by figure 2, we need to pay attention to the following two details:


Firstly, we will use the existing transaction mechanism to lock at the <strong>PartitionPath + "/" + fileID</strong>
level before checking conflicts and before creating marker files. 
Take ZK locker as an example => `LockConfiguration.ZK_LOCK_KEY_PROP_KEY, partitionPath + "/" + fileId`. 
That is to say, for the same partitionPath/fileId, only one writer can check and create the corresponding marker file 
at the same time.


In addition, during checking conflicts, we do not need to list all marker files in all directories, we can prune based 
on the current partition Path and fileID to avoid unnecessary list operations.

##### SimpleDirectMarkerConflictResolutionStrategy

Compared with `DirectMarkerWithTransactionConflictResolutionStrategy`, the current strategy drops the steps of 
Transaction including new Transaction, begin transaction and end transaction. The advantages are that the checking 
speed is faster, and multiple writers will not affect each other. But the downside is that the conflict may be 
underreported. 


For example, when two writers detect PartitionPath1 + fileID1 at the same time, 
both writers will pass conflict checking and successfully create a marker, which is an underreported. 
If there is an underreported, the conflict can only be found in the pre-commit conflict checking and fail writing. 
This will lead to waste of resources, but will not compromise the correctness of the data.

#### TimelineServerBasedWriteMarkers related strategy

##### AsyncTimelineMarkerConflictResolutionStrategy

This design will register a new Marker API called `/v1/hoodie/marker/check-marker-conflict` on timeline server.

![](figure3.png)

As shown on figure3. For the client side, it will call this check-marker-conflict api to get the latest value 
of `hasConflict`. 

For the timeline server side, there is a MarkerCheckerRunnable thread, which will check the conflicts between current 
writer and other active writers at fixed rate. If any conflict was detected, this thread will async update the value 
of hasConflict.

We did a performance test for async conflict checker based on 1500+ inflight markers located in MARKER0~MARKER19 
and it will take 179ms to finish checking conflict.

Here we need to pay attention to two details:

Firstly, There may be a concurrency issue between reading and writing the same MARKER file causing EOFException. In 
order to simplify the model, we think that the result is empty when catch any exception during reading specific MARKER 
and let the subsequent checker to make up for it. For example, Writer1 will read writer2's MARKER0 file, but at this 
moment writer2 is overwriting his MARKER0 file. Finally we will get empty set after writer1 reading writer2's MARKER0. 
In extreme cases, underreported will also occur and early conflict detection will not work. 
Fortunately, we can adjust the frequency of the schedule according to the actual situation to reduce the occurrence of 
underreported.

Secondly, the marker checker runnable scheduled and selecting the candidate active instants, only the instant smaller than 
the current writer is selected for conflict detection. This is to avoid two writers discovering conflicts with each 
other at the same time and failing together.

### [2] Check Commit Conflict: Why we still need to check commit conflict here?

As we know, when a writer completes writing and commits, its corresponding marker information will be deleted 
immediately. So that, other writers will have underreported in the detection of markers. 

Let's take Figure 4 as an example

![](figure4.png)

Writer1 starts writing data at time t1 and finishes at time t3. Writer2 starts writing at time t2, and will update a 
file already updated by writer1 at time t4. Since all marker information of writer1 has been deleted at time t4, 
such conflicts cannot be found in the stage of marker conflict detection until starting to commit. 
In order to avoid such underreported, it is necessary to add the step of check commit conflict during early conflict 
detection.

For HoodieMergeHandler we take care about `Latest Base File To Merge` so that we need to make sure this 
`Latest Base File To Merge` is the same after passed marker conflict checker.

```java
(table) -> {
  table.getMetaClient().reloadActiveTimeline();
  table.getHoodieView().sync();
  HoodieBaseFile currentBaseFileToMerge = table.getBaseFileOnlyView().getLatestBaseFile(partitionPath, fileId).get();
  
  ==> compre currentBaseFileToMerge and oriBaseFileToMerge
}
```

For HoodieAppendHandle we take care about `Latest fileSlice` so that we need to make sure this `Latest fileSlice` is 
the same after passed marker conflict checker.

```java
(table) -> {
  table.getMetaClient().reloadActiveTimeline();
  table.getHoodieView().sync();
  SliceView currentSliceView = table.getSliceView();
  Option<FileSlice> currentFileSlice = currentSliceView.getLatestFileSlice(partitionPath, fileId);
  
  ==> compre currentFileSlice and oriFileSlice
}
```

### [3] Try to resolve conflict

For now, the default behavior is throwing an `HoodieWriteConflictException`

## Configuration

This RFC adds three new configs to control the behavior of early conflict detection

1. `hoodie.write.lock.early.conflict.detection.enable` default false. Enable early conflict detection based on markers. It will try to detect writing conflict before create markers and fast fail which will release cluster resources as soon as possible.
2. `hoodie.write.lock.early.conflict.async.checker.batch.interval` default 30000L. Used for timeline based marker AsyncTimelineMarkerConflictResolutionStrategy. The time to delay first async marker conflict checking.
3. `hoodie.write.lock.early.conflict.async.checker.period` default 30000L. Used for timeline based marker AsyncTimelineMarkerConflictResolutionStrategy. The period between each marker conflict checking.



## Rollout/Adoption Plan

 - No impact will there be on existing users

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.