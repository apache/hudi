w<!-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to
You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License
at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "
AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License. -->

# Add support for cancellable clustering table service plans

## Proposers

Krishen Bhan (kbuci)

## Approvers

Sivabalan Narayanan (nsivabalan)

## Status

In Progress

JIRA: HUDI-7946

## Abstract

Clustering is a table service useed to optimize table/files layout in HUDI in order to speed up read queries. Currently
ingestion writers will abort if they attempt to write to the same data targetted by a pending clustering write.
As a result, clustering table service plans can indirectly delay ingestion writes from updating a dataset with recent data. 
Furthermore, a clustering plan that isn't executed to completion for a large amount of time (due to repeated failures, application
misconfiguration, or insufficient resources) will degrade the read/write performance of a dataset due to delaying clean,
archival, and metadata table compaction. This is because currently HUDI clustering plans, upon being scheduled, must be
executed to completion. This RFC proposes to support "Cancellable" Clustering plans. Support for such cancellable clustering plans 
will provide HUDI an avenue to fully cancel a clustering plan and allow other table service and ingestion writers to proceed and avoiding
starvation based on user needs.

## Background

### Current state of Execution of table service operations in Hudi

As of now, the table service operations `COMPACT` and `CLUSTER` are implicitly "immutable" plans by default, meaning
that once a plan is scheduled, it will stay as a pending instant until a caller invokes the table service execute API on
the table service instant and successfully completes it (referred to as "executing" a table service). Specifically, if an inflight
execution fails after transitioning the instant to inflight, the next execution attempt will implictly create and execute a rollback
plan (which will delete all new instant/data files), but will keep the table service plan. And then the table service will be
re-attempted. This process will repeat until the instant is completed. The below visualization captures these transitions at a high level

![table service lifecycle (1)](https://github.com/user-attachments/assets/4a656bde-4046-4d37-9398-db96144207aa)

## Goals

### (A) An ingestion job should be able to cancel and ignore any inflight cancellable clustering instants targeting the same data as the ingestion writer.

The current requirement of HUDI needing to execute a clustering plan to completion forces ingestion writers to abort a
commit if a conflicting table service plan is present. Becuase an ingestion writer typically determines the exact file groups it
will be updating/replacing after building a workload profile and performing record tagging, the writer may have already
spent a lot of time and resources before realizing that it needs to abort. In the face of frequent table service plans
or an old inflight plan, this will cause delays in adding recent upstream records to the dataset as well as
unnecessairly take away resources from other applications in the data lake (such as Spark executors in the case of the Spark engine).
Making the clustering plan cancellable should avoid this situation by permitting an ingestion writer to request all
conflicting cancellable clustering plans to be "cancelled" and ignore inflight plans that already have been requested
for cancellation. The latter will ensure that ingestion writers can ignore any incomplete cancellable clustering instants that have been requested
for cancellation but have not yet been aborted.

### (B) A cancellable table service plan should be eligible for cancellation at any point before committing

In conjunction with (A), any caller (ingestion writer and potentially other users) should be able to request cancellation for an inflight
cancellable clustering plan. We should not need any synchronous mechanism where in the clustering plan of interest
should be aborted and cleaned up completely before which the ingestion writer can proceeed. We should have a light weight mechanism with
which the ingestion writer make a cancellation request and moves on to carry out its operation with the assumption that
the respective clustering plan will be aborted. This requirement is needed due to presence of concurrent and async
writers for clustering execution, as another worker should not need to wait (for the respective concurrent clustering
worker to proceed with execution or fail) before confirming that its cancellation request will be honored. Once the
request for cancellation succeeds, all interested entities like the ingestion writer, reader, asynchronous clustering
execution job should assume the clustering plan is cancelled.

## Design

### Enabling a clustering plan to be cancellable

To satisfy goal (A), a new config flag named "cancellable" can be added to a clustering plan. A writer that intends to
schedule a cancellable table service plan, can enable the flag in the serialized plan metadata. Any writer executing the
plan can infer that the plan is cancellable, and when trying to commit the instant should abort, if it detects that is
has been requested for cancellation. As a future optimization, the cancellable clustering worker can continually poll
during its execution to see if it has been requested for cancellation. On the other side, with the ingestion writer
flow, the commit finalization logic for ingestion writers can be updated to ignore any inflight clustering plans if they
are cancellable. For the purpose of this design proposal, consider the pre-existing ingestion write flow as having three
steps:

1. Schedule itself on the timeline with a new instant time in a .requested file
2. Process/record tag incoming records, build a workload profile, and write the updating/replaced file groups to a "inflight"
   instant file on the timeline. Check for conflicts and abort if needed.
3. Start a transaction, and perform write conflict checks and commit the instant on the timeline

The aforementioned changes to ingestion and clustering flow will ensure that in the event of a conflicting ingestion and
cancellable table service writer, the ingestion job will take precedence (and cause the cancellable table service
instant to eventually cancel) as long as a cancellable clustering plan hasn't be completed before (2). Since if the
cancellable table service has already been completed before (2), the ingestion job will see that a completed instant (a
cancellable table service action) conflicts with its ongoing inflight write, and therefore it would not be legal to
proceed. This should be the only scenario where the ingestion writer terminates itself in the presence of a cancellable clustering plan
instead of proceeding to completion.

Note that with cancellable clustering plans we are opting for a concurrency model where ingestion writers are given precdence. The tradeoff here
is that when a clustering plan is cancellable we aren't allowing it to optionally force proceed or force ingestion to wait, but instead minimize 
the chance of having it block ingestion. Because of this users should ensure that they don't enable the `cancellable` flag if they want to guarantee
that a clustering plan will be committed within a given time window, since frequent ingestion jobs targeting the same file groups will "starve" the
cancellable clustering write from committing (which is intended and by design as mentioned above, but something the user should be aware of)

### Adding a cancel action and aborted state for cancellable plans

This proposed design will also involve adding a new instant state and internal hoodie metadata directory, by making the
following changes:

#### Cancel action

* We are proposing to add a new .hoodie/.cancel folder, where each file corresponds to an instant time that a writer
  requested for cancellation. As will be detailed below, a writer can cancel an inflight plan by adding the instant to
  this directory and execution of table service will not allow an instant to be committed, if it appears in this
  /.cancel directory. The new /.cancel folder will enable goals (A) & (B) by allowing writers to permentantly prevent an
  ongoing cancellable table service write from committing by requesting for cancellation, without needing to block/wait
  for the table service writer. Once an instant is requested for cancellation (added to /.cancel) it cannot be revoked (
  or "un-cancelled") - it must be eventually transitioned to aborted state, as detailed below. To implement (A), ingestion
  will be updated such that during write-conflict detection, it will create an entry in /.cancel for any cancellable
  plans with a detected write conflict and will ignore any candidate inflight plans that have an entry in /.cancel. Note that,
  since write conflict resoltution already happens during a transaction, this multi-step process of checking for any conflicting
  cancellable plans and adding entries for them to the /.cancel directory will be within the same transaction.

#### Aborted state

* We are proposing to add an ".aborted" state type for cancellable table service plan. This state is terminal and with
  this new addition, an instant can only be transitioned to .*commit or .aborted (not both) or could be rolledback (
  ingestion writes). The new ".aborted" state will allow writers to infer whether a cancelled table service plan still needs to
  have its partial data writes cleaned up from the dataset or can be deleted from the active timeline during archival
  (as the active timeline should not grow unbounded). Once an instant appears in /.cancel folder, it can and must eventually be
  transitioned to .aborted state. To summarize, this new state will ensure that cancelled instants are eventually "cleaned up" from the dataset
  and internal timeline metadata.

### Handling cancellation of plans

In order to ensure that other writers can indeed permanantely cancel a cancellable clustering plan (such that it can
no longer be executed), additional changes to cluster table service flow will be need to be added as well, as will be
proposed below. In addition to clustering being able to cleanup/abort an instant, a user may want to setup
a seperate utility application to directly cancel/abort cancellable cluster instants, in order to manually ensure that clean/archival
for a dataset progresses immediately or confirm that a cancellable table service plan will not be completed or attempted again.
The two new cancel APIs in the below proposal provide a method to achieve this.

#### Enabling clustering execution cancellation and automatic cleanup

The clustering execution flow will be updated to check the /.cancel folder during a pre-commit check before completing
the instant. If the instant is a target of /.cancel, then all of its data files will be deleted and the instant will be
transitioned to .aborted. These checks will be performed within a transaction, to guard against callers cancelling an
already-committed instant. In addition, in order to avoid the scenario of writer executing an instant but having its
data files being deleted by a concurrent caller cancelling & aborting the instant, the clustering execution flow will
perform heartbeats. If an instant has an active heartbeat it can be requested for cancellation (by adding an instant in
/.cancel) but it cannot yet be cleaned up and transitioned to .aborted state - this is sufficient to safely implement
goal (B) with respect to concurrent workers.

The below visualization shows the flow for cancellable table service plans (steps that are already in existing table
service flow are grey-ed out)

![cancel table service lifecycle with lock (6)](https://github.com/user-attachments/assets/087aa35e-eb87-477d-88f9-9d0ab6649b04)


Having this new .hoodie/.cancel folder (in addition to only having the .aborted state) is needed not only to allow any
caller to forcebily block an instant from being committed, but also to prevent the need for table service workers to
also perform write conflict detection (that ingestion already will perform) or unnecessarily re-attempt execution of the
instant if it has been already been requested for cancellation (by an ingestion job targeting same file groups) but not succefully aborted yet. 
The below visualized scenario shows how this clustering attempt will "short circuit" in this manner by checking /.cancel to see if clustering
execution should even proceed. This scenario also includes an example of concurrent writers to show how transaction and
heartbeating in the above proposed flow will allow correct behavior even in the face of concurrent writers attempting to
execute and/or cancel the instant.

![cancel flow table serivce (1)](https://github.com/user-attachments/assets/f130f326-952f-49eb-bdbb-b0b34206f677)

Aside from modifications to the clustering execution flow, a new pair of cancel APIs request_cancel and execute_abort
will be added for all writers to use. They will allow a writer to request an instant to be cancelled (by adding it to
/.cancel) and transition an instant to the terminal .aborted state, respectively.

The new cancel API request_cancel will perform the following steps

1. Start a transaction
2. Reload active timeline
3. If instant is already comitted, throw an exception
4. If instant is already aborted, exit without throwing exception
5. create a file under /.cancel with the target instant name (if it doesn't already exist)
6. End the transaction

If this call succeeds, then the caller can assume that the target instant will not be commited and can only transitioned
to .aborted state from that point onwards. Taking a transaction will ensure that a concurrent writer that is executing the instant
either completes/commits the instant before it is requested for cancellation or self-aborts, since both those phases of table
service execution are now done during a transaction. If there are multiple concurrent calls of request_cancel to the same instant,
then the file under /.cancel will remain as expexted.

The other API execute_abort will be added which allows a writer to explictly abort the target instant that has already
had its cancellation requested (by adding the instant under /.cancel). Note that this API will also do cleaning up all
leftover files in the process. Unlike request_cancel though, it cannot be completed if another writer is still executing
the instant. In order to enforce this, heartbeating will be used to allow writers to infer whether another instant is
being executed by a table service writer. The execute_abort API will perform the following steps

1. Start a transaction
2. Reload active timeline
3. If instant has been already aborted, exit without throwing exception. Cleanup file from /.cancel if still present
4. If instant has an active heartbeat, bail out.
5. End transaction
6. If there is no active heartbeat (4), proceed further
7. Rollback target instant without deleting plan
8. Start a transaction
9. Transition instant to .aborted
10. Delete the file for instant under /.cancel
11. End the transaction

Note that similar to the proposed clustering flow, this API will check for heartbeats within a transaction (to guarantee
there are no writers still working on this instant) and will transition the state to .aborted within a transaction. The
latter is done within a transaction (in both the clustering flow and this API) to prevent a concurrent request_cancel
call from creating a file under a /.cancel of an instant already transtioned to .aborted. Although in this scenario
calling execute_abort or cancellable table service API again after this point would be safe, this guard should prevent
files in /.cancel for already-aborted instants from accumulating.

Any user can invoke request_cancel and execute_abort. The ingestion flow is expected to use request_cancel
against any conflicting cancellable inflight clustering instants, but will not attempt calling execute_abort (to minimize overhead for ingestion).
Rather, the final "cleanup" and aborting of cancellable instants will be performed either when a writer attempts to execute the plan again (as 
detailed in the table service execution flow earlier) or when CLEAN calls execute_abort, as proposed further below.

Note that archival will also be updated to delete instants transitioned to .aborted state, since .aborted is a terminal state.

### Optional feature: Ensure incomplete cancellable clustering plan eventually have their partial writes cleaned up by CLEAN

The clean table service, in addition to performing a clean action, is responsible for rolling back any failed ingestion
writes (non-clustering/non-compaction inflight instants that are not being concurrently executed by a writer). Currently, inflight
clustering plans are not currently subject to clean's rollback of failed writes. As an added feature, clean
can also be made capable of cancelling/aborting cancellable clustering plans.

With this additional feature, the new cancellation logic can be updated to have CLEAN "cleanup" pending cancellable clustering
plans that have not been executed for a while. The clustering worker itself can/will already perform all necessary cleanup of a cancelled instant.
But relying on the clustering executution for cleanup may not be sufficient/reliable as transient failures/resource allocation issues
can prevent workers from re-attempting their execution of the plan. In such cases, the `CLEAN` table service can be used
to guarantee, that an incomplete cancellable plan which has a request for cancellation, is eventually cleaned up(rolled
back and aborted), since datasets that undergo clustering are anyway expected to undergo regular clean operations.
Because an inflight plan remaining on the timeline can degrade performance of reads/writes (as mentioned earlier), a
cancellable clustering plan should be elligible to be targeted for cleanup if it has been already requested for
cancellation or if HUDI clean deems that it has remained inflight for too long (based on some user-configured critera discussed later below). In other words,
the two main scenarios that `CLEAN` will now address are

- If a cancellable clustering plan is scheduled but is never successfully executed (due to the corresponding worker
  never running or repeatedly failing), `CLEAN` will requested the instant for cancellation (allowed by (B) ) and clean
  it up. This has a chance of prematurely cleaning up a cancellable clustering plan before it has the chance to be
  executed (if execution of clustering is handled async) but that can be minimized by allowing clean to only do this if
  instant is very old.
- If a cancellable clustering plan was requested for cancellation but never fully aborted (again due to the corresponding
  worker never running or repeatedly failing), `CLEAN` will take up task of cleaning up and aborting the inflight instant.

Note that a cancellable clustering plan that has been requestd for cancellation can still be safely cleaned up immediately by execute_abort API - the goal here
is just to make sure, an inflight plan won't stay on the timeline for an unbounded amount of time but also won't be
likely to be prematurely cleaned up by clean before it has a chance to be executed.

An additional config "cancellation-policy" will be added to the table service plan to indicate when it is eligible to be
cancelled and aborted by `CLEAN` operation. This policy can be a threshold of hours or instants on timeline, where if
that # of hours/instants have elapsed since the plan was scheduled(w/o being executed), any call to `CLEAN` operation
will also cancel and abort the instant. This policy should be configured by the worker scheduling a cancellable table
service plan, based on the amount of time they expect the plan to remain on the timeline before being picked up for
execution. For example, if the plan is expected to have its execution deferred to a few hours later, then the
cancellation-policy should be lenient in allowing the plan to remain many hours on the timeline before it could be
deemed eligible for abortion. Note that this cancellation policy is not used to infer whether a clustering plan is
currently being executed - similar to how concurrent ingestion writes are safely rolled back by lazy clean policy, A
cancellable clustering plan can only be aborted once it is confirmed that an ongoing writer is no longer progressing it
(which the aforementioned execute_abort API already enforces). Note that the cancellation-policy is only required to be honored by `CLEAN`

To implement this optional feature, the clean operation can use both of these APIs in order to cancel any incomplete plans that have
satisfied their cancellation policy or already been requested for cancellation. This is analagous to how clean already schedules and
executes the rollback of any failed ingestion writes. Specifically, clean will now perform the following steps

1. Iterate through each inflight cancellable instant, for each instant that has met their cancellation policy, call
   request_cancel if not already has an request for cancellation.
2. Iterate through each instant in /.cancel , and call execute_abort. The execute_abort call will implictly check that the target instant doesn't have an active heartbeat.
   This will prevent CLEAN from executing cancellation of an instant that is being worked on by an active writer

### Summarizing performance overheads

Since this proposal will add more cases where transactions are taken and internal HUDI metadata files are read, it is important to assess the potential impact to latency for
ingestion/clustering writes:

- For ingestion, currently ingestion (during write-conflict detection) already iterates through all pending/potentially conflicting clustering instants and deserializing them while
  holding the table lock. If this proposal is implemented, then for all of these plans that are inflight and cancellable, ingestion will spend a little bit more time creating the
  empty file for each instant in /.cancel folder. This should only increase the time window that the lock (and overall ingestion runtime) in taken by 2-3 DFS calls per pending cancellable
  instant (checking that the instant isn't already cancellable/aborted and adding the corresponding empty file to /.cancel folder)

- For clustering, this proposal will now update clustering execution to take an additional lock 1-2 times at the start of execution (when checking for heartbeat to see if
  ongoing writer exists/instant has been cancelled and again transitioning the instant to .aborted if instant has been cancelled). The most expensive operation done
  while the table lock is held will most likely be the scheduling of the rollback plan (if instant is cancelled) since the runtime/spark resources will be proportional to
  the number of file groups targeting by the clustering plan. This should only have a notable impact in the worst case where the clustering write was previously about to finish
  its commit before it was cancelled. 
  

### Summarizing proposed internal timeline/instant format changes

Taking together all changes to .hoodie & instant files structure discussed above, this RFC will apply the following
changes

- The `cancellable` boolean flag and `cancellation-policy` predicates will be added as optional serialized values to the
  table service metadata schema (tentatively just for clustering plans)
- The list of possible instant transitions for table services will now include `.aborted`
- The /.cancel folder will be added to .hoodie directory

## Rollout

This design proposal requires a general change to HDUI:

* A new state and action type needs to be added to HUDI, requiring updates to all reader/writer logic of the HUDI
  filesystem view. Readers need to account for skipping aborted plans, and archival should clean up aborted instants or
  block on pending instants(which is requested for cancellation, but not yet been aborted). In addition, it is now
  possible for an instant to be transitioned to a terminal state (.aborted) without being completed. This can complicate
  tooling that needs to infer whether an instant is committed or not.

Before users can be allowed to set cancellation policy/flag, the below must all be implemented in order

1. All File system /timeline related logic should be able to process .aborted state and consider it as a terminal state
2. Archival should handle deleting .aborted instants
3. The /.hoodie/.cancel metadata should be added & supported
4. (Optional) Clean should be updated as per the design proposal
5. The write conflict detection check for ingestion should be updated to request cancellation for any table service plans
   with cancellation flag set, as per the design proposal.
6. Table service execution flow should be updated as per the design proposal, just for clustering table service

From here, a user can update the applications they use for scheduling clustering plans to have cancellation flag/policy
enabled, whenever they want their plan to be cancellable (and henceforth be implictly be treated as lower priority with
respect to ingestion). Note that it is legal for a dataset to have both cancellable and non-cancellable table service
plans. Initially the rollout/implementaiton of cancellable plans should be limited to cluster table service, as clean &
compaction are designed to operate on older file versions of file slices, so there may not be a strong justification for
allowing those to be cancellable. If any new table service types are added they can be considered for supporting
cancellation functionality.



