w<!--
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
# Add support for cancellable clustering table service plans

## Proposers


## Approvers

## Status

JIRA: HUDI-7946


## Abstract
Clustering table service plans can delay ingestion writes from updating a dataset with recent data if potential write conflicts are detected. Furthermore, a clustering plan that isn't executed to completion for a large amount of time (due to repeated failures, application misconfiguration, or insufficient resources) will degrade the read/write performance of a dataset due to delaying clean, archival, and metadata table compaction. This is because currently HUDI clustering plans, upon being scheduled, must be executed to completion. And additonally will prevent any ingestion write targeting the same files from succeeding (due to posing as a write conflict) as well as can prevent new table service plans from targeting the same files. Enabling a user to configure a clustering table service plan as "cancellable" can prevent frequent or repeatedly failing clustering operations from delaying ingestion. Support for cancellable clustering plans will provide HUDI an avenue to fully cancel a clustering plan and allow other table service and ingestion writers to proceed.


## Background
### Execution of table services 
The table service operations compact and cluster are by default "immutable" plans, meaning that once a plan is scheduled it will stay as as a pending instant until a caller invokes the table service execute API on the table service instant and sucessfully completes it. Specifically, if an inflight execution fails after transitioning the instant to inflight, the next execution attempt will implictly create and execute a rollback plan (which will delete all new instant/data files), but will keep the table service plan. This process will repeat until the instant is completed. The below visualization captures these transitions at a high level 

![table service lifecycle (1)](https://github.com/user-attachments/assets/4a656bde-4046-4d37-9398-db96144207aa)

## Clean and rollback of failed writes
The clean table service, in addition to performing a clean action, is responsible for rolling back any failed ingestion writes (non-clustering/non-compaction inflight instants that are not being concurrently executed by a writer). This means that inflight clustering plans are not currently subject to clean's rollback of failed writes. As detailed below, this proposal for supporting cancellable clustering will benefit from enabling clean be capable of targeting clustering plans.

## Goals
### (A) An ingestion job should cancel and ignore any inflight cancellable clustering intants targeting the same data
The current requirement of HUDI needing to execute a clustering plan to completion forces ingestion writers to abort a commit if a table service plan is conflicting. Becuase an ingestion writer typically determines the exact file groups it will be updating/replacing after building a workload profile and performing record tagging, the writer may have already spent a lot of time and resources before realizing that it needs to abort. In the face of frequent table service plans or an old inflight plan, this will cause delays in adding recent upstream records to the dataset as well as unecessairly take away resources (such as Spark executors in the case of the Spark engine) from other applications in the data lake. Making the clustering plan cancellable should avoid this situation by allowing ingestion to request all conflicting cancellable clustering plans to be "cancelled" and ignore inflight plans that already have been requested for cancellation. The latter will ensure that any incomplete cancellable clustering instants that have been requested for cancellation but haven't yet been cleaned up can be ignored by ingestion writers.

### (B) A cancellable table service plan should be eligible for cancellation at any point before committing
In conjunction with (A), any caller should be able to explictly request cancellation for any inflight cancellable cluster plan, even if a concurrent worker (for example a concurrent spark job) is actively working on it. This requirement is needed due to presence of concurrent and async writers for clustering execution, as another worker should not need to wait (for the respective concurrent clustering worker to proceed with execution or fail) before confirming that its cancellation request will be honored. Note that this does not imply that the caller needs to send some signal to concurrent cancellable clustering worker to self-terminate and wait for the inflight cancellable clustering instant to be "cleaned up", but just that once the cancellable clustering plan is succesfully requested for cancellation any reader/writer can assume it will never be committed

### (C) An incomplete cancellable clustering plan should eventually have its partial writes cleaned up
Although a cancellable clustering plan being requested cancellation can ensure that the instant is never committed, there still needs to be a mechanism to have its data and instant files cleaned up permenantly. At minumum the clustering worker itself should be able to do this cleanup, but this is not sufficient as transient failures/resource allocation issues can prevent workers from re-attempting their execution of the plan. The clean table service can be used to guarantee that an incomplete cancellable plan is eventually cleaned up, since datasets that undergo clustering are anyway expected to undergo regular clean operations. Because an inflight plan remaining on the timeline can degrade performance of reads/writes (as mentioned earlier), a cancellable table service plan should be elligible to be targeted for cleanup if it has been already requested for cancellation or if HUDI clean deems that it has remained inflight for too long (or some other critera). In other words, the two main scenarios that clean will now address are

 - If a cancellable clustering plan is scheduled but is never succesfully executed (due to the corresponding worker never running or repeatdly failing) clean will requested the instant for cancellation (allowed by (B) ) and clean it up. This has a chance of prematurely cleaning up a cancellable clustering plan before it has the chance to be executed (if execution of clustering is handled async) but that can be minimized by allowing clean to only do this if instant is very old.
 - If a cancellable clustering plan was requested for cancellation but never cleaned up (again due to the corresponding worker never running or repeatdly failing) clean will take up task of cleaning up the inflight instant
   
Note that a failed cancellable clustering plan should still be able to be safely cleaned up immediately - the goal here is just to make sure an inflight plan won't stay on the timeline for an unbounded amount of time but also won't be likely to be prematurely cleaned up by clean before it has a chance to be executed.

## Design
### Enabling a clustering plan to be cancellable
To satisfy goal (A), a new config flag "cancellable" can be added to a clustering plan. A writer that intends to schedule a cancellable table service plan can enable the flag in the serialized plan metadata. Any writer executing the plan can infer that the plan is cancellable, and when trying to commit the instant should abort if it detects that is has been requested for cancellation. As a future optimization, the cancellable clustering worker can continually poll during its execution to see if it has been requested for cancellation.
On the other side in the ingestion write flow, the commit finalization logic for ingestion writers can be updated to ignore any inflight clustering plans if they are cancellable.
For the purpose of this design proposal, consider the existing ingestion write flow as having three steps:
1. Schedule itself on the timeline with a new instant time in a .requested file
2. Process/record tag incoming records, build a workload profile, and write the updating/replaced file groups to a "inflight" instant file on the timeline. Check for conflicts and abort if needed.
3. Perform write conflict checks and commit the instant on the timeline

The aforementioned changes to ingestion and clustering flow will ensure that in the event of a conflicting ingestion and cancellable table service writer, the ingestion job will take precedence (and cause the cancellable table service instant to eventually cancel) as long as a cancellable clustering plan hasn't be completed before (2). Since if the cancellable table service has already been completed before (2), the ingestion job will see that a completed instant (a cancellable table service action) conflicts with its ongoing inflight write, and therefore it would not be legal to proceed. 

### Adding a cancel action and aborted state for cancellable plans
This proposed design will also involve adding a new instant state and interal hoodie metadata directory, by making the following changes:
#### Cancel action
* Add a new .hoodie/.cancel folder, where each file corresponds to an instant time that a writer requested for cancellation. As will be detailed below, a writer can cancel an inflight plan by adding the instant to this directory and execution of table service will not allow an instant to be comitted if it appears in this /.cancel directory
The new /.cancel folder will enable goals (A) & (B) by allowing writers to permentantly prevent an ongoing cancellable table service write from comitting by requestion if for cancellation, without needing to block/wait for the table service writer. Once an instant is requested for cancellation (added to /.cancel) it cannot be "un-cancelled" - it must be eventually transitioned to aborted state, as detailed below. To implement (A), ingestion will be updated such that during write-conflict detection, it will create an entry in /.cancel for any cancellable plans with a detected write conflict and will ignore any candidate inflight plans that have an entry in /.cancel. 
#### Aborted state
* Add an ".aborted" state type for cancellable table service plan. This state is terminal and an instant can only be transitioned to .*commit or .aborted (not both)
The new ".aborted" state will allow writers to infer wether a cancelled table service plan needs to still have it's partial data writes cleaned up from the dataset, which is needed for (C). Once an instant appears in /.cancel folder, it can and must eventually be transitioned to .aborted state. This new state will ensure that cancelled instants are eventually "cleaned up" from the dataset and internal timeline metadata. The additional design change below will complete the remaining requirement for (C) of eventual cleanup.
  
### Handling cancellation of plans
An additional config "cancellation-policy" can be added to the table service plan to indicate when it is ellgible to be cancelled and aborted by clean. This policy can be a threshold of hours or instants on timeline, where if that # of hours/instants have elapsed since the plan was scheduled, any call to clean will cancel and abort the instant. This policy should be configured by the worker scheduling a cancellable table service plan, based on the amount of time they expect the plan to remain on the timeline before being picked up for execution. For example, if the plan is expected to have its execution deferred to a few hours later, then the cancellation-policy should be lenient in allowing the plan to remain many hours on the timeline before being clean is allowed to cancel and abort instant. Note that this cancellation policy is not used to infer wether a clustering plan is currently being executed - similar to how concurrent ingestion writes are rolled back by lazy clean policy, A cancellable clustering plan can only be aborted once it is confirmed that an ongoing writer is no longer progressing it. 

In order to ensure that other writers can indeed permenantely cancel a cancellable table service plan (such that it can no longer be executed), additional changes to clean and table service flow will be need to be added as well, as will be proposed below. Also, note that the cancellation-policy is only required to be honored by clean: a user can choose setup an application to aggresively cancel a failed cancellable table service plan even if it has not meet the critera for its cancellation-policy yet. This can be useful if a user wants a utility to manually ensure that clean/archival for a dataset progresses immdeitately or knows that a cancellable table service plan will not be attempted again or cancelled & aborted by a table service worker in the future. The two new cancel APIs in the below propsoal provide a method to achieve this.
  

#### Enabling clustering execution and clean table service to support cancellation and automatic cleanup

The clustering execution flow will be updated to check the /.cancel folder during a pre-commit check before completing the instant. If the instant is a target of /.cancel, then all its ata files will be deleted and the instant will be transitioned to .aborted. These checks will be performed within a transaction, to guard against callers cancelling an already-committed instant. In addition, in order to avoid the scenario of writer executing an instant but having its data files being deleted by a concurrent caller cancelling & aborting the instant, the clustering execution flow will perform heartbeating. If an instant has an active heartbeat it can be requested for cancellation (by adding an instant in /.cancel) but it cannot yet be cleaned up and transitioned to .aborted state - this is sufficient to safely implement goal (B) with respect to concurrent workers. 

The below visualization shows the flow for cancellable table service plans (steps that are already in existing table service flow are grey-ed out) 

![cancel table service lifecycle with lock (5)](https://github.com/user-attachments/assets/c05dae68-4330-4d85-b29e-e3e47754509e)



Having this new .hoodie/.cancel folder (in addition to only having the .aborted state) is needed not only to allow any caller to forcibily block an instant from being comitted, but also to prevent the need for table service workers to also perform write conflict detection (that ingestion already will perform) or unecessairly re-attempt execution of the instant if it has been already been requested for cancellation but not succefully aborted yet. The below visualized scenario shows how this clustering attempt will "short circuit" in this manner by checking /.cancel to see if clustering execution should even proceed. This scenario also includes an example of concurrent writers to show how transaction and heartbeating in the above proposed flow will allow correct behavior even in the face of concurrent writers attempting to execute and/or cancel the instant.

![cancel flow table serivce (1)](https://github.com/user-attachments/assets/f130f326-952f-49eb-bdbb-b0b34206f677)

Aside from modifications to the clustering execution flow, a new pair of cancel APIs request_cancel and execute_cancel will be added for all writers to use. They will allow a writer to request an instant to be cancelled (by adding it to /.cancel) and transition an instant to the terminal .aborted state, respectively.

The new cancel API request_cancel will perform the following steps

1. Start a transaction
2. Reload active timeline
3. If instant is already comitted, throw an exception
4. If instant is already aborted, exit without throwing exception
5. create a file under /.cancel with the target instant name
6. End the transaction
   
If this call succeeds, then the caller can assume that the target instant will not be commited and can only transitioned to .aborted state from that point onwards.

The other API execute_cancel will be added which allows a writer to explictly abort the target instant that has already had its cancellation requested (by adding the instant under /.cancel). Note that this API will also do cleaning up all leftover files in the process. Unlike request_cancel though, it cannot be completed if another writer is still executing the instant. In order to enforce this, heartbeating will be used to allow writers to infer whether another instant is being executed by a table service writer. The execute_cancel API will perform the following steps

1. Start a transaction
2. Reload active timeline
3. If instant has been already aborted, exit without throwing exception. Cleanup file from /.cancel if still present
4. If instant has an active heartbeat, abort
5. End transaction
6. Rollback target instant without deleting plan
7. Start a transaction
8. Transition instant to .aborted
9. Delete the file for instant under /.cancel
10. End the transaction

Note that similar to the proposed clustering flow, this API will check for heartbeats within a transaction (to guarantee there are no writers still working on this instant) and will transition the state to .aborted within a transaction. The latter is done within a transaction (in both the clustering flow and this API) to prevent a concurrent request_cancel call from creating a file under a /.cancel of an instant already transtioned to .aborted. Although in this scenario calling execute_cancel or cancellable table service API again after this point would be safe, this guard should prevent files in /.cancel for already-aborted instants from accumulating.

Although any user can invoke request_cancel and execute_cancel, the ingestion flow is expected to use request_cancel against any conflicting cancellable inflight clustering instants. 
In addition, the clean operation will be required to use both of these APIs in order to cancel any incomplete plans that have satisfied their cancellation policy or already been requested for cnaclleation. This is analagous to how clean schedules and executes the rollback of any failed ingestion writes. Specifically, clean will now perform the following steps
1. Iterate through each inflight cancellable instant, for each instant that has met their cancellation policy, call request_cancel
2. Iterate through each instant in /.cancel and call execute_cancel.
Note that the request_cancel and execute_cancel APIs are responsible for ensuring that already-committed instants aren't cancelled and that ongoing writers aren't concurrently having their files cleaned up by a writer doing cancellation.

Archival will also be updated to delete instants transitioned to .aborted state, since .aborted is a terminal state.

### Summarizing proposed internal timeline/instant format changes
Taking together all changes to .hoodie & instant files structure discussed above, this RFC will apply the following changes

- The `cancellable` boolean flag and `cancellation-policy` predicates will be added as optional serialized values to the table service metadata schema (tenatively just for clustering plans)
- The list of possible instant transitions for table services will now include `.aborted`
- The /.cancel folder will be added to .hoodie directory


## Rollout
This design proposal has one critical drawback:
* A new state and action type needs to be added to HUDI, requiring updates to all reader/writer logic of the HUDI filesystem view. Readers need to account for skipping aborted plans, and archival should clean up aborted plans and cancel actions accordingly. In addition, it is now possible for an instant to be transitioned to a terminal state (.aborted) without being completed. This can complicate tooling that needs to infer wether an instant is committed.

Before users can be allowed to set cancellation policy/flag, the below must all be implemented in order
1. All File system /timeline related logic should be able to process .aborted state and consider it as a terminal state
2. Archival should handle deleting .aborted instants
3. The /.hoodie/.cancel metadata should be added & supported
4. Clean should be updated as per the design proposal
5. The write conflict detection check of ingestion should be updated to request cancellation for any table service plans with cancellation flag set, as per the design proposal
6. Table service execution flow should be updated as per the design proposal, just for clustering table service

From here, a user can update the applications they use for scheduling clustering plans to have cancellation flag/policy enabled, whenever they want their plan to be cancellable (and henceforth be implictly be treated as lower priority with respect to ingestion). Note that it is legal for a dataset to have both cancellable and non-cancellable table service plans.
Initially the rollout/implementaiton of cancellable plans should be limited to cluster table service, as clean & compaction are designed to operate on older file versions of file slices, so there may not be a strong justification for allowing those to be cancellable. If any new table service types are added they can be considered for supporting cancellation functionality.



