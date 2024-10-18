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
# Add support for cancellable table service plans

## Proposers


## Approvers

## Status

JIRA: HUDI-7946


## Abstract
Table service plans can delay ingestion writes from updating a dataset with recent data if potential write conflicts are detected. Furthermore, a table service plan that isn't executed to completion for a large amount of time (due to repeated failures, application misconfiguration, or insufficient resources) will degrade the read/write performance of a dataset due to delaying clean, archival, and metadata table compaction. This is because currently HUDI table service plans, upon being scheduled, must be executed to completion. And additonally will prevent any ingestion write targeting the same files from succeeding (due to posing as a write conflict) as well as can prevent new table service plans from targeting the same files. Enabling a user to configure a table service plan as "cancellable" can prevent frequent or repeatedly failing table service plans from delaying ingestion. Support for cancellable plans will provide HUDI an avenue to fully cancel a table service plan and allow other table service and ingestion writers to proceed.


## Background
### Execution of table services 
The table service operations compact and cluster are by default "immutable" plans, meaning that once a plan is scheduled it will stay as as a pending instant until a caller invokes the table service execute API on the table service instant and sucessfully completes it. Specifically, if an inflight execution fails after transitioning the instant to inflight, the next execution attempt will implictly create and execute a rollback plan (which will delete all new instant/data files), but will keep the table service plan. This process will repeat until the instant is completed. The below visualization captures these transitions at a high level 

![table service lifecycle (1)](https://github.com/user-attachments/assets/4a656bde-4046-4d37-9398-db96144207aa)

## Clean and rollback of failed writes
The clean table service, in addition to performing a clean action, is responsible for rolling back any failed ingestion writes (non-clustering/non-compaction inflight instants that are not being concurrently executed by a writer). This means that table services plans are not currently subject to clean's rollback of failed writes. As detailed below, this proposal for supporting cancellable table service will require enabling clean be capable of targeting table service plans.

## Goals
### (A) A cancellable table service plan should be capable of preventing itself from committing upon presence of write conflict
The current requirement of HUDI needing to execute a table service plan to completion forces ingestion writers to abort a commit if a table service plan is conflicting. Becuase an ingestion writer typically determines the exact file groups it will be updating/replacing after building a workload profile and performing record tagging, the writer may have already spent a lot of time and resources before realizing that it needs to abort. In the face of frequent table service plans or an old inflight plan, this will cause delays in adding recent upstream records to the dataset as well as unecessairly take away resources (such as Spark executors in the case of the Spark engine) from other applications in the data lake. A cancellable table service plan should avoid this situation by preventing itself from being committed if a conflicting ingestion job has been comitted already, and cancel itself. In conjunction, any ingestion writer or non-cancellable table service writer should be able to infer that a conflicting inflight table service plan is cancellable, and therefore can be ignored when attempting to commit the instant. 

### (B) A cancellable table service plan should be eligible for cancellation at any point before committing
A writer should be able to explictly cancel a cancellable table service plan that an ongoing concurrent writer is executing, as long as it has not been committed yet. This requirement is needed due to presence of concurrent and async writers for table service execution, as another writer should not need to wait for a table servie writer to execute further or fail before confirming that its cancellation request will be honored.

### (C) An incomplete cancellable plan should eventually have its partial writes cleaned up
Although cancellation (be it via an explict request or due to a write conflict) can ensure that a table service write is never committed, there still needs to be a mechanism to have its data and instant files cleaned up. At minumum the table service writer itself should be able to do this cleanup, but this is not sufficient as orchestration/transient failrures/resource allocation can prevent table service writers from re-attempting their write. Clean can be used to guarantee that an incomplete cancellable plan is eventually cleaned up. Because an inflight plan remaining on the timeline can degrade performance of reads/writes (as mentioned earlier), a cancellable table service plan should be elligible to be targeted for cleanup if HUDI clean deems that it has remaining inflight for too long (or some other critera).
Note that a failed table service should still be able to be safely cleaned up immediately  - the goal here is just to make sure an inflight plan won't stay on the timeline for an unbounded amount of time but also won't be likely to be prematurely cleaned up by clean before it has a chance to be executed.

## Design
### Enabling a plan to be cancellable
To satisfy goal (A), a new config flag "cancellable" can be added to a table service plan. A writer that intends to schedule a cancellable table service plan can enable the flag in the serialized plan metadata. Any writer executing the plan can infer that the plan is cancellable, and when trying to commit the instant should abort if it detects that any ingestion write or table service plan (without cancellable config flag) is targeting the same file groups. As a future optimization, the cancellable table writer can use early conflict detection (instead of waiting until committing the instant) to repeatadly poll for any conflicting write appearing on timeline, and abort earlier if needed.
On the other side in ingestion write, the commit finalization flow for ingestion writers can be updated to ignore any inflight table service plans if they are cancellable.
For the purpose of this design proposal, consider an ingestion job as having three steps:
1. Schedule itself on the timeline with a new instant time in a .requested file
2. Process/record tag incoming records, build a workload profile, and write the updating/replaced file groups to a "inflight" instant file on the timeline. Check for conflicts and abort if needed.
3. Perform write conflict checks and commit the instant on the timeline

The aforementioned changes to ingestion and table service flow will ensure that in the event of a conflicting ingestion and cancellable table service writer, the ingestion job will take precedence (and cause the cancellable table service instant to fail) as long as a cancellable table service hasn't be completed before (2). Since if the cancellable table service has already been completed before (2), the ingestion job will see that a completed instant (a cancellable table service action) conflicts with its ongoing inflight write, and therefore it would not be legal to proceed. 

### Adding a cancel action and abort state for cancellable plans
This proposed design will also involve adding a new instant state and interal hoodie metadata directory, by making the following changes:
* Add an ".aborted" state type for cancellable table service plan. This state is terminal and an instant can only be transitioned to .*commit or .aborted (not both)
* Add a new .hoodie/.cancel folder, where each file corresponds to an instant time that a writer requested for cancellation. As will be detailed below, a writer can cancel an inflight plan by adding the instant to this directoy and execution of table service will not allow an instant to be comitted if it appears in this /.cancel directoy

The new /.cancel folder will enable goal (B) by allowing writers to permentantly prevent an ongoing cancelltable table service write from comitting, without needing to block/wait for the table service writer. The new ".aborted" state will allow writers to infer wether a cancelled table service plan needs to still have it's partial data writes cleaned up from the dataset, which is needed for (C). The additional design change below will complete the remaining requirement for (C) of eventual cleanup.
  
### Handling cancellation of plans
An additional config "cancellation-policy" can be added to the table service plan to indicate when it is ellgible to be permanently cancelled by writers other than the one responsbible for executing the table service. This policy can be a threshold of hours or instants on timeline, where if that # of hours/instants have elapsed since the plan was scheduled, any call to clean will cleanup the instant. This policy should be configured by the writer scheduling a cancellable table service, based on the amount of time they expect the plan to remain on the timeline before being picked up for execution. For example, if a table service writer is expected to immeditately start executing the plan after scheduling it, the the cancellation-policy can just be a few minutes. On the other hand, if the plan is expected to have its execution deferred to a few hours later, then the cancellation-policy should be more lenient. Note that this cancellation policy is not a repalacement for determining wether a table service plan is currently being executed - as with ingestion writes, permanent cleanup of a cancellable table service plan will only start once it is confirmed that a ongoing writer is no longer progressing it. 

In order to ensure that other writers can indeed permenantely cancel a cancellable table service plan (such that it can no longer be executed), additional changes to clean and table service flow will be need to be added as well, as proposed below. Also, note that the cancellation-policy is only required to be honored by clean: a user can choose setup an application to aggresively clean up a failed cancellable table service plan even if it has not meet the critera for its cancellation-policy yet. This can be useful if a user wants a utility to manually ensure that clean/archival for a dataset progresses immdeitately or knows that a cancellable table service plan will not be attempted again or cleaned up by another writer. The two new cancel APIs in the below propsoal provide a method to achieve this.
  

#### Enabling table service execution and clean to support cancellation and automatic cleanup

The table service flow will be updated to check the /.cancel folder during a pre-commit check before completing the instant. If the instant conflcts with other ongoing writes or is a target of /.cancel, then all data files will be rolled back and the instant will be transitioned to .aborted. These checks will be performed within a transaction, to guard against users cancelling an already-committed instant. In addition, in order to avoid the scenario of writer executing an instant but having its data files being deleted by a concurrent cancellation, the table service writer will perform heartbeating. If an instant has an active heartbeat it can be requested for cancellation (by adding an instant in /.cancel) but it cannot yet be cleaned up and transitioned to .aborted state. 

The below visualization shows the flow for cancellable table service plans (steps that are already in existing table service flow are grey-ed out) 

![cancel table service lifecycle with lock (3)](https://github.com/user-attachments/assets/b882a79d-b582-45f5-874d-57e255d13ff0)

Having this new .hoodie/.cancel folder (in addition to just the .aborted state) is needed not only to allow any writer to forcibily block an instant from being comitted, but also to avoid a table service writer attempt having to unecessairly re-attempt execution of the instant if the previous attempt issues a cancellation (but had failed during cancellation). The below visualized scenario shows how this next attempt will "short circut" in this manner. This scenario also includes an example of concurrent writers to show how transaction and heartbeating in the above proposed flow will allow correct behavior even in the face of concurrent writers attempting to execute and/or cancel the instant.

![cancel flow table serivce](https://github.com/user-attachments/assets/aa8a363c-48f7-4a6c-ae51-cc0c126983f2)


Aside from modifications to the table service execution flow, a new pair of cancel APIs request_cancel and execute_cancel will be added for all writers to use. They will allow a writer to request an instant to be cancelled (by adding it to /.cancel) and transition an instant to the terminal .aborted state, respectively.

The new cancel API request_cancel will perform the following steps

1. Start a transaction
2. Reload active timeline
3. If instant is already comitted, abort
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

Note that similar to the proposed table service flow, this API will check for heartbeats within a transaction (to guarantee there are no writers still working on this instant) and will transition the state to .aborted within a transaction. The latter is done within a transaction (in both table service flow and this API) to prevent a concurrent request_cancel call from creating a file under a /.cancel of an instant already transtioned to .aborted. Although in this scenario calling execute_cancel or cancellable table service API again after this point would be safe, this guard should prevent files in /.cancel for already-aborted instants from accumulating.

Although any user can invoke request_cancel and execute_cancel, the clean operation will be required to use these APIs in order to cancel any incomplete plans that have satisfied their cancellation policy. This is analagous to how clean schedules and executes the rollback of any failed ingestion writes. Specifically, clean will now perform the following steps
1. Iterate through each inflight cancellable instant, for each instant that has met their cancellation policy, call request_cancel
2. Iterate through each instant in /.cancel and call execute_cancel.
Note that the request_cancel and execute_cancel APIs are responsible for ensuring that already-committed instants aren't cancelled and that ongoing writers aren't concurrently having their files cleaned up by a writer doing cancellation.

Archival will also be updated to delete instants transitioned to .aborted state, since .aborted is a terminal state.

This approach has one critical drawback:
* A new state and action type needs to be added to HUDI, requiring updates to all reader/writer logic of the HUDI filesystem view. Readers need to account for skipping aborted plans, and archival should clean up aborted plans and cancel actions accordingly. In addition, it is now possible for an instant to be transitioned to a terminal state (.aborted) without being completed. This can complicate tooling that needs to infer wether an instant is committed.



