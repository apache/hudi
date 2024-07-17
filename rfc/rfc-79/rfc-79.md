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
# RFC-79: Improving reliability of concurrent table service executions and rollbacks

## Proposers

- @kbuci
- @suryaprasanna
- @nsivabalan

## Approvers

## Status

JIRA: HUDI-7946


## Abstract
In order to improve latency/throughput of writes into a HUDI dataset, HUDI does not require that table service operations (such as clustering and compaction) be serially and sequentially performed before/after an ingestion write. Instead, by enabling HUDI multiwriter and async table service execution, a user can orchesterate seperate writers to potentially execute table service plans concurrently to an ingestion writers. This setup though may face reliability issues for clustering and compaction, as failed executions and rollbacks can cause delays in table service executions and prevent cluster/compaction/clean operations from being scheduled. This RFC proposes to address these limitations by using HUDI's heartbeating and transaction manager to update the behavior for clustering, compaction, and rollback of failed writes. With these changes users can build an orchestration platform for executing each table service independently without needing to make complicated/expensive changes to prevent multiple job/threads for targeting the same table service plan.


## Background
### Heartbeating
HUDI supports a transaction manager, which allows a job to take a table-level exclusive lock. Additionally, HUDI supports a heartbeating mechanism where a job can create a heartbeat file in the DFS for a corresponding instant and setup a background thread to continually touch said file. A heartbeat is considered as expired if the heartbeat file is deleted or hasn't been updated in a configured time interval. This enables concurrent jobs to infer whether an instant is actively being worked on by a running job.
Currently this is used by ingestion instants and clean table service rolling back failed writes. Specifically, an ingestion job will start a heartbeat for an ingestion instant before scheduling the instant, and when the job finished/fails the heartbeat will either be explictly cleaned up or will eventually expire. If a clean is configured to be lazy, then when clean rolls back faled writes, it will only target ingestion instants that are pending and have an expired heartbeat, as these will be considered as failed writes.

### Execution of table services 
The table service operations compact and cluster are by default "immutable" plans, meaning that once a plan is scheduled it will stay as as a pending instant until a caller invokes the table service execute API on the table service instant and sucessfully completes it. Specifically, if an inflight execution fails after transitioning the instant to inflight, the next execution attempt will implictly create and execute a rollback plan (which will delete all new instant/data files), but will keep the table service plan. This process will repeat until the instant is completed. The below visualization captures these transitions at a high level 

If an immutable table service plan is configured for async execution then each of the aforementioned instant transitions during table service execution can potentially be preformed by seperate concurrent jobs. Typically this is expected only if an execution attempt fails after performing a step and the next execution attempt performs a rollback and re-attempts creating data files and comitting the instant. But in a distributed orchestration envrionment there may be a scenario where multiple jobs attempt to execute a table service plan at the same time, leading to one job rolling back a table service inflight instant while another is still executing it. This can undo the progress of table service execution (and in the worst case leave the dataset in an invalid state), harming the reliability of executing table services in an async manner. 
While it can be argued that HUDI does not need to provide any guarantees of correctness if concurrent writers execute the same plan, updating HUDI to guard against this scenario would reduce the operational overhead and complexity of deploying a large-scale HUDI writer orchestration. As a result it may be a worthwhile tradeoff to update HUDI to improve reliability of table service executions in these scenarios if the resulting added complexity to HUDI can be minimized.

### Exploring "mutable" table service plans
As of <insert mutable plan RFC> the concept of a "mutable" table service is proposed to be supported by HUDI. Similar to an immutable table service plan, a mutable table service plan is scheduled on the timeline and executed by a table service execution API. The difference though is that if an execution attempt fails after transitioning a mutable table service plan to inflight, the subsequent rollback will not leave the plan on the timeline but will remove it during rollback (similar to rollback of ingestion write instants like upsert commits). In addition, when the clean table service is configured to be lazy and performs a rollback of failed writes, any pending mutable table service plan will be rolled back (the same way rollback of failed writes targets pending ingestion write instants). The RFC <insert mutable plan RFC> details the states of a "mutable" table service plan

If a job schedules a mutable table service plan with an async configuration, it may be possible that the plan is left ont the timeline for an unbounded amount of time, due to table service execution repeatdily failing or being delayed. Since a pending table service plan can cause storage/performance degredations (by blocking other table services like clean that would remove/optimize files in the data partitions/timeline), a user may want to ensure that these pending/failed plans are eventually automatically cleaned up by configured clean to be lazy. Unfortunately a lazily-configured clean job can rollback a mutable table service plan before it has a chance to be executed, deleting the plan from the timeline. This is because this plan will not have an active heartbeat, due to fact that jobs that schedule or execute table service plans do not currently start a heartbeat, and even if they did the duration of time between a plan being scheduled and being picked up by an async table service plan can be longer than the heartbeat expiration threshold. This rollback will prevent the table service from ever being executed. To summarize, mutable table service plans, if implemented and configured in an async manner, can incur reliability issues by failing and never being removed or by being prematurely rolled back before they can be executed.
If mutable table services plans end up being supported, then the same reliability improvments to async/concurrent execution of immutable table service plans can be added for mutable table service plans as well, by addressing this issue with async mutable plan table service execution.

## Design 
### Supporting concurrent executions of immutable table service plans with heartbeating
As mentioned above, once a clustering/compact "immutable" table service plan is scheduled, a corresponding cluster/compact API is called to (optionally rollback) and execute the plan. Implementing a "guard" to allow only one job to invoke the "execute" API on a table service plan (at a given time) would avoid any scenario of multiple jobs concurrently creating/deleting data files in a conflicting manner. This guard can be implemented by using HUDI heaertbeating in conjunction with the transaction manager. Whenever a job calls the compact/cluster API for an immutable table service, it can start a heartbeat to indicate to concurrent jobs that this table service already has an active execution attempt. These other jobs can then self-abort instead of starting their own (concurent) execution attempt. If the current job executing the table service plan fails, after its heartbeat expires another job can re-start the active heartbeat and start executing this plan again. If there are multiple concurrent jobs that attempt to start a heartbeat for the table service at the same time, only one job should hold and start the heartbeat. This restriction will be enforced by using HUDI transaction manager.

### Improving executions/clean reliability of mutable table service plans

## Implementation
### Guarding concurrent executions of immutable table service plans with heartbeating
When invoking the clustering/compact API to execute an existing immutable table service plan, the aforementioned heartbeat guard will be implemented by running the following steps before running any other step.
1. Start a transaction
2. Get the instant time P of the desired table service plan to execute. 
3. If P has an active heartbeat, fail and abort the transaction. Otherwise start a heartbeat for P.
4. End the transaction
And once the rest of the clustering/compaction API logic completes or fails, the heartbeat will be cleaned up.

A transaction is needed to ensure that if multiple concurrent callers attempt to start a heartbeat at the same time, at most one will start a heartbeat. Although this increases the contention of multiple jobs trying to acquire a table lock, because only a few DFS calls will be made during this transaction (to find the heartbeat file and view its last update) other jobs waiting to start a transaction should ideally not have to wait for too long.

### Changes to compaction/cluster plan exection
The compact/logcompact/cluster API that executes a plan will now have the following steps performed before calling the current execution logic
1. Get the table service plan P (as usual)
2. Start a transaction
3. Check if the instant time P has an active heartbeat, if so abort transaction and throw an exception
4. If P is a  removable-plan, check if (a) P is an inflight instant or (b) there is a rollback plan on active timeline that targets P. If either is the case, then hrow an exception and abort the transaction. This is since this instant cannot be executed again but must be rolled back.
5. Start a heartbeat for P (this will implicitly re-start the heartbeat if it has been started before by another job if P is not a removable-plan)
6. Finish transaction
7. Run the existing compact/logcompact/cluster execute API logic on P. Note that if P is not a removable-plan, this will implicitly rollback P (if it is currently inflight or has a pending rollback plan targeting it) before the actual execution phase.
8. After the execution attempt succeeds/fails, clean up the heartbeat file before returning

- The check in (3) needs to be within a transaction in order to ensure at most once writer has an active heartbeat against P at any given time. If this was done outside a transaction, then concurrent writers may each check that no heartbeat exists, and then start one at the same time.
- It is possible for a removable-plan P to be in a requested state, and past writer may have scheduled a rollback for P but failed before completing it. In order to handle this case (4) i needed
### Changes to clean's rollback of failed writers
The clean logic for rolling back failed writes will be changed such that a table lock will be acquired while iterating through all existing inflight and pending rollback plans and scheduling new rollback plans. If a pending instant of a non removable-plan table service is encountered, the instant will be skipped (and will not have a rollback scheduled). In addition if the plan is not at least `table_service_rollback_delay` minutes old or has an active heartbeat, it will also be skipped. Otherwise this table service plan is neither a will also be marked for rollback, the same way that ingestion inflights are targeted for rollback, since it is neither a recent removable-plan nor a non removable-plan instant. Once all required rollback plans have been scheduled, the table lock will be released, and any/all scheduled rollback plans will be executed (this means that executions of the rollback plans won’t be under a lock). As a result of this change, the logic for rolling back failed writes will be now split into two steps:
1. Within a transaction, reload the active timeline and then get all instants that require a rollback plan be scheduled. For each, schedule a rollback plan.
2. Execute any pending rollback plans (be it pending rollback plans that already existed or new pending rollback plans scheduled during (1)).

- The reason (1) needs to be within a transaciton is in order to handle the case where a concurrent writer executing a table service plan might start the heartbeat at the same time

## Test Plan

A proposed fix should handle any combination of table service execution/rollback operations. A few scenarios for compaction, logcomopaction, and clustering that can be tested are listed below

### Compaction and table service plans that are not removable-plan
Assume that table service plan P is either a compaction or a logcompaction/clustering plan that is configured to not be a removable-plan.

| #|  Scenario | Expectations |
| - | - | -| 
|A.| Multiple writers executing  on the same new plan P , which is in a requested state | At most one attempt will complete execution and create the completed data files and commit metadata for P, the rest will either fail or return the completed metadata (due to starting after P was already completed) | 
|B.| <pre> <p> 1. Writer X starts execution of P and transitions to inflight </p> <p> 2. Writer Y starts executing P, after it has been transitioned to inflight </p> </pre> | X or Y may complete execution (or neither), though both may still succeed. If Y completed P, it would have first rolled back the existing attempt. If X completed P, Y would either fail or return completed metadata |
|C.|<pre> <p> 1. Plan P exists on the timeline, which has already been transitioned to inflight </p> <p> 2. Writer X starts executing P </p> <p> 3. Writer Y starts executing P </p> </pre> | X or Y may complete execution (or neither),  though both may still succeed. If X completed execution, it would have had to rollback P.inflight first before re-execution. If Y completed execution, it may or may not have had to do a rollback, depending on if X failed after rolling back P.inflight but before re-attempting the actual execution. |


### Removable-plan clustering / logcompaction
Assume that table service plan P is a logcompaction/clustering plan that is configured to be a removable-plan.

| #|  Scenario | Expectations |
| - | - | -| 
|A.| Multiple writers executing  on the same new plan P , which is in a requested state | At most one attempt will complete execution and create the completed data files and commit metadata for P, the rest will either fail or return the completed metadata (due to starting after P was already completed) | 
|B.| <pre> <p> 1. Writer X starts execution of P and transitions to inflight </p> <p> 2. Writer Y starts executing P, after it has been transitioned to inflight </p> </pre> | X might complete execution or fail. If X ended up completing execution, Y may either fail or return completed metadata. Otherwise if X fails, Y will fail. |
|C.| <pre> <p> 1. Plan P exists on the timeline, which has already been transitioned to inflight </p> <p> 2. Writer X starts executing P </p> <p> 3. Writer Y starts executing P </p> | Both X and Y will fail |
|D.|<pre> <p> 1. Plan P is scheduled on timeline </p> <p> 2. Writer X starts executing P <p> 3. Writer Y executes Clean </p> </pre> | X may complete execution of P or fail. If X is currently executing P or P was newer than `table_service_rollback_delay` and not transitioned to inflight yet, the Y should not attempt to rollback P. Otherwise Y may try rollback P. |
|E.| <pre> <p> 1. Plan P is scheduled on timeline </p> <p> 2. Writer X executes clean </p> <p> 3. Writer Y starts executing P </p> </pre> | If P is newer than `table_service_rollback_delay` then X should ignore P, and Y will have the chance to try to execute it. Otherwise, X will try to rollback P. The only scenario where Y might actually start executing P and transitions it to inflight/completed is if X happened to fail before creating a rollback plan. Otherwise, Y is expected to fail. |

## Rollout/Adoption Plan

- In order to not impact existing users, `table_service_rollback_delay` can be set to a large value, which will allow the current clean behavior (where removeable plans may be prematurely rolled-back) to remain.
- There is a small possibility that users using concurrent writers to execute table services may see that, after an execution attempt of a table service plan failed, retries within the next few minutes may all fail as well. This is since there is a chance that a writer may fail and terminate before it has a chance to cleanup it's heartbeat. In order to address this potential (but unlikely) scenario, the new changes to compact/cluster execution can be made to only trigger if a new config flag is enabled. 


