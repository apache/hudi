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
In order to improve latency/throughput of writes into a HUDI dataset, HUDI does not require that table service operations (such as clustering and compaction) be serially and sequentially performed before/after an ingestion write. Instead, using HUDI multiwriter, a user can orchesterate seperate writers to potentially execute table service plans concurrently to an ingestion writers. This setup though may face reliability issues for clustering and compaction, as failed executions and rollbacks can cause delays in table service executions and prevent cluster/compaction/clean operations from being scheduled. This RFC proposes to address these limitations by using HUDI's heartbeating and transaction manager to update the behavior for clustering, compaction, and rollback of failed writes. With these changes users can build an orchestration platform for executing each table service independently without needing to make complicated/expensive changes to prevent multiple job/threads for targeting the same table service plan.


## Background
The table service operations compact, logcompact, and cluster, have the following multiwriter issues when writers execute or rollback these table service plans (note that “removable-plan” is defined as a table service plan that is configured such that when it is rolled back, its plan is deleted from the timeline and cannot be re-executed - only clustering and logcompaction are expected to support this option)

### Concurrent writers can execute table service plan 

When a writer executes a compact, cluster, or logcompaction plan, it will first rollback any existing inflight attempt, and (depending on the type and configuration of the table service) optionally re-execute it. This can lead to dataset corruption if one writer is rolling back the instant while another is still executing it. See https://issues.apache.org/jira/browse/HUDI-7503 for an example.
Independent/outside of HUDI, a user may have an orchestration setup of concurrent writers where sometimes multiple writers can execute the sample plan simultaneously, due to a transient failure (at the orchestration leve) or misconfiguration. While it can be argued that HUDI does not to provide any guarantees of correctness if concurrent writers execute the same plan, updating HUDI to guard against this scenario would reduce the operational overhead and complexity of deploying a large-scale HUDI writer orchestration.

### Removable-plan table service can be rolled back by clean before it can be executed

After a writer schedules a remove-plan table service (such as logcompaction), another writer performing clean can rollback the plan before it has a chance to be executed, deleting the plan from the timeline. This will prevent the table service from ever being executed, and if the table service execution to be async (being deferred for execution later by another writer) then the chance of this happening increases.

## Implementation
In order to resolve these limitations with compact, logcompact, and cluster, a new configuration value `table_service_rollback_delay` will be introduced. The value will indicate the number of minutes that must elapse before a clean attempt is allowed to start a rollback of any removable-plan table service (logcompaction or cluster if they are configured as such). In addition, changes related to heartbeating and table lock transactions will be made to 
the compact/logcompact/cluster APIs that execute a table service plan
clean execution 
 Combining all these changes, the logic for cluster/compact/logcompact table service execution and clean will now behave the following ways:

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


