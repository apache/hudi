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

- @<proposer1 github username>
- @<proposer2 github username>

## Approvers
 - @<approver1 github username>
 - @<approver2 github username>

## Status

JIRA: HUDI-7946


## Abstract
In order to improve latency/throughput of writes into a HUDI dataset, HUDI does not require that table service operations (such as clustering and compaction) be serially and sequentially performed before/after an ingestion write. Instead, using HUDI multiwriter, a user can orchesterate seperate writers to potentially execute table service plans concurrently to an ingestion writers. This setup though may face reliability issues for clustering and compaction, as failed executions and rollbacks may cause dataset corruptions or table services plans to be prematurely aborted. This RFC proposes to address these limitations by using HUDI's heartbeating and transaction manager to update the behavior for clustering, compaction, and rollback of failed writes.


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

### Changes to clean's rollback of failed writers
The clean logic for rolling back failed writes will be changed such that a table lock will be acquired while iterating through all existing inflight and pending rollback plans and scheduling new rollback plans. If a pending instant of a non removable-plan table service is encountered, the instant will be skipped (and will not have a rollback scheduled). In addition if the plan is not at least `table_service_rollback_delay` minutes old or has an active heartbeat, it will also be skipped. Otherwise this table service plan is neither a will also be marked for rollback, the same way that ingestion inflights are targeted for rollback, since it is neither a recent removable-plan nor a non removable-plan instant. Once all required rollback plans have been scheduled, the table lock will be released, and any/all scheduled rollback plans will be executed (this means that executions of the rollback plans won’t be under a lock). As a result of this change, the logic for rolling back failed writes will be now split into two steps:
1. Within a transaction, reload the active timeline and then get all instants that require a rollback plan be scheduled. For each, schedule a rollback plan.
2. Execute any pending rollback plans (be it pending rollback plans that already existed or new pending rollback plans scheduled during (1)).


## Rollout/Adoption Plan

 - What impact (if any) will there be on existing users? 
 - If we are changing behavior how will we phase out the older behavior?
 - If we need special migration tools, describe them here.
 - When will we remove the existing behavior

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.
