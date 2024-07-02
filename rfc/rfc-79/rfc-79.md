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
In order to reduce the latency and overhead of ingestion writes into a HUDI dataset, HUDI does not require that table service operations (such as clustering and compaction) be serially and sequentially performed before/after an ingestion write. Using HUDI multiwriter, different writers can potentially execute table service plans concurrently to an ingestion writers. This setup though is currently not reliable for clustering and compaction as failed executions and rollbacks may cause dataset corruptions or table services plans to be prematurely aborted. This RFC proposes to address these limitations by using HUDI's heartbeating and transaction manager to update the behavior for clustering, compaction, and rollback of failed writes.


## Background
The table service operations compact, logcompact, and cluster, have the following multiwriter issues when writers execute or rollback these table service plans (note that “removable-plan” is defined as a table service plan that is configured such that when it is rolled back, its plan is deleted from the timeline and cannot be re-executed - only clustering and logcompaction are expected to support this option)

### s

## Implementation
Describe the new thing you want to do in appropriate detail, how it fits into the project architecture. 
Provide a detailed description of how you intend to implement this feature.This may be fairly extensive and have large subsections of its own. 
Or it may be a few sentences. Use judgement based on the scope of the change.

## Rollout/Adoption Plan

 - What impact (if any) will there be on existing users? 
 - If we are changing behavior how will we phase out the older behavior?
 - If we need special migration tools, describe them here.
 - When will we remove the existing behavior

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.
