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
# RFC-61: Snapshot view management



## Proposers

- @<proposer1 @fengjian428>

## Approvers
 - @<approver1 @xushiyan>
 - @<approver2 github username>

## Status

JIRA: [HUDI-4677](https://issues.apache.org/jira/browse/HUDI-4677)

> Please keep the status updated in `rfc/README.md`.

## Abstract

Describe the problem you are trying to solve and a brief description of why it’s needed

For the snapshot view scenario, Hudi already provides two key features to support it:
* Time travel: user provides a timestamp to query a specific snapshot view of a Hudi table
* Savepoint/restore: "savepoint" saves the table as of the commit time so that it lets you restore the table to this savepoint at a later point in time if need be.
but in this case, the user usually uses this to prevent cleaning snapshot view at a specific timestamp, hence, only clean unused files
The situation is there some inconvenience for users if use them directly

Usually users incline to use a meaningful name instead of querying Hudi table with a timestamp, using the timestamp in SQL may lead to the wrong snapshot view being used. 
for example, we can announce that a new tag of hudi table with table_nameYYYYMMDD was released, then the user can use this new table name to query.
Savepoint is not designed for this "snapshot view" scenario in the beginning, it is designed for disaster recovery. 
let's say a new snapshot view will be created every day, and it has 7 days retention, we should support lifecycle management on top of it.
What this RFC plan to do is to let Hudi support release a snapshot view and lifecycle management out-of-box.

## Background
Introduce any much background context which is relevant or necessary to understand the feature and design choices.
typical scenarios of snapshot view:

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