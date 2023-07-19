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
# RFC-72: Redesign Hudi Spark Integration

## Proposers

- @jonvex

## Approvers
 - @<approver1 github username>
 - @<approver2 github username>

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-6568

> Please keep the status updated in `rfc/README.md`.

## Abstract

Some Spark performance optimizations are limited to only Spark's HadoopFsRelation. Currently, bootstrapped and MOR 
tables (as well as COW tables with schema evolution) cannot take advantage of these optimizations because they are 
implemented as custom relations. 

Our proposal is to implement bootstrap, MOR, CDC reading as a file format so we no longer need custom relations.
This will allow Hudi to take advantage of all Spark read optimizations and improve code maintainability.

## Background
Currently, our relations have the traits 'FileRelation' and 'PrunedFilteredScan'. These traits allow for some, but not 
all of Sparks read optimizations. HadoopFsRelation has access to all of Sparks optimizations, but it is a case class and
cannot be extended. We have ported some optimizations such as nested schema pruning into Hudi, but we need to do this 
for each version of Spark. It is becoming unmaintainable to port over each optimization. By implementing at the file 
format level, it will no longer be necessary.


## Implementation

A new file format extending ParquetFileFormat will be created that will implement the merging required by MOR and 
bootstrapped tables. 

CDC: need to look into

## Rollout/Adoption Plan

In 0.14.0, we will have a feature flag that will disable this by default


- What impact (if any) will there be on existing users?
- If we are changing behavior how will we phase out the older behavior?
- If we need special migration tools, describe them here.
- When will we remove the existing behavior


## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.