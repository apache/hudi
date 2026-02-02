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
# RFC-86: DataFrame Implementation of HUDI write path

## Proposers

- @jalpeshborad

## Approvers
 - @codope
 - @<approver2 github username>

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-8908

> Please keep the status updated in `rfc/README.md`.

## Abstract

HUDI write path is majorly implemented using RDD APIs for Apache Spark and RDD APIs are not supported by accelerators like RAPiDs, Velox - Glueten etc.
Having DataFrame implementation will also add speed-up overall execution compared to RDD computations.


## Background
Major use case is to enable HUDI based workload execution on GPUs to provide computational boost during HUDI writes. 
While running HUDI workloads on GPUs, we observed that write path is executed on CPUs rather than GPUs making it un-utilized. We found that GPU accelerators mainly operate on DataFrame APIs and will fall back on CPU based execution if any operation is not supported.

## Implementation
- There are potentially 2 approaches i.e. either we introduce DataSet based interface which can fit in current eco-system or we can re-write entire write path into dataFrame APIs.
- I have tried implementing first approach [https://github.com/apache/hudi/pull/12429], but I found that 
- - strong typing of any case class will result in serialisation and de-serialisation during access adding serde overhead.
- - Transforming from case class to another case class will be performed via map/mapPartitions operations which most probably will be lambda methods and will be deemed unsupported by accelerators.
- - Reusing base interfaces like HoodieData, HoodieKey, WriteStatus has challenge as they can't be serialised directly into DataSet due to fields having circular reference like `Throwable`.
- - Using kryo serialisation over java serialisation explicitly to serialise is resulting into single bytearray column which is not allowing to access individual columns as all columns will be packed into bytearray.

## Rollout/Adoption Plan
 - 

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.