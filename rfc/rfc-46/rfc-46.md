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
# RFC-46: Optimize Record Payload handling

## Proposers

- @alexeykudinkin

## Approvers
 - @vinothchandar
 - @nsivabalan
 - @xushiyan

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-3217

> Please keep the status updated in `rfc/README.md`.

## Abstract

Avro historically has been a centerpiece of the Hudi architecture: it's a default representation that many components expect
when dealing with records (during merge, column value extractions, writing into storage, etc). 

While having a single format of the record representation is certainly making implementation of some components simpler, 
it bears unavoidable performance penalty of de-/serialization loop: every record handled by Hudi has to be converted
from (low-level) engine-specific representation (`Row` for Spark, `RowData` for Flink, `ArrayWritable` for Hive) into intermediate 
one (Avro), with some operations (like clustering, compaction) potentially incurring this penalty multiple times (on read- 
and write-paths). 

As such, goal of this effort is to remove the need of conversion from engine-specific internal representations to Avro 
while handling records. 

## Background

Historically, Avro has settled in as de-facto intermediate representation of the record's payload since the early days of Hudi.
As project matured and the scale of the installations grew, necessity to convert into an intermediate representation quickly 
become a noticeable bottleneck in terms of performance of critical Hudi flows. 

At the center of it is the hierarchy of `HoodieRecordPayload`s, which is used to hold individual record's payload 
providing an APIs like `preCombine`, `combineAndGetUpdateValue` to combine it with other record using some user-defined semantic. 

## Implementation

### Revisiting Record Classes Hierarchy

To achieve stated goals of avoiding unnecessary conversions into intermediate representation (Avro), existing Hudi workflows
operating on individual records will have to be refactored and laid out in a way that would be _unassuming about internal 
representation_ of the record, ie code should be working w/ a record as an _opaque object_: exposing certain API to access 
crucial data (precombine, primary, partition keys, etc), but not providing the access to the raw payload.

Having existing workflows re-structured in such a way around a record being an opaque object, would allow us to encapsulate 
internal representation of the record w/in its class hierarchy, which in turn would allow for us to hold engine-specific (Spark, Flink, etc)
representations of the records w/o exposing purely engine-agnostic components to it. 

Following (high-level) steps are proposed: 

1. Promote `HoodieRecord` to become a standardized API of interacting with a single record, that will be  
   1. Replacing all accesses from `HoodieRecordPayload`
   2. Split into interface and engine-specific implementations (holding internal engine-specific representation of the payload) 
   3. Implementing new standardized record-level APIs (like `getPartitionKey` , `getRecordKey`, etc)
   4. Staying **internal** component, that will **NOT** contain any user-defined semantic (like merging)
2. Extract Record Combining (Merge) API from `HoodieRecordPayload` into a standalone, stateless component (engine). Such component will be
   1. Abstracted as stateless object providing API to combine records (according to predefined semantics) for engines (Spark, Flink) of interest
   2. Plug-in point for user-defined combination semantics
3. Gradually deprecate, phase-out and eventually remove `HoodieRecordPayload` abstraction

Phasing out usage of `HoodieRecordPayload` will also bring the benefit of avoiding to use Java reflection in the hot-path, which
is known to have poor performance (compared to non-reflection based instantiation).

#### Combine API Engine

Stateless component interface providing for API Combining Records will look like following:

```java
interface HoodieRecordCombiningEngine {
  
  default HoodieRecord precombine(HoodieRecord older, HoodieRecord newer) {
    if (spark) {
      precombineSpark((SparkHoodieRecord) older, (SparkHoodieRecord) newer);
    } else if (flink) {
      // precombine for Flink
    }
  }

   /**
    * Spark-specific implementation 
    */
  SparkHoodieRecord precombineSpark(SparkHoodieRecord older, SparkHoodieRecord newer);
  
  // ...
}
```
Where user can provide their own subclass implementing such interface for the engines of interest.

#### Migration from `HoodieRecordPayload` to `HoodieRecordCombiningEngine`

To warrant backward-compatibility (BWC) on the code-level with already created subclasses of `HoodieRecordPayload` currently
already used in production by Hudi users, we will provide a BWC-bridge in the form of instance of `HoodieRecordCombiningEngine`, that will 
be using user-defined subclass of `HoodieRecordPayload` to combine the records.

Leveraging such bridge will make provide for seamless BWC migration to the 0.11 release, however will be removing the performance 
benefit of this refactoring, since it would unavoidably have to perform conversion to intermediate representation (Avro). To realize
full-suite of benefits of this refactoring, users will have to migrate their merging logic out of `HoodieRecordPayload` subclass and into
new `HoodieRecordCombiningEngine` implementation.

### Refactoring Flows Directly Interacting w/ Records:

As was called out prior to achieve the goal of being able to sustain engine-internal representations being held by `HoodieRecord` 
class w/o compromising major components' neutrality (ie being engine-agnostic), such components directly interacting w/
records' payloads today will have to be refactored to instead interact w/ standardized `HoodieRecord`s API.

Following major components will be refactored:

1. `HoodieWriteHandle`s will be  
   1. Accepting `HoodieRecord` instead of raw Avro payload (avoiding Avro conversion)
   2. Using Combining API engine to merge records (when necessary) 
   3. Passes `HoodieRecord` as is to `FileWriter`
2. `HoodieFileWriter`s will be 
   1. Accepting `HoodieRecord`
   2. Will be engine-specific (so that they're able to handle internal record representation)
3. `HoodieRealtimeRecordReader`s 
   1. API will be returning opaque `HoodieRecord` instead of raw Avro payload


## Rollout/Adoption Plan

 - What impact (if any) will there be on existing users? 
   - Users of the Hudi will observe considerably better performance for most of the routine operations: writing, reading, compaction, clustering, etc due to avoiding the superfluous intermediate de-/serialization penalty
   - By default, modified hierarchy would still leverage 
   - Users will need to rebase their logic of combining records by creating a subclass of `HoodieRecordPayload`, and instead subclass newly created interface `HoodieRecordCombiningEngine` to get full-suite of performance benefits 
 - If we are changing behavior how will we phase out the older behavior?
   - Older behavior leveraging `HoodieRecordPayload` for merging will be marked as deprecated in 0.11, and subsequently removed in 0.1x
 - If we need special migration tools, describe them here.
   - No special migration tools will be necessary (other than BWC-bridge to make sure users can use 0.11 out of the box, and there are no breaking changes to the public API)
 - When will we remove the existing behavior
   - In subsequent releases (either 0.12 or 1.0) 

## Test Plan

This refactoring will not be modifying any existing Hudi semantics other than the aforementioned, and as such to guarantee preservation of the 
logical correctness of the many flows that will be affected by the refactoring we will rely on the existing set of test-suites.

Nevertheless, we will run corresponding set of benchmarks stressing the flows being affected by the refactoring to validate
that there are considerable performance advantage of abandoning conversion into intermediate representation completely.
