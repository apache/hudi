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
from (low-level) engine-specific representation (`InternalRow` for Spark, `RowData` for Flink, `ArrayWritable` for Hive) into intermediate 
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
2. Extract Record Merge API from `HoodieRecordPayload` into a standalone, stateless component. Such component will be
   1. Abstracted as stateless object providing API to combine records (according to predefined semantics) for engines (Spark, Flink) of interest
   2. Plug-in point for user-defined combination semantics
3. Gradually deprecate, phase-out and eventually remove `HoodieRecordPayload` abstraction

Phasing out usage of `HoodieRecordPayload` will also bring the benefit of avoiding to use Java reflection in the hot-path, which
is known to have poor performance (compared to non-reflection based instantiation).

#### Record Merge API

CombineAndGetUpdateValue and Precombine will converge to one API. Stateless component interface providing for API Combining Records will look like following:

```java
interface HoodieRecordMerger {

   /**
    * The kind of merging strategy this recordMerger belongs to. A UUID represents merging strategy.
    */
   String getMergingStrategy();
  
   // This method converges combineAndGetUpdateValue and precombine from HoodiePayload. 
   // It'd be associative operation: f(a, f(b, c)) = f(f(a, b), c) (which we can translate as having 3 versions A, B, C of the single record, both orders of operations applications have to yield the same result)
   Option<HoodieRecord> merge(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException;
   
   // The record type handled by the current merger
   // SPARK, AVRO, FLINK
   HoodieRecordType getRecordType();
}

/**
 * Spark-specific implementation 
 */
class HoodieSparkRecordMerger implements HoodieRecordMerger {

  @Override
  public String getMergingStrategy() {
    return UUID_MERGER_STRATEGY;
  }
  
   @Override
   Option<HoodieRecord> merge(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
     // HoodieSparkRecord precombine and combineAndGetUpdateValue. It'd be associative operation.
   }

   @Override
   HoodieRecordType getRecordType() {
     return HoodieRecordType.SPARK;
   }
}
   
/**
 * Flink-specific implementation 
 */
class HoodieFlinkRecordMerger implements HoodieRecordMerger {

   @Override
   public String getMergingStrategy() {
      return UUID_MERGER_STRATEGY;
   }
  
   @Override
   Option<HoodieRecord> merge(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
      // HoodieFlinkRecord precombine and combineAndGetUpdateValue. It'd be associative operation.
   }

   @Override
   HoodieRecordType getRecordType() {
      return HoodieRecordType.FLINK;
   }
}
```
Where user can provide their own subclass implementing such interface for the engines of interest.

#### Migration from `HoodieRecordPayload` to `HoodieRecordMerger`

To warrant backward-compatibility (BWC) on the code-level with already created subclasses of `HoodieRecordPayload` currently
already used in production by Hudi users, we will provide a BWC-bridge in the form of instance of `HoodieRecordMerger` called `HoodieAvroRecordMerger`, that will 
be using user-defined subclass of `HoodieRecordPayload` to combine the records.

Leveraging such bridge will provide for seamless BWC migration to the 0.11 release, however will be removing the performance 
benefit of this refactoring, since it would unavoidably have to perform conversion to intermediate representation (Avro). To realize
full-suite of benefits of this refactoring, users will have to migrate their merging logic out of `HoodieRecordPayload` subclass and into
new `HoodieRecordMerger` implementation.

Precombine is used to merge records from logs or incoming records; CombineAndGetUpdateValue is used to merge record from log file and record from base file.
these two merge logics are unified in HoodieAvroRecordMerger as merge function. `HoodieAvroRecordMerger`'s API will look like following:

```java
/**
 * Backward compatibility HoodieRecordPayload implementation 
 */
class HoodieAvroRecordMerger implements HoodieRecordMerger {

   @Override
   public String getMergingStrategy() {
      return UUID_MERGER_STRATEGY;
   }
  
   @Override
   Option<HoodieRecord> merge(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
      // HoodieAvroRecordMerger precombine and combineAndGetUpdateValue. It'd be associative operation.
   }

   @Override
   HoodieRecordType getRecordType() {
      return HoodieRecordType.AVRO;
   }
}
```

### Refactoring Flows Directly Interacting w/ Records:

As was called out prior to achieve the goal of being able to sustain engine-internal representations being held by `HoodieRecord` 
class w/o compromising major components' neutrality (ie being engine-agnostic), such components directly interacting w/
records' payloads today will have to be refactored to instead interact w/ standardized `HoodieRecord`s API.

Following major components will be refactored:

1. `HoodieWriteHandle`s will be  
   1. Accepting `HoodieRecord` instead of raw Avro payload (avoiding Avro conversion)
   2. Using Record Merge API to merge records (when necessary) 
   3. Passes `HoodieRecord` as is to `FileWriter`
2. `HoodieFileWriter`s will be 
   1. Accepting `HoodieRecord`
   2. Will be engine-specific (so that they're able to handle internal record representation)
3. `HoodieRealtimeRecordReader`s 
   1. API will be returning opaque `HoodieRecord` instead of raw Avro payload

### Config for RecordMerger
The RecordMerger is engine-aware. We provide a config called MERGER_IMPLS. You can set a list of RecordMerger class name to it. And you can set MERGER_STRATEGY which is UUID of RecordMerger. Hudi will pick RecordMergers in MERGER_IMPLS which has the same MERGER_STRATEGY according to the engine type at runtime.

### Public Api in HoodieRecord
Because we implement different types of records, we need to implement functionality similar to AvroUtils in HoodieRecord for different data(avro, InternalRow, RowData).
Its public API will look like following:

```java
import java.util.Properties;

class HoodieRecord {

   /**
    * Get column in record to support RDDCustomColumnsSortPartitioner
    */
   ComparableList getComparableColumnValues(Schema recordSchema, String[] columns,
           boolean consistentLogicalTimestampEnabled);

   /**
    * Support bootstrap.
    */
   HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) throws IOException;

   /**
    * Rewrite record into new schema(add meta columns)
    */
   HoodieRecord rewriteRecord(Schema recordSchema, Properties props, Schema targetSchema)
           throws IOException;

   /**
    * Support schema evolution.
    */
   HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema,
           Map<String, String> renameCols) throws IOException;

   HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema) throws IOException;

   HoodieRecord updateMetadataValues(Schema recordSchema, Properties props,
           MetadataValues metadataValues) throws IOException;

   boolean isDelete(Schema recordSchema, Properties props) throws IOException;

   /**
    * Is EmptyRecord. Generated by ExpressionPayload.
    */
   boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException;

   Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema schema, Properties props)
           throws IOException;

   // Other functions with getter or setter ...
}
```

## Rollout/Adoption Plan

 - What impact (if any) will there be on existing users? 
   - Users of the Hudi will observe considerably better performance for most of the routine operations: writing, reading, compaction, clustering, etc due to avoiding the superfluous intermediate de-/serialization penalty
   - By default, modified hierarchy would still leverage 
   - Users will need to rebase their logic of combining records by creating a subclass of `HoodieRecordPayload`, and instead subclass newly created interface `HoodieRecordMerger` to get full-suite of performance benefits 
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
