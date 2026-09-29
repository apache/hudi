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
# RFC-70: Hudi Reverse Streamer

## Proposers

* @pratyakshsharma

## Approvers

* 

## Status
UNDER REVIEW

JIRA: https://issues.apache.org/jira/browse/HUDI-6425

> Please keep the status updated in `rfc/README.md`.

## Abstract

This RFC proposes the design and implementation of `HoodieReverseStreamer`, a tool which has the exact opposite behaviour of more popular `HoodieDeltaStreamer` tool. 
HoodieDeltaStreamer allows users to stream data incrementally into hudi tables from varied sources like kafka, DFS. HudiReverseStreamer will allow users to stream data back from Hudi to various sinks like kafka, DFS etc. 

## Background

Apache Hudi currently supports multiple ways of writing data into hudi tables and migrating existing data into hudi format. These ways include spark datasource, delta streamer, flink streamer and the lower level HoodieWriteClient. All of these cover pretty much all the common workloads, common sources and use cases. Hudi is the pioneer of the lakehouse term, and lakehouse term is nothing but the combination of data lakes and data warehouses. Data warehouses are generally created to solve a particular business use case and are served as a separate database. A typical data pipeline for creating data warehouse involves the extraction of data from primary relational databases, transforming the data and then serving it as a specialized database. 
Essentially this involved some tool (for example, debezium + secor or apache sqoop) to extract raw data from relational database and then some kind of transformation to prepare derived data.
Hudi has been a popular choice for extracting and transforming the data, but it does not allow users to move the data back to some other database for serving to end users. With this RFC, we propose a new way of creating data pipelines where HoodieDeltaStreamer + Transformer is used for preparing the derived data and at a later stage, HoodieReverseStreamer can be used for writing this data to a desired location for end user consumption.


## Implementation

On a high level, the changes for this new tool can be divided into below categories - writer, timeline metadata.

### Writer Flow
A new class `HoodieReverseStreamer` will be introduced in hudi-utilities package. This essentially is responsible for doing the following actions - 

- Can read the entire data from hudi table in case no checkpoint is provided, else read from the last checkpoint. Option to manually overwrite the checkpoint to resume from will be provided. Checkpoint information will be maintained in a new timeline action `reverseCommit`. Code from HiveIncrementalPuller can probably be reused for this step. Also since checkpoints are involved, this will only be supported via spark-submit job and not spark datasource. This is inline with current DeltaStreamer support.
- The records will be processed to remove hoodie specific metadata columns
- Converted into sink specific format and perform any serialization needed.
- Write to sink

This does not need any sort of schema provider since all the necessary checks have been performed when the data was written into hudi table.

```java
public class HoodieReverseStreamer {
    
    void readFromSource();
    void writeToSink();
}
```
A new interface needs to be introduced -

```java
public interface HoodieSink {
    
    void pushToSink(List<HoodieRecord> records);
}
```

where pushToSink() is internally called from writeToSink().

### Schema Evolution
If the writes happened with DeltaStreamer then schema is always backwards compatible. But with spark datasource, spark sql full schema evolution is supported with Hudi. 

**TODO**: Can there be some case where schema evolution possesses issues when writing to sink?

### Timeline Changes

A new action called `reversecommit` will be introduced. The metadata for this action primarily consists of the following - 
1. checkpoint (stored as part of extra metadata)
2. successWrites
3. totalWrites
4. failedWrites
5. totalWriteBytes
6. path
7. partitionPath (can be defined according to sink as discussed below)

Combination of path and partitionPath essentially gives the target information where the data is written in sink. For kafka, this is a combination of topic (path) and partition (partitionPath). For DFS, we have base path as the path and hive style partition as partitionPath. 
For relational sinks like mysql, postgresql, we propose to have database name as the path and table name as the partitionPath.

Below is the schema for reversecommit metadata - 

```
{
   "namespace":"org.apache.hudi.avro.model",
   "type":"record",
   "name":"HoodieReverseCommitMetadata",
   "fields":[
      {
         "name":"partitionToWriteStats",
         "type":["null", {
            "type":"map",
            "values":{
               "type":"array",
               "items":{
                  "name":"HoodieSinkWriteStat",
                  "type":"record",
                  "fields":[
                     {
                        "name":"fileName",
                        "type":["null","string"],
                        "default" : null
                     },
                     {
                        "name":"path",
                        "type":["null","string"],
                        "default" : null
                     },
                     {
                        "name":"numWrites",
                        "type":["null","long"],
                        "default" : null
                     },
                     {
                        "name":"numDeletes",
                        "type":["null","long"],
                        "default" : null
                     },
                     {
                        "name":"numUpserts",
                        "type":["null","long"],
                        "default" : null
                     },
                     {
                        "name":"totalWriteBytes",
                        "type":["null","long"],
                        "default" : null
                     },
                     {
                        "name":"partitionPath",
                        "type":["null","string"],
                        "default" : null
                     }
                  ]
               }
            }
         }],
         "default": null
      },
      {
         "name":"extraMetadata",
         "type":["null", {
            "type":"map",
            "values":"string",
            "default": null
         }],
         "default": null
      }
   ]
}
```

## Rollout/Adoption Plan

- HoodieReverseStreamer V1 can support kafka as a sink. Others can be added later as per use case.
- V1 will only have one time write support on trigger. Continuous support with async service can be added later.
- V1 only has this support via spark engine.

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.