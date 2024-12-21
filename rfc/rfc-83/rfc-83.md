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
# RFC-83: Incremental Table Service

## Proposers

- @zhangyue19921010

## Approvers
- @danny0405
- @yuzhaojing

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-8780

## Abstract

In Hudi, when scheduling Compaction and Clustering, the default behavior is to scan all partitions under the current table. 
When there are many historical partitions, such as 640,000 in our production environment, this scanning and planning operation becomes very inefficient. 
For Flink, it often leads to checkpoint timeouts, resulting in data delays.
As for cleaning, we already have the ability to do cleaning for incremental partitions.

This RFC will draw on the design of Incremental Clean to generalize the capability of processing incremental partitions to all table services, such as Clustering and Compaction.

## Background

`earliestInstantToRetain` in clean plan meta

HoodieCleanerPlan.avsc

```text
{
  "namespace": "org.apache.hudi.avro.model",
  "type": "record",
  "name": "HoodieCleanerPlan",
  "fields": [
    {
      "name": "earliestInstantToRetain",
      "type":["null", {
        "type": "record",
        "name": "HoodieActionInstant",
        "fields": [
          {
            "name": "timestamp",
            "type": "string"
          },
          {
            "name": "action",
            "type": "string"
          },
          {
            "name": "state",
            "type": "string"
          }
        ]
      }],
      "default" : null
    },
    xxxx
  ]
}
```

`EarliestCommitToRetan` in clean commit meta

HoodieCleanMetadata.avsc

```text
{"namespace": "org.apache.hudi.avro.model",
 "type": "record",
 "name": "HoodieCleanMetadata",
 "fields": [
     xxxx,
     {"name": "earliestCommitToRetain", "type": "string"},
     xxxx
 ]
}
```
How to get incremental partitions during cleaning

![cleanIncrementalpartitions.png](cleanIncrementalpartitions.png)

**Note**
`EarliestCommitToRetain` is recorded in `HoodieCleanMetadata`
newInstantToRetain is computed based on Clean configs such as `hoodie.clean.commits.retained` and will be record in clean meta as new EarliestCommitToRetain

## Design And Implementation
### Abstraction
Use `IncrementalPartitionAwareStrategy` to control the behavior of getting partitions, filter partitions, also incremental getting partitions if necessary etc.

```java
package org.apache.hudi.table;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.List;

public interface IncrementalPartitionAwareStrategy {

  /**
   * Get partition paths to be performed for current table service.
   * @param metaClient
   * @return
   */
  List<String> getPartitionPaths(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient, HoodieEngineContext engineContext);

  /**
   * Filter partition path for given fully paths.
   * @param metaClient
   * @return
   */
  List<String> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> partitionPaths);

  /**
   * Get incremental partitions from EarliestCommitToRetain to instantToRetain
   * @param instantToRetain
   * @param type
   * @param deleteEmptyCommit
   * @return
   * @throws IOException
   */
  List<String> getIncrementalPartitionPaths(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient);
}

```
Any Strategy implement this `IncrementalPartitionAwareStrategy` could have the ability to perform incremental partitions processing

**For Incremental Table Service including clustering and compaction, we will support a new IncrementalPartitionAwareCompactionStrategy and 
new IncrementalPartitionAwareClusteringPlanStrategy** to do incremental clustering/compaction data processing.


### Work Flow for Incremental Clustering/Compaction Strategy

Table Service Planner with Incremental Clustering/Compaction Strategy

1. Retrieve the instant request time based on the last completed table service commit as **INSTANT 1**.
2. Calculate the current instant(Request time) to be processed as **INSTANT 2**.
3. Obtain all partitions involved from **INSTANT 1** to **INSTANT 2** as incremental partitions and perform the table service plan operation.


### About archive

We need to use the request time of the last completed Table Service commit as a time window with the current latest data write commit time to 
obtain all incremental partitions with data written in the window.
Then once the last completed Table Service Commit being archived, it may cause problems such as incomplete incremental partitions.
There are two solutions for this:
1. Modify the logic of the archive service, that is, when the incremental partition function is turned on, always ensure that there is a completed table service commit in the active timeline
2. Without modifying the archive logic, add a cover-up design in obtaining incremental partitions. That is, if the last completed table service commit is not obtained, we will fallback tp obtain the full table partitions.

## Rollout/Adoption Plan

low impact for current users

## Test Plan