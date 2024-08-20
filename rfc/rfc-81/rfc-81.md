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
# RFC-81: Log Compaction with Merge Sort

## Proposers
- @usberkeley

## Approvers
- @danny0405

## Status
JIRA: https://issues.apache.org/jira/browse/HUDI-8033

## Abstract
Add lightweight LogCompaction to improve the writing performance of the write side, and improve the query performance of the read side (Spark/Presto, etc.) in some scenarios without having to wait for heavy and time-consuming operations such as Compaction or Clustering.

## Background
The previous LogCompaction mainly merged log files through HoodieMergedLogRecordScanner, and used ExternalSpillableMap internally to achieve record merging, which resulted in performance loss of writing to disk, when the amount of log data exceeds merge memory available.
LogCompaction with Merge Sort is introduced to achieve lightweight minor compaction by merging records through N-way streaming of ordered data, thus improving the writing performance of the write side. At the same time, thanks to the ordered data, the query performance on the read side can be improved when the primary key is met.

## Implementation
### HoodieConfig etc
Added a new configuration item for the HoodieLogBlock streaming read buffer size. The default value is 10MB.
#### Flink
Added a new configuration item to enable LogCompaction. The default value is false.
Note:
1. After Flink enables LogCompaction, the default implementation is MergeSort.
2. Currently, the log format only supports AVRO. After enabling LogCompaction, need to check whether the hoodie.logfile.data.block.format configuration item is correct.

#### Spark
Added a new configuration item for whether to enable MergeSort in LogCompaction. The default value is false.
Note:
1. Currently, the log format only supports AVRO. After LogCompaction turns on MergeSort, you need to check whether the hoodie.logfile.data.block.format configuration item is correct.

### DeltaCommit
#### Flink
When LogCompaction is enabled, DeltaCommit sorts the written data by RecordKey to achieve orderly records in LogBlock.
#### Spark
When LogCompaction turns on MergeSort, DeltaCommit sorts the written data by RecordKey to achieve orderly records in LogBlock.

### LogCompaction
The following contents are all about enabling LogCompaction or enabling MergeSort with LogCompaction.
#### Flink
Flink has not yet fully implemented the LogCompaction feature, so the operator needs to be implemented:
1. LogCompactionPlanOperator
2. LogCompactionOperator
3. LogCompactionCommitSink

When scanning log files, use the new log scanner: HoodieMergeSortLogRecordScanner to achieve N-way streaming record merging.

#### Spark
When scanning log files, use the new log scanner: HoodieMergeSortLogRecordScanner to achieve N-way streaming record merging.

### HoodieMergeSortLogRecordScanner
Implement a min-heap. The heap node is a HoodieLogBlock object. The heap node comparison uses HoodieRecord#RecrodKey.
When traversing the scanner record iterator, return to the top node of the heap and call the HoodieLogBlock object to return the record.

### HoodieDataBlock
Added a new streaming read abstract method to help HoodieMergeSortLogRecordScanner avoid OOM when reading N LogBlocks.
1. HoodieAvroDataBlock: Implements the streaming read member function. Internally, it gradually reads small parts of the log file into the buffer to implement streaming decoding of AVRO records. When the buffer is insufficient, it triggers the reading of the log file. 
The buffer size can be adjusted by the configuration item.
2. HFile/Parquet: To be implemented in the future.

## Rollout/Adoption Plan
- What impact (if any) will there be on existing users?
1. Flink: Since Flink did not have LogCompaction before, enabling LogCompaction may reduce write performance, which requires performance testing to determine if there is an impact and the extent of the impact.
2. Spark: None
- If we are changing behavior how will we phase out the older behavior?
  None
- If we need special migration tools, describe them here.
  None
- When will we remove the existing behavior.
  None

## Test Plan
### HoodieConfig etc
Check whether the configuration items are correct and whether the dependent configuration items are correct.
### DeltaCommit
Check if the incremental LogBlock is ordered by RecordKey.

### LogCompaction
#### Flink
1. Check whether the logic of LogCompactionPlanOperator/LogCompactionOperator/LogCompactionCommitSink operators is correct.
2. Integration test to check whether the results are successfully deduplicated.

#### Spark
Integration test to check whether the results are successfully deduplicated.

### HoodieMergeSortLogRecordScanner
1. Check whether the logic of the min-heap is correct (construction/reading/number of heap nodes).
2. Check whether the records returned by the scanner are in order by RecordKey.

### HoodieDataBlock
Check whether the streaming read logic is correct (decoding AVRO/buffer).
