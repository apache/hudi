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
### Common Configuration
- Add a new configuration item for the HoodieLogBlock streaming read buffer size. The default value is 10MB.
- Add a new configuration item for whether to enable MergeSort in LogCompaction. The default value for Flink is true, while the default value for Spark is false.
#### Flink
- Add a new configuration item to enable LogCompaction. The default value is false.
#### Note
1. LogCompaction with Merge Sort only supports AVRO log format. After LogCompaction turns on MergeSort, we need to check whether the hoodie.logfile.data.block.format configuration item is correct.
2. After enabling MergeSort in LogCompaction, it does not guarantee that LogCompaction will necessarily use Merge Sort based merge. If it is found during execution that the IS_ORDERED in a LogBlock header is false (indicating that the LogBlock data is unordered), it will fall back to the default map-based merger.

### LogBlock Header
- HeaderMetadataType: Add a new enumeration type: IS_ORDERED, indicating whether the data in the LogBlock is ordered. The default value is false.

### DeltaCommit
When LogCompaction turns on MergeSort, DeltaCommit sorts the written data by RecordKey to achieve orderly records in LogBlock, and sets the LogBlock header's IS_ORDERED to true.

### LogCompaction
The following contents are all about enabling MergeSort with LogCompaction.
#### Flink
Flink has not yet fully implemented the LogCompaction feature, so the operator needs to be implemented:
1. LogCompactionPlanOperator
2. LogCompactionOperator
3. LogCompactionCommitSink
#### Spark & Flink
Considering that when enabling LogCompaction with Merge Sort on the historical table or when multiple writers are involved and one of the writer does not enable Merge Sort, it may result in some LogBlock data being unordered and not meeting the conditions for executing MergeSort.
Therefore, during the execution of the LogCompaction Operation, the HoodieUnMergedLogRecordScanner with skipProcessingBlocks will be used to check the IS_ORDERED header of all LogBlock files in that Operation. If not all LogBlocks are ordered, it will fall back to the map-based merger, HoodieMergedLogRecordScanner.
Otherwise, use the new log scanner: HoodieMergeSortLogRecordScanner to achieve N-way streaming record merging.

### HoodieMergeSortLogRecordScanner
Implement a min-heap. The heap node is a HoodieLogBlock object. The heap node comparison uses HoodieRecord#RecrodKey.
When traversing the scanner record iterator, return to the top node of the heap and call the HoodieLogBlock object to return the record.

### HoodieDataBlock
Add a new streaming read abstract method to help HoodieMergeSortLogRecordScanner avoid OOM when reading N LogBlocks.
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

## Local Performance Test Results
### Conditions
1. Machine Configuration and Environment: 7 vCore & 36GB, Flink 1.5, Hudi 0.15
2. Table Configuration: Flink MOR with Bucket; Compaction disabled, only LogCompaction enabled; Flink Checkpoint Interval 60s
3. Data Volume: 300GB added daily (AVRO format)

### Comparison Objects
- Table A: LogCompaction with Map-Based Merger
- Table B: LogCompaction with MergeSort-Based Merger

### Results
Data freshness statistics for Table A and Table B:
- Table A: 55 minutes
- Table B: 20 minutes

## Test Plan
### HoodieConfig etc
Check whether the configuration items are correct and whether the dependent configuration items are correct.

### DeltaCommit
Check if the incremental LogBlock is ordered by RecordKey.

### LogCompaction
#### Flink
1. Check whether the logic of LogCompactionPlanOperator/LogCompactionOperator/LogCompactionCommitSink operators is correct.
2. Integration test to check whether the results are successfully deduplicated.
3. When some LogBlock data is unordered (i.e., the IS_ORDERED in the LogBlock header is false), check whether the results are successfully deduplicated.
#### Spark
1. Integration test to check whether the results are successfully deduplicated.
2. When some LogBlock data is unordered (i.e., the IS_ORDERED in the LogBlock header is false), check whether the results are successfully deduplicated.
### HoodieMergeSortLogRecordScanner
1. Check whether the logic of the min-heap is correct (construction/reading/number of heap nodes).
2. Check whether the records returned by the scanner are in order by RecordKey.

### HoodieDataBlock
Check whether the streaming read logic is correct (decoding AVRO/buffer).
