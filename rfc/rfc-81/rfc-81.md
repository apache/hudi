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
# RFC-81: Hudi Data Ordering

## Proposers
- @usberkeley

## Approvers
- @danny0405

## Status
JIRA: https://issues.apache.org/jira/browse/HUDI-8033

## Abstract
This RFC introduces the functionality of sorting log blocks and base file, allowing records to be sorted by primary key. 
This enables a merge-sort based read API and compaction to improve read and write performance. It mainly includes two modules:

1. Merge-sort based compaction
2. Merge-sort based scanner
Additionally, this RFC will assist Hudi MDT in building a primary key index similar to ClickHouse, providing multi-dimensional analysis capabilities for the read side.

## Background
Currently, the unordered nature of Hudi data affects data processing and read efficiency. During data compaction and when reading data through Spark and Trino, an ExternalSpillableMap is needed to merge records. 
This lack of order also reduces the efficiency of predicate pushdown on the read side.

### Compaction and Reader (Spark/Trino) Reading Process
During compaction and when a reader reads a Hudi table, concurrent tasks are divided based on file groups. Each task reads the base file and incremental log files of the file group. 
These records are stored in an ExternalSpillableMap, and if duplicate primary keys exist, record merging is required.

### Relationship Between Data Order and Read Efficiency
When Parquet data is ordered, the page statistics are more accurate and effective. Because the data is sorted by a certain key, the minimum and maximum values of the pages can better define the data range. 
Readers can use this information to determine which pages do not contain the required data and skip them. 
This optimization requires that the reader's predicate is one of the composite primary key dimension columns.

### Relationship Between Data Order and Storage Size
Ordered Parquet data makes RLE encoding more efficient. By clustering similar or repeated data, the size of data files can be significantly reduced. 
Reducing the size of base files also improves the efficiency of reading them.
Take the example of a loan transaction table for testing, where the primary key dimensions are user_id and an auto-generated id. 
Each user has an average of 15 records. After sorting, the storage space is reduced by 23.08%, and if scanning the files, the read volume can also be reduced by 23.08% (improving read efficiency).

### Merge Sort Based Merger
After sorting the data, we introduce a merge-sort based merger, changing the original compaction and file group reading process into a streaming process. 
The specific steps are as follows:

1. Construct a min-heap, where the nodes consist of input streams from base file or log blocks in log files. Pre-read 10MB of records from each input stream into a buffer.
2. Traverse the min-heap and merge records with the same primary key.
3. When a heap node's buffer is empty, read the next 10MB of records from the input stream to refill the buffer until reaching the end of the file. 
   If the end of the file is reached, remove the node from the min-heap.

Since the merge-sort based merger is streaming and memory-friendly, it eliminates the need for intermediate storage, addressing some issues caused by the ExternalSpillableMap:
1. Reduces IO overhead caused by memory overflow.
2. Avoids excessive memory usage that can occur when increasing maximum memory size to reduce IO overhead, which is correlated with the number of file groups.

Note: [Log Block Streaming Read PR](https://github.com/apache/hudi/pull/11924#issuecomment-2343958178)

## Implementation
### Base
#### Parameter Configuration
- `compaction_log_streaming_read_buffer`: The default value is 10MB.
- `compaction_with_merge_sort_enable`: The default value is false.

#### Log Block Header
- IS_ORDERED: Indicating whether the data in the LogBlock is ordered. The default value is false.

#### Merge Sort Based Scanner
Scan base and log files in a file group and return an iterator of merged records. Typically invoked by readers like Spark/Trino and during compaction.
The scanner maintains a min-heap internally, where each node contains an input stream from a base or log blocks in log files, along with a corresponding record queue buffer. 
When building the heap, the node's input stream initially reads 10MB of record into the buffer. During heap adjustments, the node's priority is determined by the head record of the queue buffer.
Finally, the scanner iterator retrieves the top record from the min-heap, merges records with the same key, and returns the merged, primary-key-sorted records.

### Delta Commit
When `compaction_with_merge_sort_enable` is true, DeltaCommit sorts the written data by RecordKey to achieve orderly records in LogBlock, and sets the LogBlock header's IS_ORDERED to true.

### Compaction
Iterate over the record iterator returned by the merge-sort based scanner and write to the base file.

## Rollout/Adoption Plan
- What impact (if any) will there be on existing users?
  None
- If we are changing behavior how will we phase out the older behavior?
  None
- If we need special migration tools, describe them here.
  None
- When will we remove the existing behavior.
  None

## Performance Test Results
### Compaction
Flink MOR table, 7 VCore, 36 GB memory, flink checkpoint 10min, other configurations remain default.

| Type                        | Kafka Fully Consumed | Runtime | Records Consumed | Record Size   |
|-----------------------------|----------------------|---------|------------------|---------------|
| Map-based compaction        | No                   | 11.5h   | 226,250,649      | 752 GB        |
| Merge-sort based compaction | No                   | 11.5h   | 260,188,246      | 858 GB        |

### Log Compaction
Flink MOR table, 7 VCore, 36 GB memory, flink checkpoint 10min, other configurations remain default.

| Type                            | Kafka Fully Consumed | Runtime | Records Consumed | Record Size   |
|---------------------------------|----------------------|---------|------------------|---------------|
| Map-based log compaction        | No                   | 11.5h   | 147,995,817      | 494 GB        |
| Merge-sort based log compaction | No                   | 11.5h   | 170,726,805      | 573 GB        |

Noted:
1. Delta Commit + Log Compaction: This approach has no advantage. Log Compaction is slower than Compaction because the log data grows significantly, leading to increased read times for log files. 
   Log Compaction operates in append mode, which increases the log size with each execution. Additionally, AVRO files are five times larger than Parquet files in our test table.
2. Delta Commit + Compaction + Log Compaction: Log Compaction to merge Log Block fragments in order to improve the query speed for Readers, while test results show no significant impact on write performance consumption.
   This RFC does not include Log Compaction, because it primarily supports the MDT primary key index, so it is recommended to include it in the new RFC for the MDT primary key index.
   Reference [Balancing compute cost and query performance](https://docs.google.com/presentation/d/10hHkQsd0bCdCor4w9Wduds5l-i_Z7X7A-zk_vYYi4kA/edit#slide=id.p18)

### Sparse Index for MDT Primary Key
Although this RFC itself does not involve the construction of MDT primary key indexes, but it supports the implementation of MDT primary key indexes. We introduce the concept here.
Taking the order table "t_order" as an example to explain the MDT primary key index:

1. t_order
Data is sorted by the primary key (_hoodie_record_key). The result of executing the query `SELECT _hoodie_record_key, region, uid, order_count from t_order`:

| _hoodie_record_key | region | uid  | order_count |
|--------------------|--------|------|-------------|
| "gd,1001"          | gd     | 1001 | 1           |
| "gd,1002"          | gd     | 1002 | 1           |
| "gd,1003"          | gd     | 1003 | 1           |
| "gd,1004"          | gd     | 1004 | 1           |
| "sh,1005"          | sh     | 1005 | 1           |
| "sh,1006"          | sh     | 1006 | 1           |
| "sh,1007"          | sh     | 1007 | 1           |

2. Query side
On the query side, executing the SQL `SELECT region, uid, order_count FROM t_order WHERE region = 'gd' AND uid = '1003'` does not scan the entire table. 
Instead, it first queries the MDT table "t_primary_index".

3. t_primary_index
t_primary_index extracts one record from t_order every 3 rows. The content is as follows:

| _hoodie_record_key | log_file_offset | base_file_row_num |
|--------------------|-----------------|-------------------|
| "gd,1001"          | 100             | 1                 |
| "gd,1004"          | 900             | 4                 |
| "sh,1007"          | 3000            | 7                 |

4. Binary search index
By performing a binary search in t_primary_index for the condition `region = 'gd' AND uid = '1003'`, you can find the log file offset and base file row number range that meet the condition:

| _hoodie_record_key | log_file_offset | base_file_row_num |
|--------------------|-----------------|-------------------|
| "gd,1001"          | 100             | 1                 |
| "gd,1004"          | 900             | 4                 |

5. Scan target range
Therefore, the query API only needs to query within the specific range of the log file (100–900) and the specific rows of the base file (1–4).

Summary:
A sparse index is generated based on the table records, extracting one record every N rows. During querying, the sparse index is first used to determine the target data range, and then the log file and base file are queried.

#### query performance test results
1. The Parquet file size of the test table is 2TB. Indexing is only generated for Parquet files, not for AVRO logs. A self-developed query engine is used for indexing queries on Hudi.

| QPS | Success Rate | P99 Query Time (seconds) | Average Data Freshness (minutes) |
|-----|--------------|--------------------------|----------------------------------|
| 1   | 100%         | 2.2                      | 40                               |
| 10  | 100%         | 2.5                      | 40                               |
| 30  | 100%         | 3.0                      | 40                               |

2. The Parquet file size of the test table is 2TB. Primary key indexes are generated for both Parquet files and AVRO logs. A self-developed query engine is used for indexing queries on Hudi.

| QPS | Success Rate | P99 Query Time (seconds) | Average Data Freshness (minutes) |
|-----|--------------|--------------------------|----------------------------------|
| 1   | 100%         | 3.5                      | 7                                |
| 10  | 100%         | 4.3                      | 7                                |
| 30  | 100%         | 5.0                      | 7                                |

## Test Plan

### Merge Sort Based Scanner
1. Data Order Verification
2. Correctness of Data Deduplication and Merging
3. Boundary Condition Verification

### DeltaCommit
1. Data Order Verification

### Compaction
1. Data Order Verification
2. Correctness of Data Deduplication and Merging
3. Boundary Condition Verification
