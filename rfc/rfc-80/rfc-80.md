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
# RFC-80: Support column groups for wide tables

## Proposers  
- @xiarixiaoyao
- @wombatu-kun

## Approvers
 - @vinothchandar
 - @danny0405
 - @beyond1920

## Status

Issue: https://github.com/apache/hudi/issues/13922

## Abstract

In streaming processing, there are often scenarios where the table is widened. The current mainstream real-time wide table concatenation is completed through Flink's multi-layer join;
Flink's join will cache a large amount of data in the state backend. As the data set increases, the pressure on the Flink task state backend will gradually increase, and may even become unavailable.
In multi-layer join scenarios, this problem is more obvious.  
1.x also supports partial updates being encoded in logfiles. That should be able to handle this scenario. But even with partial-update, the column groups will reduce write amplification on compaction.  

So, main gains of clustering columns for wide tables are:  
Write performance:
- Writing is similar to ordinary bucket writing, but it involves splitting column clusters and sorting. Therefore, the writing performance of full data is 10% lower than that of native bucket writing.
- However, if only some columns are updated among a large number of columns, the writing efficiency is much faster than that of non-column clustered tables. (Note: 1.0 introduces a partial update functionality, that can also avoid such writing costs, by writing only the changed columns)  

Read performance:  
Since the data is already sorted when it is written, the SortMerge method can be used directly to merge the data; compared with the native bucket data reading performance is improved a lot, and the memory consumption is reduced significantly.  

Compaction performance:  
The logic of compaction and reading is the same. Compaction costs across column groups is where there real savings are.  

The log merge we can make it pluggable to decide between hash or sort merge - we need to introduce new log headers or standard mechanism for merging to determine if base file or log files are sorted.

## Background
Currently, Hudi organizes data according to fileGroup granularity. The fileGroup is further divided into column clusters to introduce the columngroup concept.  
The organizational form of Hudi files is divided according to the following rules:  
The data in the partition is divided into buckets according to hash (each bucket maps to a file group); the files in each bucket are divided according to columngroup; multiple colgroup files in the bucket form a completed fileGroup; when there is only one columngroup, it degenerates into the native Hudi bucket table.

![table](table.png)

## Implementation
This feature should be implemented for both Spark and Flink. So, a table written by Flink this way, also can be read by Spark.

### Constraints and Restrictions
1. The overall design relies on the non-blocking concurrent writing feature of Hudi 1.0.  
2. Lower version Hudi cannot read and write column group tables.  
3. Only MOR bucketed tables support setting column groups.  
   MOR+Bucket is more suitable because it has higher write performance, but this does not mean that column group is incompatible with other indexes and cow tables.  
4. Column groups do not support repartitioning and renaming.  
5. Schema evolution does not take effect on the current column group table.  
   Not supporting Schema evolution does not mean users can not add/delete columns in their table, they just need to do it explicitly.
6. Like native bucket tables, clustering operations are not supported.

### Model change
After the column group is introduced, the storage structure of the entire Hudi bucket table changes:

![bucket](bucket.png)

The bucket is divided into multiple columngroups by column cluster. When columngroup is 1, it will automatically degenerate into the native bucket table.

![file-group](file-group.png)

### Proposed Storage Format Changes
After splitting the fileGroup by columngroup, the naming rules for base files and log files change. We add the cfName suffix at the end of all file names to facilitate Hudi itself to distinguish column groups. If it's not present, we assume default column group.
So, new file name templates will be as follows:  
- Base file: [file_id]\_[write_token]\_[begin_time][_cfName].[extension]  
- Log file: [file_id]\_[begin_instant_time][_cfName].log.[version]_[write_token]  

Also, we should evolve the metadata table files schema to additionally track a column group name.  

### Specifying column groups when creating a table
In the table creation statement, column group division is specified in the options/tblproperties attribute;
Column group attributes are specified in key-value mode:  
* Key is the column group name. Format: hoodie.colgroup. Column group name    naming rules specified.  
* Value is the specific content of the column group: it consists of all the columns included in the column group plus the preCombine field. Format: " col1,col2...colN; precombineCol", the column group list and the preCombine field are separated by ";"; in the column group list the columns are split by ",".  

Constraints: The column group list must contain the primary key, and columns contained in different column groups cannot overlap except for the primary key. The preCombine field does not need to be specified. If it is not specified, the primary key will be taken by default.

After the table is created, the column group attributes will be persisted to hoodie's metadata for subsequent use.

### Adding and deleting column groups in existing table
Use the SQL alter command to modify the column group attributes and persist it:    
* Execute ALTER TABLE table_name SET TBLPROPERTIES ('hoodie.columngroup.k'='a,b,c;a'); to add a new column group.  
* Execute ALTER TABLE table_name UNSET TBLPROPERTIES('hoodie.columngroup.k'); to delete the column group.

Specific steps are as follows:
1. Execute the ALTER command to modify the column group
2. Verify whether the column group modified by alter is legal. Column group modification must meet the following conditions, otherwise the verification will not pass:
    * The column group name of an existing column group cannot be modified.  
    * Columns in other column groups cannot be divided into new column groups.  
    * When creating a new column group, it must meet the format requirements from previous chapter.  
3. Save the modified column group to the .hoodie directory.

### Writing data
The Hudi kernel divides the input data according to column groups; the data belonging to a certain column group is sorted and directly written to the corresponding column group log file.

![process-write](process-write.png)

Specific steps:  
1. The engine divides the written data into buckets according to hash and shuffles the data (the writing engine completes it by itself and is consistent with the current writing of the native bucket).  
2. The Hudi kernel sorts the data to be written to each bucket by primary key (both Spark and Flink has its own ExternalSorter, we can refer those ExternalSorter to finish sort).  
3. After sorting, split the data into column groups.  
4. Write the segmented data into the log file of the corresponding column group.  

#### Common API interface
After the table columns are clustered, the writing process includes the process of sorting and splitting the data compared to the original bucket bucketing. A new append interface needs to be introduced to support column groups.  
Introduce ColumngroupAppendHandle extend AppendHandle to implement column group writing.

![append-handle](append-handle.png)

### Reading data
#### ColumngroupReader and RowReader
![row-reader](row-reader.png)

Hudi internal row reader reading steps:  
1. Hudi organizes files by column groups to be read.
2. Introduce groupReader to merge and read each column group's own baseFile and logfile to achieve column group-level data reading.  
    * Since log files are written after being sorted by primary key, groupReader merges its own baseFile and logFile by primary key using sortMerge.
    * groupReader supports upstream incoming column pruning to reduce IO overhead when reading data.  
    * During the merging process, if the user specifies the precombie field for the column group, the merging strategy will be selected based on the precombie field. This logic reuses Hudi's own precombine logic and does not need to be modified.    
3. Row reader merges the data read by multiple groupReaders according to the primary key.  

Since the data read by each groupReader is sorted by the primary key, the row reader merges the data read by each groupReader in the form of sortMergeJoin and returns the complete data.  

The entire reading process involves a large amount of data merging, but because the data itself is sorted, the memory consumption of the entire merging process is very low and the merging is fast. Compared with Hudi's native merging method, the memory pressure and the merging time are significantly reduced.

#### Engine reads pseudo process
![process-read](process-read.png)

1) The engine itself delivers the data files that need to be scanned to executor/woker/taskmanger.  
2) executor/worker/taskmanger calls Hudi’s rowReader interface and passes in column clipping and filter conditions to rowReader.  
3) The Hudi kernel completes the data reading of rowReader and returns complete data. The data format is Avro.  
4) The engine gets the Avro format data and needs to convert it into the data format it needs. For example, spark needs to be converted into unsaferow, hetu into block, flink into row, and hive into arrayWritable.

### Column group level compaction
Extend Hudi's compaction schedule module to merge each column group's own base file and log file:

![group-compaction](group-compaction.png)

### Full compaction
Extend Hudi's compaction schedule module to merge and update all column groups in the entire table.  
After merging at the column group level, multiple column groups are finally merged into a complete row and saved.  

![grand-compaction](grand-compaction.png)

Full compaction will be optional, only column group level compaction is required.  
Different users might need different columns, some might need columns come from multiple column groups, some might need columns from only one column group.
It's better to allow users to choose whether enable full compaction or not.  
Besides, after full compaction, projection on the reader side is less efficient because the projection could only be based on the full parquet file with all complete fields instead of based on column group names.    

Columngroup can be used not only in ML scenarios and AI feature table, but also can be used to simulate multi-stream join and concatenation of wide tables. 。
In simulation of multi-stream join scenario, Hudi should produce complete rows, so full compaction is needed in this case.

## Rollout/Adoption Plan

This feature itself is a brand-new feature. If you don’t actively turn it on, you will not be able to reach the logic of the column groups.    
Business behavior compatibility: No impact, this function will not be actively turned on, and column group logic will not be enabled.  
Syntax compatibility: No impact, the column group attributes are in the table attributes and are executed through SQL standard syntax.  
Compatibility of data type processing methods: No impact, this design will not modify the bottom-level data field format.  

## Test Plan
List to check that the implementation works as expected:  
1. All existing tests pass successfully on tables without column groups defined.  
2. Hudi SQL supports setting column groups when creating MOR bucketed tables.  
3. Column groups support adding and deleting in SQL for MOR bucketed tables.  
4. Hudi supports writing data by column groups.  
5. Hudi supports reading data by column groups.  
7. Hudi supports compaction by column group.  
8. Hudi supports full compaction, merging the data of all column groups to achieve data widening.  


## Potential Approaches 

### Approach A: Column Groups under File Group

We map each record key to a single file group, consistently across column groups. Each column group has file slices,
like we do today in file groups. How columns are split into column groups is fluid and can be different across file groups.



```
records 1-25 ==> file group 1 ==> [column group : c1-c10], [column group : c11-c74], [column group : c75-c100]

records 26-50 ==> file group 2 ==> [column group : c1-c40], [column group : c41-c100]

records 51-100 ==> file group 3 ==> [column group : c1-c20], [column group : c21-c60], [column group : c61-c80], [column group : c81-c100] 

```
_Layout for table with 100 records, 100 columns_

**Indexing**: works as is, since the mapping from key to file-group is intact. 

**Cleaning**: Column groups can be updated at different rates. i.e. one column group can receive more updates than others. 
To retain versions belonging to the last `x` writes, each column group can simply enforce retention on its own file slices, 
like today. Should work since its based off the same timeline anyway. 

**Queries**: Time-travel / Snapshot queries should work as-is, filtering each column group like a normal file group today, just reading the
columns in the projection/filter from the right column group. CDC / Incremental queries can work again by reconciling commit time across column groups.
(Column-level change tracking is a separate problem.)

**Compaction**: Works on column groups based on existing strategies. We may need to add a few different strategies for tables with blob columns. 

**Clustering**: This is where we take a hit. Even when clustering across only a few column groups, we may need to rewrite all columns to preserve the 
file-group -> column group hierarchy. Otherwise, some columns of a record may be in one file group while others are in another if clustering created new file groups.
⸻

### Approach B: File Groups under Column Groups 
We treat column groups as separate virtual tables sharing the same timeline. But this needs pre-splitting columns into groups at the table level, losing flexibility to evolve the table. 
Managing different ways combinations of columns split across records may be overwhelming.

```
columns 1-25 ==> column group 1 ==> [file group : c1-c10], [file group : c11-c74], [file group : c75-c100]

columns 26-50 ==> column group 2 ==> [file group : c1-c40], [file group : c41-c100]

columns 51-100 ==> column group 3 ==> [file group : c1-c20], [file group : c21-c60], [file group : c61-c80], [file group : c81-c100] 

```
_Layout for table with 100 records, 100 columns_


**Indexing**: RLI (Record Location Index) needs to track multiple positions per record key, since it can be in different file groups in each column group.

**Cleaning**: Achieved independently by each virtual table, enforcing cleaner retention.

**Queries**: CDC / Incremental/Snapshot / Time-travel queries are all UNIONs over query results from relevant column groups.

**Compaction**: works like Approach A.

**Clustering**: each column group can be clustered independently, without causing any write amplification.

This "virtual table" abstraction can enable other cool things e.g. Materializing the same data in multiple ways.


