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
# RFC-92: Support Bitmap Index

## Proposers

- @CTTY

## Approvers

- @yihua

## Status

JIRA: [HUDI-9048](https://issues.apache.org/jira/browse/HUDI-9048)

## Abstract

Apache Hudi is actively expanding its support for different indexing strategies to optimize query performance and data retrieval efficiency. 
However, bitmap indexes—widely recognized for their effectiveness in filtering low-cardinality columns—are not yet supported. 
This project proposes the integration of bitmap indexing into Hudi’s indexing framework. 
Bitmap indexes offer compact storage and fast bitwise operations, making them particularly well-suited for analytical workloads where predicates involve columns with a limited number of distinct values. 
By introducing bitmap index support, we aim to enhance Hudi’s performance for a broader set of query patterns, especially in use cases with filtering on categorical or boolean fields.




## Background

A bitmap index is a specialized indexing technique that enhances query performance, particularly for columns with low cardinality (few distinct values). 
Instead of storing row pointers like traditional indexes, it represents data as bitmaps, where each distinct value in a column has a corresponding bit vector indicating the presence of that value in different rows.
The main advantage of a bitmap index is its ability to perform fast bitwise operations, which allow for quick filtering and combination of multiple conditions.
![bitmap_example](./bitmap_example.png)
In Hudi, bitmap indexes can provide significant performance benefits by helping to skip unnecessary files during query execution. 
Since Hudi organizes data into files and partitions, a bitmap index can track which files contain relevant values, allowing the query engine to efficiently prune irrelevant files before scanning. 
Additionally, bitmap indexes enable bitmap joins, where bitwise operations quickly determine matching records across datasets without performing costly row-by-row comparisons.

One limitation is that Hudi currently lacks row-level skipping, so once a file group is selected, the entire file must be scanned. 
Even so, bitmap indexes are valuable, especially when multiple low-cardinality columns are indexed and commonly used in joins or filters. 
Bitwise operations on these indexes can efficiently narrow down relevant file groups. 
In the future, as Hudi adds row-level or row-group-level skipping, bitmap indexes will offer even greater performance gains by enabling precise row filtering within files.


## Design

### Storage Structure
Bitmap indexes for all columns will be stored in the `bitmap_index` partition of the table's Hudi metadata table. (`<table_path>/.hoodie/metadata/bitmap_index`)

To support bitmap indexing, we plan to introduce a new type of metadata record along with a new metadata field, `BitmapIndexMetadata`.
This field will store the serialized and encoded bitmap string directly.
Each record's key will follow the format: `<column_name>$<column_value>$<file_group_id>`,
where file_group_id is defined as `<partition_path>$<file_id>`.
> `store_type$grocery$20250401$2aa10971-11b2-41b3-9baf-ab33c0000144-0`

For non-partitioned tables, we use `.` as a placeholder for the partition path. In this case, the key would be:
>`store_type$grocery$.$2aa10971-11b2-41b3-9baf-ab33c0000144-0`

The index key can be constructed on-the-fly by concatenating strings during both read and write operations.
Since the reader must know at least the column name and column value, searching bitmaps by key prefix is also supported.

Below is another example of how the index record looks like in the storage.
![bitmap_payload](./bitmap_payload.png)


### Writing Index

### Reading Index

### New Hudi Configs


### Spark SQL Support
## Rollout/Adoption Plan

- What impact (if any) will there be on existing users?
    - None
- If we are changing behavior how will we phase out the older behavior?
    - N/A
- If we need special migration tools, describe them here.
    - N/A
- When will we remove the existing behavior
    - N/A

## Test Plan
### Unit Tests
### Performance Tests
