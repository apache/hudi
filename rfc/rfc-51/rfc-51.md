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

# RFC-50: Hudi CDC

# Proposers

- @Yann Byron

# Approvers

- @Raymond

# Statue
JIRA: [https://issues.apache.org/jira/browse/HUDI-3478](https://issues.apache.org/jira/browse/HUDI-3478)

# Hudi Supports Change-Data-Capture

## Abstract

We want to introduce the Change-Data-Capture(CDC) capacity that make hudi can produce the changeing data by which we can know how the records is changed, to response the CDC cases.


## Background

In some use cases where hudi tables is used as streaing source, We want to be aware every records' changing in one commit exactly. In a certain commit or snapshot, which records are inserted, which records are deleted, and which records are updated. Even for the updated records, both the old value before updated and the new value after updated are needed.

To implement this capacity, we have to upgrade the write and read parts. Let hudi can figure out the changing data when read. And in some cases, writing the extra data to help querying the changing data if necessary.

## Scenario Definition

Here use a simply case to explain the CDC.

![](scenario-definition.jpg)

Here one metadata column named `_changing_type` is added. It represents that how the record is changed, and it have four enum values:

- update_preimage: The old value before a certain commit;
- update_postiamge: The new value after a certain commit;
- insert: a new coming record in a certain commit;
- delete: a record that has been deleted in a certain commit;

Notice:
Here the illustration ignores all the metadata columns like `_hoodie_commit_time`.

## Goals

1. Support row-level CDC records generation and persistence;
2. Support both MOR and COW tables;
3. Support all the write operations;
4. Support Spark DataFrame/SQL/Streaming Query;

## Implementation
### CDC Architecture

![](arch.jpg)

Notice:
Other operations like `Compact`, `Clean`, `Index` do not write/change any data. So we don't need to consider in CDC scenario.
 
### Points to be upgraded

![](points.jpg)

### Config Definitions

|  | default |  |
| --- | --- | --- |
| hoodie.table.cdf.enabled | false | if true, write the changing data to FS. |
|  |  |  |
| hoodie.datasource.read.cdc.enabled | false | if true, return the CDC data. |
| hoodie.datasource.read.start.timestamp | - | requried. |
| hoodie.datasource.read.end.timestamp | - | optional. |


### Write

Hoodie writes data by `HoodieWriteHandle`. In the different sub classes of `HoodieWriteHandle`, we will create `FileWriter`which can receive data and save to `FileSystem`. So We can upgrade these sub classes to archieve the CDC data's generation and persistence.

The directory of the CDC file is`tablePath/.cdc/`. The file structure is like:
For non-partition table:
```
hudi_cdc_table/
    .hoodie/
        hoodie.properties
        00001.commit
        00002.replacecommit
        ...
    .cdc/
        xxxx123.parquet
        xxxx456.parquet
        ...
    default/
        fileId1_xxx_00001.parquet
        fileId1_xxx_00002.parquet
        ...
```

For partition table (the partition column is `year`):
```
hudi_cdc_table/
    .hoodie/
        hoodie.properties
        00001.commit
        00002.replacecommit
        ...
    .cdc/
        year=2021/xxxx123.parquet
        year=2022/xxxx456.parquet
        ...
    default/
        year=2021/
        year=2022/
        ...
```

One Design Idea is that **Write CDC files as little as possible, and reuse data files as much as possible**.

As the idea, define three file types for CDC:

- CDC File: Record all the related changing data with an extra column which name `changing_type`for one commit. For the following cases, will generate the CDC file:
   - `UPSERT` operation;
   - `DELETE` operation and the files where the data to be deleted resides has other data that doesn't need to be deleted and need to be rewrited.
- pure Add-File: all the data in this file ars incoming, and don't affect the existing data and files. In the following cases, we do not have data to be rewrited and need to write CDC data to the CDC file:
   - `BUIK_INSERT` operation;
   - `INSERT`operation;
   - `BOOTSTRAP` operation;
   - `INSERT_OVERWRITE` and `INSERT_OVERWRITE_TABLE` operations;
- pure Remove-File: all the data in the file will be deleted, and don't affect the existing data and files. In the following cases, we also do not have data to be rewrited:
   - `DELETE`operation and no old data should be rewrite.
   - `DELETE_PARTITION` operation;

Notice:

- Only CDC File is an additional workload. The pure Add-File and pure Remove-File are just representations of the existing data files in the CDC scenario. For some examples:
   - `INSERT` operation will create a list of new data files. Each of these can be considered a pure Add-File.
   - `DELETE_PARTITION` operation will delete a group of data files. Each of these can be considered a pure Remove-File.
- For a single commit, if CDC files is existed, we just load CDC files to respone. If no any CDC files, extract the list of pure Add-File and Remove-File, load these files and respone CDC query.
- every CDC file must be related to a commit. Use parquet format to storage uniformly.


### Read

This part just discuss how to make Spark (including Spark DataFram, SQL, Streaming) to read the Hudi CDC data.

Implement `CDCReader` that do these steps to response the CDC request:

- judge whether this is a table that has enabled `hoodie.table.cdf.enabled`, and the query range is valid.
- extract and filter the commits needed from `ActiveTimeline`.
- For each of commit, get and load the changing files, append the cdc columns and return `DataFrame`.

```scala
class CDCReader(
  metaClient: HoodieTableMetaClient,
  options: Map[String, String],
) extends BaseRelation with PrunedFilteredScan {

  override def schema: StructType = {
  // append the `changing_type` column
  }
  
  override def buildScan(
    requiredColumns: Array[String],
    filters: Array[Filter]): RDD[Row] = {
  // ...
  }

}
```

Notice:

- Only instants that are active can be queried in a CDC scenario.
- `CDCReader` manages all the things on CDC, and all the spark entrances(DataFrame, SQL, Streaming) call the funcations in `CDCReader`. 

#### COW table

Just follow the above steps without further consideration.

#### MOR table

For the inc data stored in log files, we need to merge them and the base file, to figure out how each record changed.
But if users don't need to the exact changing, we can use a config to skip the merge process, and return directly.

####Syntax

Spark DataFrame Syntax:
```scala
spark.read.format("hudi").
  option("hoodie.datasource.read.cdc.enabled", "true").
  option("hoodie.datasource.read.start.timestamp", "20220426103000000").
  option("hoodie.datasource.read.start.timestamp", "20220426113000000").
  load("/path/to/hudi")
```

Spark SQL Syntax:
```sql
-- query the CDC data between 20220426103000000 and 20220426113000000;
select * 
from hudi_table_changes("hudi_cdc_table", "20220426103000000", "20220426113000000");

-- query the CDC data since 20220426103000000;
select * 
from hudi_table_changes("hudi_cdc_table", "20220426103000000");

```

Spark Streaming Sytax:
```scala
val df = spark.readStream.format("hudi").
  option("hoodie.datasource.read.cdc.enabled", "true").
  load("/path/to/hudi")

// launch a streaming which start from the current snapshot of hudi table,
// and output at the console.
val stream = df.writeStream.format("console").start
```

### Others

Upgrade `Clean`: 
Since only instants is active that can be queried in a CDC scenario, the unreached CDC files should be delete in time when `Clean` is triggered.


# Rollout/Adoption Plan
This is a new feature that can enable CDF and CDC query, does not impact existing jobs and tables. Also this dos not depend on Spark versions.
# Test Plan

- [ ] Unit tests for this
- [ ] Prodect integration test
- [ ] Benchmark snapshot query for large tables

