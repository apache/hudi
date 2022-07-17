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

# RFC-50: Hudi to support Change-Data-Capture

# Proposers

- @Yann Byron

# Approvers

- @Raymond
- @Vinoth
- @Danny

# Statue
JIRA: [https://issues.apache.org/jira/browse/HUDI-3478](https://issues.apache.org/jira/browse/HUDI-3478)

# Hudi to Support Change-Data-Capture

## Abstract

The Change-Data-Capture (CDC) feature enables Hudi to show how records were changed by producing the changes and therefore to handle CDC query usecases.

## Background

In cases where Hudi tables used as streaming sources, we want to be aware of all records' changes in one commit exactly, as in which records were inserted, deleted, and updated. And for updated records, the old values before update and the new ones after.

To implement this feature, we need to implement the logic on the write and read path to let Hudi figure out the changed data when read. In some cases, we need to write extra data to help optimize CDC queries.

## Scenarios

Here is a simple case to explain the CDC.

![](scenario-definition.jpg)

We follow the debezium output format: four columns as shown below

- op: the operation of this record;
- ts_ms: the timestamp;
- source: source information such as the name of database and table. **Maybe we don't need this column in Hudi**;
- before: the previous image before this operation;
- after: the current image after this operation;

`op` column has three enum values:

- i: represent `insert`; when `op` is `i`, `before` is always null;
- u: represent `update`; when `op` is `u`, both `before` and `after` don't be null;
- d: represent `delete`; when `op` is `d`, `after` is always null;

Note: the illustration here ignores all the Hudi metadata columns like `_hoodie_commit_time` in `before` and `after` columns.

## Goals

1. Support row-level CDC records generation and persistence;
2. Support both MOR and COW tables;
3. Support all the write operations;
4. Support Spark DataFrame/SQL/Streaming Query;

## Implementation

### CDC Architecture

![](arch.jpg)

Note: Table operations like `Compact`, `Clean`, `Index` do not write/change any data. So we don't need to consider them in CDC scenario.
 
### Modifiying code paths

![](points.jpg)

### Config Definitions

Define a new config:

| key | default | description |
| --- | --- | --- |
| hoodie.table.cdc.enabled | false | `true` represents the table to be used for CDC queries and will write cdc data if needed. |
| hoodie.table.cdc.supplemental.logging | true | If true, persist all the required information about the change data, including 'before' and 'after'. Otherwise, just persist the 'op' and the record key. |

Other existing config that can be reused in cdc mode is as following:
Define another query mode named `cdc`, which is similar to `snapshpt`, `read_optimized` and `incremental`.
When read in cdc mode, set `hoodie.datasource.query.type` to `cdc`.

| key | default  | description |
| --- |---| --- |
| hoodie.datasource.query.type | snapshot | set to cdc, enable the cdc quey mode |
| hoodie.datasource.read.start.timestamp | -        | requried. |
| hoodie.datasource.read.end.timestamp | -        | optional. |


### CDC File Types

Here we define 5 cdc file types in CDC scenario.

- CDC_LOG_File: a file consists of CDC Blocks with the changing data related to one commit.
  - when `hoodie.table.cdc.supplemental.logging` is true, it keeps all the fields about the change data, including `op`, `ts_ms`, `before` and `after`. When query hudi table in cdc query mode, load this file and return directly.
  - when `hoodie.table.cdc.supplemental.logging` is false, it just keeps the `op` and the key of the changing record. When query hudi table in cdc query mode, we need to load the previous version and the current one of the touched file slice to extract the other info like `before` and `after` on the fly.
- ADD_BASE_File: a normal base file for a specified instant and a specified file group. All the data in this file are new-incoming. For example, we first write data to a new file group. So we can load this file, treat each record in this as the value of `after`, and the value of `op` of each record is `i`.
- REMOVE_BASE_FILE: a normal base file for a specified instant and a specified file group, but this file is empty. A file like this will be generated when we delete all the data in a file group. So we need to find the previous version of the file group, load it, treat each record in this as the value of `before`, and the value of `op` of each record is `d`.
- MOR_LOG_FILE: a normal log file. For this type, we need to load the previous version of file slice, and merge each record in the log file with this data loaded separately to determine how the record has changed, and get the value of `before` and `after`.
- REPLACED_FILE_GROUP: a file group that be replaced totally, like `DELETE_PARTITION` and `INSERT_OVERWRITE` operations. We load this file group, treat all the records as the value of `before`, and the value of `op` of each record is `d`.

Note:

- **Only `CDC_LOG_File` is a new file type and written out by CDC**. The `ADD_BASE_File`, `REMOVE_BASE_FILE`, `MOR_LOG_FILE` and `REPLACED_FILE_GROUP` are just representations of the existing data files in the CDC scenario. For some examples:
  - `INSERT` operation will maybe create a list of new data files. These files will be treated as ADD_BASE_FILE;
  - `DELETE_PARTITION` operation will replace a list of file slice. For each of these, we get the cdc data in the `REPLACED_FILE_GROUP` way.

### Write

The idea is to **Write CDC files as little as possible, and reuse data files as much as possible**.

Hudi writes data by `HoodieWriteHandle`.
We notice that only `HoodieMergeHandle` and it's subclasses will receive both the old record and the new-coming record at the same time, merge and write.
So we will add a `LogFormatWriter` in these classes. If there is CDC data need to be written out, then call this writer to write out a log file which consist of `CDCBlock`.
The CDC log file will be placed in the same position as the base files and other log files, so that the clean service can clean up them without extra work. The file structure is like:

```
hudi_cdc_table/
    .hoodie/
        hoodie.properties
        00001.commit
        00002.replacecommit
        ...
    year=2021/
        filegroup1-instant1.parquet
        .filegroup1-instant1.log
    year=2022/
        filegroup2-instant1.parquet
        .filegroup2-instant1.log
    ...
```

Under a partition directory, the `.log` file with `CDCBlock` above will keep the changing data we have to materialize.

There is an option to control what data is written to `CDCBlock`, that is `hoodie.table.cdc.supplemental.logging`. See the description of this config above.

Spark DataSource example:

```scala
df.write.format("hudi").
  options(commonOpts)
  option("hoodie.table.cdc.enabled", "true").
  option("hoodie.table.cdc.supplemental.logging", "true"). //enable cdc supplemental logging 
  // option("hoodie.table.cdc.supplemental.logging", "false"). //disable cdc supplemental logging 
  save("/path/to/hudi")
```

Spark SQL example:

```sql
-- create a hudi table that enable cdc
create table hudi_cdc_table
(
    id int,
    name string,
    price double,
    ts long
) using hudi
tblproperties (
    'primaryKey' = 'id',
    'preCombineField' = 'ts',
    'hoodie.table.cdc.enabled' = 'true',
    'hoodie.table.cdc.supplemental.logging' = 'true',
    'type' = 'cow'
)
```

### Read

This part just discuss how to make Spark (including Spark DataFrame, SQL, Streaming) to read the Hudi CDC data.

Implement `CDCReader` that do these steps to response the CDC request:

- judge whether this is a table that has enabled `hoodie.table.cdc.enabled`, and the query range is valid.
- extract and filter the commits needed from `ActiveTimeline`.
- For each of commit, get and load the changing files, union and return `DataFrame`.
  - We use different ways to extract data according to different file types, details see the description about CDC File Type.

```scala
class CDCReader(
  metaClient: HoodieTableMetaClient,
  options: Map[String, String],
) extends BaseRelation with PrunedFilteredScan {

  override def schema: StructType = {
  // 'op', 'source', 'ts_ms', 'before', 'after'
  }
  
  override def buildScan(
    requiredColumns: Array[String],
    filters: Array[Filter]): RDD[Row] = {
  // ...
  }

}
```

Note:

- Only instants that are active can be queried in a CDC scenario.
- `CDCReader` manages all the things on CDC, and all the spark entrances(DataFrame, SQL, Streaming) call the functions in `CDCReader`.
- If `hoodie.table.cdc.supplemental.logging` is false, we need to do more work to get the change data. The following illustration explains the difference when this config is true or false.

![](read_cdc_log_file.jpg)

#### COW table

Reading COW table in CDC query mode is equivalent to reading a simplified MOR table that has no normal log files.

#### MOR table

According to the design of the writing part, only the cases where writing mor tables will write out the base file (which call the `HoodieMergeHandle` and it's subclasses) will write out the cdc files.
In other words, cdc files will be written out only for the index and file size reasons.

Here use an illustration to explain how we can query the CDC on MOR table in kinds of cases.

![](query_cdc_on_mor.jpg)

#### Examples

Spark DataSource

```scala
spark.read.format("hudi").
  option("hoodie.datasource.query.type", "cdc").
  option("hoodie.datasource.read.begin.instanttime", "20220426103000000").
  option("hoodie.datasource.read.end.instanttime", "20220426113000000").
  load("/path/to/hudi")
```

Spark SQL

```sql
-- query the CDC data between 20220426103000000 and 20220426113000000;
select * 
from hudi_table_changes("hudi_cdc_table", "20220426103000000", "20220426113000000");

-- query the CDC data since 20220426103000000;
select * 
from hudi_table_changes("hudi_cdc_table", "20220426103000000");

```

Spark Streaming

```scala
val df = spark.readStream.format("hudi").
  option("hoodie.datasource.query.type", "cdc").
  load("/path/to/hudi")

// launch a streaming which starts from the current snapshot of the hudi table,
// and output at the console.
val stream = df.writeStream.format("console").start
```

# Rollout/Adoption Plan

The CDC feature can be enabled by the corresponding configuration, which is default false. Using this feature dos not depend on Spark versions.

# Test Plan

- [ ] Unit tests for this
- [ ] Production end-to-end integration test
- [ ] Benchmark snapshot query for large tables

