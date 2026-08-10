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

# RFC-51: Hudi to support Change-Data-Capture

# Proposers

- @Yann Byron

# Approvers

- @Raymond
- @Vinoth
- @Danny
- @Prasanna

# Statue
JIRA: [https://issues.apache.org/jira/browse/HUDI-3478](https://issues.apache.org/jira/browse/HUDI-3478)

# Hudi to Support Change-Data-Capture

## Abstract

The Change-Data-Capture (CDC) feature enables Hudi to show how records were changed by producing the changes and therefore to handle CDC query usecases.

## Background

In cases where Hudi tables used as streaming sources, we want to be aware of all records' changes in one commit exactly, as in which records were inserted, deleted, and updated. And for updated records, the old values before update and the new ones after.

To implement this feature, we need to implement the logic on the write and read path to let Hudi figure out the changed data when read. In some cases, we need to write extra data to help optimize CDC queries.

## Scenario Illustration

The diagram below illustrates a typical CDC scenario.

![](scenario-illustration.jpg)

We follow the debezium output format: four columns as shown below

- op: the operation of this record;
- ts: the timestamp;
- source: source information such as the name of database and table. **Maybe we don't need this column in Hudi**;
- before: the previous image before this operation;
- after: the current image after this operation;

`op` column has three enum values:

- i: represent `insert`; when `op` is `i`, `before` is always null;
- u: represent `update`; when `op` is `u`, both `before` and `after` don't be null;
- d: represent `delete`; when `op` is `d`, `after` is always null;

**Note**

* In case of the same record having operations like insert -> delete -> insert, CDC data should be produced to reflect the exact behaviors.
* The illustration above ignores all the Hudi metadata columns like `_hoodie_commit_time` in `before` and `after` columns.

## Design Goals

1. Support row-level CDC records generation and persistence
2. Support both MOR and COW tables
3. Support all the write operations
4. Support incremental queries in CDC format across supported engines
5. For CDC-enabled tables, non-CDC queries' performance should not be affected

## Configurations

| key                                                 | default  | description                                                                                                                                                                                                                                                                                                          |
|-----------------------------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.table.cdc.enabled                            | `false`  | The master switch of the CDC features. If `true`, writers and readers will respect CDC configurations and behave accordingly.                                                                                                                                                                                        |
| hoodie.table.cdc.supplemental.logging.mode          | `KEY_OP` | A mode to indicate the level of changed data being persisted. At the minimum level, `KEY_OP` indicates changed records' keys and operations to be persisted. `DATA_BEFORE`: persist records' before-images in addition to `KEY_OP`. `DATA_BEFORE_AFTER`: persist records' after-images in addition to `DATA_BEFORE`. |

To perform CDC queries, users need to set `hoodie.datasource.query.incremental.format=cdc` and `hoodie.datasource.query.type=incremental`.

| key                                        | default        | description                                                                                                                          |
|--------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.datasource.query.type               | `snapshot`     | set to `incremental` for incremental query.                                                                                          |
| hoodie.datasource.query.incremental.format | `latest_state` | `latest_state` (current incremental query behavior) returns the latest records' values. Set to `cdc` to return the full CDC results. |
| hoodie.datasource.read.begin.instanttime   | -              | requried.                                                                                                                            |
| hoodie.datasource.read.end.instanttime     | -              | optional.                                                                                                                            |

## When `supplemental.logging.mode=KEY_OP`

In this mode, we minimized the additional storage for CDC information.

- When write, only the change type `op`s and record keys are persisted.
- When read, changed info will be inferred on-the-fly, which costs more computation power. As `op`s and record keys are
  available, inference using current and previous committed data will be optimized by reducing IO cost of reading
  previous committed data, i.e., only read changed records.

The detailed logical flows for write and read scenarios are the same regardless of `logging.mode`, which will be
illustrated in the section below.

## When `supplemental.logging.mode=DATA_BEFORE` or `DATA_BEFORE_AFTER`

Overall logic flows are illustrated below.

![](logic-flows.jpg)

### Write

Hudi writes data by `HoodieWriteHandle`. We notice that only `HoodieMergeHandle` and its subclasses will receive both
the old record and the new-coming record at the same time, merge and write. So we will add a `LogFormatWriter` in these
classes. If there is CDC data need to be written out, then call this writer to write out a log file which consist
of `CDCBlock`. The CDC log files have `-cdc` suffix (to distinguish from data log files for read performance
consideration) will be placed in the same paths as the base files and other log files. Clean service needs to be tweaked
accordingly. An example of the file structure:

```
cow_table/
    .hoodie/
        hoodie.properties
        00001.commit
        00002.replacecommit
        ...
    year=2021/
        filegroup1-instant1.parquet
        .filegroup1-instant1-cdc
    year=2022/
        filegroup2-instant1.parquet
        .filegroup2-instant1-cdc
    ...
```

Under partition directories, the `-cdc` files with `CDCBlock` as shown above contain the persisted changed data.

#### Persisting CDC in MOR: Write-on-indexing vs Write-on-compaction

2 design choices on when to persist CDC in MOR tables:

Write-on-indexing allows CDC info to be persisted at the earliest, however, in case of Flink writer or Bucket
indexing, `op` (I/U/D) data is not available at indexing.

Write-on-compaction can always persist CDC info and achieve standardization of implementation logic across engines,
however, some delays are added to the CDC query results. Based on the business requirements, Log Compaction (RFC-48) or
scheduling more frequent compaction can be used to minimize the latency.

The semantics we propose to establish are: when base files are written, the corresponding CDC data is also persisted.

- For Spark
  - inserts are written to base files: the CDC data `op=I` will be persisted
  - updates/deletes that written to log files are compacted into base files: the CDC data `op=U|D` will be persisted
- For Flink
  - inserts/updates/deletes that written to log files are compacted into base files: the CDC data `op=I|U|D` will be
    persisted

In summary, we propose that CDC data should be persisted synchronously upon base files generation. It is therefore
write-on-indexing for Spark inserts (non-bucket index) and write-on-compaction for everything else.

- Note 1: CDC results can still be returned upon CDC-type query by doing on-the-fly inference, before compaction is
  performed. Details are illustrated in the [Read](#cdcread) section below.
- Note 2: it may also be necessary to provide capabilities for asynchronously persisting CDC data, in terms of a
  separate table service like `ChangeTrackingService`, which can be scheduled to fine-tune the CDC Availability SLA,
  effectively decoupling it with Compaction frequency.

#### Examples

Spark DataSource:

```scala
df.write.format("hudi").
  options(commonOpts)
  option("hoodie.table.cdc.enabled", "true").
  option("hoodie.table.cdc.supplemental.logging.mode", "DATA_AFTER").
  save("/path/to/hudi")
```

Spark SQL:

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
    'preCombineField' = 'ts_ms',
    'hoodie.table.cdc.enabled' = 'true',
    'hoodie.table.cdc.supplemental.logging.mode' = 'DATA_AFTER',
    'type' = 'cow'
)
```

### <a name="cdcread"></a>Read

### How to infer CDC results

| `HoodieCDCInferenceCase` | Infer case details                                                                                                        | Infer logic                                                                                                                                                               | Note                               |
|----------------------|---------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------|
| `AS_IS`              | CDC file written (suffix contains `-cdc`) alongside base files (COW) or log files (MOR)                                   | CDC info will be extracted as is                                                                                                                                          | the read-optimized way to read CDC |
| `BASE_FILE_INSERT`   | Base files were written to a new file group                                                                               | All records (in the current commit): `op=I`, `before=null`, `after=<current value>`                                                                                       | on-the-fly inference               |
| `BASE_FILE_DELETE`   | Records are not found from the current commit's base file but found in the previous commit's within the same file group   | All records (in the previous commit): `op=D`, `before=<previous value>`, `after=null`                                                                                     | on-the-fly inference               |
| `LOG_FILE`           | For MOR, log files to be read and records to be looked up in the previous file slice.                                     | Current record read from delete block, found in previous file slice => `op=D`, `before=<previous value>`, `after=null`                                                    | on-the-fly inference               |
| `LOG_FILE`           | ditto                                                                                                                     | Current record read from delete block, not found in previous file slice => skip due to the delete log block should be discarded (trying to delete non-existing records)   | on-the-fly inference               |
| `LOG_FILE`           | ditto                                                                                                                     | Current record not read from delete block, found in previous file slice => `op=U`, `before=<previous value>`, `after=<current value>`                                     | on-the-fly inference               |
| `LOG_FILE`           | ditto                                                                                                                     | Current record not read from delete block, not found in previous file slice => `op=I`, `before=null`, `after=<current value>`                                             | on-the-fly inference               |
| `REPLACE_COMMIT`     | File group corresponds to a replace commit                                                                                | All records `op=D`, `before=<value from the file group>`, `after=null`                                                                                                    | on-the-fly inference               |

### Illustrations

This section uses Spark (incl. Spark DataFrame, SQL, Streaming) as an example to perform CDC-format incremental queries.

Implement `CDCReader` that do these steps to response the CDC request:

- check if `hoodie.table.cdc.enabled=true`, and if the query range is valid.
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
- If `hoodie.table.cdc.supplemental.logging.mode=KEY_OP`, we need to compute the changed data. The following illustrates the difference.

![](read_cdc_log_file.jpg)

#### COW table

CDC queries always extract and return the persisted CDC data.

#### MOR table

According to the section "Persisting CDC in MOR", persisted CDC data is available upon base files' generation. The CDC-type query results 
should be consisted of on-the-fly read results and as-is read results. See the [Read](#cdcread) section.

The implementation should

- compute the results on-the-fly by reading log files and the corresponding base files (current and previous file slices).
- extract the results by reading persisted CDC data and the corresponding base files (current and previous file slices).
- stitch the results from previous 2 steps and return the complete freshest results

Here use an illustration to explain how we can query the CDC on MOR table in kinds of cases.

![](query_cdc_on_mor.jpg)

#### Examples

Spark DataSource

```scala
spark.read.format("hudi").
  option("hoodie.datasource.query.type", "incremental").
  option("hoodie.datasource.query.incremental.format", "cdc").
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
  option("hoodie.datasource.query.type", "incremental").
  load("/path/to/hudi")

// launch a streaming which starts from the current snapshot of the hudi table,
// and output at the console.
val stream = df.writeStream.format("console").start
```

# Rollout/Adoption Plan

Spark support phase 1

- For COW: support Spark CDC write/read fully
- For MOR: support Spark CDC write (only `OP=I` when write inserts to base files) and CDC on-the-fly inferring read.

Spark support phase 2

- For MOR: Spark CDC write (`OP=U/D` when compact updates/deletes to log files) and CDC read to combine on-the-fly
  inferred data and persisted CDC data.
  - Note: for CDC write via compaction, `HoodieMergedLogRecordScanner` needs to support producing CDC data for each
    version of the changed records. `HoodieCompactor` and `HoodieMergeHandler` are to adapt the
    changes. See [HUDI-4705](https://issues.apache.org/jira/browse/HUDI-4705).

Flink support can be developed in parallel, and can use of the common logical changes of CDC write via compaction in
Spark support phase 2.

# Test Plan

- Unit tests for this
- Production end-to-end integration test
- Benchmark snapshot query for large tables

# Appendix

## Affected code paths

For `supplemental.logging=DATA_BEFORE` or `DATA_AFTER`

![](code-paths.jpg)
