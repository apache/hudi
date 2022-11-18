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

# RFC-63: Index Function for Optimizing Query Performance

## Proposers

- @yihua
- @alexeykudinkin

## Approvers

- @vinothchandar
- @xushiyan
- @nsivabalan

## Status

JIRA: [HUDI-512](https://issues.apache.org/jira/browse/HUDI-512)

## Abstract

In this RFC, we address the problem of accelerating queries containing predicates based on functions defined on a
column, by introducing **Index Function**, a new indexing capability for efficient file pruning.

## Background

To make the queries finish faster, one major optimization technique is to scan less data by pruning rows that are not
needed by the query. This is usually done in two ways:

- **Partition pruning**:  The partition pruning relies on a table with physical partitioning, such as Hive partitioning.
  A partitioned table uses a chosen column such as the date of `timestamp` and stores the rows with the same date to the
  files under the same folder or physical partition, such as `date=2022-10-01/`. When the predicate in a query
  references the partition column of the physical partitioning, the files in the partitions not matching the predicate
  are filtered out, without scanning. For example, for the predicate `date between '2022-10-01' and '2022-10-02'`, the
  partition pruning only returns the files from two partitions, `2022-10-01` and `2022-10-02`, for further processing.
  The granularity of the pruning is at the partition level.


- **File pruning**:  The file pruning carries out the pruning of the data at the file level, with the help of file-level
  or record-level index. For example, with column stats index containing minimum and maximum values of a column for each
  file, the files falling out of the range of the values compared to the predicate can be pruned. For a predicate
  with `age < 20`, the file pruning filters out a file with columns stats of `[30, 40]` as the minimum and maximum
  values of the column `age`.

While Apache Hudi already supports partition pruning and file pruning with data skipping for different query engines, we
recognize that the following use cases need better query performance and usability:

- File pruning based on functions defined on a column
- Efficient file pruning for files without physical partitioning
- Effective file pruning after partition evolution, without rewriting data

Next, we explain these use cases in detail.

### Use Case 1: Pruning files based on functions defined on a column

Let's consider a non-partitioned table containing the events with a `timestamp` column. The events with naturally
increasing time are ingested into the table with bulk inserts every hour. In this case, assume that each file should
contain rows for a particular hour:

| File Name           | Min of `timestamp` | Max of `timestamp` | Note               |
|---------------------|--------------------|--------------------|--------------------|
| base_file_1.parquet | 1664582400         | 1664586000         | 2022-10-01 12-1 AM |
| base_file_2.parquet | 1664586000         | 1664589600         | 2022-10-01 1-2 AM  |
| ...                 | ...                | ...                | ...                |
| base_file_13.parquet | 1664625600         | 1664629200         | 2022-10-01 12-1 PM |
| base_file_14.parquet | 1664629200         | 1664632800         | 2022-10-01 1-2 PM  |
| ...                 | ...                | ...                | ...                |
| base_file_37.parquet | 1664712000         | 1664715600         | 2022-10-02 12-1 PM |
| base_file_38.parquet | 1664715600         | 1664719200         | 2022-10-02 1-2 PM  |

For a query to get the number of events between 12PM and 2PM each day in a month for time-of-day analysis, the
predicates look like `DATE_FORMAT(timestamp, '%Y-%m-%d') between '2022-10-01' and '2022-10-31'`
and `DATE_FORMAT(timestamp, '%H') between '12' and '13'`. If the data is in a good layout as above, we only need to scan
two files (instead of 24 files) for each day of data, e.g., `base_file_13.parquet` and `base_file_14.parquet` containing
the data for 2022-10-01 12-2 PM.

Currently, such a fine-grained file pruning based on a function on a column cannot be achieved in Hudi, because
transforming the `timestamp` to the hour of day is not order-preserving, thus the file pruning cannot directly leverage
the file-level column stats of the original column of `timestamp`. In this case, Hudi has to scan all the files for a
day and push the predicate down when reading parquet files, increasing the amount of data to be scanned.

### Use Case 2: Efficient file pruning for files without physical partitioning

Let's consider the same non-partitioned table as in the Use Case 1, containing the events with a `timestamp` column. The
difference here is that there are late-arriving data in each batch, meaning that some events contain the `timestamp`
from a few days ago. In realistic scenarios, this happens frequently, where the rows are not strictly grouped or
clustered for any column.

In the current write operations for a Hudi table, there is no particular data co-location scheme except for sorting mode
based on record key for bulk insert. Hudi also has a mechanism of small file handling, adding new insert records to
existing file groups. As the ingestion makes progress, each file may contain records for a wide range between minimum
and maximum values for a particular column or a function applied on a column, making file pruning based on the
file-level column stats less effective.

### Use Case 3: File pruning support after partition evolution

Partition evolution refers to the process of changing the partition columns for writing data to the storage. This
requirement comes up when a user would like to reduce the number of physical partitions and improve the file sizing.

Consider a case where event logs are stream from microservices and ingested into a raw event table. Each event log
contains a `timestamp` and an associated organization ID (`org_id`). Most queries on the table are organization specific
and fetch logs for a particular time range. A user may attempt to physically partition the data by both `org_id`
and `date(timestamp)`. If there are 1K organization IDs and one year of data, such a physical partitioning scheme writes
at least `365 days x 1K IDs = 365K` data files under 365K partitions. In most cases, the data can be highly skewed based
on the organizations, with most organizations having less data and a handful of organizations having the majority of the
data, so that there can be many small data files. In such a case, the user may want to evolve the partitioning by
using `org_id` only without rewriting existing data, resulting in the physical layout of data like below

| Physical partition path      | File Name            | Min of datestr | Max of datestr | Note                    |
|------------------------------|----------------------|----------------|----------------|-------------------------|
| org_id=1/datestr=2022-10-01/ | base_file_1.parquet  | `2022-10-01`   | `2022-10-01`   | Old partitioning scheme |
| org_id=1/datestr=2022-10-02/ | base_file_2.parquet  | `2022-10-02`   | `2022-10-02`   |                         |
| org_id=2/datestr=2022-10-01/ | base_file_3.parquet  | `2022-10-01`   | `2022-10-01`   |                         |
| org_id=3/datestr=2022-10-01/ | base_file_4.parquet  | `2022-10-01`   | `2022-10-01`   |                         |
| ...                          | ...                  | ...            | ...            | ...                     |
| org_id=1/                    | base_file_10.parquet | `2022-10-10`   | `2022-10-11`   | New partitioning scheme |
| org_id=2/                    | base_file_11.parquet | `2022-10-10`   | `2022-10-15`   |                         |
| ...                          | ...                  | ...            | ...            | ...                     |

As queries need to look for data for a particular time range, instead of relying on partition pruning, we can still use
file pruning with file-level column stats. For the example above, even in the new partitioning scheme, without data
being physically partitioned by the datestr, the data can still be co-located based the date, because of natural
ingestion order or Hudi's clustering operation. In this case, we can effectively prune files based on the range of
datestr for each file, regardless how the files are stored under different physical partition paths in the table. We
need to add such file pruning support in Hudi.

## Goals and Non-Goals

Based on the use cases we plan to support, we set the following goals and non-goals for this RFC:

### Goals

- Support file pruning on functions defined on data column
- Improve the co-location of data without folder structure for efficient file pruning
- Improve the query performance based on the new pruning capability compared existing approach in Hudi

### Non-Goals

- DO NOT remove physical partitioning, which remains as an option for physically storing data in different folders and
  partition pruning
- DO NOT tackle the support of partition evolution in this RFC, but make sure the file pruning on functions defined on
  data column can be applied on the partition evolution use case

## Design and Implementation

To achieve the goals above, in this RFC, we introduce the **Index Function**, a function defined on data column which is
indexed to facilitate file pruning, which works at high level as follows:

1. User specifies the Index Function, including the original data column, the expression applying a function on the
   column, and optionally the new column name, through SQL or Hudi's write config. One or more Index Functions can be
   added or dropped for both new and existing Hudi tables at any time.
2. The Index Function is registered to the table in the table properties, to keep track of the relationship between the
   data column and the Index Function.
3. Hudi creates the index metadata, such as file-level column stats, of Index Functions in metadata table. Hudi marks
   whether one Index Function and the index metadata are ready for use in file pruning.
4. No data is written to the data files for the Index Function, i.e., the data files do not contain a new data column
   corresponding to the Index Function, to save storage space.
5. When query engine makes a logical plan for a query, Hudi intercepts the predicate that relates to an Index Function,
   looks for the corresponding index in metadata table, and prunes files based on the index.

When writing data to a non-partitioned table or data files under one partition path, selective Index Functions can be
specified as ordering field during write operations. For inserts, Hudi's write client sorts or buckets the data based on
the Index Function(s) so that each data file has a narrow and mostly non-overlapping range of values for the
corresponding Index Function(s), to unlock efficient file pruning.

### Components

We discuss the design and implementation details of each component to realize the Index Function.

#### Index Function Interface

A new interface, `IndexFunction`, is introduced to represent an Index Function and track the relationship between the
original data column and the transformation

```java
interface IndexFunction<S, T> {
  String originalColumnName;
  String expression;
  String indexFunctionName;

  T apply(S sourceValue);
}
```

The list of Index Functions ready for use and related information is stored in the table config:

```
hoodie.table.index.function.list=datestr,hour_of_day
hoodie.table.index.function.datestr.original_column=timestamp
hoodie.table.index.function.datestr.expression=DATE_FORMAT(timestamp, '%Y-%m-%d')
hoodie.table.index.function.hour_of_day.original_column=timestamp
hoodie.table.index.function.hour_of_day.expression=DATE_FORMAT(timestamp, '%H')
```

The `hoodie.table.index.function.list` indicates the Index functions that are ready for use at query planning and
execution time.

#### Index Function Creation

To create an Index Function, we introduce two ways:

1. New SQL statement:

```
CREATE INDEX FUNCTION [ index_function_name ] ON table_name [ USING index_type ] ( { column_name | expression } );
```

`table_name`: The name of the Hudi table for creating an Index Function.
`index_function_name`: An optional name for the Index Function. If no name is specified, Hudi internally creates a new
name.
`column_name`: The column name without transformation for creating an Index Function.
`expression`: The SQL expression on a data column for creating an Index Function.
`index_type`: The type of index metadata to generate for the Index Function. By default, file-level column stats is
used.

Example:

```
CREATE INDEX datestr ON hudi_table USING col_stats (DATE_FORMAT(timestamp, '%Y-%m-%d'));
CREATE INDEX ON hudi_table (city_id);
CREATE INDEX hour_of_day ON hudi_table (DATE_FORMAT(timestamp, '%H'));
```

2. Write config in ingestion: A new write config is introduced `hoodie.write.index.function.list` to allow a user to add
   and update Index Functions.

Example:

```
hoodie.write.index.function.list=timestamp,datestr,col_stats,DATE_FORMAT(timestamp, '%Y-%m-%d');timestamp,,,DATE_FORMAT(timestamp, '%H')
```

#### Index in Metadata Table

For each Index Function, Hudi needs to populate the index metadata to facilitate file pruning. We use the metadata table
to store these index metadata. The index metadata of each individual Index Function is stored under a new partition with
the naming of the Index Function name and the index type, e.g., `datestr_col_stats`. For the default index type of
file-level column stats, the index format and logic of existing column stats index is reused. Once an Index Function is
created and the corresponding index metadata is populated in metadata table, the index metadata is always consistent
with the data table during write transactions, using the same multi-table transaction for updating metadata table.

We plan to also integrate Index Function with Hudi's Asynchronous Indexing so that the index metadata for an Index
Function can be populated in an async way.

#### File Pruning

When a query with the predicates related to an Index Functions is being planned, Hudi intercepts such predicate to carry
out the file pruning

1. Identify the relevant Index Function based on the predicate and add a new predicate based on the Index Function name
   if needed. Take the Index Function created by
   SQL `CREATE INDEX datestr ON hudi_table USING col_stats (DATE_FORMAT(timestamp, '%Y-%m-%d'));`:

- If the original data column of an Index Function is used, transform the predicate to a new predicate based on the
  Index Function name if possible, e.g., `timestamp between 1666767600 and 1666897200`
  to `datestr between '2022-10-26' and '2022-10-27'`;
- If the expression of an Index Function is used, replace the expression to the Index Function name,
  e.g., `DATE_FORMAT(timestamp, '%Y-%m-%d') between '2022-10-26' and '2022-10-27'`
  to `datestr between '2022-10-26' and '2022-10-27'`;
- If the Index Function name is used in the predicate, e.g., `datestr between '2022-10-26' and '2022-10-27'`, no action
  is needed here.

2. For the Index Function name appeared in the predicate, look up the corresponding index metadata from metadata table
   and prune files that are not needed for scanning.

For each query engine, we need to intercept the engine-specific representation of the predicates, and derive the
relevant predicate based on the function. We consider integrating Spark and Presto first in the implementation.

#### Index-Function-Based Ordering on Write Path

To unlock efficient file pruning, we plan to support Index-Function-based ordering during write operations. Similar to
clustering, the Index functions for ordering are specified by new write config `hoodie.write.sort.columns`.

When writing records to data files, the write client does the following:

- For new inserts, sort the data based on the specified Index functions before writing to data files. This reuses
  existing flow of bulk insert and clustering
    - Each data file contains one or a small subset of values corresponding to the Index Functions.
    - Different sort modes can be provided like bulk insert and clustering.

- For updates, no change in logic is needed. The record with the same key should go into the same file group.
- For small file handing, we need to take into account the Index functions for the workload profile, so that the new
  inserts can be combined into the data file without expanding the minimum and maximum values of the Index Function when
  the data file is small.

## Related Concepts

In this section, we compare Hudi's Index Function to related concepts and solutions on pruning files efficiently and
improving query performance.

### Hidden Partitioning

[Hidden Partitioning](https://iceberg.apache.org/docs/latest/partitioning/#icebergs-hidden-partitioning), introduced by
Apache Iceberg, maintains the relationship between the original column and transformed partition values, and hides the
partition columns and physical partitions from catalogs and users, using manifests containing the partition values of
each file to prune files and speed up queries. By using Hidden Partitioning, Iceberg can evolve partition schemes, i.e.,
partition evolution, without rewriting data.

Hudi's Index Function achieves the same goal of pruning files based on the column stats, with the support on any data
column or function defined on a data column, without coupling with the partition scheme.

### Index on Expression

[Index on Expression](https://www.postgresql.org/docs/current/indexes-expressional.html),
or [Function-Based Indexes](https://oracle-base.com/articles/8i/function-based-indexes) a concept in RDBMS, provides the
index on a function or scalar expression computed from one or more columns of the table. Such an index can speed up the
queries containing the expression in the predicates. The index on expression is a secondary index, meaning that it is
stored separately from the record data. There is no materialization of the values from the expression in the data area.
PostgreSQL supports Index on Expression and Oracle database supports Function-Based Indexes.

Hudi's Index Function applies similar concept which allows a new index to be built based on a function defined on a data
column at any time for file pruning.

### Generated Column

A [Generated Column](https://www.postgresql.org/docs/15/ddl-generated-columns.html) is a special column that is always
computed from other columns. There are two types of generated column: (1) stored generated column: written alongside
other columns and persisted to storage, (2) virtual generated column: computed when read and occupies no storage.
PostgreSQL and Delta Lake support stored generated column only. As data of stored generated column is persisted to
storage, it can be used as a normal column for partitioning or creating an index.

In Delta Lake, a [generated column](https://docs.delta.io/latest/delta-batch.html#use-generated-columns) can be used as
a partition column. When doing so, Delta Lake looks at the relationship between the base column and the generated
column, and populates partition filters based on the generated partition column if possible.

Hudi's Index Function is similar to virtual generated column, but the Index Function is not queryable except for the
usage in the predicates.

#### Micro Partitions

[Micro Partitions](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions.html), used by Snowflake,
automatically divide data into contiguous units of storage with each micro-partition containing between 50 MB and 500 MB
of uncompressed data, without the need of static partitioning. Snowflake stores metadata of each micro-partition,
including the range of values for each of the columns. In this way, query pruning based on the column stats metadata can
be efficient, if there are no or little overlapping among micro partitions in terms of the column value range. The
degree of overlapping is reduced through data clustering.

Hudi's Index Function depends on optimized storage layout for efficient file pruning, which can be achieved through
clustering and proposed Index-Function-based ordering during write operations.

### Rollout/Adoption Plan

The Index Function is going to be guarded by feature flags, `hoodie.write.index.function.enable` for creating and
updating Index functions, and `hoodie.read.index.function.enable` for enabling file pruning based on Index Function in
query engines.

## Test Plan

- Unit and functional tests
    - Function can be applied correctly
    - Index function is populated in metadata table
    - Expected pruning behavior is validated
    - For Index-Function-based ordering during write operations, data is co-located at write time
- Validation of the implementation on the use cases above
    - The workflow should work smoothly