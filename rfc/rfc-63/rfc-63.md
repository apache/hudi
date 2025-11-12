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

# RFC-63: Expression Indexes

## Proposers

- @yihua
- @alexeykudinkin
- @codope

## Approvers

- @vinothchandar
- @xushiyan
- @nsivabalan

## Status

JIRA: [HUDI-512](https://issues.apache.org/jira/browse/HUDI-512)

## Abstract

In this RFC, we propose **Expression Indexes**, a new capability to
Hudi's [multi-modal indexing](https://hudi.apache.org/blog/2022/05/17/Introducing-Multi-Modal-Index-for-the-Lakehouse-in-Apache-Hudi)
subsystem that offers a compelling vision to support not only accelerating queries but also reshape partitioning as
another layer of the indexing system, abstracting them from the traditional notions, while providing flexibility
and performance.

## Background

Hudi employs multi-modal indexing to optimize query performance. These indexes, ranging from simple files index to
record-level indexing, cater to a diverse set of use cases, enabling efficient point lookups and reducing the data
scanned during queries. This is usually done in two ways:

- **Partition pruning**:  The partition pruning relies on a table with physical partitioning, such as Hive-style partitioning.
  A partitioned table uses a chosen column such as the date of a `timestamp` column and stores the rows with the same
  date to the files under the same folder or physical partition, such as `date=2022-10-01/`. When the predicate in a
  query references the partition column of the physical partitioning, the files in the partitions not matching the
  predicate are filtered out, without scanning. For example, for the
  predicate `date between '2022-10-01' and '2022-10-02'`, the partition pruning only returns the files from two
  partitions, `2022-10-01` and `2022-10-02`, for further processing. The granularity of the pruning is at the partition
  level.

- **Data Skipping**:  Skipping data at the file level, with the help of column stats or record-level index. For example,
  with column stats index containing minimum and maximum values of a column for each file, the files falling out of the
  range of the values compared to the predicate can be pruned. For a predicate with `age < 20`, the file pruning filters
  out a file with columns stats of `[30, 40]` as the minimum and maximum values of the column `age`.

While Hudi already supports partition pruning and data skipping for different query engines, we
recognize that the following use cases need better query performance and usability:

- Data skipping based on functions defined on field(s)
- Support for different storage layouts and view partition as index
- Support for secondary indexes

Next, we explain these use cases in detail.

### Use Case 1: Data skipping based on functions defined on field(s)

Let's consider a non-partitioned table containing the events with a `ts`, a timestamp column. The events with naturally
increasing time are ingested into the table with bulk inserts every hour. In this case, assume that each file should
contain rows for a particular hour:

| File Name            | Min of `ts` | Max of `ts` | Note               |
|----------------------|-------------|-------------|--------------------|
| base_file_1.parquet  | 1664582400  | 1664586000  | 2022-10-01 12-1 AM |
| base_file_2.parquet  | 1664586000  | 1664589600  | 2022-10-01 1-2 AM  |
| ...                  | ...         | ...         | ...                |
| base_file_13.parquet | 1664625600  | 1664629200  | 2022-10-01 12-1 PM |
| base_file_14.parquet | 1664629200  | 1664632800  | 2022-10-01 1-2 PM  |
| ...                  | ...         | ...         | ...                |
| base_file_37.parquet | 1664712000  | 1664715600  | 2022-10-02 12-1 PM |
| base_file_38.parquet | 1664715600  | 1664719200  | 2022-10-02 1-2 PM  |

For a query to get the number of events between 12PM and 2PM each day in a month for time-of-day analysis, the
predicates look like `DATE_FORMAT(ts, '%Y-%m-%d') between '2022-10-01' and '2022-10-31'`
and `DATE_FORMAT(ts, '%H') between '12' and '13'`. If the data is in a good layout as above, we only need to scan
two files (instead of 24 files) for each day of data, e.g., `base_file_13.parquet` and `base_file_14.parquet` containing
the data for 2022-10-01 12-2 PM.

Currently, such a fine-grained data skipping based on a function on a column cannot be achieved in Hudi, because
transforming the `ts` to the hour of day is not order-preserving, thus the file pruning cannot directly leverage
the file-level column stats of the original column of `ts`. In this case, Hudi has to scan all the files for a
day and push the predicate down when reading parquet files, increasing the amount of data to be scanned.

### Use Case 2: Support for different storage layouts and view partitioning as index

Today, partitioning is mainly viewed as a query optimization technique and partition pruning certainly helps to improve 
query performance. However, if we think about it, partitions are really a storage optimization technique. Partitions
help you organize the data for your convenience, while balancing cloud storage scaling issues (e.g. throttling or having
too many files/objects under one path). From a query optimization perspective, partitions are really just a coarse index. 
We can achieve the same goals as partition pruning with indexes.

In this RFC, we propose how data is partitioned (hive-style, hashed/random prefix for cloud throttling) can be decoupled
from how the data is queried. There can be different layouts:

1. Files are stored under a base path, partitioned hive-style.
2. Files are stored under random prefixes attached to a base path, still hive-style partitioned (RFC-60) e.g. `s3://<
   random-prefix1>path/to/table/partition1=abc`, `s3://<random_prefix2>path/to/table/partition1=xyz`.
3. Files are stored across different buckets completely scattered on cloud storage e.g. `s3://a/b/c/f1`, `s3://x/y/f2`.
4. Partitions can evolve. For instance, you have an old Hive table which is horribly partitioned, can we ensure that the
   new data is not only partitioned well but queries able to efficiently skip data without rewriting the old data
   (although rewriting the data might ultimately be needed for even acceptable query performance on old data).

Consider a case where event logs are a stream of events from microservices and ingested into a raw event table. Each 
event log contains `ts`, a timestamp column, and an associated organization ID (`org_id`). Most queries on the table are
organization specific and fetch logs for a particular time range. A user may attempt to physically partition the data by
both `org_id`and `date(ts)`. If there are 1K organization IDs and one year of data, such a physical partitioning scheme
writes at least `365 days x 1K IDs = 365K` data files under 365K partitions. In most cases, the data can be highly
skewed based on the organizations, with most organizations having less data and a handful of organizations having the
majority of the data, so that there can be many small data files. In such a case, the user may want to evolve the
partitioning by using `org_id` only without rewriting existing data, resulting in the physical layout of data like
below.

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

For the example above, even with the mix of old and new partitioning scheme, we should be able to effectively skip data
based on the range of `datestr` for each file, regardless how the files are stored under different physical partition
paths in the table.

### Use Case 3: Support for different indexes

Expression index framework should be able to work with different index types such as bloom index, column stats, and at
the same time should be extensible enough to support any other secondary index such
as [vector](https://www.pinecone.io/learn/vector-database/) [index]((https://weaviate.io/developers/weaviate/concepts/vector-index))
in the future. If we think about a very simple index on a column, it is kind of a expression index with identity
function `f(X) = X`. It is important to note that these are secondary indexes in the sense they will be stored
separately from the data, and not materialized with the data.

## Goals and Non-Goals

Based on the use cases we plan to support, we set the following goals and non-goals for this RFC.

### Goals

- Modular, easy-to-use indexing subsystem, with full SQL support to manage indexes.
- Absorb partitioning into indexes and aggregate statistics at the storage partition level. 
- Support efficient data skipping with different indexing mechanisms.
- Be engine-agnostic and language-agnostic.

### Non-Goals

- DO NOT remove physical partitioning, which remains as an option for physically storing data in different folders and
  partition pruning. Viewing partitions as yet another index goes beyond the traditional view as pointed in use case 2,
  and we will see how we can support logical partitioning and partition evolution simply with indexes.
- DO NOT tackle the support of using these indexes on the write path in this RFC. That said, we will present a glimpse
  of how that can be done for expression indexes in the Appendix below.
- DO NOT build an expression engine. Building an expression engine in Hudi that unifies query predicate expressions from
  systems like Spark, Presto, and others into a standardized format is a forward-thinking idea. This would centralize
  the interpretation of queries for Hudi tables, leading to a simpler compatibility with multiple query engines.
  However, it is not a pre-requisite for the first cut of expression indexes. Expression engine should be discussed in
  another RFC.

## Design and Implementation

At a high level, **Expression Index** design principles are as follows:

1. User specifies the Expression Index, including the original data column, the expression applying a function on the
   field(s), through **SQL** or Hudi's write config. Indexes can be created or dropped for Hudi tables at any time.
2. While table properties will be the source of truth about what indexes are available, the metadata about each
   expression index is registered in a separate index metadata file, to keep track of the relationship between the data
   column and the expression Index. 
3. Each expression index will be a partition inside the Hudi metadata table (MT).
4. No data is materialized to the data files for the expression index, i.e., the data files do not contain a new data
   column corresponding to the Expression Index, to save storage space.
5. When query engine makes a logical plan for a query, Hudi intercepts the predicate that relates to a expression index,
   looks for the corresponding index in the MT, and applies data skipping based on one or more indexes.
6. In order to be engine-agnostic, the design should not make assumptions about any particular engine.

### Components

We discuss the design and implementation details of each component to realize the Expression Index.

#### SQL

Expression index can be created with the usual `CREATE INDEX` command. The function itself can be specified as `func` property in `OPTIONS`.

```sql
-- PROPOSED SYNTAX WITH FUNCTION KEYWORD --
CREATE INDEX [IF NOT EXISTS] index_name ON [TABLE] table_name
    [USING index_type]
    (column_name1 [OPTIONS(key1=value1, key2=value2, ...)], column_name2 [OPTIONS(key1=value1, key2=value2, ...)], ...)
    [OPTIONS (key1=value1, key2=value2, ...)]
-- Examples --
CREATE INDEX idx_datestr on hudi_table USING column_stats(ts) OPTIONS(expr='from_unixtime', format='yyyy-MM-dd') -- expression index using column stats for DATE_FORMAT(ts, '%Y-%m-%d')
CREATE INDEX last_name_idx ON employees USING column_stats(last_name) (expr='upper'); -- expression index using column stats for UPPER(last_name)
CREATE INDEX city_id_idx ON hudi_table USING bloom_filters (city_id); -- usual bloom filters within metadata table

-- NO CHANGE IN DROP INDEX --
DROP INDEX last_name_idx;
```

`index_name` - Required, should be validated by parser. The name will be used for the partition name in MT.

`index_type` - Optional, `column_stats` (default) if omitted and there are no functions and expressions in the command. Valid
options could be BLOOM_FILTER, RECORD_INDEX, BITMAP, COLUMN_STATS, LUCENE, etc. If `index_type` is not provided, and there are functions or
expressions in the command then a expression index using column stats will be created.

`expression` - simple scalar expressions or sql functions.

#### Expression Index Metadata

For each expression index, store a separate metadata with index details. This should be efficiently loaded. One option
is to store the below metadata in `hoodie.properties`. This way all index metadata can be loaded with the table config.
But, it would be better to not overload the table config. Let `hoodie.properties` still hold the list of indexes (MT
partitions) available, and we propose to create separate `.index_defs` directory, which will be under `basepath/.hoodie`
by default. However, the full path to the index definition will be added to `hoodie.properties`. This has the benefit of
separation of concern and can be cheaply loaded with the metaclient (one extra file i/o only if there is a functional
index in the table config). It can store the following metadata for each index.

* Name of the index
* Expression (e.g., `MONTH(ts)`)
* Data field(s) it's derived from
* Any other configuration or properties

As mentioned above, the path to index definition will be added to `hoodie.properties` as a new config
called `hoodie.table.index.defs.path`.

#### APIs

A new interface, `HoodieExpressionIndex`, is introduced to represent a expression index and track the relationship
between the original data field(s) and the transformation.

```java
/**
 * Interface representing a expression index in Hudi.
 *
 * @param <S> The source type of the values from the fields used in the expression index expression.
 *            Note that this assumes than an expression is operating on fields of same type.
 * @param <T> The target type after applying the transformation. Represents the type of the indexed value.
 */
public interface HoodieExpressionIndex<S, T> {
  /**
   * Get the name of the index.
   * @return Name of the index.
   */
  String getIndexName();

  /**
   * Get the expression associated with the index.
   * @return Expression string.
   */
  String getExpression();

  /**
   * Get the list of fields involved in the expression in order.
   * @return List of fields.
   */
  List<String> getOrderedFieldsInExpression();

  /**
   * Apply the transformation based on the source values and the expression.
   * @param orderedSourceValues List of source values corresponding to fields in the expression.
   * @return Transformed value.
   */
  T apply(List<S> orderedSourceValues);
}

```

As mentioned in the goals of this RFC, we are not going to build an expression engine. To begin with, we can simply
parse `String` for simpler functions and match it to the functional keywords available in spark-sql. We can tackle other
engines as we go. We will probably store the compiled form of expression and expression engine version in the index 
metadata in `.index_defs` directory. This RFC will cover any format changes required to that end.

#### Index in Metadata Table

For each expression index, Hudi needs to populate the index to facilitate data skipping. We reuse the metadata table and
each expression index is stored under a new partition named as `expr_index_<index_name>`. Each record in index is a
key-value and payload will depend on `index_type`. For example, if `index_type` is not provided then key is simply the
values based on function or expression and value is a collection of file paths. If `index_type` is `column_stats` then
the value will be stats based on value derived from the function and key will be hash of file path. 

For an existing table, one can use the async indexer to build the indexes concurrently with ingestion. When data is
written:

1. Use the storage strategy (hive-style, random prefix, etc.) to determine the physical location to write the data (
   which we will do anyway for commit metadata).
2. Update the index i.e. update MT for each index.
    1. Synchronous update: Data table commit is not committed until all indexes are updated.
    2. Async update: Use the [async indexer](https://hudi.apache.org/docs/metadata_indexing) to load the records
       incrementally and update the index.

#### Query Planning & Execution

On receiving a query:

1. Parse the query and index metadata (in `.index_defs`) to determine the relevant expression index is available or not.
2. Lookup (point/range) index to determine:
    * Which storage partitions to scan (using aggregated values from expression index)
    * Which files match expression index predicates
    * Where these files are physically located (irrespective of their storage layout)
3. Use the physical paths derived from metadata to access and read the relevant data.

Let's take an example. Assume that we have a expression index on `DATE_FORMAT(ts, '%Y-%m-%d')` and a query
`SELECT * FROM hudi_tbl where DATE_FORMAT(ts) > 2022-03-01`. The index will be a collection of key-values where keys are
the value after evaluating the expression and values are the list of files. Also, keys are sorted by their natural
order. In this case, the reader will look up the index and find the first key that is greater than `2022-03-01` and 
return the set of files to be scanned.

When the expression index is created using `column_stats` then the index will be a collection of key-values where keys
are the hash of storage partition and file paths and values are the stats of the value after evaluating the expression.
In this case, the reader will look up the `files` index first to prune partitions and then look up the expression index
to skip files.

Below are the concrete steps on how Hudi will intercept predicate in the query and use expression index whenever it can.

1. Identify the relevant expression index based on the predicate and add a new predicate based on the Expression Index
   name if needed. Take the expression index created by
   SQL `CREATE INDEX datestr ON hudi_table USING col_stats (DATE_FORMAT(ts, '%Y-%m-%d'));`

- If the original data column of a expression index is used, transform the predicate to a new predicate based on the
  Expression Index name if possible, e.g., `ts between 1666767600 and 1666897200`
  to `datestr between '2022-10-26' and '2022-10-27'`;
- If the expression of a expression index is used, replace the expression to the expression index name,
  e.g., `DATE_FORMAT(ts, '%Y-%m-%d') between '2022-10-26' and '2022-10-27'`
  to `datestr between '2022-10-26' and '2022-10-27'`;
- If the expression index name is used in the predicate, e.g., `datestr between '2022-10-26' and '2022-10-27'`, no
  action is needed here.

2. For the expression index name appeared in the predicate, look up the corresponding index metadata from metadata table
   and prune files that are not needed for scanning.

In order to use the expression indexes, MT has to be enabled for the readers. Spark, Hive, Presto and Trino can already
do so today. In order to generate splits, we will rely on MT. However, we will need to add some code in query engines to
understand the predicates. For each query engine, we need to intercept the engine-specific representation of the
predicates, and derive the relevant predicate based on the function, e.g. break down `TupleDomain` in Presto/Trino. We
consider integrating Spark and Presto first in the implementation.

#### Handling the Storage Layout Scenarios

Let's try to understand how expression indexes can help with different storage layouts.

1. **Hive-Style Partitioning**: This is straightforward and use the expression index that is similar to `files`.
2. **Random Prefix Storage Partitioning ([RFC-60](https://github.com/apache/hudi/blob/master/rfc/rfc-60/rfc-60.md)),
   attached to a basepath**: We can still use the `files` index provided it is enriched with storage partition mapping
   as mentioned in RFC-60.
3. **Scattered Storage Partitions Across Buckets**: The physical paths in metadata will point to the scattered
   locations. Key will be the partitions that user understands.
4. **Evolved Partitioning**: Let's say data was partitioned by month (yyyy-mm) previously. New data is going to be
   partitioned by day. Key will again be the partition value that user understands. So, metadata for the old data will
   still have key, for instance, `2020-01` and values pointing to a bunch of files. Let's say partitioning was evolved
   on `2023-01-01` and a query with predicate `DATESTR(ts) > '2022-10-01'` . Then the query will scan the set of files
   given by the metadata for keys 2022-10, 2022-11, 2022-12, and each day from 2023-01-01 onwards.

### Design Considerations

1. Metadata table has to be enabled by default for the readers.
2. We are not considering UDFs and complex expressions in the first cut. This needs some more thought and probably an
   expression engine. However, any format changes to be done in order to support UDFs and complex expressions should be
   covered.

## Rollout/Adoption Plan

The expression index will be enabled just like any other index i.e. after the `hoodie.table.metadata.partitions` config 
in `hoodie.properties` is updated after building the index.

## Test Plan

- Unit and functional tests
    - Function can be applied correctly
    - Expression index is populated in metadata table
    - Expected skipping behavior is validated
- Validation of the implementation on the use cases above
    - The workflow should work smoothly

## Appendix

### Expression Index-Based Ordering on Write Path

To unlock efficient data skipping, we can support expression index-based ordering during write operations. Similar to
clustering, the expression index for ordering can be specified by new write config `hoodie.write.sort.columns`.

When writing records to data files, the write client does the following:

- For new inserts, sort the data based on the specified expression index before writing to data files. This reuses
  existing flow of bulk insert and clustering
    - Each data file contains one or a small subset of values corresponding to the expression index.
    - Different sort modes can be provided like bulk insert and clustering.

- For updates, no change in logic is needed. The record with the same key should go into the same file group.
- For small file handing, we need to take into account the expression index for the workload profile, so that the new
  inserts can be combined into the data file without expanding the minimum and maximum values of the expression index when
  the data file is small.

Additionally, when using SQL, users can add hints to specify the expression index for ordering, e.g.,

```sql
INSERT INTO TABLE hudi_table
SELECT /*+ ORDER BY datestr */ * FROM source_table;
```

### Other Systems

In this section, we compare Hudi's Expression Index to related concepts and solutions on pruning files efficiently and
improving query performance.

#### Hidden Partitioning in Apache Iceberg

[Hidden Partitioning](https://iceberg.apache.org/docs/latest/partitioning/#icebergs-hidden-partitioning), introduced by
Apache Iceberg, maintains the relationship between the original column and transformed partition values, and hides the
partition columns and physical partitions from catalogs and users, using manifests containing the partition values of
each file to prune files and speed up queries. By using Hidden Partitioning, Iceberg can evolve partition schemes, i.e.,
partition evolution, without rewriting data.

Hudi's Expression Index achieves the same goal of pruning files based on the column stats, with the support on any data
column or function defined on a data column, without coupling with the partition scheme.

#### Index on Expression in PostgreSQL

[Index on Expression](https://www.postgresql.org/docs/current/indexes-expressional.html),
or [Function-Based Indexes](https://oracle-base.com/articles/8i/function-based-indexes) a concept in RDBMS, provides the
index on a function or scalar expression computed from one or more columns of the table. Such an index can speed up the
queries containing the expression in the predicates. The index on expression is a secondary index, meaning that it is
stored separately from the record data. There is no materialization of the values from the expression in the data area.
PostgreSQL supports Index on Expression and Oracle database supports Function-Based Indexes.

Hudi's Expression Index applies similar concept which allows a new index to be built based on a function defined on 
field(s) at any time for data skipping.

#### Generated Column in Delta Lake

A [Generated Column](https://www.postgresql.org/docs/15/ddl-generated-columns.html) is a special column that is always
computed from other columns. There are two types of generated column: (1) stored generated column: written alongside
other columns and persisted to storage, (2) virtual generated column: computed when read and occupies no storage.
PostgreSQL and Delta Lake support stored generated column only. As data of stored generated column is persisted to
storage, it can be used as a normal column for partitioning or creating an index.

In Delta Lake, a [generated column](https://docs.delta.io/latest/delta-batch.html#use-generated-columns) can be used as
a partition column. When doing so, Delta Lake looks at the relationship between the base column and the generated
column, and populates partition filters based on the generated partition column if possible.

Hudi's Expression Index is similar to virtual generated column, but the Expression Index is not queryable except for the
usage in the predicates.

#### Micro Partitions in Snowflake

[Micro Partitions](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions.html), used by Snowflake,
automatically divide data into contiguous units of storage with each micro-partition containing between 50 MB and 500 MB
of uncompressed data, without the need of static partitioning. Snowflake stores metadata of each micro-partition,
including the range of values for each of the columns. In this way, query pruning based on the column stats metadata can
be efficient, if there are no or little overlapping among micro partitions in terms of the column value range. The
degree of overlapping is reduced through data clustering.

Hudi's Expression Index depends on optimized storage layout for efficient file pruning, which can be achieved through
clustering and proposed Index-Function-based ordering during write operations.
