---
title: SQL Queries
summary: "In this page, we go over querying Hudi tables using SQL"
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Hudi stores and organizes data once on storage while providing different ways of [querying](/docs/concepts#query-types), across a wide range of query engines. 
This page will show how to issue different queries and discuss specific instructions for each query engine.

## Open 

- Once the proper hudi bundle has been installed, the table can be queried by popular query engines like Hive, Spark SQL, Spark Datasource API and PrestoDB.
- Thanks to Hudi's support for record level change streams, these incremental pipelines often offer 10x efficiency over batch counterparts by only processing the changed records.


## Spark SQL

The Spark [quickstart](/docs/quick-start-guide) provides a good overview of how to use Spark SQL to query Hudi tables. This section will go into more advanced configurations and functionalities.

### Snapshot Query 
Snapshot queries are the most common query type for Hudi tables. Spark SQL supports snapshot queries on both COPY_ON_WRITE and MERGE_ON_READ tables.
Using session properties, you can specify various options around data skipping and indexing to optimize query performance, as shown below.

```sql
-- Show how to turn on data skipping

-- Show how to turn on record index on query path.

```

### Time Travel Query

TODO: Document behavior with concurrent writers. and how it's tied to cleaning/retention. 

```sql 
-- Syntax for incremental queries

```

### Incremental Query

Incremental queries are useful when you want to process only the changed records since the last time you processed the table.

```sql 
-- Syntax for incremental queries

```

TODO: Document how they provide efficiency by amortizing the costs of compactions, how Hudi is the only system that can support this today, 
due to 


:::note Integration with Spark 
Users are encouraged to migrate to Hudi versions > 0.12.x, for the best spark experience and discouraged from using any older approaches
using path filters. We expect that native integration with Spark's optimized table readers along with Hudi's automatic table 
management will yield great performance benefits in those versions.
:::

Please refer to [configurations](/docs/basic_configurations) section for the important configuration options.

## Flink SQL
Once the Flink Hudi tables have been registered to the Flink catalog, they can be queried using the Flink SQL. It supports all query types across both Hudi table types,
relying on the custom Hudi input formats like Hive. Typically notebook users and Flink SQL CLI users leverage flink sql for querying Hudi tables. Please add hudi-flink-bundle as described in the [Flink Quickstart](/docs/flink-quick-start-guide).


### Snapshot Query 
By default, Flink SQL will try to use its optimized native readers (for e.g. reading parquet files) instead of Hive SerDes.
Additionally, partition pruning is applied by Flink if a partition predicate is specified in the filter. Filters push down may not be supported yet (please check Flink roadmap).

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `metadata.enabled` | `false` | false | Set to `true` to enable |
| `read.data.skipping.enabled` | `false` | false | Whether to enable data skipping for batch snapshot read, by default disabled |
| `hoodie.metadata.index.column.stats.enable` | `false` | false | Whether to enable column statistics (max/min) |
| `hoodie.metadata.index.column.stats.column.list` | `false` | N/A | Columns(separated by comma) to collect the column statistics  |

### Streaming Query
By default, the hoodie table is read as batch, that is to read the latest snapshot data set and returns. Turns on the streaming read
mode by setting option `read.streaming.enabled` as `true`. Sets up option `read.start-commit` to specify the read start offset, specifies the
value as `earliest` if you want to consume all the history data set.

```sql
-- Show an example query.
```

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `read.streaming.enabled` | false | `false` | Specify `true` to read as streaming |
| `read.start-commit` | false | the latest commit | Start commit time in format 'yyyyMMddHHmmss', use `earliest` to consume from the start commit |
| `read.streaming.skip_compaction` | false | `false` | Whether to skip compaction instants for streaming read, generally for two purpose: 1) Avoid consuming duplications from compaction instants created for created by Hudi versions < 0.11.0 or when `hoodie.compaction.preserve.commit.metadata` is disabled 2) When change log mode is enabled, to only consume change for right semantics. |
| `clean.retain_commits` | false | `10` | The max number of commits to retain before cleaning, when change log mode is enabled, tweaks this option to adjust the change log live time. For example, the default strategy keeps 50 minutes of change logs if the checkpoint interval is set up as 5 minutes. |

:::note
Users are encouraged to use Hudi versions > 0.12.3, for the best experience and discouraged from using any older versions.
Specifically, `read.streaming.skip_compaction` should only be enabled if the MOR table is compacted by Hudi with versions `< 0.11.0`.
This is so as the `hoodie.compaction.preserve.commit.metadata` feature is only introduced in Hudi versions `>=0.11.0`.
Older versions will overwrite the original commit time for each row with the compaction plan's instant time and cause 
row-level instant range checks to not work properly.
:::

### Incremental Query
There are 3 use cases for incremental query:
1. Streaming query: specify the start commit with option `read.start-commit`;
2. Batch query: specify the start commit with option `read.start-commit` and end commit with option `read.end-commit`,
   the interval is a closed one: both start commit and end commit are inclusive;
3. Time Travel: consume as batch for an instant time, specify the `read.end-commit` is enough because the start commit is latest by default.

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `read.start-commit` | `false` | the latest commit | Specify `earliest` to consume from the start commit |
| `read.end-commit` | `false` | the latest commit | -- |

### Catalog
A Hudi catalog can manage the tables created by Flink, table metadata is persisted to avoid redundant table creation.
The catalog in `hms` mode will supplement the Hive syncing parameters automatically.

A SQL demo for Catalog SQL in hms mode:

```sql
CREATE CATALOG hoodie_catalog
WITH (
  'type'='hudi',
  'catalog.path' = '${catalog root path}', -- only valid if the table options has no explicit declaration of table path
  'hive.conf.dir' = '${dir path where hive-site.xml is located}',
  'mode'='hms' -- also support 'dfs' mode so that all the table metadata are stored with the filesystem
);
```

#### Options
| Option Name        | Required | Default   | Remarks                                                                                                                |
|--------------------|----------|-----------|------------------------------------------------------------------------------------------------------------------------|
| `catalog.path`     | `true`   | --        | Default catalog root path, it is used to infer a full table path in format: `${catalog.path}/${db_name}/${table_name}` |
| `default-database` | `false`  | `default` | Default database name                                                                                                  |
| `hive.conf.dir`    | `false`  | --        | Directory where hive-site.xml is located, only valid in `hms` mode                                                     |
| `mode`             | `false`  | `dfs`     | Specify as `hms` to keep the table metadata with Hive metastore                                                        |
| `table.external`   | `false`  | `false`   | Whether to create external tables, only valid under `hms` mode                                                         |

## Hive

[Hive](https://hive.apache.org/) has support for snapshot and incremental queries (with limitations) on Hudi tables.

In order for Hive to recognize Hudi tables and query correctly, the `hudi-hadoop-mr-bundle-<hudi.version>.jar` needs to be 
provided to Hive2Server [aux jars path](https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cm_mc_hive_udf.html#concept_nc3_mms_lr), as well as 
additionally, the bundle needs to be put on the hadoop/hive installation across the cluster. In addition to setup above, for beeline cli access, 
the `hive.input.format` variable needs to be set to the fully qualified path name of the inputformat `org.apache.hudi.hadoop.HoodieParquetInputFormat`. 
For Tez, additionally, the `hive.tez.input.format` needs to be set to `org.apache.hadoop.hive.ql.io.HiveInputFormat`. 

Then users should be able to issue snapshot queries against the table like any other Hive table.


### Incremental Query 

```sql
 -- Show example from docker demo.
```

:::note Hive incremental queries that are executed using Fetch task
Since Hive Fetch tasks invoke InputFormat.listStatus() per partition, metadata can be listed in
every such listStatus() call. In order to avoid this, it might be useful to disable fetch tasks
using the hive session property for incremental queries: `set hive.fetch.task.conversion=none;` This
would ensure Map Reduce execution is chosen for a Hive query, which combines partitions (comma
separated) and calls InputFormat.listStatus() only once with all those partitions.
:::

## Presto

[Presto](https://prestodb.io/) is a popular query engine for interactive query performance. Support for querying Hudi tables using PrestoDB is offered
via two connectors - Hive connector and Hudi connector (Presto version 0.275 onwards). Both connectors currently support snapshot queries on
COPY_ON_WRITE tables and snapshot and read optimized queries on MERGE_ON_READ Hudi tables.

Since PrestoDB-Hudi integration has evolved over time, the installation instructions for PrestoDB would vary based on versions. 
Please check the below table for query types supported and installation instructions. 

| **PrestoDB Version** | **Installation description** | **Query types supported** |
|----------------------|------------------------------|---------------------------|
| < 0.233              | Requires the `hudi-presto-bundle` jar to be placed into `<presto_install>/plugin/hive-hadoop2/`, across the installation. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| > = 0.233             | No action needed. Hudi (0.5.1-incubating) is a compile time dependency. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| > = 0.240             | No action needed. Hudi 0.5.3 version is a compile time dependency. | Snapshot querying on both COW and MOR tables. |
| > = 0.268             | No action needed. Hudi 0.9.0 version is a compile time dependency. | Snapshot querying on bootstrap tables. |
| > = 0.272             | No action needed. Hudi 0.10.1 version is a compile time dependency. | File listing optimizations. Improved query performance. |
| > = 0.275             | No action needed. Hudi 0.11.0 version is a compile time dependency. | All of the above. Native Hudi connector that is on par with Hive connector. |


:::note
Incremental queries and point in time queries are not supported either through the Hive connector or Hudi
connector. However, it is in our roadmap, and you can track the development
under [HUDI-3210](https://issues.apache.org/jira/browse/HUDI-3210).
:::

To use the Hudi connector, please configure hudi catalog in ` /presto-server-0.2xxx/etc/catalog/hudi.properties` as follows:

```properties
connector.name=hudi
hive.metastore.uri=thrift://xxx.xxx.xxx.xxx:9083
hive.config.resources=.../hadoop-2.x/etc/hadoop/core-site.xml,.../hadoop-2.x/etc/hadoop/hdfs-site.xml
```

To learn more about the usage of Hudi connector, please read [prestodb documentation](https://prestodb.io/docs/current/connector/hudi.html).

## Trino

Similar to PrestoDB, Trino allows querying Hudi tables via either the [Hive](https://trino.io/docs/current/connector/hive.html) connector or 
the native [Hudi](https://trino.io/docs/current/connector/hudi.html) connector (introduced in version 398). For Trino version 411 or newer, 
the Hive connector redirects to the Hudi catalog for Hudi table reads. Ensure you configure the necessary settings for 
table redirection when using the Hive connector on these versions.

```properties
hive.hudi-catalog-name=hudi
```

:::note Installation instructions
We recommend using `hudi-trino-bundle` version 0.12.2 or later for optimal query performance with Hive connector. Table below
summarizes how the support for Hudi is achieved across different versions of Trino.

| **Trino Version** | **Installation description** | **Query types supported** |
|-------------------|------------------------------|---------------------------|
| < 398             | NA - can only use Hive connector to query Hudi tables | Same as that of Hive connector version < 406. |
| > = 398           | NA - no need to place bundle jars manually, as they are compile-time dependency | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| < 406             | `hudi-trino-bundle` jar to be placed into `<trino_install>/plugin/hive` | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| > = 406           | `hudi-trino-bundle` jar to be placed into `<trino_install>/plugin/hive` | Snapshot querying on COW tables. Read optimized querying on MOR tables. **Redirection to Hudi catalog also supported.** |
| > = 411           | NA | Snapshot querying on COW tables. Read optimized querying on MOR tables. Hudi tables can be **only** queried by [table redirection](https://trino.io/docs/current/connector/hive.html#table-redirection). |
:::


For details on the Hudi connector, see the [connector documentation](https://trino.io/docs/current/connector/hudi.html). 
Both connectors offer 'Snapshot' queries for COW tables and 'Read Optimized' queries for MOR tables. 
Support for [MOR table snapshot queries](https://github.com/trinodb/trino/pull/14786) is anticipated shortly.

## Impala 

Impala (versions > 3.4) is able to query Hudi Copy-on-write tables as an [EXTERNAL TABLES](https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_tables.html#external_tables).

To create a Hudi read optimized table on Impala:
```sql
CREATE EXTERNAL TABLE database.table_name
LIKE PARQUET '/path/to/load/xxx.parquet'
STORED AS HUDIPARQUET
LOCATION '/path/to/load';
```

Impala is able to take advantage of the partition pruning to improve the query performance, using traditional Hive style partitioning.
To create a partitioned table, the folder should follow the naming convention like `year=2020/month=1`.

To create a partitioned Hudi table on Impala:
```
CREATE EXTERNAL TABLE database.table_name
LIKE PARQUET '/path/to/load/xxx.parquet'
PARTITION BY (year int, month int, day int)
STORED AS HUDIPARQUET
LOCATION '/path/to/load';
ALTER TABLE database.table_name RECOVER PARTITIONS;
```
After Hudi made a new commit, refresh the Impala table to get the latest snapshot exposed to queries.
```
REFRESH database.table_name
```

## Redshift Spectrum

Copy on Write Tables in Apache Hudi versions 0.5.2, 0.6.0, 0.7.0, 0.8.0, 0.9.0, 0.10.x and 0.11.x can be queried
via Amazon Redshift Spectrum external tables. To be able to query Hudi versions 0.10.0 and above please try latest
versions of Redshift.
:::note
Hudi tables are supported only when AWS Glue Data Catalog is used. It's not supported when you use an Apache
Hive metastore as the external catalog.
:::

Please refer
to [Redshift Spectrum Integration with Apache Hudi](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html#c-spectrum-column-mapping-hudi)
for more details.

## Doris

Copy on Write Tables in Hudi version 0.10.0 can be queried via Doris external tables starting from Doris version 1.1.
Please refer
to [Doris Hudi external table](https://doris.apache.org/docs/ecosystem/external-table/hudi-external-table/ )
for more details on the setup.

:::note
The current default supported version of Hudi is 0.10.0 and has not been tested in other versions. More versions
will be supported in the future.
:::

## StarRocks

Copy on Write tables in Apache Hudi 0.10.0 and above can be queried via StarRocks external tables from StarRocks version
2.2.0. Only snapshot queries are supported currently. In future releases Merge on Read tables will also be supported.
Please refer to [StarRocks Hudi external table](https://docs.starrocks.io/en-us/latest/using_starrocks/External_table#hudi-external-table)
for more details on the setup.

## ClickHouse

[ClickHouse](https://clickhouse.com/docs/en/intro) is a column-oriented database for online analytical processing. It
provides a read-only integration with Copy on Write Hudi tables. To query such Hudi tables, first we need
to create a table in Clickhouse using `Hudi` [table function](https://clickhouse.com/docs/en/sql-reference/table-functions/hudi).

```
CREATE TABLE hudi_table
    ENGINE = Hudi(s3_base_path, [aws_access_key_id, aws_secret_access_key,])
```

Please refer [Clickhouse docs](https://clickhouse.com/docs/en/engines/table-engines/integrations/hudi/) for more
details.

## Support Matrix

Following tables show whether a given query is supported on specific query engine.

### Copy-On-Write tables

| Query Engine          |Snapshot Queries|Incremental Queries|
|-----------------------|--------|-----------|
| **Hive**              |Y|Y|
| **Spark SQL**         |Y|Y|
| **Flink SQL**         |Y|N|
| **PrestoDB**          |Y|N|
| **Trino**             |Y|N|
| **AWS Athena**        |Y|N|
| **BigQuery**          |Y|N|
| **Impala**            |Y|N|
| **Redshift Spectrum** |Y|N|
| **Doris**             |Y|N|
| **StarRocks**         |Y|N|
| **ClickHouse**        |Y|N|

### Merge-On-Read tables

| Query Engine        |Snapshot Queries|Incremental Queries|Read Optimized Queries|
|---------------------|--------|-----------|--------------|
| **Hive**            |Y|Y|Y|
| **Spark SQL**       |Y|Y|Y|
| **Spark Datasource** |Y|Y|Y|
| **Flink SQL**       |Y|Y|Y|
| **PrestoDB**        |Y|N|Y|
| **AWS Athena**      |Y|N|Y|
| **Big Query**       |Y|N|Y|
| **Trino**           |N|N|Y|
| **Impala**          |N|N|Y|
| **Redshift Spectrum** |N|N|N|
| **Doris**           |N|N|N|
| **StarRocks**       |N|N|N|
| **ClickHouse**      |N|N|N|


