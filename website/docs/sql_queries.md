---
title: SQL Queries
summary: "In this page, we go over querying Hudi tables using SQL"
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Conceptually, Hudi stores data physically once on DFS, while providing 3 different ways of querying, as explained [before](/docs/concepts#query-types).
Once the table is synced to the Hive metastore, it provides external Hive tables backed by Hudi's custom inputformats. Once the proper hudi
bundle has been installed, the table can be queried by popular query engines like Hive, Spark SQL, Spark Datasource API and PrestoDB.

In this page, we will show how to issues different types of queries on Hudi tables, across a wide range of query engines. 
We will also discuss specific setup to access different query types from different query engines.


## Spark SQL

For examples, refer to [Incremental Queries](/docs/quick-start-guide#incremental-query) in the Spark quickstart.
Please refer to [configurations](/docs/configurations#SPARK_DATASOURCE) section, to view all datasource options.

Additionally, `HoodieReadClient` offers the following functionality using Hudi's implicit indexing.

| **API** | **Description** |
|-------|--------|
| read(keys) | Read out the data corresponding to the keys as a DataFrame, using Hudi's own index for faster lookup |
| filterExists() | Filter out already existing records from the provided `RDD[HoodieRecord]`. Useful for de-duplication |
| checkExists(keys) | Check if the provided keys exist in a Hudi table |

Once the Hudi tables have been registered to the Hive metastore, they can be queried using the Spark-Hive integration.
By default, Spark SQL will try to use its own parquet reader instead of Hive SerDe when reading from Hive metastore parquet tables.
The following are important settings to consider when querying COPY_ON_WRITE or MERGE_ON_READ tables.

#### Copy On Write tables
For COPY_ON_WRITE tables, Spark's default parquet reader can be used to retain Sparks built-in optimizations for reading parquet files like vectorized reading on Hudi Hive tables.
If using the default parquet reader, a path filter needs to be pushed into sparkContext as follows.

```scala
spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter], classOf[org.apache.hadoop.fs.PathFilter]);
```

Thanks to Hudi's support for record level change streams, these incremental pipelines often offer 10x efficiency over batch counterparts by only processing the changed records.



#### Merge On Read tables
No special configurations are needed for querying MERGE_ON_READ tables with Hudi version 0.9.0+

If you are querying MERGE_ON_READ tables using Hudi version <= 0.8.0, you need to turn off the SparkSQL default parquet reader by setting: `spark.sql.hive.convertMetastoreParquet=false`.

```java
$ spark-shell --driver-class-path /etc/hive/conf  --packages org.apache.hudi:hudi-spark-bundle_2.11:0.5.3,org.apache.spark:spark-avro_2.11:2.4.4 --conf spark.sql.hive.convertMetastoreParquet=false

scala> sqlContext.sql("select count(*) from hudi_trips_mor_rt where datestr = '2016-10-02'").show()
scala> sqlContext.sql("select count(*) from hudi_trips_mor_rt where datestr = '2016-10-02'").show()
```
:::note
Note: COPY_ON_WRITE tables can also still be read if you turn off the default parquet reader.
:::

## Flink SQL
Once the flink Hudi tables have been registered to the Flink catalog, it can be queried using the Flink SQL. It supports all query types across both Hudi table types,
relying on the custom Hudi input formats again like Hive. Typically notebook users and Flink SQL CLI users leverage flink sql for querying Hudi tables. Please add hudi-flink-bundle as described in the [Flink Quickstart](/docs/flink-quick-start-guide).

By default, Flink SQL will try to use its own parquet reader instead of Hive SerDe when reading from Hive metastore parquet tables.

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

./bin/sql-client.sh embedded -j .../hudi-flink-bundle_2.1?-*.*.*.jar shell
```

```sql
-- this defines a COPY_ON_WRITE table named 't1'
CREATE TABLE t1(
  uuid VARCHAR(20), -- you can use 'PRIMARY KEY NOT ENFORCED' syntax to specify the field as record key
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'table_base+path'
);

-- query the data
select * from t1 where `partition` = 'par1';
```

Flink's built-in support parquet is used for both COPY_ON_WRITE and MERGE_ON_READ tables,
additionally partition prune is applied by Flink engine internally if a partition path is specified
in the filter. Filters push down is not supported yet (already on the roadmap).

For MERGE_ON_READ table, in order to query hudi table as a streaming, you need to add option `'read.streaming.enabled' = 'true'`,
when querying the table, a Flink streaming pipeline starts and never ends until the user cancel the job manually.
You can specify the start commit with option `read.start-commit` and source monitoring interval with option
`read.streaming.check-interval`.

### Streaming Query
By default, the hoodie table is read as batch, that is to read the latest snapshot data set and returns. Turns on the streaming read
mode by setting option `read.streaming.enabled` as `true`. Sets up option `read.start-commit` to specify the read start offset, specifies the
value as `earliest` if you want to consume all the history data set.

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `read.streaming.enabled` | false | `false` | Specify `true` to read as streaming |
| `read.start-commit` | false | the latest commit | Start commit time in format 'yyyyMMddHHmmss', use `earliest` to consume from the start commit |
| `read.streaming.skip_compaction` | false | `false` | Whether to skip compaction instants for streaming read, generally for two purpose: 1) Avoid consuming duplications from compaction instants created for created by Hudi versions < 0.11.0 or when `hoodie.compaction.preserve.commit.metadata` is disabled 2) When change log mode is enabled, to only consume change for right semantics. |
| `clean.retain_commits` | false | `10` | The max number of commits to retain before cleaning, when change log mode is enabled, tweaks this option to adjust the change log live time. For example, the default strategy keeps 50 minutes of change logs if the checkpoint interval is set up as 5 minutes. |

:::note
When option `read.streaming.skip_compaction` is enabled and the streaming reader lags behind by commits of number
`clean.retain_commits`, data loss may occur.

The compaction table service action preserves the original commit time for each row. When iterating through the parquet files,
the streaming reader will perform a check on whether the row's commit time falls within the specified instant range to
skip over rows that have been read before.

For efficiency, option `read.streaming.skip_compaction` can be enabled to skip reading of parquet files entirely.
:::

:::note
`read.streaming.skip_compaction` should only be enabled if the MOR table is compacted by Hudi with versions `< 0.11.0`.

This is so as the `hoodie.compaction.preserve.commit.metadata` feature is only introduced in Hudi versions `>=0.11.0`.
Older versions will overwrite the original commit time for each row with the compaction plan's instant time.

This will render Hudi-on-Flink's stream reader's row-level instant-range checks to not work properly.
When the original instant time is overwritten with a newer instant time, the stream reader will not be able to
differentiate rows that have already been read before with actual new rows.
:::

### Incremental Query
There are 3 use cases for incremental query:
1. Streaming query: specify the start commit with option `read.start-commit`;
2. Batch query: specify the start commit with option `read.start-commit` and end commit with option `read.end-commit`,
   the interval is a closed one: both start commit and end commit are inclusive;
3. TimeTravel: consume as batch for an instant time, specify the `read.end-commit` is enough because the start commit is latest by default.

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `read.start-commit` | `false` | the latest commit | Specify `earliest` to consume from the start commit |
| `read.end-commit` | `false` | the latest commit | -- |

### Metadata Table
The metadata table holds the metadata index per hudi table, it holds the file list and all kinds of indexes that we called multi-model index.
Current these indexes are supported:

1. partition -> files mapping
2. column max/min statistics for each file
3. bloom filter for each file

The partition -> files mappings can be used for fetching the file list for writing/reading path, it is cost friendly for object storage that charges per visit,
for HDFS, it can ease the access burden of the NameNode.

The column max/min statistics per file is used for query acceleration, in the writing path, when enable this feature, hudi would
book-keep the max/min values for each column in real-time, thus would decrease the writing throughput. In the reading path, hudi uses
this statistics to filter out the useless files first before scanning.

The bloom filter index is currently only used for spark bloom filter index, not for query acceleration yet.

In general, enable the metadata table would increase the commit time, it is not very friendly for the use cases for very short checkpoint interval (say 30s).
And for these use cases you should test the stability first.

#### Options
|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| `metadata.enabled` | `false` | false | Set to `true` to enable |
| `read.data.skipping.enabled` | `false` | false | Whether to enable data skipping for batch snapshot read, by default disabled |
| `hoodie.metadata.index.column.stats.enable` | `false` | false | Whether to enable column statistics (max/min) |
| `hoodie.metadata.index.column.stats.column.list` | `false` | N/A | Columns(separated by comma) to collect the column statistics  |

### Hudi Catalog
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

In order for Hive to recognize Hudi tables and query correctly,

- the HiveServer2 needs to be provided with the `hudi-hadoop-mr-bundle-<hudi.version>.jar` in
  its [aux jars path](https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cm_mc_hive_udf.html#concept_nc3_mms_lr)
  . This will ensure the input format classes with their dependencies are available for query planning & execution.
- For MERGE_ON_READ tables, additionally, the bundle needs to be put on the hadoop/hive installation across the cluster,
  so that queries can pick up the custom RecordReader as well.

In addition to setup above, for beeline cli access, the `hive.input.format` variable needs to be set to the fully
qualified path name of the inputformat `org.apache.hudi.hadoop.HoodieParquetInputFormat`. For Tez, additionally,
the `hive.tez.input.format` needs to be set to `org.apache.hadoop.hive.ql.io.HiveInputFormat`. Then proceed to query the
table like any other Hive table.

### Incremental query
`HiveIncrementalPuller` allows incrementally extracting changes from large fact/dimension tables via HiveQL, combining the benefits of Hive (reliably process complex SQL queries) and
incremental primitives (speed up querying tables incrementally instead of scanning fully). The tool uses Hive JDBC to run the hive query and saves its results in a temp table.
that can later be upserted. Upsert utility (`HoodieStreamer`) has all the state it needs from the directory structure to know what should be the commit time on the target table.
e.g: `/app/incremental-hql/intermediate/{source_table_name}_temp/{last_commit_included}`.The Delta Hive table registered will be of the form `{tmpdb}.{source_table}_{last_commit_included}`.

The following are the configuration options for HiveIncrementalPuller

| **Config** | **Description** | **Default** |
|-------|--------|--------|
|hiveUrl| Hive Server 2 URL to connect to |  |
|hiveUser| Hive Server 2 Username |  |
|hivePass| Hive Server 2 Password |  |
|queue| YARN Queue name |  |
|tmp| Directory where the temporary delta data is stored in DFS. The directory structure will follow conventions. Please see the below section.  |  |
|extractSQLFile| The SQL to execute on the source table to extract the data. The data extracted will be all the rows that changed since a particular point in time. |  |
|sourceTable| Source Table Name. Needed to set hive environment properties. |  |
|sourceDb| Source DB name. Needed to set hive environment properties.| |
|targetTable| Target Table Name. Needed for the intermediate storage directory structure.  |  |
|targetDb| Target table's DB name.| |
|tmpdb| The database to which the intermediate temp delta table will be created | hoodie_temp |
|fromCommitTime| This is the most important parameter. This is the point in time from which the changed records are queried from.  |  |
|maxCommits| Number of commits to include in the query. Setting this to -1 will include all the commits from fromCommitTime. Setting this to a value > 0, will include records that ONLY changed in the specified number of commits after fromCommitTime. This may be needed if you need to catch up say 2 commits at a time. | 3 |
|help| Utility Help |  |


Setting fromCommitTime=0 and maxCommits=-1 will fetch the entire source table and can be used to initiate backfills. If the target table is a Hudi table,
then the utility can determine if the target table has no commits or is behind more than 24 hour (this is configurable),
it will automatically use the backfill configuration, since applying the last 24 hours incrementally could take more time than doing a backfill. The current limitation of the tool
is the lack of support for self-joining the same table in mixed mode (snapshot and incremental modes).

**NOTE on Hive incremental queries that are executed using Fetch task:**
Since Fetch tasks invoke InputFormat.listStatus() per partition, Hoodie metadata can be listed in
every such listStatus() call. In order to avoid this, it might be useful to disable fetch tasks
using the hive session property for incremental queries: `set hive.fetch.task.conversion=none;` This
would ensure Map Reduce execution is chosen for a Hive query, which combines partitions (comma
separated) and calls InputFormat.listStatus() only once with all those partitions.

## PrestoDB

PrestoDB is a popular query engine, providing interactive query performance. One can use both the Hive or Hudi connector (
Presto version 0.275 onwards) for querying Hudi tables. Both connectors currently support snapshot querying on
COPY_ON_WRITE tables, and snapshot and read optimized queries on MERGE_ON_READ Hudi tables.

Since PrestoDB-Hudi integration has evolved over time, the installation instructions for PrestoDB would vary based on
versions. Please check the below table for query types supported and installation instructions for different versions of
PrestoDB.

| **PrestoDB Version** | **Installation description** | **Query types supported** |
|----------------------|------------------------------|---------------------------|
| < 0.233              | Requires the `hudi-presto-bundle` jar to be placed into `<presto_install>/plugin/hive-hadoop2/`, across the installation. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| > = 0.233             | No action needed. Hudi (0.5.1-incubating) is a compile time dependency. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| > = 0.240             | No action needed. Hudi 0.5.3 version is a compile time dependency. | Snapshot querying on both COW and MOR tables. |
| > = 0.268             | No action needed. Hudi 0.9.0 version is a compile time dependency. | Snapshot querying on bootstrap tables. |
| > = 0.272             | No action needed. Hudi 0.10.1 version is a compile time dependency. | File listing optimizations. Improved query performance. |
| > = 0.275             | No action needed. Hudi 0.11.0 version is a compile time dependency. | All of the above. Native Hudi connector that is on par with Hive connector. |

To learn more about the usage of Hudi connector, please
checkout [prestodb documentation](https://prestodb.io/docs/current/connector/hudi.html).

:::note
Incremental queries and point in time queries are not supported either through the Hive connector or Hudi
connector. However, it is in our roadmap, and you can track the development
under [HUDI-3210](https://issues.apache.org/jira/browse/HUDI-3210).
:::

### Presto Environment

1. Configure Presto according to
   the [Presto configuration document](https://prestodb.io/docs/current/installation/deployment.html).
2. Configure hive catalog in ` /presto-server-0.2xxx/etc/catalog/hive.properties` as follows:

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://xxx.xxx.xxx.xxx:9083
hive.config.resources=.../hadoop-2.x/etc/hadoop/core-site.xml,.../hadoop-2.x/etc/hadoop/hdfs-site.xml
```

3. Alternatively, configure hudi catalog in ` /presto-server-0.2xxx/etc/catalog/hudi.properties` as follows:

```properties
connector.name=hudi
hive.metastore.uri=thrift://xxx.xxx.xxx.xxx:9083
hive.config.resources=.../hadoop-2.x/etc/hadoop/core-site.xml,.../hadoop-2.x/etc/hadoop/hdfs-site.xml
```

### Query

Beginning query by connecting hive metastore with presto client. The presto client connection command is as follows:

```bash
# The presto client connection command where <catalog_name> is either hudi or hive,
# and <schema_name> is the database name used in hive sync.
./presto --server xxx.xxx.xxx.xxx:9999 --catalog <catalog_name> --schema <schema_name>
```

## Trino
Just like PrestoDB, there are two ways to query Hudi tables using Trino i.e. either using [Hive](https://trino.io/docs/current/connector/hive.html) connector or the native
[Hudi](https://trino.io/docs/current/connector/hudi.html) connector (available since version 398). However, since version 411, Hive connector redirects to Hudi catalog for reading Hudi tables.

### Hive Connector

| **Trino Version** | **Installation description** | **Query types supported** |
|-------------------|------------------------------|---------------------------|
| < 406             | Requires the `hudi-trino-bundle` jar to be placed into `<trino_install>/plugin/hive` | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| > = 406           | Requires the `hudi-trino-bundle` jar to be placed into `<trino_install>/plugin/hive` | Snapshot querying on COW tables. Read optimized querying on MOR tables. **Redirection to Hudi catalog also supported.** |
| > = 411           | NA | Snapshot querying on COW tables. Read optimized querying on MOR tables. Hudi tables can be **only** queried by [table redirection](https://trino.io/docs/current/connector/hive.html#table-redirection). |

If you are using Trino version 411 or greater, and also using Hive connector to query Hudi tables, please set the below config to support table redirection.
```
hive.hudi-catalog-name=hudi
```
It is recommended to use `hudi-trino-bundle` version 0.12.2 or later for optimal query performance with Hive connector.

### Hudi Connector

| **Trino Version** | **Installation description** | **Query types supported** |
|-------------------|------------------------------|---------------------------|
| < 398             | NA - can only use Hive connector to query Hudi tables | Same as that of Hive connector version < 406. |
| > = 398           | NA - no need to place bundle jars manually, as they are compile-time dependency | Snapshot querying on COW tables. Read optimized querying on MOR tables. |

To learn more about the usage of Hudi connector, please check out
the [connector documentation](https://trino.io/docs/current/connector/hudi.html). Both the connectors are on par in
terms of query support, i.e. 'Snapshot' queries for COW tables and 'Read Optimized' queries for MOR
tables. We have an active [PR](https://github.com/trinodb/trino/pull/16034) under review to bring the performance of
Hudi connector on par with Hive connector. Furthermore, we
expect [MOR table snapshot query](https://github.com/trinodb/trino/pull/14786) support will soon be added to the Hudi
connector.

## Impala (3.4 or later)

### Snapshot Query

Impala is able to query Hudi Copy-on-write table as an [EXTERNAL TABLE](https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_tables.html#external_tables) on HDFS.

To create a Hudi read optimized table on Impala:
```
CREATE EXTERNAL TABLE database.table_name
LIKE PARQUET '/path/to/load/xxx.parquet'
STORED AS HUDIPARQUET
LOCATION '/path/to/load';
```
Impala is able to take advantage of the physical partition structure to improve the query performance.
To create a partitioned table, the folder should follow the naming convention like `year=2020/month=1`.
Impala use `=` to separate partition name and partition value.  
To create a partitioned Hudi read optimized table on Impala:
```
CREATE EXTERNAL TABLE database.table_name
LIKE PARQUET '/path/to/load/xxx.parquet'
PARTITION BY (year int, month int, day int)
STORED AS HUDIPARQUET
LOCATION '/path/to/load';
ALTER TABLE database.table_name RECOVER PARTITIONS;
```
After Hudi made a new commit, refresh the Impala table to get the latest results.
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
provides a read-only integration with Copy on Write Hudi tables in Amazon S3. To query such Hudi tables, first we need
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


