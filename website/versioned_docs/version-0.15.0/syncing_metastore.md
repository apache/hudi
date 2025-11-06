---
title: Hive Metastore
keywords: [hudi, hive, sync]
---

[Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration) is an
RDBMS-backed service from Apache Hive that acts as a catalog for your data warehouse or data lake. It can store all the
metadata about the tables, such as partitions, columns, column types, etc. One can sync the Hudi table metadata to the
Hive metastore as well. This unlocks the capability to query Hudi tables not only through Hive but also using
interactive query engines such as Presto and Trino. In this document, we will go through different ways to sync the Hudi
table to Hive metastore.

## Spark Data Source example

Prerequisites: setup hive metastore properly and configure the Spark installation to point to the hive metastore by placing `hive-site.xml` under `$SPARK_HOME/conf`

Assume that
  - hiveserver2 is running at port 10000
  - metastore is running at port 9083

Then start a spark-shell with Hudi spark bundle jar as a dependency (refer to Quickstart example)

We can run the following script to create a sample hudi table and sync it to hive.

```scala
// spark-shell
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row


val databaseName = "my_db"
val tableName = "hudi_cow"
val basePath = "/user/hive/warehouse/hudi_cow"

val schema = StructType(Array(
StructField("rowId", StringType,true),
StructField("partitionId", StringType,true),
StructField("preComb", LongType,true),
StructField("name", StringType,true),
StructField("versionId", StringType,true),
StructField("toBeDeletedStr", StringType,true),
StructField("intToLong", IntegerType,true),
StructField("longToInt", LongType,true)
))

val data0 = Seq(Row("row_1", "2021/01/01",0L,"bob","v_0","toBeDel0",0,1000000L), 
               Row("row_2", "2021/01/01",0L,"john","v_0","toBeDel0",0,1000000L), 
               Row("row_3", "2021/01/02",0L,"tom","v_0","toBeDel0",0,1000000L))

var dfFromData0 = spark.createDataFrame(data0,schema)

dfFromData0.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option("hoodie.datasource.write.precombine.field", "preComb").
  option("hoodie.datasource.write.recordkey.field", "rowId").
  option("hoodie.datasource.write.partitionpath.field", "partitionId").
  option("hoodie.database.name", databaseName).
  option("hoodie.table.name", tableName).
  option("hoodie.datasource.write.table.type", "COPY_ON_WRITE").
  option("hoodie.datasource.write.operation", "upsert").
  option("hoodie.datasource.write.hive_style_partitioning","true").
  option("hoodie.datasource.meta.sync.enable", "true").
  option("hoodie.datasource.hive_sync.mode", "hms").
  option("hoodie.datasource.hive_sync.metastore.uris", "thrift://hive-metastore:9083").
  mode(Overwrite).
  save(basePath)
```

:::note
If prefer to use JDBC instead of HMS sync mode, omit `hoodie.datasource.hive_sync.metastore.uris` and configure these instead

```
hoodie.datasource.hive_sync.mode=jdbc
hoodie.datasource.hive_sync.jdbcurl=<e.g., jdbc:hive2://hiveserver:10000>
hoodie.datasource.hive_sync.username=<username>
hoodie.datasource.hive_sync.password=<password>
```
:::

### Query using HiveQL

```
beeline -u jdbc:hive2://hiveserver:10000/my_db \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false
  
Beeline version 1.2.1.spark2 by Apache Hive
0: jdbc:hive2://hiveserver:10000> show tables;
+-----------+--+
| tab_name  |
+-----------+--+
| hudi_cow  |
+-----------+--+
1 row selected (0.531 seconds)
0: jdbc:hive2://hiveserver:10000> select * from hudi_cow limit 1;
+-------------------------------+--------------------------------+------------------------------+----------------------------------+----------------------------------------------------------------------------+-----------------+-------------------+----------------+---------------------+--------------------------+---------------------+---------------------+-----------------------+--+
| hudi_cow._hoodie_commit_time  | hudi_cow._hoodie_commit_seqno  | hudi_cow._hoodie_record_key  | hudi_cow._hoodie_partition_path  |                         hudi_cow._hoodie_file_name                         | hudi_cow.rowid  | hudi_cow.precomb  | hudi_cow.name  | hudi_cow.versionid  | hudi_cow.tobedeletedstr  | hudi_cow.inttolong  | hudi_cow.longtoint  | hudi_cow.partitionid  |
+-------------------------------+--------------------------------+------------------------------+----------------------------------+----------------------------------------------------------------------------+-----------------+-------------------+----------------+---------------------+--------------------------+---------------------+---------------------+-----------------------+--+
| 20220120090023631             | 20220120090023631_1_2          | row_1                        | partitionId=2021/01/01           | 0bf9b822-928f-4a57-950a-6a5450319c83-0_1-24-314_20220120090023631.parquet  | row_1           | 0                 | bob            | v_0                 | toBeDel0                 | 0                   | 1000000             | 2021/01/01            |
+-------------------------------+--------------------------------+------------------------------+----------------------------------+----------------------------------------------------------------------------+-----------------+-------------------+----------------+---------------------+--------------------------+---------------------+---------------------+-----------------------+--+
1 row selected (5.475 seconds)
0: jdbc:hive2://hiveserver:10000>
```

### Use partition extractor properly

When sync to hive metastore, partition values are extracted using `hoodie.datasource.hive_sync.partition_value_extractor`. Before 0.12, this is by default set to 
`org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor` and users usually need to overwrite this manually. Since 0.12, the default value is changed to a more
generic `org.apache.hudi.hive.MultiPartKeysValueExtractor` which extracts partition values using `/` as the separator.

In case of using some key generator like `TimestampBasedKeyGenerator`, the partition values can be in form of `yyyy/MM/dd`. This is usually undesirable to have the
partition values extracted as multiple parts like `[yyyy, MM, dd]`. Users can set `org.apache.hudi.hive.SinglePartPartitionValueExtractor` to extract the partition
values as `yyyy-MM-dd`.

When the table is not partitioned, `org.apache.hudi.hive.NonPartitionedExtractor` should be set. And this is automatically inferred from partition fields configs,
so users may not need to set it manually. Similarly, if hive-style partitioning is used for the table, then `org.apache.hudi.hive.HiveStylePartitionValueExtractor`
will be inferred and set automatically.

## Hive Sync Tool

Writing data with [DataSource](writing_data) writer or [HoodieStreamer](hoodie_streaming_ingestion) supports syncing of the table's latest schema to Hive metastore, such that queries can pick up new columns and partitions.
In case, it's preferable to run this from commandline or in an independent jvm, Hudi provides a `HiveSyncTool`, which can be invoked as below,
once you have built the hudi-hive module. Following is how we sync the above Datasource Writer written table to Hive metastore.

```java
cd hudi-hive
./run_sync_tool.sh  --jdbc-url jdbc:hive2:\/\/hiveserver:10000 --user hive --pass hive --partitioned-by partition --base-path <basePath> --database default --table <tableName>
```

Starting with Hudi 0.5.1 version read optimized version of merge-on-read tables are suffixed '_ro' by default. For backwards compatibility with older Hudi versions, an optional HiveSyncConfig - `--skip-ro-suffix`, has been provided to turn off '_ro' suffixing if desired. Explore other hive sync options using the following command:

```java
cd hudi-hive
./run_sync_tool.sh
 [hudi-hive]$ ./run_sync_tool.sh --help
```

### Hive Sync Configuration

Please take a look at the arguments that can be passed to `run_sync_tool` in [HiveSyncConfig](https://github.com/apache/hudi/blob/master/hudi-sync/hudi-hive-sync/src/main/java/org/apache/hudi/hive/HiveSyncConfig.java).
Among them, following are the required arguments:
```java
@Parameter(names = {"--database"}, description = "name of the target database in Hive", required = true);
@Parameter(names = {"--table"}, description = "name of the target table in Hive", required = true);
@Parameter(names = {"--base-path"}, description = "Basepath of Hudi table to sync", required = true);
```
Corresponding datasource options for the most commonly used hive sync configs are as follows:

:::note 
In the table below **(N/A)** means there is no default value set.
:::

| HiveSyncConfig | DataSourceWriteOption | Default Value | Description |
| -----------   | ----------- | ----------- | ----------- |
| --database       | hoodie.datasource.hive_sync.database  |  default   | Name of the target database in Hive metastore       |
| --table   | hoodie.datasource.hive_sync.table |  (N/A)     | Name of the target table in Hive. Inferred from the table name in Hudi table config if not specified.        |
| --user   | hoodie.datasource.hive_sync.username |   hive     | Username for hive metastore        | 
| --pass   | hoodie.datasource.hive_sync.password  |  hive    | Password for hive metastore        | 
| --jdbc-url   | hoodie.datasource.hive_sync.jdbcurl  |  jdbc:hive2://localhost:10000    | Hive server url if using `jdbc` mode to sync     |
| --sync-mode   | hoodie.datasource.hive_sync.mode    |  (N/A)  | Mode to choose for Hive ops. Valid values are `hms`, `jdbc` and `hiveql`. More details in the following section.       |
| --partitioned-by   | hoodie.datasource.hive_sync.partition_fields   |  (N/A)   | Comma-separated column names in the table to use for determining hive partition.        |
| --partition-value-extractor   | hoodie.datasource.hive_sync.partition_extractor_class   |  `org.apache.hudi.hive.MultiPartKeysValueExtractor`   | Class which implements `PartitionValueExtractor` to extract the partition values. Inferred automatically depending on the partition fields specified.        |


### Sync modes

`HiveSyncTool` supports three modes, namely `HMS`, `HIVEQL`, `JDBC`, to connect to Hive metastore server. 
These modes are just three different ways of executing DDL against Hive. Among these modes, JDBC or HMS is preferable over
HIVEQL, which is mostly used for running DML rather than DDL.

:::note
All these modes assume that hive metastore has been configured and the corresponding properties set in 
`hive-site.xml` configuration file. Additionally, if you're using spark-shell/spark-sql to sync Hudi table to Hive then 
the `hive-site.xml` file also needs to be placed under `<SPARK_HOME>/conf` directory.
:::

#### HMS

HMS mode uses the hive metastore client to sync Hudi table using thrift APIs directly.
To use this mode, pass `--sync-mode=hms` to `run_sync_tool` and set `--use-jdbc=false`. 
Additionally, if you are using remote metastore, then `hive.metastore.uris` need to be set in hive-site.xml configuration file.
Otherwise, the tool assumes that metastore is running locally on port 9083 by default. 

#### JDBC

This mode uses the JDBC specification to connect to the hive metastore. 

```java
@Parameter(names = {"--jdbc-url"}, description = "Hive jdbc connect url");
```

#### HIVEQL

HQL is Hive's own SQL dialect. 
This mode simply uses the Hive QL's [driver](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/Driver.java) to execute DDL as HQL command.
To use this mode, pass `--sync-mode=hiveql` to `run_sync_tool` and set `--use-jdbc=false`.

## Flink Setup

### Install

Now you can git clone Hudi master branch to test Flink hive sync. The first step is to install Hudi to get `hudi-flink1.1x-bundle-0.x.x.jar`.
 `hudi-flink-bundle` module pom.xml sets the scope related to hive as `provided` by default. If you want to use hive sync, you need to use the
profile `flink-bundle-shade-hive` during packaging. Executing command below to install:

```bash
# Maven install command
mvn install -DskipTests -Drat.skip=true -Pflink-bundle-shade-hive2

# For hive1, you need to use profile -Pflink-bundle-shade-hive1
# For hive3, you need to use profile -Pflink-bundle-shade-hive3 
```

:::note
Hive1.x can only synchronize metadata to hive, but cannot use hive query now. If you need to query, you can use spark to query hive table.
:::

:::note
If using hive profile, you need to modify the hive version in the profile to your hive cluster version (Only need to modify the hive version in this profile).
The location of this `pom.xml` is `packaging/hudi-flink-bundle/pom.xml`, and the corresponding profile is at the bottom of this file.
:::

### Hive Environment

1. Import `hudi-hadoop-mr-bundle` into hive. Creating `auxlib/` folder under the root directory of hive, and moving `hudi-hadoop-mr-bundle-0.x.x-SNAPSHOT.jar` into `auxlib`.
   `hudi-hadoop-mr-bundle-0.x.x-SNAPSHOT.jar` is at `packaging/hudi-hadoop-mr-bundle/target`.

2. When Flink sql client connects hive metastore remotely, `hive metastore` and `hiveserver2` services need to be enabled, and the port number need to
   be set correctly. Command to turn on the services:

```bash
# Enable hive metastore and hiveserver2
nohup ./bin/hive --service metastore &
nohup ./bin/hive --service hiveserver2 &

# While modifying the jar package under auxlib, you need to restart the service.
```

### Sync Template

Flink hive sync now supports two hive sync mode, `hms` and `jdbc`. `hms` mode only needs to configure metastore uris. For
the `jdbc` mode, the JDBC attributes and metastore uris both need to be configured. The options template is as below:

```sql
-- hms mode template
CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = '${db_path}/t1',
  'table.type' = 'COPY_ON_WRITE',  -- If MERGE_ON_READ, hive query will not have output until the parquet file is generated
  'hive_sync.enable' = 'true',     -- Required. To enable hive synchronization
  'hive_sync.mode' = 'hms',        -- Required. Setting hive sync mode to hms, default hms. (Before 0.13, the default sync mode was jdbc.)
  'hive_sync.metastore.uris' = 'thrift://${ip}:9083' -- Required. The port need set on hive-site.xml
);


-- jdbc mode template
CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = '${db_path}/t1',
  'table.type' = 'COPY_ON_WRITE',  --If MERGE_ON_READ, hive query will not have output until the parquet file is generated
  'hive_sync.enable' = 'true',     -- Required. To enable hive synchronization
  'hive_sync.mode' = 'jdbc',       -- Required. Setting hive sync mode to jdbc, default hms. (Before 0.13, the default sync mode was jdbc.)
  'hive_sync.metastore.uris' = 'thrift://${ip}:9083', -- Required. The port need set on hive-site.xml
  'hive_sync.jdbc_url'='jdbc:hive2://${ip}:10000',    -- required, hiveServer port
  'hive_sync.table'='${table_name}',                  -- required, hive table name
  'hive_sync.db'='${db_name}',                        -- required, hive database name
  'hive_sync.username'='${user_name}',                -- required, JDBC username
  'hive_sync.password'='${password}'                  -- required, JDBC password
);
```

### Query

While using hive beeline query, you need to enter settings:
```bash
set hive.input.format = org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat;
```
