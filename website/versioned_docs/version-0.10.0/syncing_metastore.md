---
title: Syncing to Metastore
keywords: [hudi, hive, sync]
---

## Spark and DeltaStreamer

Writing data with [DataSource](/docs/writing_data) writer or [HoodieDeltaStreamer](/docs/0.10.0/hoodie_deltastreamer) supports syncing of the table's latest schema to Hive metastore, such that queries can pick up new columns and partitions.
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


## Flink Setup

### Install

Now you can git clone Hudi master branch to test Flink hive sync. The first step is to install Hudi to get `hudi-flink-bundle_2.11-0.x.jar`.
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
  'table.type' = 'COPY_ON_WRITE',  --If MERGE_ON_READ, hive query will not have output until the parquet file is generated
  'hive_sync.enable' = 'true',     -- Required. To enable hive synchronization
  'hive_sync.mode' = 'hms',        -- Required. Setting hive sync mode to hms, default jdbc
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
  'table.type' = 'COPY_ON_WRITE',  -- If MERGE_ON_READ, hive query will not have output until the parquet file is generated
  'hive_sync.enable' = 'true',     -- Required. To enable hive synchronization
  'hive_sync.mode' = 'jdbc'        -- Required. Setting hive sync mode to hms, default jdbc
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