---
title: Syncing to Metastore
keywords: [hudi, hive, sync]
---

## Syncing to Hive

Writing data with [DataSource](https://hudi.apache.org/docs/writing_data) writer or [HoodieDeltaStreamer](https://hudi.apache.org/docs/hoodie_deltastreamer) supports syncing of the table's latest schema to Hive metastore, such that queries can pick up new columns and partitions.
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
