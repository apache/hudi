---
title: "Registering sample dataset to Hive via beeline"
excerpt: "How to manually register HUDI dataset into Hive using beeline"
author: vinoth
category: blog
tags:
- how-to
- apache hudi
---

Hudi hive sync tool typically handles registration of the dataset into Hive metastore. In case, there are issues with quickstart around this, following page shows commands that can be used to do this manually via beeline.  

<!--truncate-->
Add in the _packaging/hoodie-hive-bundle/target/hoodie-hive-bundle-0.4.6-SNAPSHOT.jar,_ so that Hive can read the Hudi dataset and answer the query.

```java
hive> set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
hive> set hive.stats.autogather=false;
hive> add jar file:///path/to/hoodie-hive-bundle-0.5.2-SNAPSHOT.jar;
Added [file:///path/to/hoodie-hive-bundle-0.5.2-SNAPSHOT.jar] to class path
Added resources: [file:///path/to/hoodie-hive-bundle-0.5.2-SNAPSHOT.jar]
```


Then, you need to create a *ReadOptimized* Hive table as below and register the sample partitions

```java
DROP TABLE hoodie_test;
CREATE EXTERNAL TABLE hoodie_test(`_row_key` string,
    `_hoodie_commit_time` string,
    `_hoodie_commit_seqno` string,
    rider string,
    driver string,
    begin_lat double,
    begin_lon double,
    end_lat double,
    end_lon double,
    fare double)
    PARTITIONED BY (`datestr` string)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'com.uber.hoodie.hadoop.HoodieInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'hdfs:///tmp/hoodie/sample-table';
     
ALTER TABLE `hoodie_test` ADD IF NOT EXISTS PARTITION (datestr='2016-03-15') LOCATION 'hdfs:///tmp/hoodie/sample-table/2016/03/15';
ALTER TABLE `hoodie_test` ADD IF NOT EXISTS PARTITION (datestr='2015-03-16') LOCATION 'hdfs:///tmp/hoodie/sample-table/2015/03/16';
ALTER TABLE `hoodie_test` ADD IF NOT EXISTS PARTITION (datestr='2015-03-17') LOCATION 'hdfs:///tmp/hoodie/sample-table/2015/03/17';
     
set mapreduce.framework.name=yarn;
```

And you can add a *Realtime* Hive table, as below

```java
DROP TABLE hoodie_rt;
CREATE EXTERNAL TABLE hoodie_rt(
    `_hoodie_commit_time` string,
    `_hoodie_commit_seqno` string,
    `_hoodie_record_key` string,
    `_hoodie_partition_path` string,
    `_hoodie_file_name` string,
    timestamp double,
    `_row_key` string,
    rider string,
    driver string,
    begin_lat double,
    begin_lon double,
    end_lat double,
    end_lon double,
    fare double)
    PARTITIONED BY (`datestr` string)
    ROW FORMAT SERDE
    'com.uber.hoodie.hadoop.realtime.HoodieParquetSerde'
    STORED AS INPUTFORMAT
    'com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'file:///tmp/hoodie/sample-table';
     
ALTER TABLE `hoodie_rt` ADD IF NOT EXISTS PARTITION (datestr='2016-03-15') LOCATION 'file:///tmp/hoodie/sample-table/2016/03/15';
ALTER TABLE `hoodie_rt` ADD IF NOT EXISTS PARTITION (datestr='2015-03-16') LOCATION 'file:///tmp/hoodie/sample-table/2015/03/16';
ALTER TABLE `hoodie_rt` ADD IF NOT EXISTS PARTITION (datestr='2015-03-17') LOCATION 'file:///tmp/hoodie/sample-table/2015/03/17';
```

