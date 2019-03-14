---
title: Querying Hudi Datasets
keywords: hudi, hive, spark, sql, presto
sidebar: mydoc_sidebar
permalink: querying_data.html
toc: false
summary: In this page, we go over how to enable SQL queries on Hudi built tables.
---

Hudi registers the dataset into the Hive metastore backed by `HoodieInputFormat`. This makes the data accessible to
Hive & Spark & Presto automatically. To be able to perform normal SQL queries on such a dataset, we need to get the individual query engines
to call `HoodieInputFormat.getSplits()`, during query planning such that the right versions of files are exposed to it.


In the following sections, we cover the configs needed across different query engines to achieve this.

{% include callout.html content="Instructions are currently only for Copy-on-write storage" type="info" %}


## Hive

For HiveServer2 access, [install](https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cm_mc_hive_udf.html#concept_nc3_mms_lr)
the hoodie-hadoop-mr-bundle-x.y.z-SNAPSHOT.jar into the aux jars path and we should be able to recognize the Hudi tables and query them correctly.

For beeline access, the `hive.input.format` variable needs to be set to the fully qualified path name of the inputformat `com.uber.hoodie.hadoop.HoodieInputFormat`
For Tez, additionally the `hive.tez.input.format` needs to be set to `org.apache.hadoop.hive.ql.io.HiveInputFormat`

## Spark

There are two ways of running Spark SQL on Hudi datasets.

First method involves, setting `spark.sql.hive.convertMetastoreParquet=false`, forcing Spark to fallback
to using the Hive Serde to read the data (planning/executions is still Spark). This turns off optimizations in Spark
towards Parquet reading, which we will address in the next method based on path filters.
However benchmarks have not revealed any real performance degradation with Hudi & SparkSQL, compared to native support.

Sample command is provided below to spin up Spark Shell

```
$ spark-shell --jars hoodie-spark-bundle-x.y.z-SNAPSHOT.jar --driver-class-path /etc/hive/conf  --packages com.databricks:spark-avro_2.11:4.0.0 --conf spark.sql.hive.convertMetastoreParquet=false --num-executors 10 --driver-memory 7g --executor-memory 2g  --master yarn-client

scala> sqlContext.sql("select count(*) from uber.trips where datestr = '2016-10-02'").show()

```


For scheduled Spark jobs, a dependency to [hoodie-hadoop-mr](https://mvnrepository.com/artifact/com.uber.hoodie/hoodie-hadoop-mr) and [hoodie-client](https://mvnrepository.com/artifact/com.uber.hoodie/hoodie-client) modules needs to be added
and the same config needs to be set on `SparkConf` or conveniently via `HoodieReadClient.addHoodieSupport(conf)`

{% include callout.html content="Don't instantiate a HoodieWriteClient against a table you don't own. Hudi is a single writer & multiple reader system as of now. You may accidentally cause incidents otherwise.
" type="warning" %}

The second method uses a new feature in Spark 2.x, which allows for the work of HoodieInputFormat to be done via a path filter as below. This method uses Spark built-in optimizations for
reading Parquet files, just like queries on non-Hudi tables.

```
spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[com.uber.hoodie.hadoop.HoodieROTablePathFilter], classOf[org.apache.hadoop.fs.PathFilter]);
```


## Presto

Presto requires the `hoodie-presto-bundle` jar to be placed into `<presto_install>/plugin/hive-hadoop2/`, across the installation.
