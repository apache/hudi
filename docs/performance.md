---
title: Implementation
keywords: hudi, index, storage, compaction, cleaning, implementation
sidebar: mydoc_sidebar
toc: false
permalink: performance.html
---
## Performance

In this section, we go over some real world performance numbers for Hudi upserts, incremental pull and compare them against
the conventional alternatives for achieving these tasks. Following shows the speed up obtained for NoSQL ingestion, 
by switching from bulk loads off HBase to Parquet to incrementally upserting on a Hudi dataset, on 5 tables ranging from small to huge.

{% include image.html file="hudi_upsert_perf1.png" alt="hudi_upsert_perf1.png" max-width="1000" %}

Given Hudi can build the dataset incrementally, it opens doors for also scheduling ingesting more frequently thus reducing latency, with
significant savings on the overall compute cost.

{% include image.html file="hudi_upsert_perf2.png" alt="hudi_upsert_perf2.png" max-width="1000" %}

Hudi upserts have been stress tested upto 4TB in a single commit across the t1 table.

### Tuning

Writing data via Hudi happens as a Spark job and thus general rules of spark debugging applies here too. Below is a list of things to keep in mind, if you are looking to improving performance or reliability.

**Input Parallelism** : By default, Hudi tends to over-partition input (i.e `withParallelism(1500)`), to ensure each Spark partition stays within the 2GB limit for inputs upto 500GB. Bump this up accordingly if you have larger inputs. We recommend having shuffle parallelism `hoodie.[insert|upsert|bulkinsert].shuffle.parallelism` such that its atleast input_data_size/500MB

**Off-heap memory** : Hudi writes parquet files and that needs good amount of off-heap memory proportional to schema width. Consider setting something like `spark.yarn.executor.memoryOverhead` or `spark.yarn.driver.memoryOverhead`, if you are running into such failures.

**Spark Memory** : Typically, hudi needs to be able to read a single file into memory to perform merges or compactions and thus the executor memory should be sufficient to accomodate this. In addition, Hoodie caches the input to be able to intelligently place data and thus leaving some `spark.storage.memoryFraction` will generally help boost performance.

**Sizing files** : Set `limitFileSize` above judiciously, to balance ingest/write latency vs number of files & consequently metadata overhead associated with it.

**Timeseries/Log data** : Default configs are tuned for database/nosql changelogs where individual record sizes are large. Another very popular class of data is timeseries/event/log data that tends to be more volumnious with lot more records per partition. In such cases
    - Consider tuning the bloom filter accuracy via `.bloomFilterFPP()/bloomFilterNumEntries()` to achieve your target index look up time
    - Consider making a key that is prefixed with time of the event, which will enable range pruning & significantly speeding up index lookup.

**GC Tuning** : Please be sure to follow garbage collection tuning tips from Spark tuning guide to avoid OutOfMemory errors
[Must] Use G1/CMS Collector. Sample CMS Flags to add to spark.executor.extraJavaOptions :

```
-XX:NewSize=1g -XX:SurvivorRatio=2 -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintTenuringDistribution -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof
````

If it keeps OOMing still, reduce spark memory conservatively: `spark.memory.fraction=0.2, spark.memory.storageFraction=0.2` allowing it to spill rather than OOM. (reliably slow vs crashing intermittently)

Below is a full working production config

```
 spark.driver.extraClassPath    /etc/hive/conf
 spark.driver.extraJavaOptions    -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof
 spark.driver.maxResultSize    2g
 spark.driver.memory    4g
 spark.executor.cores    1
 spark.executor.extraJavaOptions    -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof
 spark.executor.id    driver
 spark.executor.instances    300
 spark.executor.memory    6g
 spark.rdd.compress true

 spark.kryoserializer.buffer.max    512m
 spark.serializer    org.apache.spark.serializer.KryoSerializer
 spark.shuffle.memoryFraction    0.2
 spark.shuffle.service.enabled    true
 spark.sql.hive.convertMetastoreParquet    false
 spark.storage.memoryFraction    0.6
 spark.submit.deployMode    cluster
 spark.task.cpus    1
 spark.task.maxFailures    4

 spark.yarn.driver.memoryOverhead    1024
 spark.yarn.executor.memoryOverhead    3072
 spark.yarn.max.executor.failures    100

````


#### Read Optimized Query Performance

The major design goal for read optimized view is to achieve the latency reduction & efficiency gains in previous section,
with no impact on queries. Following charts compare the Hudi vs non-Hudi datasets across Hive/Presto/Spark queries and demonstrate this.

**Hive**

{% include image.html file="hudi_query_perf_hive.png" alt="hudi_query_perf_hive.png" max-width="800" %}

**Spark**

{% include image.html file="hudi_query_perf_spark.png" alt="hudi_query_perf_spark.png" max-width="1000" %}

**Presto**

{% include image.html file="hudi_query_perf_presto.png" alt="hudi_query_perf_presto.png" max-width="1000" %}
