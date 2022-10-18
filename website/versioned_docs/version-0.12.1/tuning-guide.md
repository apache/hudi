---
title: Tuning Guide
keywords: [hudi, tuning, performance]
last_modified_at: 2021-09-29T15:59:57-04:00
---
# Tuning Guide

:::note Profiling Tip
To get a better understanding of where your Hudi jobs is spending its time, use a tool like [YourKit Java Profiler](https://www.yourkit.com/download/), to obtain heap dumps/flame graphs.
:::

Writing data via Hudi happens as a Spark job and thus general rules of spark debugging applies here too. Below is a list of things to keep in mind, if you are looking to improving performance or reliability.

**Input Parallelism** : By default, Hudi tends to over-partition input (i.e `withParallelism(1500)`), to ensure each Spark partition stays within the 2GB limit for inputs upto 500GB. Bump this up accordingly if you have larger inputs. We recommend having shuffle parallelism `hoodie.[insert|upsert|bulkinsert].shuffle.parallelism` such that its atleast input_data_size/500MB

**Off-heap memory** : Hudi writes parquet files and that needs good amount of off-heap memory proportional to schema width. Consider setting something like `spark.executor.memoryOverhead` or `spark.driver.memoryOverhead`, if you are running into such failures.

**Spark Memory** : Typically, hudi needs to be able to read a single file into memory to perform merges or compactions and thus the executor memory should be sufficient to accomodate this. In addition, Hoodie caches the input to be able to intelligently place data and thus leaving some `spark.memory.storageFraction` will generally help boost performance.

**Sizing files**: Set `limitFileSize` above judiciously, to balance ingest/write latency vs number of files & consequently metadata overhead associated with it.

**Timeseries/Log data** : Default configs are tuned for database/nosql changelogs where individual record sizes are large. Another very popular class of data is timeseries/event/log data that tends to be more volumnious with lot more records per partition. In such cases consider tuning the bloom filter accuracy via `.bloomFilterFPP()/bloomFilterNumEntries()` to achieve your target index look up time. Also, consider making a key that is prefixed with time of the event, which will enable range pruning & significantly speeding up index lookup.

**GC Tuning**: Please be sure to follow garbage collection tuning tips from Spark tuning guide to avoid OutOfMemory errors. [Must] Use G1/CMS Collector. Sample CMS Flags to add to spark.executor.extraJavaOptions:

```java
-XX:NewSize=1g -XX:SurvivorRatio=2 -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintTenuringDistribution -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof
```

**OutOfMemory Errors**: If it keeps OOMing still, reduce spark memory conservatively: `spark.memory.fraction=0.2, spark.memory.storageFraction=0.2` allowing it to spill rather than OOM. (reliably slow vs crashing intermittently)

Below is a full working production config

```scala
spark.driver.extraClassPath /etc/hive/conf
spark.driver.extraJavaOptions -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof
spark.driver.maxResultSize 2g
spark.driver.memory 4g
spark.executor.cores 1
spark.executor.extraJavaOptions -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof
spark.executor.id driver
spark.executor.instances 300
spark.executor.memory 6g
spark.rdd.compress true
 
spark.kryoserializer.buffer.max 512m
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.shuffle.service.enabled true
spark.sql.hive.convertMetastoreParquet false
spark.submit.deployMode cluster
spark.task.cpus 1
spark.task.maxFailures 4
 
spark.driver.memoryOverhead 1024
spark.executor.memoryOverhead 3072
spark.yarn.max.executor.failures 100
```