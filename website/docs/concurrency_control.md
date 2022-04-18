---
title: "Concurrency Control"
summary: In this page, we will discuss how to perform concurrent writes to Hudi Tables.
toc: true
last_modified_at: 2021-03-19T15:59:57-04:00
---

In this section, we will cover Hudi's concurrency model and describe ways to ingest data into a Hudi Table from multiple writers; using the [DeltaStreamer](#deltastreamer) tool as well as 
using the [Hudi datasource](#datasource-writer).

## Supported Concurrency Controls

- **MVCC** : Hudi table services such as compaction, cleaning, clustering leverage Multi Version Concurrency Control to provide snapshot isolation
between multiple table service writers and readers. Additionally, using MVCC, Hudi provides snapshot isolation between an ingestion writer and multiple concurrent readers. 
  With this model, Hudi supports running any number of table service jobs concurrently, without any concurrency conflict. 
  This is made possible by ensuring that scheduling plans of such table services always happens in a single writer mode to ensure no conflict and avoids race conditions.

- **[NEW] OPTIMISTIC CONCURRENCY** : Write operations such as the ones described above (UPSERT, INSERT) etc, leverage optimistic concurrency control to enable multiple ingestion writers to
the same Hudi Table. Hudi supports `file level OCC`, i.e., for any 2 commits (or writers) happening to the same table, if they do not have writes to overlapping files being changed, both writers are allowed to succeed. 
  This feature is currently *experimental* and requires either Zookeeper or HiveMetastore to acquire locks.

It may be helpful to understand the different guarantees provided by [write operations](/docs/write_operations/) via Hudi datasource or the delta streamer.

## Single Writer Guarantees

 - *UPSERT Guarantee*: The target table will NEVER show duplicates.
 - *INSERT Guarantee*: The target table wilL NEVER have duplicates if [dedup](/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) is enabled.
 - *BULK_INSERT Guarantee*: The target table will NEVER have duplicates if [dedup](/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) is enabled.
 - *INCREMENTAL PULL Guarantee*: Data consumption and checkpoints are NEVER out of order.

## Multi Writer Guarantees

With multiple writers using OCC, some of the above guarantees change as follows

- *UPSERT Guarantee*: The target table will NEVER show duplicates.
- *INSERT Guarantee*: The target table MIGHT have duplicates even if [dedup](/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) is enabled.
- *BULK_INSERT Guarantee*: The target table MIGHT have duplicates even if [dedup](/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) is enabled.
- *INCREMENTAL PULL Guarantee*: Data consumption and checkpoints MIGHT be out of order due to multiple writer jobs finishing at different times.

## Enabling Multi Writing

The following properties are needed to be set properly to turn on optimistic concurrency control.

```
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.cleaner.policy.failed.writes=LAZY
hoodie.write.lock.provider=<lock-provider-classname>
```

There are 3 different server based lock providers that require different configuration to be set.

**`Zookeeper`** based lock provider

```
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
hoodie.write.lock.zookeeper.url
hoodie.write.lock.zookeeper.port
hoodie.write.lock.zookeeper.lock_key
hoodie.write.lock.zookeeper.base_path
```

**`HiveMetastore`** based lock provider

```
hoodie.write.lock.provider=org.apache.hudi.hive.HiveMetastoreBasedLockProvider
hoodie.write.lock.hivemetastore.database
hoodie.write.lock.hivemetastore.table
```

`The HiveMetastore URI's are picked up from the hadoop configuration file loaded during runtime.`

**`Amazon DynamoDB`** based lock provider

Amazon DynamoDB based lock provides a simple way to support multi writing across different clusters

```
hoodie.write.lock.provider=org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider
hoodie.write.lock.dynamodb.table
hoodie.write.lock.dynamodb.partition_key
hoodie.write.lock.dynamodb.region
```
Also, to set up the credentials for accessing AWS resources, customers can pass the following props to Hudi jobs:
```
hoodie.aws.access.key
hoodie.aws.secret.key
hoodie.aws.session.token
```
If not configured, Hudi falls back to use [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).

## Datasource Writer

The `hudi-spark` module offers the DataSource API to write (and read) a Spark DataFrame into a Hudi table.

Following is an example of how to use optimistic_concurrency_control via spark datasource

```java
inputDF.write.format("hudi")
       .options(getQuickstartWriteConfigs)
       .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
       .option("hoodie.cleaner.policy.failed.writes", "LAZY")
       .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
       .option("hoodie.write.lock.zookeeper.url", "zookeeper")
       .option("hoodie.write.lock.zookeeper.port", "2181")
       .option("hoodie.write.lock.zookeeper.lock_key", "test_table")
       .option("hoodie.write.lock.zookeeper.base_path", "/test")
       .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
       .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
       .option(TABLE_NAME, tableName)
       .mode(Overwrite)
       .save(basePath)
```

## DeltaStreamer

The `HoodieDeltaStreamer` utility (part of hudi-utilities-bundle) provides ways to ingest from different sources such as DFS or Kafka, with the following capabilities.

Using optimistic_concurrency_control via delta streamer requires adding the above configs to the properties file that can be passed to the
job. For example below, adding the configs to kafka-source.properties file and passing them to deltastreamer will enable optimistic concurrency.
A deltastreamer job can then be triggered as follows:

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \
  --props file://${PWD}/hudi-utilities/src/test/resources/delta-streamer-config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --target-base-path file:\/\/\/tmp/hudi-deltastreamer-op \ 
  --target-table uber.impressions \
  --op BULK_INSERT
```

## Best Practices when using Optimistic Concurrency Control

Concurrent Writing to Hudi tables requires acquiring a lock with either Zookeeper or HiveMetastore. Due to several reasons you might want to configure retries to allow your application to acquire the lock. 
1. Network connectivity or excessive load on servers increasing time for lock acquisition resulting in timeouts
2. Running a large number of concurrent jobs that are writing to the same hudi table can result in contention during lock acquisition can cause timeouts
3. In some scenarios of conflict resolution, Hudi commit operations might take upto 10's of seconds while the lock is being held. This can result in timeouts for other jobs waiting to acquire a lock.

Set the correct native lock provider client retries. NOTE that sometimes these settings are set on the server once and all clients inherit the same configs. Please check your settings before enabling optimistic concurrency.
   
```
hoodie.write.lock.wait_time_ms
hoodie.write.lock.num_retries
```

Set the correct hudi client retries for Zookeeper & HiveMetastore. This is useful in cases when native client retry settings cannot be changed. Please note that these retries will happen in addition to any native client retries that you may have set. 

```
hoodie.write.lock.client.wait_time_ms
hoodie.write.lock.client.num_retries
```

*Setting the right values for these depends on a case by case basis; some defaults have been provided for general cases.*

## Disabling Multi Writing

Remove the following settings that were used to enable multi-writer or override with default values.

```
hoodie.write.concurrency.mode=single_writer
hoodie.cleaner.policy.failed.writes=EAGER
```

## Caveats

If you are using the `WriteClient` API, please note that multiple writes to the table need to be initiated from 2 different instances of the write client. 
It is NOT recommended to use the same instance of the write client to perform multi writing. 