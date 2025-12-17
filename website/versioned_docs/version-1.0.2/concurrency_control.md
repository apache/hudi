---
title: "Concurrency Control"
summary: In this page, we will discuss how to perform concurrent writes to Hudi Tables.
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
last_modified_at: 2021-03-19T15:59:57-04:00
---
Concurrency control defines how different writers/readers/table services coordinate access to a Hudi table. Hudi ensures atomic writes, by way of publishing commits atomically to the timeline, 
stamped with an instant time that denotes the time at which the action is deemed to have occurred. Unlike general purpose file version control, Hudi draws clear distinction between 
writer processes that issue [write operations](write_operations.md) and table services that (re)write data/metadata to optimize/perform bookkeeping and 
readers (that execute queries and read data). 

Hudi provides
* **Snapshot isolation** between all three types of processes, meaning they all operate on a consistent snapshot of the table.
* **Optimistic concurrency control (OCC)** between writers to provide standard relational database semantics.
* **Multiversion Concurrency Control (MVCC)** based concurrency control between writers and table-services and between different table services.
* **Non-blocking Concurrency Control (NBCC)** between writers, to provide streaming semantics and avoiding live-locks/starvation between writers.

In this section, we will discuss the different concurrency controls supported by Hudi and how they are leveraged to provide flexible deployment models for single and multiple writer scenarios. 
We’ll also describe ways to ingest data into a Hudi Table from multiple writers using different writers, like  Hudi Streamer, Hudi datasource, Spark Structured Streaming and Spark SQL.

:::note
If there is only one process performing writing AND async/inline table services on the table, you can
avoid the overhead of a distributed lock requirement by configuring the in process lock provider.

```properties
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider
```
:::

## Distributed Locking 
A pre-requisite for distributed co-ordination in Hudi, like many other distributed database systems is a distributed lock provider, that different processes can use to plan, schedule and 
execute actions on the Hudi timeline in a concurrent fashion. Locks are also used to [generate TrueTime](https://hudi.apache.org/docs/timeline#truetime-generation), as discussed before.

External locking is typically used in conjunction with optimistic concurrency control 
because it provides a way to prevent conflicts that might occur when two or more transactions (commits in our case) attempt to modify the same resource concurrently. 
When a transaction attempts to modify a resource that is currently locked by another transaction, it must wait until the lock is released before proceeding.

In case of multi-writing in Hudi, the locks are acquired on the Hudi table for a very short duration during specific phases (such as just before committing the writes or before scheduling table services) instead of locking for the entire span of time. This approach allows multiple writers to work on the same table simultaneously, increasing concurrency and avoids conflicts.

There are 4 different lock providers that require different configurations to be set. 1 lock provider requires no configurations. Please refer to comprehensive locking configs [here](https://hudi.apache.org/docs/next/configurations#LOCK).

#### Storage with conditional writes based
```
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.StorageBasedLockProvider
```
No configs are required for this lock provider, however as of 1.0.2 this lock provider is only supported for S3.
All writers compete to acquire a lock using the file `.hoodie/.locks/table_lock.json`.

#### Zookeeper based
```
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
```
Following are the basic configs required to setup this lock provider:

| Config Name| Default| Description                                                                                                                                                                                                |
| ---------------------------------------------------------------------------- | ------------------------ |------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.write.lock.zookeeper.base_path   | N/A **(Required)**   | The base path on Zookeeper under which to create lock related ZNodes. This should be same for all concurrent writers to the same table<br /><br />`Config Param: ZK_BASE_PATH`<br />`Since Version: 0.8.0` |
| hoodie.write.lock.zookeeper.port        | N/A **(Required)**   | Zookeeper port to connect to.<br /><br />`Config Param: ZK_PORT`<br />`Since Version: 0.8.0`                                                                                                               |
| hoodie.write.lock.zookeeper.url         | N/A **(Required)**   | Zookeeper URL to connect to.<br /><br />`Config Param: ZK_CONNECT_URL`<br />`Since Version: 0.8.0`                                                                                                         |

#### HiveMetastore based

```
hoodie.write.lock.provider=org.apache.hudi.hive.transaction.lock.HiveMetastoreBasedLockProvider
```
Following are the basic configs required to setup this lock provider:

| Config Name| Default| Description                                                                                                                                                                                                |
| ----------------------------------------------------------------------- | ------------------------ |------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.write.lock.hivemetastore.database                     | N/A **(Required)**                                                                                 | For Hive based lock provider, the Hive database to acquire lock against<br /><br />`Config Param: HIVE_DATABASE_NAME`<br />`Since Version: 0.8.0`                                                                                                                                    |
| hoodie.write.lock.hivemetastore.table                         | N/A **(Required)**                                                                                 | For Hive based lock provider, the Hive table to acquire lock against<br /><br />`Config Param: HIVE_TABLE_NAME`<br />`Since Version: 0.8.0`                                                                                                                                          |

`The HiveMetastore URI's are picked up from the hadoop configuration file loaded during runtime.`

:::note
Zookeeper is required for HMS lock provider. Users can set the zookeeper configs for Hive using Hudi

```properties
hoodie.write.lock.zookeeper.url
hoodie.write.lock.zookeeper.port
```
or via Hive config for Hive Metastore (HMS)

```properties
hive.zookeeper.quorum
hive.zookeeper.client.port
```
:::

#### Amazon DynamoDB based
```
hoodie.write.lock.provider=org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider
```
Amazon DynamoDB based lock provides a simple way to support multi writing across different clusters.  You can refer to the
[DynamoDB based Locks Configurations](https://hudi.apache.org/docs/configurations#DynamoDB-based-Locks-Configurations)
section for the details of each related configuration knob. Following are the basic configs required to setup this lock provider:

| Config Name| Default| Description                                                                                                                                                                                                |
| ----------------------------------------------------------------------- | ------------------------ |------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.write.lock.dynamodb.endpoint_url| N/A **(Required)**         | For DynamoDB based lock provider, the url endpoint used for Amazon DynamoDB service. Useful for development with a local dynamodb instance.<br /><br />`Config Param: DYNAMODB_ENDPOINT_URL`<br />`Since Version: 0.10.1`|

For advanced configs refer [here](https://hudi.apache.org/docs/next/configurations#DynamoDB-based-Locks-Configurations)


When using the DynamoDB-based lock provider, the name of the DynamoDB table acting as the lock table for Hudi is
specified by the config `hoodie.write.lock.dynamodb.table`. This DynamoDB table is automatically created by Hudi, so you
don't have to create the table yourself. If you want to use an existing DynamoDB table, make sure that an attribute with
the name `key` is present in the table.  The `key` attribute should be the partition key of the DynamoDB table. The
config `hoodie.write.lock.dynamodb.partition_key` specifies the value to put for the `key` attribute (not the attribute
name), which is used for the lock on the same table. By default, `hoodie.write.lock.dynamodb.partition_key` is set to
the table name, so that multiple writers writing to the same table share the same lock. If you customize the name, make
sure it's the same across multiple writers.

Also, to set up the credentials for accessing AWS resources, customers can pass the following props to Hudi jobs:
```
hoodie.aws.access.key
hoodie.aws.secret.key
hoodie.aws.session.token
```
If not configured, Hudi falls back to use [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).

IAM policy for your service instance will need to add the following permissions:

```json
{
  "Sid":"DynamoDBLocksTable",
  "Effect": "Allow",
  "Action": [
    "dynamodb:CreateTable",
    "dynamodb:DeleteItem",
    "dynamodb:DescribeTable",
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:Scan",
    "dynamodb:UpdateItem"
  ],
  "Resource": "arn:${Partition}:dynamodb:${Region}:${Account}:table/${TableName}"
}
```
- `TableName` : same as `hoodie.write.lock.dynamodb.partition_key`
- `Region`: same as `hoodie.write.lock.dynamodb.region`

AWS SDK dependencies are not bundled with Hudi from v0.10.x and will need to be added to your classpath.
Add the following Maven packages (check the latest versions at time of install):
```
com.amazonaws:dynamodb-lock-client
com.amazonaws:aws-java-sdk-dynamodb
com.amazonaws:aws-java-sdk-core
```

#### FileSystem based (not for production use)

FileSystem based lock provider supports multiple writers cross different jobs/applications based on atomic create/delete operations of the underlying filesystem.

```
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider
```

When using the FileSystem based lock provider, by default, the lock file will store into `hoodie.base.path`+`/.hoodie/lock`. You may use a custom folder to store the lock file by specifying `hoodie.write.lock.filesystem.path`.

In case the lock cannot release during job crash, you can set `hoodie.write.lock.filesystem.expire` (lock will never expire by default) to a desired expire time in minutes. You may also delete lock file manually in such situation.
:::note
FileSystem based lock provider is not supported with cloud storage like S3 or GCS.
:::


## Simple Single writer + table services

Data lakehouse pipelines tend to be predominantly single writer, with the most common need for distributed co-ordination on a table coming from table management. For e.g. a Apache Flink 
job producing fast writes into a table, requiring regular file-size management or cleaning. Hudi's storage engine and platform tools provide a lot of support for such common scenarios.

### Inline table services

This is the simplest form of concurrency, meaning there is no concurrency at all in the write processes. In this model, Hudi eliminates the need for concurrency control and maximizes throughput by supporting these table services out-of-box and running inline after every write to the table. Execution plans are idempotent, persisted to the timeline and auto-recover from failures. For most simple use-cases, this means just writing is sufficient to get a well-managed table that needs no concurrency control.

There is no actual concurrent writing in this model. **MVCC** is leveraged to provide snapshot isolation guarantees between ingestion writer and multiple readers and also between multiple table service writers and readers. Writes to the table either from ingestion or from table services produce versioned data that are available to readers only after the writes are committed. Until then, readers can access only the previous version of the data.

A single writer with all table services such as cleaning, clustering, compaction, etc can be configured to be inline (such as Hudi Streamer sync-once mode and Spark Datasource with default configs) without any additional configs.

### Async table services

Hudi provides the option of running the table services in an async fashion, where most of the heavy lifting (e.g actually rewriting the columnar data by compaction service) is done asynchronously. In this model, the async deployment eliminates any repeated wasteful retries and optimizes the table using clustering techniques while a single writer consumes the writes to the table without having to be blocked by such table services. This model avoids the need for taking an [external lock](#external-locking-and-lock-providers) to control concurrency and avoids the need to separately orchestrate and monitor offline table services jobs..

A single writer along with async table services runs in the same process. For example, you can have a  Hudi Streamer in continuous mode write to a MOR table using async compaction; you can use Spark Streaming (where [compaction](https://hudi.apache.org/docs/compaction) is async by default), and you can use Flink streaming or your own job setup and enable async table services inside the same writer.

Hudi leverages **MVCC** in this model to support running any number of table service jobs concurrently, without any concurrency conflict.  This is made possible by ensuring Hudi 's ingestion writer and async table services coordinate among themselves to ensure no conflicts and avoid race conditions. The same single write guarantees described in Model A above can be achieved in this model as well.
With this model users don't need to spin up different spark jobs and manage the orchestration among it. For larger deployments, this model can ease the operational burden significantly while getting the table services running without blocking the writers.

**Single Writer Guarantees**

In this model, the following are the guarantees on [write operations](https://hudi.apache.org/docs/write_operations/) to expect:

- *UPSERT Guarantee*: The target table will NEVER show duplicates.
- *INSERT Guarantee*: The target table wilL NEVER have duplicates if dedup: [`hoodie.datasource.write.insert.drop.duplicates`](https://hudi.apache.org/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) & [`hoodie.combine.before.insert`](https://hudi.apache.org/docs/configurations/#hoodiecombinebeforeinsert), is enabled.
- *BULK_INSERT Guarantee*: The target table will NEVER have duplicates if dedup: [`hoodie.datasource.write.insert.drop.duplicates`](https://hudi.apache.org/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) & [`hoodie.combine.before.insert`](https://hudi.apache.org/docs/configurations/#hoodiecombinebeforeinsert), is enabled.
- *INCREMENTAL QUERY Guarantee*: Data consumption and checkpoints are NEVER out of order.

## Full-on Multi-writer + Async table services

Hudi has introduced a new concurrency mode `NON_BLOCKING_CONCURRENCY_CONTROL`, where unlike OCC, multiple writers can
operate on the table with non-blocking conflict resolution. The writers can write into the same file group with the
conflicts resolved automatically by the query reader and the compactor. The new concurrency mode is currently
available for preview in version 1.0.1-beta only. You can read more about it under section [Model C: Multi-writer](#model-c-multi-writer).

It is not always possible to serialize all write operations to a table (such as UPSERT, INSERT or DELETE) into the same write process and therefore, multi-writing capability may be required. 
In multi-writing, disparate distributed processes run in parallel or overlapping time windows to write to the same table. In such cases, an external locking mechanism is a must to safely 
coordinate concurrent accesses. Here are few different scenarios that would all fall under multi-writing:

- Multiple ingestion writers to the same table:For instance, two Spark Datasource writers working on different sets of partitions form a source kafka topic.
- Multiple ingestion writers to the same table, including one writer with async table services: For example, a Hudi Streamer with async compaction for regular ingestion & a Spark Datasource writer for backfilling.
- A single ingestion writer and a separate compaction (HoodieCompactor) or clustering (HoodieClusteringJob) job apart from the ingestion writer: This is considered as multi-writing as they are not running in the same process.

Hudi's concurrency model intelligently differentiates actual writing to the table from table services that manage or optimize the table. Hudi offers similar **optimistic concurrency control across multiple writers**, but **table services can still execute completely lock-free and async** as long as they run in the same process as one of the writers.
For multi-writing, Hudi leverages file level optimistic concurrency control(OCC). For example, when two writers write to non overlapping files, both writes are allowed to succeed. However, when the writes from different writers overlap (touch the same set of files), only one of them will succeed. Please note that this feature is currently experimental and requires external lock providers to acquire locks briefly at critical sections during the write. More on lock providers below.

#### Multi Writer Guarantees

With multiple writers using OCC, these are the write guarantees to expect:

- *UPSERT Guarantee*: The target table will NEVER show duplicates.
- *INSERT Guarantee*: The target table MIGHT have duplicates even if dedup is enabled.
- *BULK_INSERT Guarantee*: The target table MIGHT have duplicates even if dedup is enabled.
- *INCREMENTAL PULL Guarantee*: Data consumption and checkpoints are NEVER out of order. If there are inflight commits 
  (due to multi-writing), incremental queries will not expose the completed commits following the inflight commits. 

## Non-Blocking Concurrency Control  

`NON_BLOCKING_CONCURRENCY_CONTROL`, offers the same set of guarantees as mentioned in the case of OCC but without
explicit locks for serializing the writes. Lock is only needed for writing the commit metadata to the Hudi timeline. The
completion time for the commits reflects the serialization order and file slicing is done based on completion time.
Multiple writers can operate on the table with non-blocking conflict resolution. The writers can write into the same
file group with the conflicts resolved automatically by the query reader and the compactor. The new concurrency mode is
currently available for preview in version 1.0.1-beta only with the caveat that conflict resolution is not supported yet
between clustering and ingestion. It works for compaction and ingestion, and we can see an example of that with Flink
writers [here](sql_dml.md#non-blocking-concurrency-control-experimental).

:::note
`NON_BLOCKING_CONCURRENCY_CONTROL` between ingestion writer and table service writer is not yet supported for clustering.
Please use `OPTIMISTIC_CONCURRENCY_CONTROL` for clustering.
:::

## Early conflict Detection

Multi writing using OCC allows multiple writers to concurrently write and atomically commit to the Hudi table if there is no overlapping data file to be written, to guarantee data consistency, integrity and correctness. Prior to 0.13.0 release, as the OCC (optimistic concurrency control) name suggests, each writer will optimistically proceed with ingestion and towards the end, just before committing will go about conflict resolution flow to deduce overlapping writes and abort one if need be. But this could result in lot of compute waste, since the aborted commit will have to retry from beginning. With 0.13.0, Hudi introduced early conflict deduction leveraging markers in hudi to deduce the conflicts eagerly and abort early in the write lifecyle instead of doing it in the end. For large scale deployments, this might avoid wasting lot o compute resources if there could be overlapping concurrent writers.

To improve the concurrency control, the [0.13.0 release](https://hudi.apache.org/releases/release-0.13.0#early-conflict-detection-for-multi-writer) introduced a new feature, early conflict detection in OCC, to detect the conflict during the data writing phase and abort the writing early on once a conflict is detected, using Hudi's marker mechanism. Hudi can now stop a conflicting writer much earlier because of the early conflict detection and release computing resources necessary to cluster, improving resource utilization.

By default, this feature is turned off. To try this out, a user needs to set `hoodie.write.concurrency.early.conflict.detection.enable` to true, when using OCC for concurrency control (Refer [configs](https://hudi.apache.org/docs/next/configurations#Write-Configurations-advanced-configs) page for all relevant configs).
:::note
Early conflict Detection in OCC is an **EXPERIMENTAL** feature
:::


## Enabling Multi Writing

The following properties are needed to be set appropriately to turn on optimistic concurrency control to achieve multi writing.

```
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=<lock-provider-classname>
hoodie.cleaner.policy.failed.writes=LAZY
```

| Config Name                         | Default                                                                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|-------------------------------------|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.write.concurrency.mode       | SINGLE_WRITER (Optional)                                                      | <u>[Concurrency modes](https://github.com/apache/hudi/blob/00ece7bce0a4a8d0019721a28049723821e01842/hudi-common/src/main/java/org/apache/hudi/common/model/WriteConcurrencyMode.java)</u> for write operations.<br />Possible values:<br /><ul><li>`SINGLE_WRITER`: Only one active writer to the table. Maximizes throughput.</li><li>`OPTIMISTIC_CONCURRENCY_CONTROL`: Multiple writers can operate on the table with lazy conflict resolution using locks. This means that only one writer succeeds if multiple writers write to the same file group.</li><li>`NON_BLOCKING_CONCURRENCY_CONTROL`: Multiple writers can operate on the table with non-blocking conflict resolution. The writers can write into the same file group with the conflicts resolved automatically by the query reader and the compactor.</li></ul><br />`Config Param: WRITE_CONCURRENCY_MODE` |
| hoodie.write.lock.provider          | org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider (Optional) | Lock provider class name, user can provide their own implementation of LockProvider which should be subclass of org.apache.hudi.common.lock.LockProvider<br /><br />`Config Param: LOCK_PROVIDER_CLASS_NAME`<br />`Since Version: 0.8.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| hoodie.cleaner.policy.failed.writes | EAGER (Optional)                                                              | org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy: Policy that controls how to clean up failed writes. Hudi will delete any files written by failed writes to re-claim space.     EAGER(default): Clean failed writes inline after every write operation.     LAZY: Clean failed writes lazily after heartbeat timeout when the cleaning service runs. This policy is required when multi-writers are enabled.     NEVER: Never clean failed writes.<br /><br />`Config Param: FAILED_WRITES_CLEANER_POLICY`                                                                                                                                                                                                                                                                                                                                                    |


### Multi Writing via Hudi Streamer

The `HoodieStreamer` utility (part of hudi-utilities-slim-bundle) provides ways to ingest from different sources such as DFS or Kafka, with the following capabilities.

Using optimistic_concurrency_control via Hudi Streamer requires adding the above configs to the properties file that can be passed to the
job. For example below, adding the configs to kafka-source.properties file and passing them to Hudi Streamer will enable optimistic concurrency.
A Hudi Streamer job can then be triggered as follows:

```java
[hoodie]$ spark-submit \
  --jars "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar,packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.1.jar" \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer `ls packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle-*.jar` \
  --props file://${PWD}/hudi-utilities/src/test/resources/streamer-config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --target-base-path file:\/\/\/tmp/hudi-streamer-op \ 
  --target-table tableName \
  --op BULK_INSERT
```

### Multi Writing via Spark Datasource Writer

The `hudi-spark` module offers the DataSource API to write (and read) a Spark DataFrame into a Hudi table.

Following is an example of how to use optimistic_concurrency_control via spark datasource

```java
inputDF.write.format("hudi")
       .options(getQuickstartWriteConfigs)
       .option("hoodie.datasource.write.precombine.field", "ts")
       .option("hoodie.cleaner.policy.failed.writes", "LAZY")
       .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
       .option("hoodie.write.lock.zookeeper.url", "zookeeper")
       .option("hoodie.write.lock.zookeeper.port", "2181")
       .option("hoodie.write.lock.zookeeper.base_path", "/test")
       .option("hoodie.datasource.write.recordkey.field", "uuid")
       .option("hoodie.datasource.write.partitionpath.field", "partitionpath")
       .option("hoodie.table.name", tableName)
       .mode(Overwrite)
       .save(basePath)
```

## Disabling Multi Writing

Remove the following settings that were used to enable multi-writer or override with default values.

```
hoodie.write.concurrency.mode=single_writer
hoodie.cleaner.policy.failed.writes=EAGER
```

## OCC Best Practices

Concurrent Writing to Hudi tables requires acquiring a lock with one of the lock providers mentioned above. Due to several reasons you might want to configure retries to allow your application to acquire the lock. 
1. Network connectivity or excessive load on servers increasing time for lock acquisition resulting in timeouts
2. Running a large number of concurrent jobs that are writing to the same hudi table can result in contention during lock acquisition can cause timeouts
3. In some scenarios of conflict resolution, Hudi commit operations might take upto 10's of seconds while the lock is being held. This can result in timeouts for other jobs waiting to acquire a lock.

Set the correct native lock provider client retries. 
:::note 
Please note that sometimes these settings are set on the server once and all clients inherit the same configs. Please check your settings before enabling optimistic concurrency.
:::
   
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


## Caveats

If you are using the `WriteClient` API, please note that multiple writes to the table need to be initiated from 2 different instances of the write client. 
It is **NOT** recommended to use the same instance of the write client to perform multi writing. 

## Related Resources

<h3>Blogs</h3>
* [Data Lakehouse Concurrency Control](https://www.onehouse.ai/blog/lakehouse-concurrency-control-are-we-too-optimistic)
* [Multi-writer support with Apache Hudi](https://medium.com/@simpsons/multi-writer-support-with-apache-hudi-e1b75dca29e6)

<h3>Videos</h3>

* [Hands on Lab with using DynamoDB as lock table for Apache Hudi Data Lakes](https://youtu.be/JP0orl9_0yQ)
* [Non Blocking Concurrency Control Flink Demo](/blog/2024/12/06/non-blocking-concurrency-control)
