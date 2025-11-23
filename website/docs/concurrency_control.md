---
title: "Concurrency Control"
summary: On this page, we discuss how to perform concurrent writes to Hudi tables.
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
last_modified_at: 2025-11-23T14:20:00
---

Concurrency control defines how different writers, readers, and table services coordinate access to a Hudi table. Hudi ensures atomic writes by publishing commits atomically to the timeline, stamped with an instant time that denotes when the action is deemed to have occurred. Unlike general-purpose file version control, Hudi draws a clear distinction between writer processes that issue [write operations](write_operations), table services that (re)write data/metadata to optimize or perform bookkeeping, and readers (that execute queries and read data).

Hudi provides:

* **Snapshot isolation** between all three types of processes, meaning they all operate on a consistent snapshot of the table.
* **Optimistic Concurrency Control (OCC)** between writers to provide standard relational database semantics.
* **Multiversion Concurrency Control (MVCC)** between writers and table services and between different table services.
* **Non-Blocking Concurrency Control (NBCC)** between writers to provide streaming semantics and avoid live-locks/starvation between writers.

In this section, we discuss the different concurrency controls supported by Hudi and how they enable flexible deployment models for single- and multi-writer scenarios. We also describe ways to ingest data into a Hudi table from multiple writers using components like Hudi Streamer, the Hudi DataSource, Spark Structured Streaming, and Spark SQL.

:::note
If there is only one process performing writing AND async/inline table services on the table, you can avoid the overhead of a distributed lock requirement by configuring the in-process lock provider.

```properties
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider
```

:::

## Distributed Locking

A prerequisite for distributed coordination in Hudi, like many other distributed systems, is a distributed lock provider that processes can use to plan, schedule, and execute actions on the Hudi timeline concurrently. Locks are also used to [generate TrueTime](timeline#truetime-generation).

External locking is typically used with optimistic concurrency control because it prevents conflicts that occur when two or more commits attempt to modify the same resource concurrently. When a transaction attempts to modify a locked resource, it must wait until the lock is released.

In multi-writing scenarios, locks are acquired on the Hudi table for very short periods during specific phases (such as just before committing writes or before scheduling table services) instead of for the entire job duration. This approach allows multiple writers to work on the same table simultaneously, increasing concurrency and avoiding conflicts.

Hudi provides multiple lock providers requiring different configurations. Refer to the [locking configs](configurations#LOCK) for the complete list.

### Storage-Based Lock Provider

The storage-based lock provider eliminates the need for external lock infrastructure by managing concurrency directly using the `.hoodie/` directory in your table's storage layer. This removes dependencies on DynamoDB, ZooKeeper, or Hive Metastore, reducing operational complexity and cost.

It uses conditional writes on a single lock file so only one writer holds the lock at a time, with heartbeat-based renewal and automatic expiration for fault tolerance. Basic setup requires only setting the provider class name, though optional parameters allow tuning lock validity and renewal intervals.

Add the corresponding cloud bundle to your classpath:

* For S3: `hudi-aws-bundle`
* For GCS: `hudi-gcp-bundle`

Set this configuration:

```properties
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.StorageBasedLockProvider
```

Supported for S3 and GCS (additional systems planned). This cloud-native design works directly with storage features, simplifying large-scale cloud operations.

Optional tuning configurations:

| Config Name                                     | Default        | Description |
|-------------------------------------------------|----------------|-------------|
| hoodie.write.lock.storage.validity.timeout.secs | 300 (Optional) | Validity period (seconds) for each new lock. The provider renews its lock until the lease extends or timeout occurs.<br /><br />`Config Param: STORAGE_BASED_LOCK_VALIDITY_TIMEOUT_SECS`<br />`Since Version: 1.0.2` |
| hoodie.write.lock.storage.renew.interval.secs   | 30 (Optional)  | Interval (seconds) between renewal attempts.<br /><br />`Config Param: STORAGE_BASED_LOCK_RENEW_INTERVAL_SECS`<br />`Since Version: 1.0.2` |

### Zookeeper-Based Lock Provider

```properties
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
```

Basic configuration:

| Config Name                           | Default            | Description |
|---------------------------------------|--------------------|-------------|
| hoodie.write.lock.zookeeper.base_path | N/A **(Required)** | Base path on ZooKeeper under which to create lock-related ZNodes. Must be the same for all concurrent writers.<br /><br />`Config Param: ZK_BASE_PATH`<br />`Since Version: 0.8.0` |
| hoodie.write.lock.zookeeper.port      | N/A **(Required)** | ZooKeeper port.<br /><br />`Config Param: ZK_PORT`<br />`Since Version: 0.8.0` |
| hoodie.write.lock.zookeeper.url       | N/A **(Required)** | ZooKeeper connection URL.<br /><br />`Config Param: ZK_CONNECT_URL`<br />`Since Version: 0.8.0` |

### HiveMetastore-Based Lock Provider

```properties
hoodie.write.lock.provider=org.apache.hudi.hive.transaction.lock.HiveMetastoreBasedLockProvider
```

Basic configuration:

| Config Name                              | Default            | Description |
|------------------------------------------|--------------------|-------------|
| hoodie.write.lock.hivemetastore.database | N/A **(Required)** | Hive database to acquire lock against.<br /><br />`Config Param: HIVE_DATABASE_NAME`<br />`Since Version: 0.8.0` |
| hoodie.write.lock.hivemetastore.table    | N/A **(Required)** | Hive table to acquire lock against.<br /><br />`Config Param: HIVE_TABLE_NAME`<br />`Since Version: 0.8.0` |

Hive Metastore URIs are picked up from the Hadoop configuration at runtime.

:::note
ZooKeeper is required for the HMS lock provider. Set ZooKeeper configs via Hudi:

```properties
hoodie.write.lock.zookeeper.url
hoodie.write.lock.zookeeper.port
```

Or via Hive configuration:

```properties
hive.zookeeper.quorum
hive.zookeeper.client.port
```

:::

### DynamoDB-Based Lock Provider

```properties
hoodie.write.lock.provider=org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider
```

The Amazon DynamoDB–based lock provider supports multi-writing across clusters. See [DynamoDB-Based Locks Configurations](configurations#DynamoDB-based-Locks-Configurations) for all options. Basic configuration:

| Config Name                             | Default            | Description |
|-----------------------------------------|--------------------|-------------|
| hoodie.write.lock.dynamodb.endpoint_url | N/A **(Required)** | Endpoint URL for DynamoDB (useful for local testing).<br /><br />`Config Param: DYNAMODB_ENDPOINT_URL`<br />`Since Version: 0.10.1` |

Further configurations: [DynamoDB-Based Locks Configurations](configurations#DynamoDB-based-Locks-Configurations)

Table creation: Hudi auto-creates the DynamoDB table specified by `hoodie.write.lock.dynamodb.table`. If using an existing table, ensure a `key` attribute (as partition key) exists. `hoodie.write.lock.dynamodb.partition_key` is the value written for the partition key (default: table name), ensuring multiple writers share the same lock.

Credential props (if not using the default provider chain):

```properties
hoodie.aws.access.key
hoodie.aws.secret.key
hoodie.aws.session.token
```

If unset, Hudi falls back to the [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).

Required IAM permissions:

```json
{
  "Sid": "DynamoDBLocksTable",
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

* `TableName`: same as `hoodie.write.lock.dynamodb.partition_key`
* `Region`: same as `hoodie.write.lock.dynamodb.region`

Add these Maven dependencies (not bundled since v0.10.x):

```text
com.amazonaws:dynamodb-lock-client
com.amazonaws:aws-java-sdk-dynamodb
com.amazonaws:aws-java-sdk-core
```

### FileSystem-Based Lock Provider

```properties
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider
```

The FileSystem-based lock provider supports multiple writers across jobs/applications using atomic create/delete operations. By default, the lock file is stored at `hoodie.base.path/.hoodie/lock`. Override with `hoodie.write.lock.filesystem.path`.

If the lock is not released during a crash, set `hoodie.write.lock.filesystem.expire` (never expires by default) to a desired expiration time (minutes), or delete the lock file manually.

:::warning
The FileSystem-based lock provider is not for production use and is not supported on S3 or GCS.
:::

## Simple Single Writer + Table Services

Data lakehouse pipelines tend to be predominantly single-writer, with most coordination needs coming from table management (e.g., an Apache Flink job producing fast writes requiring regular file-size management or cleaning). Hudi's storage engine and tools support such scenarios well.

### Inline Table Services

This is the simplest form of concurrency: there is no concurrency in write processes. Hudi maximizes throughput by running table services inline after every write. Execution plans are idempotent, persisted to the timeline, and auto-recover. For most simple cases, just writing yields a well-managed table.

A single writer with services like cleaning, clustering, and compaction can run inline (e.g., Hudi Streamer sync-once mode or Spark DataSource defaults) without extra configs.

There is no actual concurrent writing. **MVCC** provides snapshot isolation between the ingestion writer, table-service writers, and readers. Writes become visible only after commit; until then, readers see the previous version.

### Async Table Services

Async mode runs heavy operations (e.g., compaction rewriting columnar data) in the background while ingestion continues. This avoids wasted retries and optimizes layout (e.g., clustering) without blocking ingestion. No external lock provider or separate orchestration is needed.

A single writer plus async services runs in one process (e.g., Hudi Streamer continuous mode with async compaction, Spark Streaming with async [compaction](compaction), or Flink streaming with enabled async services).

Hudi leverages **MVCC** to run any number of table-service jobs concurrently without conflicts. Coordination avoids race conditions while preserving single-writer guarantees. This reduces operational burden for larger deployments.

**Single Writer Guarantees**

* *UPSERT Guarantee*: The target table will NEVER show duplicates.
* *INSERT Guarantee*: The target table will NEVER have duplicates if dedup is enabled via [`hoodie.datasource.write.insert.drop.duplicates`](configurations#hoodiedatasourcewriteinsertdropduplicates) and [`hoodie.combine.before.insert`](configurations#hoodiecombinebeforeinsert).
* *BULK_INSERT Guarantee*: Same as INSERT for dedup-enabled configuration.
* *INCREMENTAL QUERY Guarantee*: Data consumption and checkpoints are NEVER out of order.

## Full-on Multi-Writer + Async Table Services

`NON_BLOCKING_CONCURRENCY_CONTROL` (preview) lets multiple writers operate with non-blocking conflict resolution. Writers can write into the same file group with conflicts resolved automatically by readers and the compactor (see [Non-Blocking Concurrency Control](#non-blocking-concurrency-control)).

When serializing all write operations (UPSERT, INSERT, DELETE) into one process is impractical, multi-writing is needed. Disparate distributed processes may overlap in time writing to the same table; a lock provider helps coordinate access safely.

Scenarios:

* Multiple ingestion writers (e.g., two Spark DataSource writers on different partition sets from a Kafka topic).
* Multiple ingestion writers including one with async services (e.g., Hudi Streamer with async compaction + a Spark DataSource backfill job).
* A single ingestion writer plus separate compaction (HoodieCompactor) or clustering (HoodieClusteringJob) job not in the same process.

Hudi offers **optimistic concurrency control across multiple writers**, while table services can still run lock-free and async if co-located with a writer. It uses file-level OCC: non-overlapping writes both succeed; overlapping writes—touching the same files—allow only one to succeed. (Experimental; requires brief external lock acquisition.)

### Multi-Writer Guarantees

With multiple writers using OCC:

* *UPSERT Guarantee*: The target table will NEVER show duplicates.
* *INSERT Guarantee*: The target table MIGHT have duplicates even if dedup is enabled.
* *BULK_INSERT Guarantee*: The target table MIGHT have duplicates even if dedup is enabled.
* *INCREMENTAL PULL Guarantee*: Data consumption and checkpoints are NEVER out of order. If there are inflight commits, incremental queries will not expose completed commits that follow inflight commits.

## Non-Blocking Concurrency Control

`NON_BLOCKING_CONCURRENCY_CONTROL` offers OCC-like guarantees without explicit locks except for writing commit metadata. Completion time reflects serialization; file slicing uses completion order. Multiple writers can write into the same file group with conflicts resolved automatically by readers and the compactor. Preview in 1.0.1-beta; clustering + ingestion conflict resolution not yet supported (works for compaction + ingestion). See Flink examples in the [SQL DML documentation](sql_dml#non-blocking-concurrency-control-experimental).

:::note
`NON_BLOCKING_CONCURRENCY_CONTROL` between ingestion writer and table-service writer is not yet supported for clustering. Use `OPTIMISTIC_CONCURRENCY_CONTROL` for clustering.
:::

## Early Conflict Detection

Prior to 0.13.0, writers using OCC detected conflicts only near commit time, wasting compute on aborted conflicting writes. Early conflict detection (0.13.0) leverages markers to detect overlaps sooner and abort earlier, saving resources at scale.

By default off; enable via `hoodie.write.concurrency.early.conflict.detection.enable=true` when using OCC (see [configs](configurations#Write-Configurations-advanced-configs)).

:::note
Early conflict detection in OCC is an **EXPERIMENTAL** feature.
:::

## Enabling Multi-Writing

Set the following to enable optimistic concurrency control:

```properties
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.write.lock.provider=<lock-provider-classname>
hoodie.cleaner.policy.failed.writes=LAZY
```

| Config Name                         | Default                                                                       | Description |
|-------------------------------------|-------------------------------------------------------------------------------|-------------|
| hoodie.write.concurrency.mode       | SINGLE_WRITER (Optional)                                                      | <u>[Concurrency modes](https://github.com/apache/hudi/blob/00ece7bce0a4a8d0019721a28049723821e01842/hudi-common/src/main/java/org/apache/hudi/common/model/WriteConcurrencyMode.java)</u>.<br />Values: `SINGLE_WRITER`, `OPTIMISTIC_CONCURRENCY_CONTROL`, `NON_BLOCKING_CONCURRENCY_CONTROL`. |
| hoodie.write.lock.provider          | org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider (Optional) | Lock provider class name; must subclass `org.apache.hudi.common.lock.LockProvider`.<br />`Config Param: LOCK_PROVIDER_CLASS_NAME`<br />`Since Version: 0.8.0` |
| hoodie.cleaner.policy.failed.writes | EAGER (Optional)                                                              | `HoodieFailedWritesCleaningPolicy`: EAGER (inline), LAZY (after heartbeat timeout; required for multi-writers), NEVER (no cleanup).<br />`Config Param: FAILED_WRITES_CLEANER_POLICY` |

### Multi-Writing via Hudi Streamer

Add configs to the properties file (e.g., `kafka-source.properties`) and run:

```java
[hoodie]$ spark-submit \
  --jars "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.1.jar,packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.1.jar" \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer `ls packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle-*.jar` \
  --props file://${PWD}/hudi-utilities/src/test/resources/streamer-config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --target-base-path file:///tmp/hudi-streamer-op \
  --target-table tableName \
  --op BULK_INSERT
```

### Multi-Writing via Spark DataSource Writer

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

## Disabling Multi-Writing

Remove or reset these settings:

```properties
hoodie.write.concurrency.mode=single_writer
hoodie.cleaner.policy.failed.writes=EAGER
```

## OCC Best Practices

Configure retries so applications can reliably acquire locks.

1. Network latency or server load lengthens acquisition time causing timeouts.
2. High concurrency causes contention on the same table.
3. Conflict resolution may hold locks for tens of seconds.

Native client retry settings:

```properties
hoodie.write.lock.wait_time_ms
hoodie.write.lock.num_retries
```

Hudi client retries (in addition to native retries):

```properties
hoodie.write.lock.client.wait_time_ms
hoodie.write.lock.client.num_retries
```

*Set values case-by-case; defaults cover general needs.*

## Caveats

When using the `WriteClient` API, initiate multi-writing from two different `WriteClient` instances. Reusing the same instance for concurrent writes is **NOT** recommended.

## Related Resources

<h3>Blogs</h3>
* [Data Lakehouse Concurrency Control](https://www.onehouse.ai/blog/lakehouse-concurrency-control-are-we-too-optimistic)
* [Multi-writer support with Apache Hudi](https://medium.com/@simpsons/multi-writer-support-with-apache-hudi-e1b75dca29e6)

<h3>Videos</h3>

* [Hands on Lab with using DynamoDB as lock table for Apache Hudi Data Lakes](https://youtu.be/JP0orl9_0yQ)
* [Non Blocking Concurrency Control Flink Demo](/blog/2024/12/06/non-blocking-concurrency-control)
