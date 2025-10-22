---
title: Writing Tables FAQ
keywords: [hudi, writing, reading]
---

# Writing Tables FAQ

### What are some ways to write a Hudi table?

Typically, you obtain a set of partial updates/inserts from your source and issue [write operations](/docs/write_operations/) against a Hudi table. If you ingesting data from any of the standard sources like Kafka, or tailing DFS, the [delta streamer](/docs/hoodie_streaming_ingestion#hudi-streamer) tool is invaluable and provides an easy, self-managed solution to getting data written into Hudi. You can also write your own code to capture data from a custom source using the Spark datasource API and use a [Hudi datasource](/docs/writing_data#spark-datasource-api) to write into Hudi.

### How is a Hudi writer job deployed?

The nice thing about Hudi writing is that it just runs like any other spark job would on a YARN/Mesos or even a K8S cluster. So you could simply use the Spark UI to get visibility into write operations.

### Can I implement my own logic for how input records are merged with record on storage?

Here is the payload interface that is used in Hudi to represent any hudi record.

```java
public interface HoodieRecordPayload<T extends HoodieRecordPayload> extends Serializable {
 /**
   * When more than one HoodieRecord have the same HoodieKey, this function combines them before attempting to insert/upsert by taking in a property map.
   * Implementation can leverage the property to decide their business logic to do preCombine.
   * @param another instance of another {@link HoodieRecordPayload} to be combined with.
   * @param properties Payload related properties. For example pass the ordering field(s) name to extract from value in storage.
   * @return the combined value
   */
  default T preCombine(T another, Properties properties);
/**
   * This methods lets you write custom merging/combining logic to produce new values as a function of current value on storage and whats contained
   * in this object. Implementations can leverage properties if required.
   * <p>
   * eg:
   * 1) You are updating counters, you may want to add counts to currentValue and write back updated counts
   * 2) You may be reading DB redo logs, and merge them with current image for a database row on storage
   * </p>
   *
   * @param currentValue Current value in storage, to merge/combine this payload with
   * @param schema Schema used for record
   * @param properties Payload related properties. For example pass the ordering field(s) name to extract from value in storage.
   * @return new combined/merged value to be written back to storage. EMPTY to skip writing this record.
   */
  default Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException;
   
/**
   * Generates an avro record out of the given HoodieRecordPayload, to be written out to storage. Called when writing a new value for the given
   * HoodieKey, wherein there is no existing record in storage to be combined against. (i.e insert) Return EMPTY to skip writing this record.
   * Implementations can leverage properties if required.
   * @param schema Schema used for record
   * @param properties Payload related properties. For example pass the ordering field(s) name to extract from value in storage.
   * @return the {@link IndexedRecord} to be inserted.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  default Option<IndexedRecord> getInsertValue(Schema schema, Properties properties) throws IOException;
/**
   * This method can be used to extract some metadata from HoodieRecordPayload. The metadata is passed to {@code WriteStatus.markSuccess()} and
   * {@code WriteStatus.markFailure()} in order to compute some aggregate metrics using the metadata in the context of a write success or failure.
   * @return the metadata in the form of Map<String, String> if any.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  default Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }
}
```

As you could see, ([combineAndGetUpdateValue(), getInsertValue()](https://github.com/apache/hudi/blob/master/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordPayload.java)) that control how the record on storage is combined with the incoming update/insert to generate the final value to be written back to storage. preCombine() is used to merge records within the same incoming batch.

### How do I delete records in the dataset using Hudi?

GDPR has made deletes a must-have tool in everyone's data management toolbox. Hudi supports both soft and hard deletes. For details on how to actually perform them, see [here](/docs/writing_data#deletes).

### Should I need to worry about deleting all copies of the records in case of duplicates?

No. Hudi removes all the copies of a record key when deletes are issued. Here is the long form explanation - Sometimes accidental user errors can lead to duplicates introduced into a Hudi table by either [concurrent inserts](/faq/writing_tables#can-concurrent-inserts-cause-duplicates) or by [not deduping the input records](/faq/writing_tables#can-single-writer-inserts-have-duplicates) for an insert operation. However, using the right index (e.g., in the default [Simple Index](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/simple/HoodieSimpleIndex.java#L116) and [Bloom Index](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/bloom/HoodieBloomIndex.java#L309)), any subsequent updates and deletes are applied to all copies of the same primary key. This is because the indexing phase identifies records of a primary key in all locations.  So deletes in Hudi remove all copies of the same primary key, i.e., duplicates, and comply with GDPR or CCPA requirements.  Here are two examples [1](https://gist.github.com/yihua/6eb11ce3f888a71935dbf21c77199a48), [2](https://gist.github.com/yihua/e3afe0f34400e60f81f6da925560118e) demonstrating that duplicates are properly deleted from a Hudi table. Hudi is adding [auto key generation](https://github.com/apache/hudi/pull/8107), which will remove the burden of key generation from the user for insert workloads.

### How does Hudi handle duplicate record keys in an input?

When issuing an `upsert` operation on a table and the batch of records provided contains multiple entries for a given key, then all of them are reduced into a single final value by repeatedly calling payload class's [preCombine()](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordPayload.java#L40) method . By default, we pick the record with the greatest value (determined by calling .compareTo()) giving latest-write-wins style semantics. [This FAQ entry](/faq/writing_tables#can-i-implement-my-own-logic-for-how-input-records-are-merged-with-record-on-storage) shows the interface for HoodieRecordPayload if you are interested.

For an insert or bulk_insert operation, no such pre-combining is performed. Thus, if your input contains duplicates, the table would also contain duplicates. If you don't want duplicate records either issue an **upsert** or consider specifying option to de-duplicate input in either datasource using [`hoodie.datasource.write.insert.drop.duplicates`](/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) & [`hoodie.combine.before.insert`](/docs/configurations/#hoodiecombinebeforeinsert) or in deltastreamer using [`--filter-dupes`](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/hudi-utilities/src/main/java/org/apache/hudi/utilities/deltastreamer/HoodieDeltaStreamer.java#L229).

### How can I pass hudi configurations to my spark writer job?

Hudi configuration options covering the datasource and low level Hudi write client (which both deltastreamer & datasource internally call) are [here](/docs/configurations/). Invoking _--help_ on any tool such as DeltaStreamer would print all the usage options. A lot of the options that control upsert, file sizing behavior are defined at the write client level and below is how we pass them to different options available for writing data.

*   For Spark DataSource, you can use the "options" API of DataFrameWriter to pass in these configs.

```scala
inputDF.write().format("org.apache.hudi")
  .options(clientOpts) // any of the Hudi client opts can be passed in as well
  .option("hoodie.datasource.write.recordkey.field", "_row_key")
  ...
```

*   When using `HoodieWriteClient` directly, you can simply construct HoodieWriteConfig object with the configs in the link you mentioned.
*   When using HoodieDeltaStreamer tool to ingest, you can set the configs in properties file and pass the file as the cmdline argument "_--props_"

### How to create Hive style partition folder structure?

By default Hudi creates the partition folders with just the partition values, but if would like to create partition folders similar to the way Hive will generate the structure, with paths that contain key value pairs, like country=us/… or datestr=2021-04-20. This is Hive style (or format) partitioning. The paths include both the names of the partition keys and the values that each path represents.

To enable hive style partitioning, you need to add this hoodie config when you write your data:

```plain
hoodie.datasource.write.hive_style_partitioning: true
```

### Can I register my Hudi table with Apache Hive metastore?

Yes. This can be performed either via the standalone [Hive Sync tool](/docs/syncing_metastore#hive-sync-tool) or using options in [Hudi Streamer](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/docker/demo/sparksql-incremental.commands#L50) tool or [datasource](/docs/configurations#hoodiedatasourcehive_syncenable).

### What's Hudi's schema evolution story?

Hudi uses Avro as the internal canonical representation for records, primarily due to its nice [schema compatibility & evolution](https://docs.confluent.io/platform/current/schema-registry/avro.html) properties. This is a key aspect of having reliability in your ingestion or ETL pipelines. As long as the schema passed to Hudi (either explicitly in Hudi Streamer schema provider configs or implicitly by Spark Datasource's Dataset schemas) is backwards compatible (e.g no field deletes, only appending new fields to schema), Hudi will seamlessly handle read/write of old and new data and also keep the Hive schema up-to date.

Starting 0.11.0, Spark SQL DDL support (experimental) was added for Spark 3.1.x and Spark 3.2.1 via ALTER TABLE syntax. Please refer to the [schema evolution guide](/docs/schema_evolution) for more details on  Schema-on-read for Spark..

### What performance/ingest latency can I expect for Hudi writing?

The speed at which you can write into Hudi depends on the [write operation](/docs/write_operations) and some trade-offs you make along the way like file sizing. Just like how databases incur overhead over direct/raw file I/O on disks, Hudi operations may have overhead from supporting database like features compared to reading/writing raw DFS files. That said, Hudi implements advanced techniques from database literature to keep these minimal. User is encouraged to have this perspective when trying to reason about Hudi performance. As the saying goes : there is no free lunch (not yet atleast)

| Storage Type | Type of workload | Performance | Tips |
| ---| ---| ---| --- |
| copy on write | bulk_insert | Should match vanilla spark writing + an additional sort to properly size files | properly size [bulk insert parallelism](/docs/configurations#hoodiebulkinsertshuffleparallelism) to get right number of files. Use insert if you want this auto tuned. Configure [hoodie.bulkinsert.sort.mode](/docs/configurations#hoodiebulkinsertsortmode) for better file sizes at the cost of memory. The default value `NONE` offers the fastest performance and matches `spark.write.parquet()` in terms of number of files, overheads. |
| copy on write | insert | Similar to bulk insert, except the file sizes are auto tuned requiring input to be cached into memory and custom partitioned. | Performance would be bound by how parallel you can write the ingested data. Tune [this limit](/docs/configurations#hoodieinsertshuffleparallelism) up, if you see that writes are happening from only a few executors. |
| copy on write | upsert/ de-duplicate & insert | Both of these would involve index lookup. Compared to naively using Spark (or similar framework)'s JOIN to identify the affected records, Hudi indexing is often 7-10x faster as long as you have ordered keys (discussed below) or less than 50% updates. Compared to naively overwriting entire partitions, Hudi write can be several magnitudes faster depending on how many files in a given partition is actually updated. For example, if a partition has 1000 files out of which only 100 is dirtied every ingestion run, then Hudi would only read/merge a total of 100 files and thus 10x faster than naively rewriting entire partition. | Ultimately performance would be bound by how quickly we can read and write a parquet file and that depends on the size of the parquet file, configured [here](/docs/configurations#hoodieparquetmaxfilesize). Also be sure to properly tune your [bloom filters](/docs/configurations#INDEX). [HUDI-56](https://issues.apache.org/jira/browse/HUDI-56) will auto-tune this. |
| merge on read | bulk insert | Currently new data only goes to parquet files and thus performance here should be similar to copy on write bulk insert. This has the nice side-effect of getting data into parquet directly for query performance. [HUDI-86](https://issues.apache.org/jira/browse/HUDI-86) will add support for logging inserts directly and this up drastically. |  |
| merge on read | insert | Similar to above |  |
| merge on read | upsert/ de-duplicate & insert | Indexing performance would remain the same as copy-on-write, while ingest latency for updates (costliest I/O operation in copy on write) are sent to log files and thus with asynchronous compaction provides very good ingest performance with low write amplification. |  |

Like with many typical system that manage time-series data, Hudi performs much better if your keys have a timestamp prefix or monotonically increasing/decreasing. You can almost always achieve this. Even if you have UUID keys, you can follow tricks like [this](https://www.percona.com/blog/2014/12/19/store-uuid-optimized-way/) to get keys that are ordered. See also [Tuning Guide](/docs/tuning-guide) for more tips on JVM and other configurations.

### What performance can I expect for Hudi reading/queries?

*   For ReadOptimized views, you can expect the same best in-class columnar query performance as a standard parquet table in Hive/Spark/Presto
*   For incremental views, you can expect speed up relative to how much data usually changes in a given time window and how much time your entire scan takes. For e.g, if only 100 files changed in the last hour in a partition of 1000 files, then you can expect a speed of 10x using incremental pull in Hudi compared to full scanning the partition to find out new data.
*   For real time views, you can expect performance similar to the same avro backed table in Hive/Spark/Presto

### How do I to avoid creating tons of small files?

A key design decision in Hudi was to avoid creating small files and always write properly sized files.

There are 2 ways to avoid creating tons of small files in Hudi and both of them have different trade-offs:

a) **Auto Size small files during ingestion**: This solution trades ingest/writing time to keep queries always efficient. Common approaches to writing very small files and then later stitching them together only solve for system scalability issues posed by small files and also let queries slow down by exposing small files to them anyway.

Hudi has the ability to maintain a configured target file size, when performing **upsert/insert** operations. (Note: **bulk_insert** operation does not provide this functionality and is designed as a simpler replacement for normal `spark.write.parquet` )

For **copy-on-write**, this is as simple as configuring the [maximum size for a base/parquet file](/docs/configurations#hoodieparquetmaxfilesize) and the [soft limit](/docs/configurations#hoodieparquetsmallfilelimit) below which a file should be considered a small file. For the initial bootstrap to Hudi table, tuning record size estimate is also important to ensure sufficient records are bin-packed in a parquet file. For subsequent writes, Hudi automatically uses average record size based on previous commit. Hudi will try to add enough records to a small file at write time to get it to the configured maximum limit. For e.g , with `hoodie.parquet.max.file.size=100MB` and hoodie.parquet.small.file.limit=120MB, Hudi will pick all files < 100MB and try to get them upto 120MB.

For **merge-on-read**, there are few more configs to set. MergeOnRead works differently for different INDEX choices.

*   Indexes with **canIndexLogFiles = true** : Inserts of new data go directly to log files. In this case, you can configure the [maximum log size](/docs/configurations#hoodielogfilemaxsize) and a [factor](/docs/configurations#hoodielogfiletoparquetcompressionratio) that denotes reduction in size when data moves from avro to parquet files.
*   Indexes with **canIndexLogFiles = false** : Inserts of new data go only to parquet files. In this case, the same configurations as above for the COPY_ON_WRITE case applies.

NOTE : In either case, small files will be auto sized only if there is no PENDING compaction or associated log file for that particular file slice. For example, for case 1: If you had a log file and a compaction C1 was scheduled to convert that log file to parquet, no more inserts can go into that log file. For case 2: If you had a parquet file and an update ended up creating an associated delta log file, no more inserts can go into that parquet file. Only after the compaction has been performed and there are NO log files associated with the base parquet file, can new inserts be sent to auto size that parquet file.

b) [**Clustering**](/blog/2021/01/27/hudi-clustering-intro) : This is a feature in Hudi to group small files into larger ones either synchronously or asynchronously. Since first solution of auto-sizing small files has a tradeoff on ingestion speed (since the small files are sized during ingestion), if your use-case is very sensitive to ingestion latency where you don't want to compromise on ingestion speed which may end up creating a lot of small files, clustering comes to the rescue. Clustering can be scheduled through the ingestion job and an asynchronus job can stitch small files together in the background to generate larger files. NOTE that during this, ingestion can continue to run concurrently.

_Please note that Hudi always creates immutable files on disk. To be able to do auto-sizing or clustering, Hudi will always create a newer version of the smaller file, resulting in 2 versions of the same file. The cleaner service will later kick in and delte the older version small file and keep the latest one._

### How do I use DeltaStreamer or Spark DataSource API to write to a Non-partitioned Hudi table ?

Hudi supports writing to non-partitioned tables. For writing to a non-partitioned Hudi table and performing hive table syncing, you need to set the below configurations in the properties passed:

```plain
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator
hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.NonPartitionedExtractor
```

### How can I reduce table versions created by Hudi in AWS Glue Data Catalog/ metastore?

With each commit, Hudi creates a new table version in the metastore. This can be reduced by setting the option

[hoodie.datasource.meta_sync.condition.sync](/docs/configurations#hoodiedatasourcemeta_syncconditionsync) to true.

This will ensure that hive sync is triggered on schema or partitions changes.

### If there are failed writes in my timeline, do I see duplicates?

No, Hudi does not expose uncommitted files/blocks to the readers. Further, Hudi strives to automatically manage the table for the user, by actively cleaning up files created from failed/aborted writes. See [marker mechanism](/blog/2021/08/18/improving-marker-mechanism/).

### How are conflicts detected in Hudi between multiple writers?

Hudi employs [optimistic concurrency control](/docs/concurrency_control) between writers, while implementing MVCC based concurrency control between writers and the table services. Concurrent writers to the same table need to be configured with the same lock provider configuration, to safely perform writes. By default (implemented in “[SimpleConcurrentFileWritesConflictResolutionStrategy](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/SimpleConcurrentFileWritesConflictResolutionStrategy.java)”), Hudi allows multiple writers to concurrently write data and commit to the timeline if there is no conflicting writes to the same underlying file group IDs. This is achieved by holding a lock, checking for changes that modified the same file IDs. Hudi then supports a pluggable interface “[ConflictResolutionStrategy](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/ConflictResolutionStrategy.java)” that determines how conflicts are handled. By default, the later conflicting write is aborted. Hudi also support eager conflict detection to help speed up conflict detection and release cluster resources back early to reduce costs.

### Can single-writer inserts have duplicates?

By default, Hudi turns off key based de-duplication for INSERT/BULK_INSERT operations and thus the table could contain duplicates. If users believe, they have duplicates in inserts, they can either issue UPSERT or consider specifying the option to de-duplicate input in either datasource using [`hoodie.datasource.write.insert.drop.duplicates`](/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) & [`hoodie.combine.before.insert`](/docs/configurations/#hoodiecombinebeforeinsert) or in deltastreamer using [`--filter-dupes`](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/hudi-utilities/src/main/java/org/apache/hudi/utilities/deltastreamer/HoodieDeltaStreamer.java#L229).

### Can concurrent inserts cause duplicates?

Yes. As mentioned before, the default conflict detection strategy only check for conflicting updates to the same file group IDs. In the case of concurrent inserts, inserted records end up creating new file groups and thus can go undetected. Most common workload patterns use multi-writer capability in the case of running ingestion of new data and concurrently backfilling/deleting older data, with NO overlap in the primary keys of the records. However, this can be implemented (or better yet contributed) by a new “[ConflictResolutionStrategy](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/ConflictResolutionStrategy.java)”, that reads out keys of new conflicting operations, to check the uncommitted data against other concurrent writes and then decide whether or not to commit/abort. This is rather a fine tradeoff between saving the additional cost of reading keys on most common workloads. Historically, users have preferred to take this into their control to save costs e.g we turned off de-duplication for inserts due to the same feedback. Hudi supports a pre-commit validator mechanism already where such tests can be authored as well.
