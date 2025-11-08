---
title: FAQs
keywords: [hudi, writing, reading]
last_modified_at: 2021-08-18T15:59:57-04:00
---
# FAQs

## General

### When is Hudi useful for me or my organization?

If you are looking to quickly ingest data onto HDFS or cloud storage, Hudi can provide you tools to [help](https://hudi.apache.org/docs/writing_data/). Also, if you have ETL/hive/spark jobs which are slow/taking up a lot of resources, Hudi can potentially help by providing an incremental approach to reading and writing data.

As an organization, Hudi can help you build an [efficient data lake](https://docs.google.com/presentation/d/1FHhsvh70ZP6xXlHdVsAI0g__B_6Mpto5KQFlZ0b8-mM/edit#slide=id.p), solving some of the most complex, low-level storage management problems, while putting data into hands of your data analysts, engineers and scientists much quicker.

### What are some non-goals for Hudi?

Hudi is not designed for any OLTP use-cases, where typically you are using existing NoSQL/RDBMS data stores. Hudi cannot replace your in-memory analytical database (at-least not yet!). Hudi support near-real time ingestion in the order of few minutes, trading off latency for efficient batching. If you truly desirable sub-minute processing delays, then stick with your favorite stream processing solution.

### What is incremental processing? Why does Hudi docs/talks keep talking about it?

Incremental processing was first introduced by Vinoth Chandar, in the O'reilly [blog](https://www.oreilly.com/content/ubers-case-for-incremental-processing-on-hadoop/), that set off most of this effort. In purely technical terms, incremental processing merely refers to writing mini-batch programs in streaming processing style. Typical batch jobs consume **all input** and recompute **all output**, every few hours. Typical stream processing jobs consume some **new input** and recompute **new/changes to output**, continuously/every few seconds. While recomputing all output in batch fashion can be simpler, it's wasteful and resource expensive. Hudi brings ability to author the same batch pipelines in streaming fashion, run every few minutes.

While we can merely refer to this as stream processing, we call it _incremental processing_, to distinguish from purely stream processing pipelines built using Apache Flink or Apache Kafka Streams.

### How is Hudi optimized for CDC and streaming use cases?

One of the core use-cases for Apache Hudi is enabling seamless, efficient database ingestion to your lake, and change data capture is a direct application of that. Hudi’s core design primitives support fast upserts and deletes of data that are suitable for CDC and streaming use cases. Here is a glimpse of some of the challenges accompanying streaming and cdc workloads that Hudi handles efficiently out of the box.

*   **_Processing of deletes:_** Deletes are treated no differently than updates and are logged with the same filegroups where the corresponding keys exist. This helps process deletes faster same like regular inserts and updates and Hudi processes deletes at file group level using compaction in MOR tables. This can be very expensive in other open source systems that store deletes as separate files than data files and incur N(Data files)\*N(Delete files) merge cost to process deletes every time, soon lending into a complex graph problem to solve whose planning itself is expensive. This gets worse with volume, especially when dealing with CDC style workloads that streams changes to records frequently.
*   **_Operational overhead of merging deletes at scale:_** When deletes are stored as separate files without any notion of data locality, the merging of data and deletes can become a run away job that cannot complete in time due to various reasons (Spark retries, executor failure, OOM, etc.). As more data files and delete files are added, the merge becomes even more expensive and complex later on, making it hard to manage in practice causing operation overhead. Hudi removes this complexity from users by treating deletes similarly to any other write operation.
*   **_File sizing with updates:_** Other open source systems, process updates by generating new data files for inserting the new records after deletion, where both data files and delete files get introduced for every batch of updates. This yields to small file problem and requires file sizing. Whereas, Hudi embraces mutations to the data, and manages the table automatically by keeping file sizes in check without passing the burden of file sizing to users as manual maintenance.
*   **_Support for partial updates and payload ordering:_** Hudi support partial updates where already existing record can be updated for specific fields that are non null from newer records (with newer timestamps). Similarly, Hudi supports payload ordering with timestamp through specific payload implementation where late-arriving data with older timestamps will be ignored or dropped. Users can even implement custom logic and plug in to handle what they want.

### How do I choose a storage type for my workload?

A key goal of Hudi is to provide **upsert functionality** that is orders of magnitude faster than rewriting entire tables or partitions.

Choose Copy-on-write storage if :

*   You are looking for a simple alternative, that replaces your existing parquet tables without any need for real-time data.
*   Your current job is rewriting entire table/partition to deal with updates, while only a few files actually change in each partition.
*   You are happy keeping things operationally simpler (no compaction etc), with the ingestion/write performance bound by the [parquet file size](https://hudi.apache.org/docs/configurations#hoodieparquetmaxfilesize) and the number of such files affected/dirtied by updates
*   Your workload is fairly well-understood and does not have sudden bursts of large amount of update or inserts to older partitions. COW absorbs all the merging cost on the writer side and thus these sudden changes can clog up your ingestion and interfere with meeting normal mode ingest latency targets.

Choose merge-on-read storage if :

*   You want the data to be ingested as quickly & queryable as much as possible.
*   Your workload can have sudden spikes/changes in pattern (e.g bulk updates to older transactions in upstream database causing lots of updates to old partitions on DFS). Asynchronous compaction helps amortize the write amplification caused by such scenarios, while normal ingestion keeps up with incoming stream of changes.

Immaterial of what you choose, Hudi provides

*   Snapshot isolation and atomic write of batch of records
*   Incremental pulls
*   Ability to de-duplicate data

Find more [here](https://hudi.apache.org/docs/concepts/).

### Is Hudi an analytical database?

A typical database has a bunch of long running storage servers always running, which takes writes and reads. Hudi's architecture is very different and for good reasons. It's highly decoupled where writes and queries/reads can be scaled independently to be able to handle the scale challenges. So, it may not always seems like a database.

Nonetheless, Hudi is designed very much like a database and provides similar functionality (upserts, change capture) and semantics (transactional writes, snapshot isolated reads).

### How do I model the data stored in Hudi?

When writing data into Hudi, you model the records like how you would on a key-value store - specify a key field (unique for a single partition/across table), a partition field (denotes partition to place key into) and preCombine/combine logic that specifies how to handle duplicates in a batch of records written. This model enables Hudi to enforce primary key constraints like you would get on a database table. See [here](https://hudi.apache.org/docs/writing_data/) for an example.

When querying/reading data, Hudi just presents itself as a json-like hierarchical table, everyone is used to querying using Hive/Spark/Presto over Parquet/Json/Avro.

### Why does Hudi require a key field to be configured?

Hudi was designed to support fast record level Upserts and thus requires a key to identify whether an incoming record is
an insert or update or delete, and process accordingly. Additionally, Hudi automatically maintains indexes on this primary
key and for many use-cases like CDC, ensuring such primary key constraints is crucial to ensure data quality. In this context,
pre combine key helps reconcile multiple records with same key in a single batch of input records. Even for append-only data
streams, Hudi supports key based de-duplication before inserting records. For e-g; you may have atleast once data integration
systems like Kafka MirrorMaker that can introduce duplicates during failures. Even for plain old batch pipelines, keys
help eliminate duplication that could be caused by backfill pipelines, where commonly it's unclear what set of records
need to be re-written. We are actively working on making keys easier by only requiring them for Upsert and/or automatically
generate the key internally (much like RDBMS row\_ids)

### How does Hudi actually store data inside a table?

At a high level, Hudi is based on MVCC design that writes data to versioned parquet/base files and log files that contain changes to the base file. All the files are stored under a partitioning scheme for the table, which closely resembles how Apache Hive tables are laid out on DFS. Please refer [here](https://hudi.apache.org/docs/concepts/) for more details.

### How Hudi handles partition evolution requirements ?

Hudi recommends keeping coarse grained top level partition paths e.g date(ts) and within each such partition do clustering in a flexible way to z-order, sort data based on interested columns. This provides excellent performance by : minimzing the number of files in each partition, while still packing data that will be queried together physically closer (what partitioning aims to achieve).

Let's take an example of a table, where we store log\_events with two fields `ts` (time at which event was produced) and `cust_id` (user for which event was produced) and a common option is to partition by both date(ts) and cust\_id.
Some users may want to start granular with hour(ts) and then later evolve to new partitioning scheme say date(ts). But this means, the number of partitions in the table could be very high - 365 days x 1K customers = at-least 365K potentially small parquet files, that can significantly slow down queries, facing throttling issues on the actual S3/DFS reads.

For the afore mentioned reasons, we don't recommend mixing different partitioning schemes within the same table, since it adds operational complexity, and unpredictable performance.
Old data stays in old partitions and only new data gets into newer evolved partitions. If you want to tidy up the table, one has to rewrite all partition/data anwyay! This is where we suggest start with coarse grained partitions
and lean on clustering techniques to optimize for query performance.

We find that most datasets have at-least one high fidelity field, that can be used as a coarse partition. Clustering strategies in Hudi provide a lot of power - you can alter which partitions to cluster, and which fields to cluster each by etc.
Unlike Hive partitioning, Hudi does not remove the partition field from the data files i.e if you write new partition paths, it does not mean old partitions need to be rewritten.
Partitioning by itself is a relic of the Hive era; Hudi is working on replacing partitioning with database like indexing schemes/functions,
for even more flexibility and get away from Hive-style partition evol route.

## Concepts

### How does Hudi ensure atomicity?

Hudi writers atomically move an inflight write operation to a "completed" state by writing an object/file to the [timeline](https://hudi.apache.org/docs/timeline) folder, identifying the write operation with an instant time that denotes the time the action is deemed to have occurred. This is achieved on the underlying DFS (in the case of S3/Cloud Storage, by an atomic PUT operation) and can be observed by files of the pattern `<instant>.<action>.<state>` in Hudi’s timeline.

### Does Hudi extend the Hive table layout?

Hudi is very different from Hive in important aspects described below. However, based on practical considerations, it chooses to be compatible with Hive table layout by adopting partitioning, schema evolution and being queryable through Hive query engine. Here are the key aspect where Hudi differs:

*   Unlike Hive, Hudi does not remove the partition columns from the data files. Hudi in fact adds record level [meta fields](https://hudi.apache.org/tech-specs#meta-fields) including instant time, primary record key, and partition path to the data to support efficient upserts and [incremental queries/ETL](use_cases#efficient-data-lakes-with-incremental-processing).  Hudi tables can be non-partitioned and the Hudi metadata table adds rich indexes on Hudi tables which are beyond simple Hive extensions.
*   Hive advocates partitioning as the main remedy for most performance-based issues. Features like partition evolution and hidden partitioning are primarily based on this Hive based principle of partitioning and aim to tackle the metadata problem partially.  Whereas, Hudi biases to coarse-grained partitioning and emphasizes [clustering](https://hudi.apache.org/docs/clustering) for more fine-grained partitioning. Further, users can strategize and evolve the clustering asynchronously which “actually” help users experiencing performance issues with too granular partitions.
*   Hudi considers partition evolution as an anti-pattern and avoids such schemes due to the inconsistent performance of queries that goes to depend on which part of the table is being queried. Hudi’s design favors consistent performance and is aware of the need to redesign to partitioning/tables to achieve the same.

### What concurrency control approaches does Hudi adopt?

Hudi provides snapshot isolation between all three types of processes - writers, readers, and table services, meaning they all operate on a consistent snapshot of the table. Hudi provides optimistic concurrency control (OCC) between writers, while providing lock-free, non-blocking MVCC-based concurrency control between writers and table-services and between different table services. Widely accepted database literature like “[Architecture of a database system, pg 81](https://dsf.berkeley.edu/papers/fntdb07-architecture.pdf)” clearly lays out 2Phase Locking, OCC and MVCC as the different concurrency control approaches. Purely OCC-based approaches assume conflicts rarely occur and suffer from significant retries and penalties for any continuous/incremental workloads which are normal for modern lake based workloads. Hudi has been cognizant about this, and has a less enthusiastic view on [OCC](https://hudi.apache.org/blog/2021/12/16/lakehouse-concurrency-control-are-we-too-optimistic/), built out things like MVCC-based non-blocking async compaction (the commit time decision significantly aids this), that can have writers working non-stop with table services like compactions running in the background.

### Hudi’s commits are based on transaction start time instead of completed time. Does this cause data loss or inconsistency in case of incremental and time travel queries?

Let’s take a closer look at the scenario here: two commits C1 and C2 (with C2 starting later than C1) start with a later commit (C2) finishing first leaving the inflight transaction of the earlier commit (C1)
before the completed write of the later transaction (C2) in Hudi’s timeline. This is not an uncommon scenario, especially with various ingestions needs such as backfilling, deleting, bootstrapping, etc
alongside regular writes. When/Whether the first job would commit will depend on factors such as conflicts between concurrent commits, inflight compactions, other actions on the table’s timeline etc.
If the first job fails for some reason, Hudi will abort the earlier commit inflight (c1) and the writer has to retry next time with a new instant time > c2 much similar to other OCC implementations.
Firstly, for snapshot queries the order of commits should not matter at all, since any incomplete writes on the active timeline is ignored by queries and cause no side-effects.

In these scenarios, it might be tempting to think of data inconsistencies/data loss when using Hudi’s incremental queries. However, Hudi takes special handling
(examples [1](https://github.com/apache/hudi/blob/aea5bb6f0ab824247f5e3498762ad94f643a2cb6/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/helpers/IncrSourceHelper.java#L76),
[2](https://github.com/apache/hudi/blame/7a6543958368540d221ddc18e0c12b8d526b6859/hudi-hadoop-mr/src/main/java/org/apache/hudi/hadoop/utils/HoodieInputFormatUtils.java#L173)) in incremental queries to ensure that no data
is served beyond the point there is an inflight instant in its timeline, so no data loss or drop happens. This detection is made possible because Hudi writes first request a transaction on the timeline, before planning/executing
the write, as explained in the [timeline](https://hudi.apache.org/docs/timeline#states) section.

In this case, on seeing C1’s inflight commit (publish to timeline is atomic), C2 data (which is > C1 in the timeline) is not served until C1 inflight transitions to a terminal state such as completed or marked as failed.
This [test](https://github.com/apache/hudi/blob/master/hudi-utilities/src/test/java/org/apache/hudi/utilities/sources/TestHoodieIncrSource.java#L137) demonstrates how Hudi incremental source stops proceeding until C1 completes.
Hudi favors [safety and sacrifices liveness](https://en.wikipedia.org/wiki/Safety_and_liveness_properties), in such a case. For a single writer, the start times of the transactions are the same as the order of completion of transactions, and both incremental and time-travel queries work as expected.
In the case of multi-writer, incremental queries still work as expected but time travel queries don't. Since most time travel queries are on historical snapshots with a stable continuous timeline, this has not been implemented upto Hudi 0.13.
However, a similar approach like above can be easily applied to failing time travel queries as well in this window.

### How does Hudi plan to address the liveness issue above for incremental queries?

Hudi 0.14 improves the liveness aspects by enabling change streams, incremental query and time-travel based on the file/object's timestamp (similar to [Delta Lake](https://docs.delta.io/latest/delta-batch.html#query-an-older-snapshot-of-a-table-time-travel)).

To expand more on the long term approach, Hudi has had a proposal to streamline/improve this experience by adding a transition-time to our timeline, which will remove the [liveness sacrifice](https://en.wikipedia.org/wiki/Safety_and_liveness_properties) and makes it easier to understand.
This has been delayed for a few reasons

- Large hosted query engines and users not upgrading fast enough.
- The issues brought up - \[[1](https://hudi.apache.org/docs/faq#does-hudis-use-of-wall-clock-timestamp-for-instants-pose-any-clock-skew-issues),[2](https://hudi.apache.org/docs/faq#hudis-commits-are-based-on-transaction-start-time-instead-of-completed-time-does-this-cause-data-loss-or-inconsistency-in-case-of-incremental-and-time-travel-queries)\],
  relevant to this are not practically very important to users beyond good pedantic discussions,
- Wanting to do it alongside [non-blocking concurrency control](https://github.com/apache/hudi/pull/7907) in Hudi version 1.x.

It's planned to be addressed in the first 1.x release.

### Does Hudi’s use of wall clock timestamp for instants pose any clock skew issues?

Theoretically speaking, a clock skew between two writers can result in different notions of time, and order the timeline differently. But, the current NTP implementations and regions standardizing on UTC make this very impractical to happen in practice. Even many popular OLTP-based systems such as DynamoDB and Cassandra use timestamps for record level conflict detection, cloud providers/OSS NTP are moving towards atomic/synchronized clocks all the time \[[1](https://aws.amazon.com/about-aws/whats-new/2017/11/introducing-the-amazon-time-sync-service/),[2](https://engineering.fb.com/2020/03/18/production-engineering/ntp-service/)\]. We haven't had these as practical issues raised over the last several years, across several large scale data lakes.

Further - Hudi’s commit time can be a logical time and need not strictly be a timestamp. If there are still uniqueness concerns over clock skew, it is easy for Hudi to further extend the timestamp implementation with salts or employ [TrueTime](https://www.cockroachlabs.com/blog/living-without-atomic-clocks/) approaches that have been proven at planet scale. In short, this is not a design issue, but more of a pragmatic implementation choice, that allows us to implement unique features like async compaction in face of updates to the same file group, by scheduling actions on discrete timestamp space.

## Writing Tables

### What are some ways to write a Hudi table?

Typically, you obtain a set of partial updates/inserts from your source and issue [write operations](https://hudi.apache.org/docs/write_operations/) against a Hudi table. If you ingesting data from any of the standard sources like Kafka, or tailing DFS, the [Hudi Streamer](https://hudi.apache.org/docs/0.14.0/hoodie_streaming_ingestion#hudi-streamer) tool is invaluable and provides an easy, self-managed solution to getting data written into Hudi. You can also write your own code to capture data from a custom source using the Spark datasource API and use a [Hudi datasource](https://hudi.apache.org/docs/writing_data/#spark-datasource-writer) to write into Hudi.

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

GDPR has made deletes a must-have tool in everyone's data management toolbox. Hudi supports both soft and hard deletes. For details on how to actually perform them, see [here](https://hudi.apache.org/docs/writing_data/#deletes).

### Should I need to worry about deleting all copies of the records in case of duplicates?

No. Hudi removes all the copies of a record key when deletes are issued. Here is the long form explanation - Sometimes accidental user errors can lead to duplicates introduced into a Hudi table by either [concurrent inserts](https://hudi.apache.org/docs/faq#can-concurrent-inserts-cause-duplicates) or by [not deduping the input records](https://hudi.apache.org/docs/faq#can-single-writer-inserts-have-duplicates) for an insert operation. However, using the right index (e.g., in the default [Simple Index](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/simple/HoodieSimpleIndex.java#L116) and [Bloom Index](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/bloom/HoodieBloomIndex.java#L309)), any subsequent updates and deletes are applied to all copies of the same primary key. This is because the indexing phase identifies records of a primary key in all locations.  So deletes in Hudi remove all copies of the same primary key, i.e., duplicates, and comply with GDPR or CCPA requirements.  Here are two examples [1](https://gist.github.com/yihua/6eb11ce3f888a71935dbf21c77199a48), [2](https://gist.github.com/yihua/e3afe0f34400e60f81f6da925560118e) demonstrating that duplicates are properly deleted from a Hudi table. Hudi is adding [auto key generation](https://github.com/apache/hudi/pull/8107), which will remove the burden of key generation from the user for insert workloads.

### How does Hudi handle duplicate record keys in an input?

When issuing an `upsert` operation on a table and the batch of records provided contains multiple entries for a given key, then all of them are reduced into a single final value by repeatedly calling payload class's [preCombine()](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordPayload.java#L40) method . By default, we pick the record with the greatest value (determined by calling .compareTo()) giving latest-write-wins style semantics. [This FAQ entry](https://hudi.apache.org/docs/faq#can-i-implement-my-own-logic-for-how-input-records-are-merged-with-record-on-storage) shows the interface for HoodieRecordPayload if you are interested.

For an insert or bulk\_insert operation, no such pre-combining is performed. Thus, if your input contains duplicates, the table would also contain duplicates. If you don't want duplicate records either issue an **upsert** or consider specifying option to de-duplicate input in either datasource using [`hoodie.datasource.write.insert.drop.duplicates`](https://hudi.apache.org/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) & [`hoodie.combine.before.insert`](https://hudi.apache.org/docs/configurations/#hoodiecombinebeforeinsert) or in deltastreamer using [`--filter-dupes`](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/hudi-utilities/src/main/java/org/apache/hudi/utilities/deltastreamer/HoodieDeltaStreamer.java#L229).

### How can I pass hudi configurations to my spark writer job?

Hudi configuration options covering the datasource and low level Hudi write client (which both deltastreamer & datasource internally call) are [here](https://hudi.apache.org/docs/configurations/). Invoking _\--help_ on any tool such as DeltaStreamer would print all the usage options. A lot of the options that control upsert, file sizing behavior are defined at the write client level and below is how we pass them to different options available for writing data.

*   For Spark DataSource, you can use the "options" API of DataFrameWriter to pass in these configs.

```scala
inputDF.write().format("org.apache.hudi")
  .options(clientOpts) // any of the Hudi client opts can be passed in as well
  .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
  ...
```

*   When using `HoodieWriteClient` directly, you can simply construct HoodieWriteConfig object with the configs in the link you mentioned.
*   When using HoodieDeltaStreamer tool to ingest, you can set the configs in properties file and pass the file as the cmdline argument "_\--props_"

### How to create Hive style partition folder structure?

By default Hudi creates the partition folders with just the partition values, but if would like to create partition folders similar to the way Hive will generate the structure, with paths that contain key value pairs, like country=us/… or datestr=2021-04-20. This is Hive style (or format) partitioning. The paths include both the names of the partition keys and the values that each path represents.

To enable hive style partitioning, you need to add this hoodie config when you write your data:

```plain
hoodie.datasource.write.hive_style_partitioning: true
```

### Can I register my Hudi table with Apache Hive metastore?

Yes. This can be performed either via the standalone [Hive Sync tool](https://hudi.apache.org/docs/syncing_metastore#hive-sync-tool) or using options in [Hudi Streamer](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/docker/demo/sparksql-incremental.commands#L50) tool or [datasource](https://hudi.apache.org/docs/configurations#hoodiedatasourcehive_syncenable).

### What's Hudi's schema evolution story?

Hudi uses Avro as the internal canonical representation for records, primarily due to its nice [schema compatibility & evolution](https://docs.confluent.io/platform/current/schema-registry/avro.html) properties. This is a key aspect of having reliability in your ingestion or ETL pipelines. As long as the schema passed to Hudi (either explicitly in Hudi Streamer schema provider configs or implicitly by Spark Datasource's Dataset schemas) is backwards compatible (e.g no field deletes, only appending new fields to schema), Hudi will seamlessly handle read/write of old and new data and also keep the Hive schema up-to date.

Starting 0.11.0, Spark SQL DDL support (experimental) was added for Spark 3.1.x and Spark 3.2.1 via ALTER TABLE syntax. Please refer to the [schema evolution guide](https://hudi.apache.org/docs/schema_evolution) for more details on  Schema-on-read for Spark..

### What performance/ingest latency can I expect for Hudi writing?

The speed at which you can write into Hudi depends on the [write operation](https://hudi.apache.org/docs/write_operations) and some trade-offs you make along the way like file sizing. Just like how databases incur overhead over direct/raw file I/O on disks, Hudi operations may have overhead from supporting database like features compared to reading/writing raw DFS files. That said, Hudi implements advanced techniques from database literature to keep these minimal. User is encouraged to have this perspective when trying to reason about Hudi performance. As the saying goes : there is no free lunch (not yet atleast)

| Storage Type | Type of workload | Performance | Tips |
| ---| ---| ---| --- |
| copy on write | bulk\_insert | Should match vanilla spark writing + an additional sort to properly size files | properly size [bulk insert parallelism](https://hudi.apache.org/docs/configurations#hoodiebulkinsertshuffleparallelism) to get right number of files. use insert if you want this auto tuned . Configure [hoodie.bulkinsert.sort.mode](https://hudi.apache.org/docs/configurations#hoodiebulkinsertsortmode) for better file sizes at the cost of memory. The default value NONE offers the fastest performance and matches `spark.write.parquet()` in terms of number of files, overheads. |
| copy on write | insert | Similar to bulk insert, except the file sizes are auto tuned requiring input to be cached into memory and custom partitioned. | Performance would be bound by how parallel you can write the ingested data. Tune [this limit](https://hudi.apache.org/docs/configurations#hoodieinsertshuffleparallelism) up, if you see that writes are happening from only a few executors. |
| copy on write | upsert/ de-duplicate & insert | Both of these would involve index lookup. Compared to naively using Spark (or similar framework)'s JOIN to identify the affected records, Hudi indexing is often 7-10x faster as long as you have ordered keys (discussed below) or \<50% updates. Compared to naively overwriting entire partitions, Hudi write can be several magnitudes faster depending on how many files in a given partition is actually updated. For e.g, if a partition has 1000 files out of which only 100 is dirtied every ingestion run, then Hudi would only read/merge a total of 100 files and thus 10x faster than naively rewriting entire partition. | Ultimately performance would be bound by how quickly we can read and write a parquet file and that depends on the size of the parquet file, configured [here](https://hudi.apache.org/docs/configurations#hoodieparquetmaxfilesize). Also be sure to properly tune your [bloom filters](https://hudi.apache.org/docs/configurations#INDEX). [HUDI-56](https://issues.apache.org/jira/browse/HUDI-56) will auto-tune this. |
| merge on read | bulk insert | Currently new data only goes to parquet files and thus performance here should be similar to copy\_on\_write bulk insert. This has the nice side-effect of getting data into parquet directly for query performance. [HUDI-86](https://issues.apache.org/jira/browse/HUDI-86) will add support for logging inserts directly and this up drastically. |  |
| merge on read | insert | Similar to above |  |
| merge on read | upsert/ de-duplicate & insert | Indexing performance would remain the same as copy-on-write, while ingest latency for updates (costliest I/O operation in copy\_on\_write) are sent to log files and thus with asynchronous compaction provides very very good ingest performance with low write amplification. |  |

Like with many typical system that manage time-series data, Hudi performs much better if your keys have a timestamp prefix or monotonically increasing/decreasing. You can almost always achieve this. Even if you have UUID keys, you can follow tricks like [this](https://www.percona.com/blog/2014/12/19/store-uuid-optimized-way/) to get keys that are ordered. See also [Tuning Guide](https://hudi.apache.org/docs/tuning-guide) for more tips on JVM and other configurations.

### What performance can I expect for Hudi reading/queries?

*   For ReadOptimized views, you can expect the same best in-class columnar query performance as a standard parquet table in Hive/Spark/Presto
*   For incremental views, you can expect speed up relative to how much data usually changes in a given time window and how much time your entire scan takes. For e.g, if only 100 files changed in the last hour in a partition of 1000 files, then you can expect a speed of 10x using incremental pull in Hudi compared to full scanning the partition to find out new data.
*   For real time views, you can expect performance similar to the same avro backed table in Hive/Spark/Presto

### How do I to avoid creating tons of small files?

A key design decision in Hudi was to avoid creating small files and always write properly sized files.

There are 2 ways to avoid creating tons of small files in Hudi and both of them have different trade-offs:

a) **Auto Size small files during ingestion**: This solution trades ingest/writing time to keep queries always efficient. Common approaches to writing very small files and then later stitching them together only solve for system scalability issues posed by small files and also let queries slow down by exposing small files to them anyway.

Hudi has the ability to maintain a configured target file size, when performing **upsert/insert** operations. (Note: **bulk\_insert** operation does not provide this functionality and is designed as a simpler replacement for normal `spark.write.parquet` )

For **copy-on-write**, this is as simple as configuring the [maximum size for a base/parquet file](https://hudi.apache.org/docs/configurations#hoodieparquetmaxfilesize) and the [soft limit](https://hudi.apache.org/docs/configurations#hoodieparquetsmallfilelimit) below which a file should be considered a small file. For the initial bootstrap to Hudi table, tuning record size estimate is also important to ensure sufficient records are bin-packed in a parquet file. For subsequent writes, Hudi automatically uses average record size based on previous commit. Hudi will try to add enough records to a small file at write time to get it to the configured maximum limit. For e.g , with `hoodie.parquet.max.file.size=100MB` and hoodie.parquet.small.file.limit=120MB, Hudi will pick all files < 100MB and try to get them upto 120MB.

For **merge-on-read**, there are few more configs to set. MergeOnRead works differently for different INDEX choices.

*   Indexes with **canIndexLogFiles = true** : Inserts of new data go directly to log files. In this case, you can configure the [maximum log size](https://hudi.apache.org/docs/configurations#hoodielogfilemaxsize) and a [factor](https://hudi.apache.org/docs/configurations#hoodielogfiletoparquetcompressionratio) that denotes reduction in size when data moves from avro to parquet files.
*   Indexes with **canIndexLogFiles = false** : Inserts of new data go only to parquet files. In this case, the same configurations as above for the COPY\_ON\_WRITE case applies.

NOTE : In either case, small files will be auto sized only if there is no PENDING compaction or associated log file for that particular file slice. For example, for case 1: If you had a log file and a compaction C1 was scheduled to convert that log file to parquet, no more inserts can go into that log file. For case 2: If you had a parquet file and an update ended up creating an associated delta log file, no more inserts can go into that parquet file. Only after the compaction has been performed and there are NO log files associated with the base parquet file, can new inserts be sent to auto size that parquet file.

b) [**Clustering**](https://hudi.apache.org/blog/2021/01/27/hudi-clustering-intro) : This is a feature in Hudi to group small files into larger ones either synchronously or asynchronously. Since first solution of auto-sizing small files has a tradeoff on ingestion speed (since the small files are sized during ingestion), if your use-case is very sensitive to ingestion latency where you don't want to compromise on ingestion speed which may end up creating a lot of small files, clustering comes to the rescue. Clustering can be scheduled through the ingestion job and an asynchronus job can stitch small files together in the background to generate larger files. NOTE that during this, ingestion can continue to run concurrently.

_Please note that Hudi always creates immutable files on disk. To be able to do auto-sizing or clustering, Hudi will always create a newer version of the smaller file, resulting in 2 versions of the same file. The cleaner service will later kick in and delte the older version small file and keep the latest one._

### How do I use DeltaStreamer or Spark DataSource API to write to a Non-partitioned Hudi table ?

Hudi supports writing to non-partitioned tables. For writing to a non-partitioned Hudi table and performing hive table syncing, you need to set the below configurations in the properties passed:

```plain
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator
hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.NonPartitionedExtractor
```

### How can I reduce table versions created by Hudi in AWS Glue Data Catalog/ metastore?

With each commit, Hudi creates a new table version in the metastore. This can be reduced by setting the option

[hoodie.datasource.meta\_sync.condition.sync](https://hudi.apache.org/docs/configurations#hoodiedatasourcemeta_syncconditionsync) to true.

This will ensure that hive sync is triggered on schema or partitions changes.

### If there are failed writes in my timeline, do I see duplicates?

No, Hudi does not expose uncommitted files/blocks to the readers. Further, Hudi strives to automatically manage the table for the user, by actively cleaning up files created from failed/aborted writes. See [marker mechanism](https://hudi.apache.org/blog/2021/08/18/improving-marker-mechanism/).

### How are conflicts detected in Hudi between multiple writers?

Hudi employs [optimistic concurrency control](https://hudi.apache.org/docs/concurrency_control#supported-concurrency-controls) between writers, while implementing MVCC based concurrency control between writers and the table services. Concurrent writers to the same table need to be configured with the same lock provider configuration, to safely perform writes. By default (implemented in “[SimpleConcurrentFileWritesConflictResolutionStrategy](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/SimpleConcurrentFileWritesConflictResolutionStrategy.java)”), Hudi allows multiple writers to concurrently write data and commit to the timeline if there is no conflicting writes to the same underlying file group IDs. This is achieved by holding a lock, checking for changes that modified the same file IDs. Hudi then supports a pluggable interface “[ConflictResolutionStrategy](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/ConflictResolutionStrategy.java)” that determines how conflicts are handled. By default, the later conflicting write is aborted. Hudi also support eager conflict detection to help speed up conflict detection and release cluster resources back early to reduce costs.

### Can single-writer inserts have duplicates?

By default, Hudi turns off key based de-duplication for INSERT/BULK\_INSERT operations and thus the table could contain duplicates. If users believe, they have duplicates in inserts, they can either issue UPSERT or consider specifying the option to de-duplicate input in either datasource using [`hoodie.datasource.write.insert.drop.duplicates`](https://hudi.apache.org/docs/configurations#hoodiedatasourcewriteinsertdropduplicates) & [`hoodie.combine.before.insert`](https://hudi.apache.org/docs/configurations/#hoodiecombinebeforeinsert) or in deltastreamer using [`--filter-dupes`](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/hudi-utilities/src/main/java/org/apache/hudi/utilities/deltastreamer/HoodieDeltaStreamer.java#L229).

### Can concurrent inserts cause duplicates?

Yes. As mentioned before, the default conflict detection strategy only check for conflicting updates to the same file group IDs. In the case of concurrent inserts, inserted records end up creating new file groups and thus can go undetected. Most common workload patterns use multi-writer capability in the case of running ingestion of new data and concurrently backfilling/deleting older data, with NO overlap in the primary keys of the records. However, this can be implemented (or better yet contributed) by a new “[ConflictResolutionStrategy](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/ConflictResolutionStrategy.java)”, that reads out keys of new conflicting operations, to check the uncommitted data against other concurrent writes and then decide whether or not to commit/abort. This is rather a fine tradeoff between saving the additional cost of reading keys on most common workloads. Historically, users have preferred to take this into their control to save costs e.g we turned off de-duplication for inserts due to the same feedback. Hudi supports a pre-commit validator mechanism already where such tests can be authored as well.

## Querying Tables

### Does deleted records appear in Hudi's incremental query results?

Soft Deletes (unlike hard deletes) do appear in the incremental pull query results. So, if you need a mechanism to propagate deletes to downstream tables, you can use Soft deletes.

### How do I pass hudi configurations to my beeline Hive queries?

If Hudi's input format is not picked the returned results may be incorrect. To ensure correct inputformat is picked, please use `org.apache.hadoop.hive.ql.io.HiveInputFormat` or `org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat` for `hive.input.format` config. This can be set like shown below:

```plain
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat
```

or

```plain
set hive.input.format=org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat
```

### Does Hudi guarantee consistent reads? How to think about read optimized queries?

Hudi does offer consistent reads. To read the latest snapshot of a MOR table, a user should use snapshot query. The [read-optimized queries](https://hudi.apache.org/docs/table_types#query-types) (targeted for the MOR table ONLY) are an add on benefit to provides users with a practical tradeoff of decoupling writer performance vs query performance, leveraging the fact that most queries query say the most recent data in the table.

Hudi’s read-optimized query is targeted for the MOR table only, with guidance around how compaction should be run to achieve predictable results. In the MOR table, the compaction, which runs every few commits (or “deltacommit” to be exact for the MOR table) by default, merges the base (parquet) file and corresponding change log files to a new base file within each file group, so that the snapshot query serving the latest data immediately after compaction reads the base files only.  Similarly, the read-optimized query always reads the base files only as of the latest compaction commit, usually a few commits before the latest commit, which is still a valid table state.

Users must use snapshot queries to read the latest snapshot of a MOR table.  Popular engines including Spark, Presto, and Hive already support snapshot queries on MOR table and the snapshot query support in Trino is in progress (the [PR](https://github.com/trinodb/trino/pull/14786) is under review).  Note that the read-optimized query does not apply to the COW table.

## Table Services

### What does the Hudi cleaner do?

The Hudi cleaner process often runs right after a commit and deltacommit and goes about deleting old files that are no longer needed. If you are using the incremental pull feature, then ensure you configure the cleaner to [retain sufficient amount of last commits](https://hudi.apache.org/docs/configurations#hoodiecleanercommitsretained) to rewind. Another consideration is to provide sufficient time for your long running jobs to finish running. Otherwise, the cleaner could delete a file that is being or could be read by the job and will fail the job. Typically, the default configuration of 10 allows for an ingestion running every 30 mins to retain up-to 5 hours worth of data. If you run ingestion more frequently or if you want to give more running time for a query, consider increasing the value for the config : `hoodie.cleaner.commits.retained`

### How do I run compaction for a MOR table?

Simplest way to run compaction on MOR table is to run the [compaction inline](https://hudi.apache.org/docs/configurations#hoodiecompactinline), at the cost of spending more time ingesting; This could be particularly useful, in common cases where you have small amount of late arriving data trickling into older partitions. In such a scenario, you may want to just aggressively compact the last N partitions while waiting for enough logs to accumulate for older partitions. The net effect is that you have converted most of the recent data, that is more likely to be queried to optimized columnar format.

That said, for obvious reasons of not blocking ingesting for compaction, you may want to run it asynchronously as well. This can be done either via a separate [compaction job](https://github.com/apache/hudi/blob/master/hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieCompactor.java) that is scheduled by your workflow scheduler/notebook independently. If you are using Hudi Streamer, then you can run in [continuous mode](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/hudi-utilities/src/main/java/org/apache/hudi/utilities/deltastreamer/HoodieDeltaStreamer.java#L241) where the ingestion and compaction are both managed concurrently in a single spark run time.

### What options do I have for asynchronous/offline compactions on MOR table?

There are a couple of options depending on how you write to Hudi. But first let us understand briefly what is involved. There are two parts to compaction

*   Scheduling: In this step, Hudi scans the partitions and selects file slices to be compacted. A compaction plan is finally written to Hudi timeline. Scheduling needs tighter coordination with other writers (regular ingestion is considered one of the writers). If scheduling is done inline with the ingestion job, this coordination is automatically taken care of. Else when scheduling happens asynchronously a lock provider needs to be configured for this coordination among multiple writers.
*   Execution: In this step the compaction plan is read and file slices are compacted. Execution doesnt need the same level of coordination with other writers as Scheduling step and can be decoupled from ingestion job easily.

Depending on how you write to Hudi these are the possible options currently.

*   DeltaStreamer:
  *   In Continuous mode, asynchronous compaction is achieved by default. Here scheduling is done by the ingestion job inline and compaction execution is achieved asynchronously by a separate parallel thread.
  *   In non continuous mode, only inline compaction is possible.
  *   Please note in either mode, by passing --disable-compaction compaction is completely disabled
*   Spark datasource:
  *   Async scheduling and async execution can be achieved by periodically running an offline Hudi Compactor Utility or Hudi CLI. However this needs a lock provider to be configured.
  *   Alternately, from 0.11.0, to avoid dependency on lock providers, scheduling alone can be done inline by regular writer using the config `hoodie.compact.schedule.inline` . And compaction execution can be done offline by periodically triggering the Hudi Compactor Utility or Hudi CLI.
*   Spark structured streaming:
  *   Compactions are scheduled and executed asynchronously inside the streaming job. Async Compactions are enabled by default for structured streaming jobs on Merge-On-Read table.
  *   Please note it is not possible to disable async compaction for MOR table with spark structured streaming.
*   Flink:
  *   Async compaction is enabled by default for Merge-On-Read table.
  *   Offline compaction can be achieved by setting `compaction.async.enabled` to `false` and periodically running [Flink offline Compactor](https://hudi.apache.org/docs/compaction/#flink-offline-compaction). When running the offline compactor, one needs to ensure there are no active writes to the table.
  *   Third option (highly recommended over the second one) is to schedule the compactions from the regular ingestion job and executing the compaction plans from an offline job. To achieve this set `compaction.async.enabled` to `false`, `compaction.schedule.enabled` to `true` and then run the [Flink offline Compactor](https://hudi.apache.org/docs/compaction/#flink-offline-compaction) periodically to execute the plans.

### How to disable all table services in case of multiple writers?

[hoodie.table.services.enabled](https://hudi.apache.org/docs/configurations#hoodietableservicesenabled) is an umbrella config that can be used to turn off all table services at once without having to individually disable them. This is handy in use cases where there are multiple writers doing ingestion. While one of the main pipelines can take care of the table services, other ingestion pipelines can disable them to avoid frequent trigger of cleaning/clustering etc. This does not apply to singe writer scenarios.

### Why does Hudi retain at-least one previous commit even after setting hoodie.cleaner.commits.retained': 1 ?

Hudi runs cleaner to remove old file versions as part of writing data either in inline or in asynchronous mode (0.6.0 onwards). Hudi Cleaner retains at-least one previous commit when cleaning old file versions. This is to prevent the case when concurrently running queries which are reading the latest file versions suddenly see those files getting deleted by cleaner because a new file version got added . In other words, retaining at-least one previous commit is needed for ensuring snapshot isolation for readers.

### Can I get notified when new commits happen in my Hudi table?

Yes. Hudi provides the ability to post a callback notification about a write commit. You can use a http hook or choose to

be notified via a Kafka/pulsar topic or plug in your own implementation to get notified. Please refer [here](https://hudi.apache.org/docs/writing_data/#commit-notifications)

for details

## Storage

### Does Hudi support cloud storage/object stores?

Yes. Generally speaking, Hudi is able to provide its functionality on any Hadoop FileSystem implementation and thus can read and write tables on [Cloud stores](https://hudi.apache.org/docs/cloud) (Amazon S3 or Microsoft Azure or Google Cloud Storage). Over time, Hudi has also incorporated specific design aspects that make building Hudi tables on the cloud easy, such as [consistency checks for s3](https://hudi.apache.org/docs/configurations#hoodieconsistencycheckenabled), Zero moves/renames involved for data files.

### What is the difference between copy-on-write (COW) vs merge-on-read (MOR) table types?

**Copy On Write** - This storage type enables clients to ingest data on columnar file formats, currently parquet. Any new data that is written to the Hudi table using COW storage type, will write new parquet files. Updating an existing set of rows will result in a rewrite of the entire parquet files that collectively contain the affected rows being updated. Hence, all writes to such tables are limited by parquet writing performance, the larger the parquet file, the higher is the time taken to ingest the data.

**Merge On Read** - This storage type enables clients to ingest data quickly onto row based data format such as avro. Any new data that is written to the Hudi table using MOR table type, will write new log/delta files that internally store the data as avro encoded bytes. A compaction process (configured as inline or asynchronous) will convert log file format to columnar file format (parquet). Two different InputFormats expose 2 different views of this data, Read Optimized view exposes columnar parquet reading performance while Realtime View exposes columnar and/or log reading performance respectively. Updating an existing set of rows will result in either a) a companion log/delta file for an existing base parquet file generated from a previous compaction or b) an update written to a log/delta file in case no compaction ever happened for it. Hence, all writes to such tables are limited by avro/log file writing performance, much faster than parquet. Although, there is a higher cost to pay to read log/delta files vs columnar (parquet) files.

More details can be found [here](https://hudi.apache.org/docs/concepts/) and also [Design And Architecture](https://cwiki.apache.org/confluence/display/HUDI/Design+And+Architecture).

### How do I migrate my data to Hudi?

Hudi provides built in support for rewriting your entire table into Hudi one-time using the HDFSParquetImporter tool available from the hudi-cli . You could also do this via a simple read and write of the dataset using the Spark datasource APIs. Once migrated, writes can be performed using normal means discussed [here](https://hudi.apache.org/docs/faq#what-are-some-ways-to-write-a-hudi-table). This topic is discussed in detail [here](https://hudi.apache.org/docs/migration_guide/), including ways to doing partial migrations.

### How to convert an existing COW table to MOR?

All you need to do is to edit the table type property in hoodie.properties(located at hudi_table_path/.hoodie/hoodie.properties).

But manually changing it will result in checksum errors. So, we have to go via hudi-cli.

1. Copy existing hoodie.properties to a new location.
2. Edit table type to MERGE\_ON\_READ
3. launch hudi-cli
  1. connect --path hudi\_table\_path
  2. repair overwrite-hoodie-props --new-props-file new\_hoodie.properties

### How can I find the average record size in a commit?

The `commit showpartitons` command in [HUDI CLI](https://hudi.apache.org/docs/cli) will show both "bytes written" and

"records inserted." Divide the bytes written by records inserted to find the average size. Note that this answer assumes

metadata overhead is negligible. For a small table (such as 5 columns, 100 records) this will not be the case.

### How does the Hudi indexing work & what are its benefits?

The indexing component is a key part of the Hudi writing and it maps a given recordKey to a fileGroup inside Hudi consistently. This enables faster identification of the file groups that are affected/dirtied by a given write operation.

Hudi supports a few options for indexing as below

*   _HoodieBloomIndex_ : Uses a bloom filter and ranges information placed in the footer of parquet/base files (and soon log files as well)
*   _HoodieGlobalBloomIndex_ : The non global indexing only enforces uniqueness of a key inside a single partition i.e the user is expected to know the partition under which a given record key is stored. This helps the indexing scale very well for even [very large datasets](https://www.uber.com/en-IN/blog/uber-big-data-platform/). However, in some cases, it might be necessary instead to do the de-duping/enforce uniqueness across all partitions and the global bloom index does exactly that. If this is used, incoming records are compared to files across the entire table and ensure a recordKey is only present in one partition.
*   _HBaseIndex_ : Apache HBase is a key value store, typically found in close proximity to HDFS. You can also store the index inside HBase, which could be handy if you are already operating HBase.
*   _HoodieSimpleIndex (default)_ : A simple index which reads interested fields (record key and partition path) from base files and joins with incoming records to find the tagged location.
*   _HoodieGlobalSimpleIndex_ : Global version of Simple Index, where in uniqueness is on record key across entire table.
*   _HoodieBucketIndex_ : Each partition has statically defined buckets to which records are tagged with. Since locations are tagged via hashing mechanism, this index lookup will be very efficient.
*   _HoodieSparkConsistentBucketIndex_ : This is also similar to Bucket Index. Only difference is that, data skews can be tackled by dynamically changing the bucket number.

You can implement your own index if you'd like, by subclassing the `HoodieIndex` class and configuring the index class name in configs.

### Can I switch from one index type to another without having to rewrite the entire table?

It should be okay to switch between Bloom index and Simple index as long as they are not global.

Moving from global to non-global and vice versa may not work. Also switching between Hbase (gloabl index) and regular bloom might not work.

### I have an existing dataset and want to evaluate Hudi using portion of that data ?

You can bulk import portion of that data to a new hudi table. For example, if you want to try on a month of data -

```scala
spark.read.parquet("your_data_set/path/to/month")
     .write.format("org.apache.hudi")
     .option("hoodie.datasource.write.operation", "bulk_insert")
     .option("hoodie.datasource.write.storage.type", "storage_type") // COPY_ON_WRITE or MERGE_ON_READ
     .option(RECORDKEY_FIELD_OPT_KEY, "<your key>").
     .option(PARTITIONPATH_FIELD_OPT_KEY, "<your_partition>")
     ...
     .mode(SaveMode.Append)
     .save(basePath);
```

Once you have the initial copy, you can simply run upsert operations on this by selecting some sample of data every round

```scala
spark.read.parquet("your_data_set/path/to/month").limit(n) // Limit n records
     .write.format("org.apache.hudi")
     .option("hoodie.datasource.write.operation", "upsert")
     .option(RECORDKEY_FIELD_OPT_KEY, "<your key>").
     .option(PARTITIONPATH_FIELD_OPT_KEY, "<your_partition>")
     ...
     .mode(SaveMode.Append)
     .save(basePath);
```

For merge on read table, you may want to also try scheduling and running compaction jobs. You can run compaction directly using spark submit on org.apache.hudi.utilities.HoodieCompactor or by using [HUDI CLI](https://hudi.apache.org/docs/cli).

### Why does maintain record level commit metadata? Isn't tracking table version at file level good enough? 

By generating a commit time ahead of time, Hudi is able to stamp each record with effectively a transaction id that it's part of that commit enabling record level change tracking. This means, that even if that file is compacted/clustered ([they mean different things in Hudi](https://hudi.apache.org/docs/clustering#how-is-compaction-different-from-clustering)) many times, in between incremental queries, we are able to [preserve history of the records](https://hudi.apache.org/blog/2023/05/19/hudi-metafields-demystified). Further more, Hudi is able to leverage compaction to amortize the cost of "catching up" for incremental readers by handing latest state of a record after a point in time - which is orders of magnitude efficient than processing each record. Other similar systems lack such decoupling of change streams from physical files the records were part of and core table management services being aware of the history of records. Such similar approaches of record level metadata fields for efficient incremental processing has been also applied in other leading industry [data warehouses](https://twitter.com/apachehudi/status/1676021143697002496?s=20).

### Why partition fields are also stored in parquet files in addition to the partition path ?

Hudi supports customizable partition values which could be a derived value of another field. Also, storing the partition value only as part of the field results in losing type information when queried by various query engines.

### How do I configure Bloom filter (when Bloom/Global\_Bloom index is used)?

Bloom filters are used in bloom indexes to look up the location of record keys in write path. Bloom filters are used only when the index type is chosen as “BLOOM” or “GLOBAL\_BLOOM”. Hudi has few config knobs that users can use to tune their bloom filters.

On a high level, hudi has two types of blooms: Simple and Dynamic.

Simple, as the name suggests, is simple. Size is statically allocated based on few configs.

`hoodie.bloom.index.filter.type`: SIMPLE

`hoodie.index.bloom.num_entries` refers to the total number of entries per bloom filter, which refers to one file slice. Default value is 60000.

`hoodie.index.bloom.fpp` refers to the false positive probability with the bloom filter. Default value: 1\*10^-9.

Size of the bloom filter depends on these two values. This is statically allocated and here is the formula that determines the size of bloom. Until the total number of entries added to the bloom is within the configured `hoodie.index.bloom.num_entries` value, the fpp will be honored. i.e. with default values of 60k and 1\*10^-9, bloom filter serialized size = 430kb. But if more entries are added, then the false positive probability will not be honored. Chances that more false positives could be returned if you add more number of entries than the configured value. So, users are expected to set the right values for both num\_entries and fpp.

Hudi suggests to have roughly 100 to 120 mb sized files for better query performance. So, based on the record size, one could determine how many records could fit into one data file.

Lets say your data file max size is 128Mb and default avg record size is 1024 bytes. Hence, roughly this translates to 130k entries per data file. For this config, you should set num\_entries to ~130k.

Dynamic bloom filter:

`hoodie.bloom.index.filter.type` : DYNAMIC

This is an advanced version of the bloom filter which grows dynamically as the number of entries grows. So, users are expected to set two values wrt num\_entries. `hoodie.index.bloom.num_entries` will determine the starting size of the bloom. `hoodie.bloom.index.filter.dynamic.max.entries` will determine the max size to which the bloom can grow upto. And fpp needs to be set similar to “Simple” bloom filter. Bloom size will be allotted based on the first config `hoodie.index.bloom.num_entries`. Once the number of entries reaches this value, bloom will dynamically grow its size to 2X. This will go on until the size reaches a max of `hoodie.bloom.index.filter.dynamic.max.entries` value. Until the size reaches this max value, fpp will be honored. If the entries added exceeds the max value, then the fpp may not be honored.

### How do I verify datasource schema reconciliation in Hudi?

With Hudi you can reconcile schema, meaning you can apply target table schema on your incoming data, so if there's a missing field in your batch it'll be injected null value. You can enable schema reconciliation using [hoodie.datasource.write.reconcile.schema](https://hudi.apache.org/docs/configurations/#hoodiedatasourcewritereconcileschema) config.

Example how schema reconciliation works with Spark:

```scala
hudi_options = {
    'hoodie.table.name': "test_recon1",
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.table.name': "test_recon1",
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
    "hoodie.datasource.write.hive_style_partitioning":"true",
    "hoodie.datasource.write.reconcile.schema": "true",
    "hoodie.datasource.hive_sync.jdbcurl":"thrift://localhost:9083",
    "hoodie.datasource.hive_sync.database":"hudi",
    "hoodie.datasource.hive_sync.table":"test_recon1",
    "hoodie.datasource.hive_sync.enable":"true",
    "hoodie.datasource.hive_sync.mode": "hms"
}

some_json = '{"uuid":1,"ts":1,"Url":"hudi.apache.com"}'
df = spark.read.json(sc.parallelize([some_json]))

df.write.format("hudi").mode("append").options(**hudi_options).save(base_path)

spark.sql("select * from hudi.test_recon1;").show()

missing_field_json = '{"uuid":2,"ts":1}'
df = spark.read.json(sc.parallelize([missing_field_json]))

df.write.format("hudi").mode("append").options(**hudi_options).save(base_path)

spark.sql("select * from hudi.test_recon1;").show()
```

After first write:

| \_hoodie\_commit\_time | \_hoodie\_commit\_seqno | \_hoodie\_record\_key | \_hoodie\_partition\_path | \_hoodie\_file\_name | Url | ts | uuid |
| ---| ---| ---| ---| ---| ---| ---| --- |
| 20220622204044318 | 20220622204044318... | 1 |  | 890aafc0-d897-44d... | [hudi.apache.com](http://hudi.apache.com) | 1 | 1 |

After the second write:

| \_hoodie\_commit\_time | \_hoodie\_commit\_seqno | \_hoodie\_record\_key | \_hoodie\_partition\_path | \_hoodie\_file\_name | Url | ts | uuid |
| ---| ---| ---| ---| ---| ---| ---| --- |
| 20220622204044318 | 20220622204044318... | 1 |  | 890aafc0-d897-44d... | [hudi.apache.com](http://hudi.apache.com) | 1 | 1 |
| 20220622204208997 | 20220622204208997... | 2 |  | 890aafc0-d897-44d... | null | 1 | 2 |

### Can I change keygenerator for an existing table?

No. There are small set of properties that cannot change once chosen. KeyGenerator is one among them. [Here](https://github.com/apache/hudi/blob/3f37d4fb08169c95930f9cc32389abf4e5cd5551/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieWriterUtils.scala#L128) is a code referecne where we

validate the properties.

### Is Hudi JVM dependent? Does Hudi leverage Java specific serialization?

Hudi was not originally designed as a database layer that would fit under the various big data query engines, that were painfully hard to integrate with (Spark did not have DataSet/DataSource APIs, Trino was still Presto, Presto SPI was still budding, Hive storage handlers were just out). Popular engines including Spark, Flink, Presto, Trino, and Athena do not have issues integrating with Hudi as they are all based on JVM, and access access to Timeline, Metadata table are well-abstracted by Hudi APIs. Even non-jvm engines like Redshift have successfully integrated with Hudi.

Since it was not thought of as a "format", the focus on the APIs for such lower level integrations and documenting the serialized bytes has been historically inadequate. However, with some understanding of the serialization, looking beyond the APIs used and focus on what the serialized bytes are, its possible to integrate Hudi from outside the JVM. For e.g Bloom filters are serialized as hex strings, from byte arrays/primitive types, and should be **readable cross language**. The Hudi Log Format bytes and layout are clearly defined as well, the header/footers are also binary serialized only with primitive types/byte arrays. So with the right endianity information and documentation of these bytes, **cross jvm clients can read this**. The Hudi metadata table uses [HFile format](https://hbase.apache.org/book.html#_hfile_format_2) as the base file format, which while being a well-documented open file format with clear protobuf specifications, does not have native readers. Community has taken efforts towards improving the docs on [tech specs](https://hudi.apache.org/tech-specs). Going forward, Hudi community plans on improving the [table APIs](https://github.com/apache/hudi/pull/7080) to facilitate faster engine integrations, including native language support, as a big part of the [Hudi 1.0](https://github.com/apache/hudi/blob/master/rfc/rfc-69/rfc-69.md) format changes to generalize Hudi more.

**_Note_**: _In a recent release the delete block keys were unintentionally serialized as kryo, and is being fixed in the 0.14 release. Thankfully, since Hudi’s log blocks and format are versioned, when the file slice is compacted things return to normal._

## Integrations

### Does AWS GLUE support Hudi ?

AWS Glue jobs can write, read and update Glue Data Catalog for hudi tables. In order to successfully integrate with Glue Data Catalog, you need to subscribe to one of the AWS provided Glue connectors named "AWS Glue Connector for Apache Hudi". Glue job needs to have "Use Glue data catalog as the Hive metastore" option ticked. Detailed steps with a sample scripts is available on this article provided by AWS - [https://aws.amazon.com/blogs/big-data/writing-to-apache-hudi-tables-using-aws-glue-connector/](https://aws.amazon.com/blogs/big-data/writing-to-apache-hudi-tables-using-aws-glue-connector/).

In case if your using either notebooks or Zeppelin through Glue dev-endpoints, your script might not be able to integrate with Glue DataCatalog when writing to hudi tables.

### How to override Hudi jars in EMR?

If you are looking to override Hudi jars in your EMR clusters one way to achieve this is by providing the Hudi jars through a bootstrap script.

Here are the example steps for overriding Hudi version 0.7.0 in EMR 0.6.2.

**Build Hudi Jars:**

```bash
# Git clone
git clone https://github.com/apache/hudi.git && cd hudi   

# Get version 0.7.0
git checkout --track origin/release-0.7.0

# Build jars with spark 3.0.0 and scala 2.12 (since emr 6.2.0 uses spark 3 which requires scala 2.12):
mvn clean package -DskipTests -Dspark3  -Dscala-2.12 -T 30 
```

**Copy jars to s3:**

These are the jars we are interested in after build completes. Copy them to a temp location first.

```bash
mkdir -p ~/Downloads/hudi-jars
cp packaging/hudi-hadoop-mr-bundle/target/hudi-hadoop-mr-bundle-0.7.0.jar ~/Downloads/hudi-jars/
cp packaging/hudi-hive-sync-bundle/target/hudi-hive-sync-bundle-0.7.0.jar ~/Downloads/hudi-jars/
cp packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.12-0.7.0.jar ~/Downloads/hudi-jars/
cp packaging/hudi-timeline-server-bundle/target/hudi-timeline-server-bundle-0.7.0.jar ~/Downloads/hudi-jars/
cp packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.7.0.jar ~/Downloads/hudi-jars/
```

Upload all jars from ~/Downloads/hudi-jars/ to the s3 location s3://xxx/yyy/hudi-jars

**Include Hudi jars as part of the emr bootstrap script:**

Below script downloads Hudi jars from above s3 location. Use this script as part `bootstrap-actions` when launching the EMR cluster to install the jars in each node.

```bash
#!/bin/bash
sudo mkdir -p /mnt1/hudi-jars

sudo aws s3 cp s3://xxx/yyy/hudi-jars /mnt1/hudi-jars --recursive

# create symlinks
cd /mnt1/hudi-jars
sudo ln -sf hudi-hadoop-mr-bundle-0.7.0.jar hudi-hadoop-mr-bundle.jar
sudo ln -sf hudi-hive-sync-bundle-0.7.0.jar hudi-hive-sync-bundle.jar
sudo ln -sf hudi-spark-bundle_2.12-0.7.0.jar hudi-spark-bundle.jar
sudo ln -sf hudi-timeline-server-bundle-0.7.0.jar hudi-timeline-server-bundle.jar
sudo ln -sf hudi-utilities-bundle_2.12-0.7.0.jar hudi-utilities-bundle.jar
```

**Using the overriden jar in Deltastreamer:**

When invoking DeltaStreamer specify the above jar location as part of spark-submit command.
