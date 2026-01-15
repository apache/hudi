---
title: Storage FAQ
keywords: [hudi, writing, reading]
---

# Storage FAQ

### Does Hudi support cloud storage/object stores?

Yes. Generally speaking, Hudi is able to provide its functionality on any Hadoop FileSystem implementation and thus can read and write tables on [Cloud stores](/docs/cloud) (Amazon S3 or Microsoft Azure or Google Cloud Storage). Over time, Hudi has also incorporated specific design aspects that make building Hudi tables on the cloud easy, such as [consistency checks for s3](/docs/configurations#hoodieconsistencycheckenabled), Zero moves/renames involved for data files.

### What is the difference between copy-on-write (COW) vs merge-on-read (MOR) table types?

**Copy On Write** - This storage type enables clients to ingest data on columnar file formats, currently parquet. Any new data that is written to the Hudi table using COW storage type, will write new parquet files. Updating an existing set of rows will result in a rewrite of the entire parquet files that collectively contain the affected rows being updated. Hence, all writes to such tables are limited by parquet writing performance, the larger the parquet file, the higher is the time taken to ingest the data.

**Merge On Read** - This storage type enables clients to ingest data quickly onto row based data format such as avro. Any new data that is written to the Hudi table using MOR table type, will write new log/delta files that internally store the data as avro encoded bytes. A compaction process (configured as inline or asynchronous) will convert log file format to columnar file format (parquet). Two different InputFormats expose 2 different views of this data, Read Optimized view exposes columnar parquet reading performance while Realtime View exposes columnar and/or log reading performance respectively. Updating an existing set of rows will result in either a) a companion log/delta file for an existing base parquet file generated from a previous compaction or b) an update written to a log/delta file in case no compaction ever happened for it. Hence, all writes to such tables are limited by avro/log file writing performance, much faster than parquet. Although, there is a higher cost to pay to read log/delta files vs columnar (parquet) files.

More details can be found [here](/docs/concepts/) and also [Design And Architecture](https://cwiki.apache.org/confluence/display/HUDI/Design+And+Architecture).

### How do I migrate my data to Hudi?

Hudi provides built in support for rewriting your entire table into Hudi one-time using the HDFSParquetImporter tool available from the hudi-cli . You could also do this via a simple read and write of the dataset using the Spark datasource APIs. Once migrated, writes can be performed using normal means discussed [here](/faq/writing_tables#what-are-some-ways-to-write-a-hudi-table). This topic is discussed in detail [here](/docs/migration_guide/), including ways to doing partial migrations.

### How to convert an existing COW table to MOR?

All you need to do is to edit the table type property in hoodie.properties(located at hudi_table_path/.hoodie/hoodie.properties).

But manually changing it will result in checksum errors. So, we have to go via hudi-cli.

1. Copy existing hoodie.properties to a new location.
2. Edit table type to MERGE_ON_READ
3. launch hudi-cli
  1. connect --path hudi_table_path
  2. repair overwrite-hoodie-props --new-props-file new_hoodie.properties

### How can I find the average record size in a commit?

The `commit showpartitons` command in [HUDI CLI](/docs/cli) will show both "bytes written" and

"records inserted." Divide the bytes written by records inserted to find the average size. Note that this answer assumes

metadata overhead is negligible. For a small table (such as 5 columns, 100 records) this will not be the case.

### How does the Hudi indexing work & what are its benefits?

The indexing component is a key part of the Hudi writing and it maps a given recordKey to a fileGroup inside Hudi consistently. This enables faster identification of the file groups that are affected/dirtied by a given write operation.

Hudi supports a few options for indexing as below

*   _HoodieBloomIndex_ : Uses a bloom filter and ranges information placed in the footer of parquet/base files (and soon log files as well)
*   _HoodieGlobalBloomIndex_ : The non global indexing only enforces uniqueness of a key inside a single partition i.e the user is expected to know the partition under which a given record key is stored. This helps the indexing scale very well for even [very large datasets](https://eng.uber.com/uber-big-data-platform/). However, in some cases, it might be necessary instead to do the de-duping/enforce uniqueness across all partitions and the global bloom index does exactly that. If this is used, incoming records are compared to files across the entire table and ensure a recordKey is only present in one partition.
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
     .option("hoodie.datasource.write.recordkey.field", "<your key>").
     .option("hoodie.datasource.write.partitionpath.field", "<your_partition>")
     ...
     .mode(SaveMode.Append)
     .save(basePath);
```

Once you have the initial copy, you can simply run upsert operations on this by selecting some sample of data every round

```scala
spark.read.parquet("your_data_set/path/to/month").limit(n) // Limit n records
     .write.format("org.apache.hudi")
     .option("hoodie.datasource.write.operation", "upsert")
     .option("hoodie.datasource.write.recordkey.field", "<your key>").
     .option("hoodie.datasource.write.partitionpath.field", "<your_partition>")
     ...
     .mode(SaveMode.Append)
     .save(basePath);
```

For merge on read table, you may want to also try scheduling and running compaction jobs. You can run compaction directly using spark submit on org.apache.hudi.utilities.HoodieCompactor or by using [HUDI CLI](/docs/cli).

### Why does maintain record level commit metadata? Isn't tracking table version at file level good enough? 

By generating a commit time ahead of time, Hudi is able to stamp each record with effectively a transaction id that it's part of that commit enabling record level change tracking. This means, that even if that file is compacted/clustered ([they mean different things in Hudi](/docs/clustering#how-is-compaction-different-from-clustering)) many times, in between incremental queries, we are able to [preserve history of the records](/blog/2023/05/19/hudi-metafields-demystified). Further more, Hudi is able to leverage compaction to amortize the cost of "catching up" for incremental readers by handing latest state of a record after a point in time - which is orders of magnitude efficient than processing each record. Other similar systems lack such decoupling of change streams from physical files the records were part of and core table management services being aware of the history of records. Such similar approaches of record level metadata fields for efficient incremental processing has been also applied in other leading industry [data warehouses](https://twitter.com/apachehudi/status/1676021143697002496?s=20).

### Why partition fields are also stored in parquet files in addition to the partition path ?

Hudi supports customizable partition values which could be a derived value of another field. Also, storing the partition value only as part of the field results in losing type information when queried by various query engines.

### How do I configure Bloom filter (when Bloom/Global_Bloom index is used)?

Bloom filters are used in bloom indexes to look up the location of record keys in write path. Bloom filters are used only when the index type is chosen as “BLOOM” or “GLOBAL_BLOOM”. Hudi has few config knobs that users can use to tune their bloom filters.

On a high level, hudi has two types of blooms: Simple and Dynamic.

Simple, as the name suggests, is simple. Size is statically allocated based on few configs.

`hoodie.bloom.index.filter.type`: SIMPLE

`hoodie.index.bloom.num_entries` refers to the total number of entries per bloom filter, which refers to one file slice. Default value is 60000.

`hoodie.index.bloom.fpp` refers to the false positive probability with the bloom filter. Default value: 1*10^-9.

Size of the bloom filter depends on these two values. This is statically allocated and here is the formula that determines the size of bloom. Until the total number of entries added to the bloom is within the configured `hoodie.index.bloom.num_entries` value, the fpp will be honored. i.e. with default values of 60k and 1*10^-9, bloom filter serialized size = 430kb. But if more entries are added, then the false positive probability will not be honored. Chances that more false positives could be returned if you add more number of entries than the configured value. So, users are expected to set the right values for both num_entries and fpp.

Hudi suggests to have roughly 100 to 120 mb sized files for better query performance. So, based on the record size, one could determine how many records could fit into one data file.

Lets say your data file max size is 128Mb and default avg record size is 1024 bytes. Hence, roughly this translates to 130k entries per data file. For this config, you should set num_entries to ~130k.

Dynamic bloom filter:

`hoodie.bloom.index.filter.type` : DYNAMIC

This is an advanced version of the bloom filter which grows dynamically as the number of entries grows. So, users are expected to set two values wrt num_entries. `hoodie.index.bloom.num_entries` will determine the starting size of the bloom. `hoodie.bloom.index.filter.dynamic.max.entries` will determine the max size to which the bloom can grow upto. And fpp needs to be set similar to “Simple” bloom filter. Bloom size will be allotted based on the first config `hoodie.index.bloom.num_entries`. Once the number of entries reaches this value, bloom will dynamically grow its size to 2X. This will go on until the size reaches a max of `hoodie.bloom.index.filter.dynamic.max.entries` value. Until the size reaches this max value, fpp will be honored. If the entries added exceeds the max value, then the fpp may not be honored.

### How do I verify datasource schema reconciliation in Hudi?

With Hudi you can reconcile schema, meaning you can apply target table schema on your incoming data, so if there's a missing field in your batch it'll be injected null value. You can enable schema reconciliation using [hoodie.datasource.write.reconcile.schema](/docs/configurations/#hoodiedatasourcewritereconcileschema) config.

Example how schema reconciliation works with Spark:

```scala
hudi_options = {
    'hoodie.table.name': "test_recon1",
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.table.name': "test_recon1",
    'hoodie.table.ordering.fields': 'ts',
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

| _hoodie_commit_time | _hoodie_commit_seqno | _hoodie_record_key | _hoodie_partition_path | _hoodie_file_name | Url | ts | uuid |
| ---| ---| ---| ---| ---| ---| ---| --- |
| 20220622204044318 | 20220622204044318... | 1 |  | 890aafc0-d897-44d... | hudi.apache.org | 1 | 1 |

After the second write:

| _hoodie_commit_time | _hoodie_commit_seqno | _hoodie_record_key | _hoodie_partition_path | _hoodie_file_name | Url | ts | uuid |
| ---| ---| ---| ---| ---| ---| ---| --- |
| 20220622204044318 | 20220622204044318... | 1 |  | 890aafc0-d897-44d... | hudi.apache.org | 1 | 1 |
| 20220622204208997 | 20220622204208997... | 2 |  | 890aafc0-d897-44d... | null | 1 | 2 |

### Can I change keygenerator for an existing table?

No. There are small set of properties that cannot change once chosen. KeyGenerator is one among them. [Here](https://github.com/apache/hudi/blob/3f37d4fb08169c95930f9cc32389abf4e5cd5551/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieWriterUtils.scala#L128) is a code referecne where we

validate the properties.

### Is Hudi JVM dependent? Does Hudi leverage Java specific serialization?

Hudi was not originally designed as a database layer that would fit under the various big data query engines, that were painfully hard to integrate with (Spark did not have DataSet/DataSource APIs, Trino was still Presto, Presto SPI was still budding, Hive storage handlers were just out). Popular engines including Spark, Flink, Presto, Trino, and Athena do not have issues integrating with Hudi as they are all based on JVM, and access access to Timeline, Metadata table are well-abstracted by Hudi APIs. Even non-jvm engines like Redshift have successfully integrated with Hudi.

Since it was not thought of as a "format", the focus on the APIs for such lower level integrations and documenting the serialized bytes has been historically inadequate. However, with some understanding of the serialization, looking beyond the APIs used and focus on what the serialized bytes are, its possible to integrate Hudi from outside the JVM. For e.g Bloom filters are serialized as hex strings, from byte arrays/primitive types, and should be **readable cross language**. The Hudi Log Format bytes and layout are clearly defined as well, the header/footers are also binary serialized only with primitive types/byte arrays. So with the right endianity information and documentation of these bytes, **cross jvm clients can read this**. The Hudi metadata table uses [HFile format](https://hbase.apache.org/book.html#_hfile_format_2) as the base file format, which while being a well-documented open file format with clear protobuf specifications, does not have native readers. Community has taken efforts towards improving the docs on [tech specs](/learn/tech-specs). Going forward, Hudi community plans on improving the [table APIs](https://github.com/apache/hudi/pull/7080) to facilitate faster engine integrations, including native language support, as a big part of the [Hudi 1.0](https://github.com/apache/hudi/blob/master/rfc/rfc-69/rfc-69.md) format changes to generalize Hudi more.

**_Note_**: _In a recent release the delete block keys were unintentionally serialized as kryo, and is being fixed in the 0.14 release. Thankfully, since Hudi’s log blocks and format are versioned, when the file slice is compacted things return to normal._
