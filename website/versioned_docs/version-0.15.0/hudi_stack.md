---
title: Apache Hudi Stack
summary: "Explains about the various layers of software components that make up Hudi"
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 3
last_modified_at:
---

Apache Hudi is a Transactional [Data Lakehouse](https://hudi.apache.org/blog/2024/07/11/what-is-a-data-lakehouse/) Platform built around a database kernel. It brings core warehouse and database functionality directly to a data lake thereby providing a table-level abstraction over open file formats like Apache Parquet/ORC (more recently known as the lakehouse architecture) and enabling transactional capabilities such as updates/deletes. Hudi also incorporates essential table services that are tightly integrated with the database kernel. These services can be executed automatically across both ingested and derived data to manage various aspects such as table bookkeeping, metadata, and storage layout. This integration along with various platform-specific services extends Hudi's role from being just a 'table format' to a comprehensive and robust [data lakehouse](https://hudi.apache.org/blog/2024/07/11/what-is-a-data-lakehouse/) platform.

In this section, we will explore the Hudi stack and deconstruct the layers of software components that constitute Hudi. The features marked with an asterisk (*) represent work in progress, and the dotted boxes indicate planned future work. These components collectively aim to fulfill the [vision](https://github.com/apache/hudi/blob/master/rfc/rfc-69/rfc-69.md) for the project. 

![Hudi Stack](/assets/images/blog/hudistack/hstck_new.png)
<p align = "center">Figure: Apache Hudi Architectural stack</p>

## Lake Storage
The storage layer is where the data files (such as Parquet) are stored. Hudi interacts with the storage layer through the [Hadoop FileSystem API](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html), enabling compatibility with various systems including HDFS for fast appends, and various cloud stores such as Amazon S3, Google Cloud Storage (GCS), and Azure Blob Storage. Additionally, Hudi offers its own storage APIs that can rely on Hadoop-independent file system implementation to simplify the integration of various file systems. Hudi adds a custom wrapper filesystem that lays out the foundation for improved storage optimizations.

## File Formats
![File Format](/assets/images/blog/hudistack/file_format_2.png)
<p align = "center">Figure: File format structure in Hudi</p>

File formats hold the raw data and are physically stored on the lake storage. Hudi operates on logical structures of File Groups and File Slices, which consist of Base File and Log Files. Base Files are compacted and optimized for reads and are augmented with Log Files for efficient append. Future updates aim to integrate diverse formats like unstructured data (e.g., JSON, images), and compatibility with different storage layers in event-streaming, OLAP engines, and warehouses. Hudi's layout scheme encodes all changes to a Log File as a sequence of blocks (data, delete, rollback). By making data available in open file formats (such as Parquet), Hudi enables users to bring any compute engine for specific workloads.

## Transactional Database Layer
The transactional database layer of Hudi comprises the core components that are responsible for the fundamental operations and services that enable Hudi to store, retrieve, and manage data efficiently on [data lakehouse](https://hudi.apache.org/blog/2024/07/11/what-is-a-data-lakehouse/) storages.

### Table Format
![Table Format](/assets/images/blog/hudistack/table_format_1.png)
<p align = "center">Figure: Apache Hudi's Table format</p>

Drawing an analogy to file formats, a table format simply comprises the file layout of the table, the schema, and metadata tracking changes. Hudi organizes files within a table or partition into File Groups. Updates are captured in Log Files tied to these File Groups, ensuring efficient merges. There are three major components related to Hudi’s table format.

- **Timeline** : Hudi's [timeline](timeline), stored in the /.hoodie folder, is a crucial event log recording all table actions in an ordered manner, with events kept for a specified period. Hudi uniquely designs each File Group as a self-contained log, enabling record state reconstruction through delta logs, even after archival of related actions. This approach effectively limits metadata size based on table activity frequency, essential for managing tables with frequent updates. 

- **File Group and File Slice** : Within each partition the data is physically stored as base and Log Files and organized into logical concepts as [File groups](https://hudi.apache.org/tech-specs-1point0/#storage-layout) and File Slices. File groups contain multiple versions of File Slices and are split into multiple File Slices. A File Slice comprises the Base and Log File. Each File Slice within the file-group is uniquely identified by the commit's timestamp that created it.

- **Metadata Table** : Implemented as a merge-on-read table, Hudi's [metadata table](metadata) efficiently handles quick updates with low write amplification. It leverages the HFile format for quick, indexed key lookups, storing vital information like file paths, column statistics, bloom filters, and record indexes. This approach streamlines operations by reducing the necessity for expensive cloud file listings. The metadata table in Hudi acts as an additional [indexing system](metadata#supporting-multi-modal-index-in-hudi) to uplevel the read and write performance.

Hudi’s approach of recording updates into Log Files is more efficient and involves low merge overhead than systems like Hive ACID, where merging all delta records against all Base Files is required. Read more about the various table types in Hudi [here](table_types).

### Indexes
![Indexes](/assets/images/blog/hudistack/index_3.png)
<p align = "center">Figure: Indexes in Hudi</p>

[Indexes](indexing) in Hudi enhance query planning, minimizing I/O, speeding up response times and providing faster writes with low merge costs. Hudi’s [metadata table](metadata/#metadata-table-indices) brings the benefits of indexes generally to both the readers and writers. Compute engines can leverage various indexes in the metadata table, like file listings, column statistics, bloom filters, record-level indexes, and [functional indexes](https://github.com/apache/hudi/blob/master/rfc/rfc-63/rfc-63.md) to quickly generate optimized query plans and improve read performance. In addition to the metadata table indexes, Hudi supports Simple, Bloom, HBase, and Bucket indexes, to efficiently locate File Groups containing specific record keys. Hudi also provides reader indexes such as [functional](https://github.com/apache/hudi/blob/master/rfc/rfc-63/rfc-63.md) and secondary indexes to boost reads. The table partitioning scheme in Hudi is consciously exploited for implementing global and non-global indexing strategies.

### Table Services
![Table Services](/assets/images/blog/hudistack/table_services_2.png)
<p align = "center">Figure: Table services in Hudi</p>

Apache Hudi offers various table services to help keep the table storage layout and metadata management performant. Hudi was designed with built-in table services that enables running them in inline, semi-asynchronous or full-asynchronous modes. Furthermore, Spark and Flink streaming writers can run in continuous mode, and invoke table services asynchronously sharing the underlying executors intelligently with writers. Let’s take a look at these services.

#### Clustering
The [clustering](clustering) service, akin to features in cloud data warehouses, allows users to group frequently queried records using sort keys or merge smaller Base Files into larger ones for optimal file size management. It's fully integrated with other timeline actions like cleaning and compaction, enabling smart optimizations such as avoiding compaction for File Groups undergoing clustering, thereby saving on I/O.

#### Compaction
Hudi's [compaction](compaction) service, featuring strategies like date partitioning and I/O bounding, merges Base Files with delta logs to create updated Base Files. It allows concurrent writes to the same File Froup, enabled by Hudi's file grouping and flexible log merging. This facilitates non-blocking execution of deletes even during concurrent record updates.

#### Cleaning
[Cleaner](http://hudi.apache.org/blog/2021/06/10/employing-right-configurations-for-hudi-cleaner) service works off the timeline incrementally, removing File Slices that are past the configured retention period for incremental queries, while also allowing sufficient time for long running batch jobs (e.g Hive ETLs) to finish running. This allows users to reclaim storage space, thereby saving on costs.

#### Indexing
Hudi's scalable metadata table contains auxiliary data about the table. This subsystem encompasses various indices, including files, column_stats, and bloom_filters, facilitating efficient record location and data skipping. Balancing write throughput with index updates presents a fundamental challenge, as traditional indexing methods, like locking out writes during indexing, are impractical for large tables due to lengthy processing times. Hudi addresses this with its innovative asynchronous [metadata indexing](metadata_indexing), enabling the creation of various indices without impeding writes. This approach not only improves write latency but also minimizes resource waste by reducing contention between writing and indexing activities.

### Concurrency Control
[Concurrency control](concurrency_control) defines how different writers/readers coordinate access to the table. Hudi’s way of publishing monotonically increasing timestamped commits to the timeline lays out the foundation for atomicity guarantees, clearly differentiating between writers (responsible for upserts/deletes), table services (focusing on storage optimization and bookkeeping), and readers (for query execution). Hudi provides snapshot isolation, offering a consistent view of the table across these different operations. It employs lock-free, non-blocking MVCC for concurrency between writers and table-services, as well as between different table services, and optimistic concurrency control (OCC) for multi-writers with early conflict detection. With [Hudi 1.0](https://github.com/apache/hudi/blob/master/rfc/rfc-69/rfc-69.md), non-blocking concurrency control ([NBCC](https://github.com/apache/hudi/blob/master/rfc/rfc-66/rfc-66.md)) is introduced, allowing multiple writers to concurrently operate on the table with non-blocking conflict resolution.

### Lake Cache*
![Lake Cache](/assets/images/blog/hudistack/lake_cache_3.png)
<p align = "center">Figure: Proposed Lake Cache in Hudi</p>

Data lakes today face a tradeoff between fast data writing and optimal query performance. Writing smaller files or logging deltas enhances writing speed, but superior query performance typically requires opening fewer files and pre-materializing merges. Most databases use a buffer pool to reduce storage access costs. Hudi’s design supports creating a multi-tenant caching tier that can store pre-merged File Slices. Hudi’s timeline can then be used to simply communicate caching policies. Traditionally, caching is near query engines or in-memory file systems. Integrating a [caching layer](https://issues.apache.org/jira/browse/HUDI-6489) with Hudi's transactional storage enables shared caching across query engines, supporting updates and deletions, and reducing costs. The goal is to build a buffer pool for lakes, compatible with all major engines, with the contributions from the rest of the community.

### Metaserver*
![Metaserver](/assets/images/blog/hudistack/metaserver_2.png)
<p align = "center">Figure: Proposed Metaserver in Hudi</p>

Storing table metadata on lake storage, while scalable, is less efficient than RPCs to a scalable meta server. Hudi addresses this with its metadata server, called "metaserver," an efficient alternative for managing table metadata for a large number of tables. Currently, the timeline server, embedded in Hudi's writer processes, uses a local rocksDB store and [Javalin](https://javalin.io/) REST API to serve file listings, reducing cloud storage listings. Since version 0.6.0, there's a trend towards standalone timeline servers, aimed at horizontal scaling and improved security. These developments are set to create a more efficient lake [metastore](https://issues.apache.org/jira/browse/HUDI-3345) for future needs.

## Programming APIs

### Writers
Hudi tables can be used as sinks for Spark/Flink pipelines and the Hudi writing path provides several enhanced capabilities over file writing done by vanilla parquet/avro sinks. It categorizes write operations into incremental (`insert`, `upsert`, `delete`) and batch/bulk (`insert_overwrite`, `delete_partition`, `bulk_insert`) with specific functionalities. `upsert` and `delete` efficiently merge records with identical keys and integrate with the file sizing mechanism, while `insert` operations smartly bypass certain steps like pre-combining, maintaining pipeline benefits. Similarly, `bulk_insert` operation offers control over file sizes for data imports. Batch operations integrate MVCC for seamless transitions between incremental and batch processing. Additionally, the write pipeline includes optimizations like handling large merges via rocksDB and concurrent I/O, enhancing write performance.

### Readers
Hudi provides snapshot isolation for writers and readers, enabling consistent table snapshot queries across major query engines (Spark, Hive, Flink, Presto, Trino, Impala) and cloud warehouses. It optimizes query performance by utilizing lightweight processes, especially for base columnar file reads, and integrates engine-specific vectorized readers like in Presto and Trino. This scalable model surpasses the need for separate readers and taps into each engine's unique optimizations, such as Presto and Trino's data/metadata caches. For queries merging Base and Log Files, Hudi employs mechanisms such as spillable maps and lazy reading to boost merge performance. Additionally, Hudi offers a read-optimized query option, trading off data freshness for improved query speed. There are also recently added features such as positional merge, encoding partial Log File to only changed columns and support for Parquet as the Log File format to improve MoR snapshot query performance.

## User Interface

### Platform Services
![Platform Services](/assets/images/blog/hudistack/platform_2.png)
<p align = "center">Figure: Various platform services in Hudi</p>

Platform services offer functionality that is specific to data and workloads, and they sit directly on top of the table services, interfacing with writers and readers. Services, like [Hudi Streamer](hoodie_streaming_ingestion#hudi-streamer), are specialized in handling data and workloads, seamlessly integrating with Kafka streams and various formats to build data lakes. They support functionalities like automatic checkpoint management, integration with major schema registries (including Confluent), and deduplication of data. Hudi Streamer also offers features for backfills, one-off runs, and continuous mode operation with Spark/Flink streaming writers. Additionally, Hudi provides tools for [snapshotting](snapshot_exporter) and incrementally [exporting](snapshot_exporter#examples) Hudi tables, importing new tables, and [post-commit callback](platform_services_post_commit_callback) for analytics or workflow management, enhancing the deployment of production-grade incremental pipelines. Apart from these services, Hudi also provides broad support for different catalogs such as [Hive Metastore](syncing_metastore), [AWS Glue](syncing_aws_glue_data_catalog/), [Google BigQuery](gcp_bigquery), [DataHub](syncing_datahub), etc. that allows syncing of Hudi tables to be queried by interactive engines such as Trino and Presto.

### Query Engines
Apache Hudi is compatible with a wide array of query engines, catering to various analytical needs. For distributed ETL batch processing, Apache Spark is frequently utilized, leveraging its efficient handling of large-scale data. In the realm of streaming use cases, compute engines such as Apache Flink and Apache Spark's Structured Streaming provide robust support when paired with Hudi. Moreover, Hudi supports modern data lake query engines such as Trino and Presto, as well as modern analytical databases such as ClickHouse and StarRocks. This diverse support of compute engines positions Apache Hudi as a flexible and adaptable platform for a broad spectrum of use cases.
