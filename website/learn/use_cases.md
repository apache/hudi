---
title: "Use Cases"
keywords: [ hudi, data ingestion, etl, real time, use cases]
summary: "Following are some sample use-cases for Hudi, which illustrate the benefits in terms of faster processing & increased efficiency"
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

## Near Real-Time Ingestion

Hudi offers some great benefits across ingestion of all kinds. Hudi helps __enforces a minimum file size on DFS__. This helps
solve the ["small files problem"](https://blog.cloudera.com/blog/2009/02/the-small-files-problem/) for HDFS and Cloud Stores alike,
significantly improving query performance. Hudi adds the much needed ability to atomically commit new data, shielding queries from
ever seeing partial writes and helping ingestion recover gracefully from failures.

Ingesting data from OLTP sources like (event logs, databases, external sources) into a [Data Lake](http://martinfowler.com/bliki/DataLake) is a common problem,
that is unfortunately solved in a piecemeal fashion, using a medley of ingestion tools. This "raw data" layer of the data lake often forms the bedrock on which
more value is created.

For RDBMS ingestion, Hudi provides __faster loads via Upserts__, as opposed costly & inefficient bulk loads. It's very common to use a change capture solution like
[Debezium](http://debezium.io/) or [Kafka Connect](https://docs.confluent.io/platform/current/connect/index) or 
[Sqoop Incremental Import](https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide#_incremental_imports) and apply them to an
equivalent Hudi table on DFS. For NoSQL datastores like [Cassandra](http://cassandra.apache.org/) / [Voldemort](http://www.project-voldemort.com/voldemort/) / [HBase](https://hbase.apache.org/), 
even moderately big installations store billions of rows. It goes without saying that __full bulk loads are simply infeasible__ and more efficient approaches 
are needed if ingestion is to keep up with the typically high update volumes.

Even for immutable data sources like [Kafka](https://kafka.apache.org), there is often a need to de-duplicate the incoming events against what's stored on DFS.
Hudi achieves this by [employing indexes](http://hudi.apache.org/blog/hudi-indexing-mechanisms/) of different kinds, quickly and efficiently.

All of this is seamlessly achieved by the Hudi DeltaStreamer tool, which is maintained in tight integration with rest of the code 
and we are always trying to add more capture sources, to make this easier for the users. The tool also has a continuous mode, where it
can self-manage clustering/compaction asynchronously, without blocking ingestion, significantly improving data freshness.

## Data Deletion

Hudi also offers ability to delete the data stored in the data lake, and more so provides efficient ways of dealing with 
large write amplification, resulting from random deletes based on user_id (or any secondary key), by way of the `Merge On Read` table types.
Hudi's elegant log based concurrency control, ensures that the ingestion/writing can continue happening,as a background compaction job
amortizes the cost of rewriting data/enforcing deletes.

Hudi also unlocks special capabilities like data clustering, which allow users to optimize the data layout for deletions. Specifically,
users can cluster older event log data based on user_id, such that, queries that evaluate candidates for data deletion can do so, while
more recent partitions are optimized for query performance and clustered on say timestamp.

## Unified Storage For Analytics

The world we live in is polarized - even on data analytics storage - into real-time and offline/batch storage. Typically, real-time [datamarts](https://en.wikipedia.org/wiki/Data_mart) 
are powered by specialized analytical stores such as [Druid](http://druid.io/) or [Memsql](http://www.memsql.com/) or [Clickhouse](https://clickhouse.tech/), fed by event buses like
[Kafka](https://kafka.apache.org) or [Pulsar](https://pulsar.apache.org). This model is prohibitively expensive, unless a small fraction of your data lake data 
needs sub-second query responses such as system monitoring or interactive real-time analysis.

The same data gets ingested into data lake storage much later (say every few hours or so) and then runs through batch ETL pipelines, with intolerable data freshness
to do any kind of near-realtime analytics. On the other hand, the data lakes provide access to interactive SQL engines like Presto/SparkSQL, which can horizontally scale 
easily and provide return even more complex queries, within few seconds. 

By bringing streaming primitives to data lake storage, Hudi opens up new possibilities by being able to ingest data within few minutes and also author incremental data
pipelines that are orders of magnitude faster than traditional batch processing. By bringing __data freshness to a few minutes__, Hudi can provide a much efficient alternative, 
for a large class of data applications, compared to real-time datamarts. Also, Hudi has no upfront server infrastructure investments
and thus enables faster analytics on much fresher analytics, without increasing the operational overhead. This external [article](https://www.analyticsinsight.net/can-big-data-solutions-be-affordable/) 
further validates this newer model.

## Incremental Processing Pipelines

Data Lake ETL typically involves building a chain of tables derived from each other via DAGs expressed as workflows. Workflows often depend on new data being output by 
multiple upstream workflows and traditionally, availability of new data is indicated by a new DFS Folder/Hive Partition.
Let's take a concrete example to illustrate this. An upstream workflow `U` can create a Hive partition for every hour, with data for that hour (event_time) at the end of each hour (processing_time), providing effective freshness of 1 hour.
Then, a downstream workflow `D`, kicks off immediately after `U` finishes, and does its own processing for the next hour, increasing the effective latency to 2 hours.

The above paradigm simply ignores late arriving data i.e when `processing_time` and `event_time` drift apart.
Unfortunately, in today's post-mobile & pre-IoT world, __late data from intermittently connected mobile devices & sensors are the norm, not an anomaly__.
In such cases, the only remedy to guarantee correctness is to reprocess the last few hours worth of data, over and over again each hour, 
which can significantly hurt the efficiency across the entire ecosystem. For e.g; imagine reprocessing TBs worth of data every hour across hundreds of workflows.

Hudi comes to the rescue again, by providing a way to consume new data (including late data) from an upstream Hudi table `HU` at a record granularity (not folders/partitions),
apply the processing logic, and efficiently update/reconcile late data with a downstream Hudi table `HD`. Here, `HU` and `HD` can be continuously scheduled at a much more frequent schedule
like 15 mins, and providing an end-end latency of 30 mins at `HD`.

To achieve this, Hudi has embraced similar concepts from stream processing frameworks like [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide#join-operations) , Pub/Sub systems like [Kafka](http://kafka.apache.org/documentation/#theconsumer)
[Flink](https://flink.apache.org) or database replication technologies like [Oracle XStream](https://docs.oracle.com/cd/E11882_01/server.112/e16545/xstrm_cncpt.htm#XSTRM187).
For the more curious, a more detailed explanation of the benefits of Incremental Processing can be found [here](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop)

