---
title: Use Cases
keywords: hudi, data ingestion, etl, real time, use cases
sidebar: mydoc_sidebar
permalink: use_cases.html
toc: false
summary: "Following are some sample use-cases for Hudi, which illustrate the benefits in terms of faster processing & increased efficiency"

---



## Near Real-Time Ingestion

Ingesting data from external sources like (event logs, databases, external sources) into a [Hadoop Data Lake](http://martinfowler.com/bliki/DataLake.html) is a well known problem.
In most (if not all) Hadoop deployments, it is unfortunately solved in a piecemeal fashion, using a medley of ingestion tools,
even though this data is arguably the most valuable for the entire organization.


For RDBMS ingestion, Hudi provides __faster loads via Upserts__, as opposed costly & inefficient bulk loads. For e.g, you can read the MySQL BIN log or [Sqoop Incremental Import](https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide.html#_incremental_imports) and apply them to an
equivalent Hudi table on DFS. This would be much faster/efficient than a [bulk merge job](https://sqoop.apache.org/docs/1.4.0-incubating/SqoopUserGuide.html#id1770457)
or [complicated handcrafted merge workflows](http://hortonworks.com/blog/four-step-strategy-incremental-updates-hive/)


For NoSQL datastores like [Cassandra](http://cassandra.apache.org/) / [Voldemort](http://www.project-voldemort.com/voldemort/) / [HBase](https://hbase.apache.org/), even moderately big installations store billions of rows.
It goes without saying that __full bulk loads are simply infeasible__ and more efficient approaches are needed if ingestion is to keep up with the typically high update volumes.


Even for immutable data sources like [Kafka](kafka.apache.org) , Hudi helps __enforces a minimum file size on HDFS__, which improves NameNode health by solving one of the [age old problems in Hadoop land](https://blog.cloudera.com/blog/2009/02/the-small-files-problem/) in a holistic way. This is all the more important for event streams, since typically its higher volume (eg: click streams) and if not managed well, can cause serious damage to your Hadoop cluster.

Across all sources, Hudi adds the much needed ability to atomically publish new data to consumers via notion of commits, shielding them from partial ingestion failures


## Near Real-time Analytics

Typically, real-time [datamarts](https://en.wikipedia.org/wiki/Data_mart) are powered by specialized analytical stores such as [Druid](http://druid.io/) or [Memsql](http://www.memsql.com/) or [even OpenTSDB](http://opentsdb.net/) .
This is absolutely perfect for lower scale ([relative to Hadoop installations like this](https://blog.twitter.com/2015/hadoop-filesystem-at-twitter)) data, that needs sub-second query responses such as system monitoring or interactive real-time analysis.
But, typically these systems end up getting abused for less interactive queries also since data on Hadoop is intolerably stale. This leads to under utilization & wasteful hardware/license costs.


On the other hand, interactive SQL solutions on Hadoop such as Presto & SparkSQL excel in __queries that finish within few seconds__.
By bringing __data freshness to a few minutes__, Hudi can provide a much efficient alternative, as well unlock real-time analytics on __several magnitudes larger datasets__ stored in DFS.
Also, Hudi has no external dependencies (like a dedicated HBase cluster, purely used for real-time analytics) and thus enables faster analytics on much fresher analytics, without increasing the operational overhead.


## Incremental Processing Pipelines

One fundamental ability Hadoop provides is to build a chain of datasets derived from each other via DAGs expressed as workflows.
Workflows often depend on new data being output by multiple upstream workflows and traditionally, availability of new data is indicated by a new DFS Folder/Hive Partition.
Let's take a concrete example to illustrate this. An upstream workflow `U` can create a Hive partition for every hour, with data for that hour (event_time) at the end of each hour (processing_time), providing effective freshness of 1 hour.
Then, a downstream workflow `D`, kicks off immediately after `U` finishes, and does its own processing for the next hour, increasing the effective latency to 2 hours.

The above paradigm simply ignores late arriving data i.e when `processing_time` and `event_time` drift apart.
Unfortunately, in today's post-mobile & pre-IoT world, __late data from intermittently connected mobile devices & sensors are the norm, not an anomaly__.
In such cases, the only remedy to guarantee correctness is to [reprocess the last few hours](https://falcon.apache.org/FalconDocumentation.html#Handling_late_input_data) worth of data,
over and over again each hour, which can significantly hurt the efficiency across the entire ecosystem. For e.g; imagine reprocessing TBs worth of data every hour across hundreds of workflows.


Hudi comes to the rescue again, by providing a way to consume new data (including late data) from an upsteam Hudi dataset `HU` at a record granularity (not folders/partitions),
apply the processing logic, and efficiently update/reconcile late data with a downstream Hudi dataset `HD`. Here, `HU` and `HD` can be continuously scheduled at a much more frequent schedule
like 15 mins, and providing an end-end latency of 30 mins at `HD`.


{% include callout.html content="To achieve this, Hudi has embraced similar concepts from stream processing frameworks like [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html#join-operations) , Pub/Sub systems like [Kafka](http://kafka.apache.org/documentation/#theconsumer)
or database replication technologies like [Oracle XStream](https://docs.oracle.com/cd/E11882_01/server.112/e16545/xstrm_cncpt.htm#XSTRM187).
For the more curious, a more detailed explanation of the benefits of Incremental Processing (compared to Stream Processing & Batch Processing) can be found [here](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop)" type="info" %}


## Data Dispersal From DFS

A popular use-case for Hadoop, is to crunch data and then disperse it back to an online serving store, to be used by an application.
For e.g, a Spark Pipeline can [determine hard braking events on Hadoop](https://eng.uber.com/telematics/) and load them into a serving store like ElasticSearch, to be used by the Uber application to increase safe driving. Typical architectures for this employ a `queue` between Hadoop and serving store, to prevent overwhelming the target serving store.
A popular choice for this queue is Kafka and this model often results in __redundant storage of same data on DFS (for offline analysis on computed results) and Kafka (for dispersal)__

Once again Hudi can efficiently solve this problem, by having the Spark Pipeline upsert output from
each run into a Hudi dataset, which can then be incrementally tailed (just like a Kafka topic) for new data & written into the serving store.
