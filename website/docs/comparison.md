---
title: "Comparison"
keywords: [ apache, hudi, kafka, kudu, hive, hbase, stream processing]
last_modified_at: 2019-12-30T15:59:57-04:00
---

Apache Hudi fills a big void for processing data on top of DFS, and thus mostly co-exists nicely with these technologies. However,
it would be useful to understand how Hudi fits into the current big data ecosystem, contrasting it with a few related systems
and bring out the different tradeoffs these systems have accepted in their design.

## Kudu

[Apache Kudu](https://kudu.apache.org) is a storage system that has similar goals as Hudi, which is to bring real-time analytics on petabytes of data via first
class support for `upserts`. A key differentiator is that Kudu also attempts to serve as a datastore for OLTP workloads, something that Hudi does not aspire to be.
Consequently, Kudu does not support incremental pulling (as of early 2017), something Hudi does to enable incremental processing use cases.


Kudu diverges from a distributed file system abstraction and HDFS altogether, with its own set of storage servers talking to each  other via RAFT.
Hudi, on the other hand, is designed to work with an underlying Hadoop compatible filesystem (HDFS,S3 or Ceph) and does not have its own fleet of storage servers,
instead relying on Apache Spark to do the heavy-lifting. Thus, Hudi can be scaled easily, just like other Spark jobs, while Kudu would require hardware
& operational support, typical to datastores like HBase or Vertica. We have not at this point, done any head to head benchmarks against Kudu (given RTTable is WIP).
But, if we were to go with results shared by [CERN](https://db-blog.web.cern.ch/blog/zbigniew-baranowski/2017-01-performance-comparison-different-file-formats-and-storage-engines) ,
we expect Hudi to positioned at something that ingests parquet with superior performance.


## Hive Transactions

[Hive Transactions/ACID](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions) is another similar effort, which tries to implement storage like
`merge-on-read`, on top of ORC file format. Understandably, this feature is heavily tied to Hive and other efforts like [LLAP](https://cwiki.apache.org/confluence/display/Hive/LLAP).
Hive transactions does not offer the read-optimized storage option or the incremental pulling, that Hudi does. In terms of implementation choices, Hudi leverages
the full power of a processing framework like Spark, while Hive transactions feature is implemented underneath by Hive tasks/queries kicked off by user or the Hive metastore.
Based on our production experience, embedding Hudi as a library into existing Spark pipelines was much easier and less operationally heavy, compared with the other approach.
Hudi is also designed to work with non-hive engines like PrestoDB/Spark and will incorporate file formats other than parquet over time.

## HBase

Even though [HBase](https://hbase.apache.org) is ultimately a key-value store for OLTP workloads, users often tend to associate HBase with analytics given the proximity to Hadoop.
Given HBase is heavily write-optimized, it supports sub-second upserts out-of-box and Hive-on-HBase lets users query that data. However, in terms of actual performance for analytical workloads,
hybrid columnar storage formats like Parquet/ORC handily beat HBase, since these workloads are predominantly read-heavy. Hudi bridges this gap between faster data and having
analytical storage formats. From an operational perspective, arming users with a library that provides faster data, is more scalable, than managing a big farm of HBase region servers,
just for analytics. Finally, HBase does not support incremental processing primitives like `commit times`, `incremental pull` as first class citizens like Hudi.

## Stream Processing

A popular question, we get is : "How does Hudi relate to stream processing systems?", which we will try to answer here. Simply put, Hudi can integrate with
batch (`copy-on-write table`) and streaming (`merge-on-read table`) jobs of today, to store the computed results in Hadoop. For Spark apps, this can happen via direct
integration of Hudi library with Spark/Spark streaming DAGs. In case of Non-Spark processing systems (eg: Flink, Hive), the processing can be done in the respective systems
and later sent into a Hudi table via a Kafka topic/DFS intermediate file. In more conceptual level, data processing
pipelines just consist of three components : `source`, `processing`, `sink`, with users ultimately running queries against the sink to use the results of the pipeline.
Hudi can act as either a source or sink, that stores data on DFS. Applicability of Hudi to a given stream processing pipeline ultimately boils down to suitability
of PrestoDB/SparkSQL/Hive for your queries.

More advanced use cases revolve around the concepts of [incremental processing](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop), which effectively
uses Hudi even inside the `processing` engine to speed up typical batch pipelines. For e.g: Hudi can be used as a state store inside a processing DAG (similar
to how [rocksDB](https://ci.apache.org/projects/flink/flink-docs-release-1.2/ops/state_backends#the-rocksdbstatebackend) is used by Flink). This is an item on the roadmap
and will eventually happen as a [Beam Runner](https://issues.apache.org/jira/browse/HUDI-60)
