---
title: Structure
keywords: big data, stream processing, cloud, hdfs, storage, upserts, change capture
permalink: /docs/structure
summary: "Hudi brings stream processing to big data, providing fresh data while being an order of magnitude efficient over traditional batch processing."
---

Hudi (pronounced “Hoodie”) ingests & manages storage of large analytical datasets over DFS ([HDFS](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) or cloud stores) and provides three logical views for query access.

 * **Read Optimized View** - Provides excellent query performance on pure columnar storage, much like plain [Parquet](https://parquet.apache.org/) tables.
 * **Incremental View** - Provides a change stream out of the dataset to feed downstream jobs/ETLs.
 * **Near-Real time Table** - Provides queries on real-time data, using a combination of columnar & row based storage (e.g Parquet + [Avro](http://avro.apache.org/docs/current/mr.html))

<figure>
    <img class="docimage" src="/assets/images/hudi_intro_1.png" alt="hudi_intro_1.png" />
</figure>

By carefully managing how data is laid out in storage & how it’s exposed to queries, Hudi is able to power a rich data ecosystem where external sources can be ingested in near real-time and made available for interactive SQL Engines like [Presto](https://prestodb.io) & [Spark](https://spark.apache.org/sql/), while at the same time capable of being consumed incrementally from processing/ETL frameworks like [Hive](https://hive.apache.org/) & [Spark](https://spark.apache.org/docs/latest/) to build derived (Hudi) datasets.

Hudi broadly consists of a self contained Spark library to build datasets and integrations with existing query engines for data access. See [quickstart](/docs/quick-start-guide) for a demo.
