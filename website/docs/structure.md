---
title: Structure
keywords: [ big data, stream processing, cloud, hdfs, storage, upserts, change capture]
summary: "Hudi brings stream processing to big data, providing fresh data while being an order of magnitude efficient over traditional batch processing."
last_modified_at: 2019-12-30T15:59:57-04:00
---

Hudi (pronounced “Hoodie”) ingests & manages storage of large analytical tables over DFS ([HDFS](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign) or cloud stores) and provides three types of queries.

 * **Read Optimized query** - Provides excellent query performance on pure columnar storage, much like plain [Parquet](https://parquet.apache.org/) tables.
 * **Incremental query** - Provides a change stream out of the dataset to feed downstream jobs/ETLs.
 * **Snapshot query** - Provides queries on real-time data, using a combination of columnar & row based storage (e.g Parquet + [Avro](https://avro.apache.org/docs/++version++/mapreduce-guide/))

<figure>
    <img className="docimage" src={require("/assets/images/hudi_intro_1.png").default} alt="hudi_intro_1.png" />
</figure>

By carefully managing how data is laid out in storage & how it’s exposed to queries, Hudi is able to power a rich data ecosystem where external sources can be ingested in near real-time and made available for interactive SQL Engines like [PrestoDB](https://prestodb.io) & [Spark](https://spark.apache.org/sql/), while at the same time capable of being consumed incrementally from processing/ETL frameworks like [Hive](https://hive.apache.org/) & [Spark](https://spark.apache.org/docs/latest/) to build derived (Hudi) tables.

Hudi broadly consists of a self contained Spark library to build tables and integrations with existing query engines for data access. See [quickstart](../quick-start-guide) for a demo.
