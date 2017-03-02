---
title: Hoodie Overview
keywords: homepage
tags: [getting_started]
sidebar: mydoc_sidebar
permalink: index.html
summary: "Hoodie lowers data latency across the board, while simultaneously achieving orders of magnitude of efficiency over traditional batch processing."
---




Hoodie manages storage of large analytical datasets on [HDFS](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) and serve them out via two types of tables

 * **Read Optimized Table** - Provides excellent query performance via purely columnar storage (e.g. [Parquet](https://parquet.apache.org/))
 * **Near-Real time Table** - Provides queries on real-time data, using a combination of columnar & row based storage (e.g Parquet + [Avro](http://avro.apache.org/docs/current/mr.html))


{% include image.html file="hoodie_intro_1.png" alt="hoodie_intro_1.png" %}

By carefully managing how data is laid out in storage & how it’s exposed to queries, Hoodie is able to power a rich data ecosystem where external sources can be ingested into Hadoop in near real-time. The ingested data is then available for interactive SQL Engines like [Presto](https://prestodb.io) & [Spark](https://spark.apache.org/sql/), while at the same time capable of being consumed incrementally from processing/ETL frameworks like [Hive](https://hive.apache.org/) & [Spark](https://spark.apache.org/docs/latest/) to build derived (Hoodie) datasets.

Hoodie broadly consists of a self contained Spark library to build datasets and integrations with existing query engines for data access.

{% include callout.html content="Hoodie is a new project. Near Real-Time  Table implementation is currently underway. Get involved [here](https://github.com/uber/hoodie/projects/1)" type="info" %}

