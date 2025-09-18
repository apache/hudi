---
title: "How Freewheel Uses Apache Hudi to Power Its Data Lakehouse"
excerpt: ""
author: Deepak Panda · Based on a session by Bing Jiang
category: blog
image: /assets/images/blog/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse.fig8.jpg
tags:
- Apache Hudi
- Freewheel
- Community
---

:::tip TL;DR

Freewheel migrated from a complex Lambda architecture to a unified data lakehouse using Apache Hudi. They replaced separate, fragmented pipelines with a single real-time data flow, which enabled real-time upserts and incremental reads. This dramatically improved data consistency and freshness. Ad inventory updates improved from once daily to every 5 minutes, boosting forecast accuracy and real-time responsiveness. Ingests and updates audience data at billion-record scale across 63000+ partitions and 620+ TB using Hudi COW tables. As a result, they achieved massive performance gains, including 12 million upserts per second and 89% faster queries, while also cutting costs.

:::

Freewheel, a division of Comcast, provides advanced video ad solutions across TV and digital platforms. As their business scaled, Freewheel faced growing challenges with maintaining consistency, freshness, and operational efficiency in their data systems. To address these challenges, they began transitioning from their legacy Lambda architecture to a modern, Hudi-powered Lakehouse approach.

Their original stack, shown below, used multiple systems like **Presto**, **ClickHouse**, and **Druid** to serve analytical and real-time use cases.


<img src="/assets/images/blog/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse.fig1.png" alt="challenge" width="800" align="middle"/>


However, the architecture had some limitations:

**Data freshness issues:** 
* Presto tables had 3-4 hours delay which is too slow for operational use cases.  
* Only [ClickHouse](https://hudi.apache.org/docs/next/sql_queries/#clickhouse) and Druid offered near real-time access (2-5 mins), but added complexity.

**Complex ingestion:**
* Data came from logs, CDC streams, files, and databases.  
* Each system had its own ingestion pipeline and refresh logic.

**Query performance bottlenecks:**
* With \~15 PB of data and 20M+ queries/day, scaling across three engines was costly and hard to maintain.  
  

## Use Case 1: Lambda Architecture and Its Drawbacks

<img src="/assets/images/blog/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse.fig2.png" alt="challenge" width="800" align="middle"/>

Freewheel initially followed a traditional Lambda architecture, where batch data and real-time data were processed separately. 

This caused the following issues:
* Duplicate pipelines for batch and real-time  
* Inefficient engineering workflows  
* Limitations in scaling ClickHouse for large aggregates

Adopting Apache Hudi brought these benefits:
* Use a single storage layer for both streaming and batch use cases  
* Run real-time pipelines using Spark Structured Streaming  
* Achieve real-time analytics while retaining historical context  
* Reducing engineering overhead and improving scalability

## Use Case 2: Real-Time Inventory Management

<img src="/assets/images/blog/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse.fig3.png" alt="challenge" width="800" align="middle"/>

Challenges with daily ad inventory updates included:

* Ad inventory was updated only once per day, which:  
  * Affected forecasting accuracy  
  * Created delivery-performance mismatches

Hudi brought the following enhancements:

* Updates now happen every 5 minutes  
* Forecasts are accurate and responsive to real-time order changes  
* Improved the freshness of the inventory data

## Use Case 3: Scalable Audience Data Processing

<img src="/assets/images/blog/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse.fig4.png" alt="challenge" width="800" align="middle"/>

Freewheel ingests audience segments into Aerospike for online services.

Challenges in handling real-time audience data:

* Need for analytical insight on top of this real-time data  
* Bulk loads and frequent updates

Here is How Apache Hudi enhanced audience data processing:

* Hudi maintains a snapshot table for all audience data  
* Supports bulk insert, upserts, and change data capture (CDC)  
* The back-end analytics got stronger, while the online systems stayed responsive.  
* The online targeting system became more stable, as heavy analytical workloads were moved off the KV store.

## Hudi Practice \- 1: Billion-Level Updates for Audience Segments Ingestion

### Use Case Overview:

This Hudi implementation showcases how a large-scale platform ingests and updates audience segmentation data at the billion-record scale using Apache Hudi's [Copy-on-Write](https://hudi.apache.org/docs/table_types#copy-on-write-table) (COW) tables. The architecture efficiently handles high-frequency updates across over 63,000 partitions and a 620+ TB table, with performance optimizations at both the data and infrastructure levels.

### Key Architecture and Design Principles:

<img src="/assets/images/blog/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse.fig5.png" alt="challenge" width="600" align="middle"/>

#### Partitioning Strategy

* Audience Segment ID is used as the partition key.   
* One job per partition is executed, allowing parallel processing.  
* Each job runs a Spark batch that upserts data into the lakehouse using Hudi.

#### Modular & Decoupled Architecture

* Trigger Detector  
  * Runs every 10 minutes to check for new audience segment data files.  
* Meta Table  
  * Stores metadata about the files.  
  * Helps separate the detection step from the ingestion step.  
* Scheduler : Reads the metadata table to create execution plans based on:  
  * Size of input data  
  * Available resources in the cluster  
  * Priority of the data  
  * Whether any previous jobs failed  
  * Concurrency limits for writing  
* Data Ingestion Job  
  * Uses Spark to process data.  
  * Final output is written to the Hudi Segment Table in the lakehouse.

### Challenges of Input Data at Scale:

* Total Data Volume: Over 620 TB of input data.  
* Partition Count: 63,000 audience segment partitions.  
* Data Skew: Massive variation in partition sizes, ranging from 1 million to 100 billion records.

### Metrics and Performance Insights:

<img src="/assets/images/blog/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse.fig6.png" alt="challenge" width="600" align="middle"/>

* Data Volume & Records per Commit  
  * Over 434 TiB written across multiple commits.  
  * Total records written exceeded 6 trillion records across the ingestion period.  
* Update Amplification  
  * 10x amplification in write volume from input Parquet files to Hudi COW tables.  
  * This includes metadata updates, file rewrites, [indexing](https://hudi.apache.org/docs/metadata_indexing/), and [deduplication](https://www.onehouse.ai/blog/data-deduplication-strategies-in-an-open-lakehouse-architecture) overhead.  
* Cost Optimization  
  * Unit Cost on AWS: \~$0.10 per 1 million records updated.  
  * Throughput: The pipeline supports up to 12 million upserts/sec

### Performance Tips:

* Handle S3 throttling by increasing partition parallelism by hashing the partition key.  
* Auto-tune cluster resources based on input data volume for optimal performance.  
* Dynamically scale Spark jobs based on partition count and record volume.  
* Deduplicate data  before writing by keeping the latest record using group-by key and timestamp ordering.

## Hudi Practice \- 2: Real-Time Aggregated Ingestion Using Spark Streaming \+ Clustering

### Use Case Overview:

This implementation showcases an efficient pipeline where Spark Streaming ingests aggregated data into a Hudi Lakehouse using the bulk\_insert operation followed by [asynchronous clustering](https://hudi.apache.org/blog/2021/08/23/async-clustering/).

<img src="/assets/images/blog/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse/2025-09-17-How-Freewheel-Uses-Apache-Hudi-to-Power-Its-Data-Lakehouse.fig7.png" alt="challenge" width="800" align="middle"/>

### Data Ingestion Flow:

* Kafka: Raw events are first streamed into Kafka.  
* Spark SQL on Streaming: Consumes Kafka messages, aggregates them in near real-time.  
* bulk\_insert into Hudi Lakehouse: Aggregated data is appended using the efficient [bulk\_insert](https://hudi.apache.org/docs/write_operations/#bulk_insert) operation.  
* Clustering Plan Generation: A clustering plan is dynamically generated.  
* HoodieClusteringJob: A cron job runs every hour to execute [clustering](https://hudi.apache.org/docs/clustering/) and optimize small files.

### Key Features of Streaming into Hudi:

* Massive File Reduction:  
  * Clustering reduced the number of files from 11,863 to just 660, minimizing small file issues and improving metadata performance.  
* Write Throughput Boost:  
  * Write speed increased from 7,000 to 15,000 records/sec, a 114% improvement, thanks to optimized file layout.  
* Query Performance Gains:   
  * Query time dropped from 144s to 15.35s, making Presto queries on Hudi \~89% faster after clustering.

## Conclusion

Freewheel’s journey with Apache Hudi transformed their data architecture by offering unified access, real-time freshness, and scalable operations. They credit Hudi’s community and feature set as key to their success.

\> “We’re lucky to choose Hudi as our Lakehouse. Thanks to the powerful Hudi community\!” – Bing Jiang

This blog is based on Freewheel’s presentation at the Apache Hudi Community Sync. If you are interested in watching the recorded version of the video, you can find it [here](https://www.youtube.com/watch?v=hQNSf82o3Rk).