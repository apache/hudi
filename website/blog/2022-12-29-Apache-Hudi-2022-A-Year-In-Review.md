---
title: "Apache Hudi 2022 - A year in Review"
excerpt: "2022 was the best year for Apache Hudi yet! Huge thank you to everyone who contributed!"
author: Sivabalan Narayanan
category: blog
image: /assets/images/blog/Apache-Hudi-2022-Review.png
tags:
- apache hudi
- community
---

<img src="/assets/images/blog/Apache-Hudi-2022-Review.png" alt="drawing" style={{width:'80%', display:'block', marginLeft:'auto', marginRight:'auto'}} />

## Apache Hudi Momentum
As we wrap up 2022 I want to take the opportunity to reflect on and highlight the incredible progress of the Apache Hudi 
project and most importantly, the community. First and foremost, I want to thank all of the contributors who have made 
2022 the best year for the project ever. There were [over 2,200 PRs](https://ossinsight.io/analyze/apache/hudi#pull-requests) 
created (+38% YoY) and over 600+ users engaged on [Github](https://github.com/apache/hudi/). The Apache Hudi community 
[slack channel](https://join.slack.com/t/apache-hudi/shared_invite/zt-2ggm1fub8-_yt4Reu9djwqqVRFC7X49g) has grown to more 
than 2,600 users (+100% YoY growth) averaging nearly 200 messages per month! The most impressive stat is that with this 
volume growth, the median response time to questions is ~3h. [Come join the community](https://join.slack.com/t/apache-hudi/shared_invite/zt-2ggm1fub8-_yt4Reu9djwqqVRFC7X49g) 
where people are sharing and helping each other!

<img src="/assets/images/blog/Apache-Hudi-Pull-Request-History.png" alt="drawing" style={{width:'80%', display:'block', marginLeft:'auto', marginRight:'auto'}} />

## Key Releases in 2022
2022 has been a year jam packed with exciting new features for Apache Hudi across 0.11.0 and 0.12.0 releases. In addition to new features, vendor/ecosystem partnerships and relationships have been strengthened across many in the community. [AWS continues to double down](https://www.onehouse.ai/blog/apache-hudi-native-aws-integrations) on Apache Hudi, upgrading versions in [EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hudi.html), [Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html), [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html), and announcing a new [native connector inside Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-hudi.html). [Presto](https://prestodb.io/docs/current/connector/hudi.html) and [Trino](https://trino.io/docs/current/connector/hudi.html) merged native Hudi connectors for interactive analytics. [DBT](https://hudi.apache.org/blog/2022/07/11/build-open-lakehouse-using-apache-hudi-and-dbt/), [Confluent](https://github.com/apache/hudi/tree/master/hudi-kafka-connect), [Datahub](https://hudi.apache.org/docs/syncing_datahub), and several others have added support for Hudi tables. While Google has supported Hudi for a while in [BigQuery](https://hudi.apache.org/docs/gcp_bigquery/) and [Dataproc](https://cloud.google.com/blog/products/data-analytics/getting-started-with-new-table-formats-on-dataproc), it also announced plans to add Hudi in [BigLake](https://cloud.google.com/blog/products/data-analytics/building-most-open-data-cloud-all-data-all-source-any-platform). The first tutorial for [Hudi on Azure Synapse Analytics](https://www.onehouse.ai/blog/apache-hudi-on-microsoft-azure) was published.

While there are too many features added in 2022 to list them all, take a look at some of the exciting highlights:

- [Multi-Modal Index](https://hudi.apache.org/blog/2022/05/17/Introducing-Multi-Modal-Index-for-the-Lakehouse-in-Apache-Hudi) is a first-of-its-kind high-performance indexing subsystem for the Lakehouse. It improves metadata lookup performance by up to 100x and reduces overall query latency by up to 30x. Two new indices were added to the metadata table - Bloom filter index that enables faster upsert performance and[  column stats index along with Data skipping](https://hudi.apache.org/blog/2022/06/09/Singificant-queries-speedup-from-Hudi-Column-Stats-Index-and-Data-Skipping-features)  helps speed up queries dramatically.
- Hudi added support for [asynchronous indexing](https://hudi.apache.org/releases/release-0.11.0/#async-indexer) to assist building such indices without blocking ingestion so that regular writers don't need to scale up resources for such one off spikes.
- A new type of index called Bucket Index was introduced this year. This could be game changing for deterministic workloads with partitioned datasets. It is very light-weight and allows the distribution of records to buckets using a hash function.
- Filesystem based Lock Provider - This implementation avoids the need of external systems and leverages the abilities of underlying filesystem to support lock provider needed for optimistic concurrency control in case of multiple writers. Please check the [lock configuration](https://hudi.apache.org/docs/configurations#Locks-Configurations) for details.
- Deltastreamer Graceful Completion - Users can now configure a post-write completion strategy with deltastreamer continuous mode for graceful shutdown.
- Schema on read is supported as an experimental feature since 0.11.0, allowing users to leverage Spark SQL DDL  support for [evolving data schema](https://hudi.apache.org/docs/schema_evolution) needs(drop, rename etc).  Added support for a lot of [CALL commands](https://hudi.apache.org/docs/procedures/) to invoke an array of actions on Hudi tables.
- It is now feasible to [encrypt](https://hudi.apache.org/docs/encryption/) your data that you store with Apache Hudi.
- Pulsar Write Commit Callback - On new events to the Hudi table, users can get notified via Pulsar.
- Flink Enhancements: We added metadata table support, async clustering, data skipping, and bucket index for write paths. We also extended flink support to versions 1.13.x, 1.14.x and[  1.15.x](https://hudi.apache.org/releases/release-0.12.0/#bundle-updates).
- Presto Hudi integration: In addition to the hive connector we have had for a long time, we added [native Presto Hudi connector](https://prestodb.io/docs/current/connector/hudi.html). This enables users to get access to advanced features of Hudi faster. Users can now leverage metadata table to reduce file listing cost. We also added support for accessing clustered datasets this year.
- Trino Hudi integration: We also added [native Trino Hudi connector](https://trino.io/docs/current/connector/hudi.html) to assist in querying Hudi tables via Trino Engine. Users can now leverage metadata table to make their queries performant.
- Performance enhancements: Many performance optimizations were landed by the community throughout the year to keep Hudi on par with competition or better. Check out this [TPC-DS benchmark](https://hudi.apache.org/blog/2022/06/29/Apache-Hudi-vs-Delta-Lake-transparent-tpc-ds-lakehouse-performance-benchmarks) comparing Hudi vs Delta Lake.
- [Long Term Support](https://hudi.apache.org/releases/release-0.12.3#long-term-support): We start to maintain 0.12 as the Long Term Support releases for users to migrate to and stay for a longer duration. In lieu of that, we have made 0.12.1  and 0.12.2 releases to assist users with stable release that comes packed with a lot of stability and bug fixes.

## Community Events
Apache Hudi is a global community and thankfully we live in a world today that empowers virtual collaboration and productivity. In addition to connecting virtually this year we have seen the Apache Hudi community gather at many events in person. Re:Invent, Data+AI Summit, Flink Forward, Alluxio Day, Data Council, PrestoCon, Confluent Current, DBT Coalesce, Cinco de Trino, Data Platform Summit, and many more.

<img src="/assets/images/blog/Apache-Hudi-Conferences.png" alt="drawing" style={{width:'80%', display:'block', marginLeft:'auto', marginRight:'auto'}} />

You don’t have to travel far to meet and collaborate with the Hudi community. We hold monthly virtual meetups, weekly office hours, and there are plenty of friendly faces on Hudi Slack who like to talk shop. Join us via Zoom for the next Hudi meetup!

## Community Content
A wide diversity of organizations around the globe use Apache Hudi as the foundation of their production data platforms. Over 800+ organizations have engaged with Hudi (up 60% YoY) Here are a few highlights of content written by the community sharing their experiences, designs, and best practices:

1. [Build your Hudi data lake on AWS](https://aws.amazon.com/blogs/big-data/part-1-build-your-apache-hudi-data-lake-on-aws-using-amazon-emr/) - Suthan Phillips and Dylan Qu from AWS
2. [Soumil Shah Hudi Youtube Playlist](https://www.youtube.com/playlist?list=PLL2hlSFBmWwwbMpcyMjYuRn8cN99gFSY6) - Soumil Shah from JobTarget
3. [SCD-2 with Apache Hudi](https://medium.com/walmartglobaltech/implementation-of-scd-2-slowly-changing-dimension-with-apache-hudi-465e0eb94a5) - Jayasheel Kalgal from Walmart
4. [Hudi vs Delta vs Iceberg comparisons](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison) - Kyle Weller from Onehouse
5. [Serverless, real-time analytics platform](https://aws.amazon.com/blogs/big-data/how-nerdwallet-uses-aws-and-apache-hudi-to-build-a-serverless-real-time-analytics-platform/) - Kevin Chun from NerdWallet
6. [DBT and Hudi to Build Open Lakehouse](https://hudi.apache.org/blog/2022/07/11/build-open-lakehouse-using-apache-hudi-and-dbt/) - Vinoth Govindarajan from Apple
7. [TPC-DS Benchmarks Hudi vs Delta Lake](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-transparent-tpc-ds-lakehouse-performance-benchmarks) - Alexey Kudinkin from Onehouse
8. [Key Learnings Using Hudi building a Lakehouse](https://blogs.halodoc.io/key-learnings-on-using-apache-hudi-in-building-lakehouse-architecture-halodoc/) - Jitendra Shah from Halodoc
9. [Growing your business with modern data capabilities](https://aws.amazon.com/blogs/architecture/insights-for-ctos-part-3-growing-your-business-with-modern-data-capabilities/) - Jonathan Hwang from Zendesk
10. [Low-latency data lake using MSK, Flink, and Hudi](https://aws.amazon.com/blogs/big-data/create-a-low-latency-source-to-data-lake-pipeline-using-amazon-msk-connect-apache-flink-and-apache-hudi/) - Ali Alemi from AWS
11. [Fresher data lakes on AWS S3](https://robinhood.engineering/author-balaji-varadarajan-e3f496815ebf) - Balaji Varadarajan from Robinhood
12. [Experiences with Hudi from Uber meetup](https://www.youtube.com/watch?v=ZamXiT9aqs8) - Sam Guleff from Walmart and Vinay Patil from Disney+ Hotstar

## What to look for in 2023
Thanks to the strength of the community, Apache Hudi has a bright future for 2023. Check out [this recording](https://youtu.be/9LPSdd-AS8E?t=2090) from our Re:Invent meetup where Vinoth Chandar talks about exciting new features to expect in 2023.

0.13.0 will be the next major release, with a package of exciting new features. Here are a few highlights:

- [Record-key-based index](https://cwiki.apache.org/confluence/display/HUDI/RFC-08++Record+level+indexing+mechanisms+for+Hudi+datasets) to speed up the lookup of records for UUID-based updates and deletes, well tested with 10+ TB index data for hundreds of billions of records at Uber;
- [Consistent Hashing Index](https://github.com/apache/hudi/blob/master/rfc/rfc-42/rfc-42.md) with dynamically-sized buckets to achieve fast upsert performance with no data skew among file groups compared to existing [Bucket Index](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+29%3A+Hash+Index);
- [New CDC format](https://github.com/apache/hudi/blob/master/rfc/rfc-51/rfc-51.md) with Debezium-like database change logs to provide before and after image and operation field for streaming changes from Hudi tables, friendly to engines like Flink;
- [New Record Merge API](https://github.com/apache/hudi/blob/master/rfc/rfc-46/rfc-46.md) to support engine-specific record representation for more efficient writes;
- [Early detection of conflicts](https://github.com/apache/hudi/blob/master/rfc/rfc-56/rfc-56.md) among concurrent writers to give back compute resources proactively.

The long-term vision of Apache Hudi is to make streaming data lake the mainstream, achieving sub-minute commit SLAs with stellar query performance and incremental ETLs.  We plan to harden the indexing subsystem with [Table APIs](https://github.com/apache/hudi/pull/7080) for easy integration with query engines and access to Hudi metadata and indexes, [Indexing Functions](https://github.com/apache/hudi/pull/7235) and [a Federated Storage Layer](https://github.com/apache/hudi/blob/master/rfc/rfc-60/rfc-60.md) to eliminate the notion of partitions and reduce I/O, and new [secondary indexes](https://github.com/apache/hudi/pull/5370).  To realize fast queries, we will provide an option of a standalone [MetaServer](https://github.com/apache/hudi/pull/4718) serving Hudi metadata to plan queries in milliseconds and a [Hudi-aware lake cache](https://docs.google.com/presentation/d/1QBgLw11TM2Qf1KUESofGrQDb63EuggNCpPaxc82Kldo/edit#slide=id.gf7e0551254_0_5) that speeds up the read performance of MOR tables along with fast writes for updates.  Incremental and streaming SQL will be enhanced in Spark and Flink.  For Hudi on Flink, we plan to make the multi-modal indexing production-ready, bring read and write compatibility between Flink and Spark engines, and harden the streaming capabilities, including CDC, streaming ETL semantics, pre-aggregation models and materialized views.

Check out [Hudi Roadmap](https://hudi.apache.org/roadmap) for more to come in 2023!

If you haven't tried Apache Hudi yet, 2023 is your year! Here are a few useful links to help you get started:
 
1. [Apache Hudi Docs](https://hudi.apache.org/docs/overview)
2. [Hudi Slack Channel](https://join.slack.com/t/apache-hudi/shared_invite/zt-2ggm1fub8-_yt4Reu9djwqqVRFC7X49g)
3. [Hudi Weekly Office Hours](https://hudi.apache.org/community/office_hours) and [Monthly Meetup](https://hudi.apache.org/community/syncs#monthly-community-call)
4. [Contributor Guide](https://hudi.apache.org/contribute/how-to-contribute)

If you enjoyed Hudi in 2022 don't forget to give it a little star on [Github](https://github.com/apache/hudi/) ⭐