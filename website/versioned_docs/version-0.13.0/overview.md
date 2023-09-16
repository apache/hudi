---
title: "Overview"
keywords: [ hudi, design, table, queries, timeline]
summary: "Here we introduce some basic concepts & give a broad technical overview of Hudi"
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

Welcome to Apache Hudi! This overview will provide a high level summary of what Apache Hudi is and will orient you on 
how to learn more to get started.

## What is Apache Hudi
Apache Hudi (pronounced “hoodie”) is the next generation [streaming data lake platform](/blog/2021/07/21/streaming-data-lake-platform). 
Apache Hudi brings core warehouse and database functionality directly to a data lake. Hudi provides [tables](/docs/next/table_management), 
[transactions](/docs/next/timeline), [efficient upserts/deletes](/docs/next/write_operations), [advanced indexes](/docs/next/indexing), 
[streaming ingestion services](/docs/next/hoodie_streaming_ingestion), data [clustering](/docs/next/clustering)/[compaction](/docs/next/compaction) optimizations, 
and [concurrency](/docs/next/concurrency_control) all while keeping your data in open source file formats.

Not only is Apache Hudi great for streaming workloads, but it also allows you to create efficient incremental batch pipelines. 
Read the docs for more [use case descriptions](/docs/use_cases) and check out [who's using Hudi](/powered-by), to see how some of the 
largest data lakes in the world including [Uber](https://eng.uber.com/uber-big-data-platform/), [Amazon](https://aws.amazon.com/blogs/big-data/how-amazon-transportation-service-enabled-near-real-time-event-analytics-at-petabyte-scale-using-aws-glue-with-apache-hudi/),
[ByteDance](http://hudi.apache.org/blog/2021/09/01/building-eb-level-data-lake-using-hudi-at-bytedance), 
[Robinhood](https://s.apache.org/hudi-robinhood-talk) and more are transforming their production data lakes with Hudi.

Apache Hudi can easily be used on any [cloud storage platform](/docs/cloud). 
Hudi’s advanced performance optimizations, make analytical workloads faster with any of 
the popular query engines including, Apache Spark, Flink, Presto, Trino, Hive, etc.

## Core Concepts to Learn
If you are relatively new to Apache Hudi, it is important to be familiar with a few core concepts:
- [Hudi Timeline](/docs/next/timeline) – How Hudi manages transactions and other table services
- [Hudi File Layout](/docs/next/file_layouts) - How the files are laid out on storage
- [Hudi Table Types](/docs/next/table_types) – `COPY_ON_WRITE` and `MERGE_ON_READ`
- [Hudi Query Types](/docs/next/table_types#query-types) – Snapshot Queries, Incremental Queries, Read-Optimized Queries

See more in the "Concepts" section of the docs.

Take a look at recent [blog posts](/blog) that go in depth on certain topics or use cases.

## Getting Started
Sometimes the fastest way to learn is by doing. Try out these Quick Start resources to get up and running in minutes:
- [Spark Quick Start Guide](/docs/quick-start-guide) – if you primarily use Apache Spark
- [Flink Quick Start Guide](/docs/flink-quick-start-guide) – if you primarily use Apache Flink

If you want to experience Apache Hudi integrated into an end to end demo with Kafka, Spark, Hive, Presto, etc, try out the Docker Demo:
- [Docker Demo](/docs/docker_demo)

## Connect With The Community
Apache Hudi is community focused and community led and welcomes new-comers with open arms. Leverage the following 
resources to learn more, engage, and get help as you get started.

### Join in on discussions
See all the ways to [engage with the community here](/community/get-involved). Two most popular methods include:
- [Hudi Slack Channel](https://join.slack.com/t/apache-hudi/shared_invite/zt-20r833rxh-627NWYDUyR8jRtMa2mZ~gg)
- [Hudi mailing list](mailto:users-subscribe@hudi.apache.org) - (send any msg to subscribe)

### Come to Office Hours for help
Weekly office hours are [posted here](/community/syncs#weekly-office-hours)

### Community Calls
Attend [monthly community calls](/community/syncs#monthly-community-call) to learn best practices and see what others are building.

## Contribute
Apache Hudi welcomes you to join in on the fun and make a lasting impact on the industry as a whole. See our 
[contributor guide](/contribute/how-to-contribute) to learn more, and don’t hesitate to directly reach out to any of the 
current committers to learn more.

Have an idea, an ask, or feedback about a pain-point, but don’t have time to contribute? Join the [Hudi Slack Channel](https://join.slack.com/t/apache-hudi/shared_invite/zt-20r833rxh-627NWYDUyR8jRtMa2mZ~gg)
and share!