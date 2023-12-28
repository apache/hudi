---
title: "Apache Hudi 2023: a year in review"
excerpt: ""
author: Shiyan Xu
category: blog
image: /assets/images/blog/2023-12-28-a-year-in-review-2023/00.cover.png
tags:

- apache hudi
- community

---

<img src="/assets/images/blog/2023-12-28-a-year-in-review-2023/00.cover.png" alt="drawing" style={{width:'80%', display:'block', marginLeft:'auto', marginRight:'auto'}} />

In the warm glow of the holiday season, I am delighted to convey a message of deep appreciation on behalf of the 
Hudi Project Management Committee (PMC) to all the contributors and users whose invaluable efforts have made 2023 
an extraordinary year for Hudi. 

In 2023, the Hudi community has exemplified exceptional engagement and activity, evident in the 
[1832 pull requests created](https://ossinsight.io/analyze/apache/hudi#pull-requests), 
with a significant 1363 of these being merged. We proudly welcomed 2 new PMC members and 3 new Committers. Our 
[Slack platform](https://apache-hudi.slack.com/join/shared_invite/zt-20r833rxh-627NWYDUyR8jRtMa2mZ~gg#/) 
has witnessed a remarkable 44% increase in users, with numbers exceeding 3800. Additionally, 
our presence on social media platforms has been impressive, with our [X (Twitter) account](https://x.com/apachehudi) 
garnering 2274 followers, and our newly established [LinkedIn page](https://www.linkedin.com/company/apache-hudi/) 
rapidly gaining 2245 followers in just three months. Let’s take a moment to reflect on and celebrate the myriad of 
exciting developments and accomplishments that have defined the year 2023 for the Hudi community.

<img src="/assets/images/blog/2023-12-28-a-year-in-review-2023/01.PR_histogram.png" alt="drawing" style={{width:'80%', display:'block', marginLeft:'auto', marginRight:'auto'}} />

## Development Highlights

The year 2023 has been exceptionally productive for Hudi, marked by significant advancements and innovations.
There have been three major releases: [0.13.0](https://hudi.apache.org/releases/release-0.13.0), 
[0.14.0](https://hudi.apache.org/releases/release-0.14.0), and the trailblazing 
[1.0.0-beta](https://hudi.apache.org/releases/release-1.0.0-beta1). These releases have 
collectively reshaped the database-like experience for Hudi lakehouses. Here are some brief summaries highlighting 
key features introduced:

### Indexing has elevated to a whole new level

[Record Level Index](https://hudi.apache.org/releases/release-0.14.0#record-level-index)
is a game-changing feature that boosts write performance for large tables. It achieves this by efficiently 
storing per-record locations, enabling rapid retrieval during index look-ups. Benchmarks indicate a 72% 
improvement in write latency compared to the Global Simple Index, alongside notable reductions in query latency 
for equality-matching queries. [Consistent Hash Index](https://hudi.apache.org/releases/release-0.14.0#consistent-hashing-index-support)
dynamically scales the buckets for hash-based indexing schemes. By addressing data skew issues inherent in bucket
index, it can achieve blazing fast look-up similar to the Record Level Index during the write process.
[Functional Index](https://hudi.apache.org/releases/release-1.0.0-beta1#functional-index)
enables the creation and deletion of indexes on specific columns, providing users with additional means to
speed up queries and adjust partitioning.

### Write throughput achieves remarkable advancement

[Early-conflict detection for OCC](https://hudi.apache.org/releases/release-0.13.0#early-conflict-detection-for-multi-writer)
proactively validates concurrent writes before they are written to disk, avoiding significant resource wastage
and enhancing throughput. Up-leveling this, the
[Non-Blocking Concurrency Control](https://hudi.apache.org/releases/release-1.0.0-beta1#concurrency-control)
introduced in 1.0 further optimizes multi-writer throughput by allowing conflicts to be resolved later in query
or via compaction. Responding to popular community requests, 
[partial update capability](https://hudi.apache.org/releases/release-0.13.0#support-for-partial-payload-update)
was implemented to allow updates to be applied only to changed fields, particularly benefiting the dimension 
tables that are usually super wide.

### Programming APIs have a brand-new look

[HoodieRecordMerger](https://hudi.apache.org/releases/release-0.13.0#optimizing-record-payload-handling)
is a new abstraction that unifies the merging semantics and makes use of the engine-native representation for
records in the process. Benchmark shows a ballpark of 10% boost for upsert performance.
[File Group Reader](https://hudi.apache.org/releases/release-1.0.0-beta1#new-filegroup-reader)
is another API that standardizes File Group access, reducing MoR tables' read latencies by approximately 20%. 
Enabling position-based merging and page-skipping can further accelerate snapshot queries by 5.7 times.

### Usability receives significant attention

[Table-valued function `hudi_table_changes`](https://hudi.apache.org/releases/release-0.14.0#table-valued-function-named-hudi_table_changes-designed-for-incremental-reading-through-spark-sql)
simplifies performing incremental queries via SQLs.
[Auto-generated key](https://hudi.apache.org/releases/release-0.14.0#support-for-hudi-tables-with-autogenerated-keys)
is another usability improvement; it allows users to omit setting record key field explicitly, a boon for
append-only tables. Among many user-friendly updates, two notable ones are the addition of
[`hudi-cli-bundle` jar](https://hudi.apache.org/releases/release-0.13.0#hudi-cli-bundle)
and a revamped [configuration page](https://hudi.apache.org/docs/basic_configurations).

### Platform capabilities undergo notable expansions

[Changed Data Capture](https://hudi.apache.org/releases/release-0.13.0#change-data-capture)
was supported by logging additional information alongside writers. The changed data, including `before` 
and `after` images, can be served through incremental queries, offering rich analytical insights. 
[Metaserver](https://hudi.apache.org/releases/release-0.13.0#metaserver)
offers centralized management services for operating numerous tables in lakehouse projects, signifying a major
step in Hudi's platform features. 
[`HoodieStreamer`](https://hudi.apache.org/releases/release-0.14.0#hoodiedeltastreamer-renamed-to-hoodiestreamer) 
(formerly `HoodieDeltaStreamer`) remains a highly popular tool for data ingestion:
[new sources](https://hudi.apache.org/releases/release-0.13.0#new-source-support-in-deltastreamer) 
such as Protobuf Kafka source, GCS incremental source, and Pulsar source were added, further expanding 
the integration capabilities.

On the engine integration, there has been significant progress, notably with 
[AWS Athena supporting Hudi's metadata table](https://aws.amazon.com/about-aws/whats-new/2023/05/amazon-athena-apache-hudi/) 
thereby elevating query performance, 
[the support with AWS Glue crawlers](https://aws.amazon.com/blogs/big-data/introducing-apache-hudi-support-with-aws-glue-crawlers/),
and [Starburst adding the Hudi connector](https://docs.starburst.io/latest/connector/hudi.html).
Execution engine support has also been extended to newer versions, including Spark 3.4 and 3.5, as well as Flink 1.16, 1.17, and 1.18.

### Interoperability is the key

The [recent announcement about OneTable becoming open source](https://www.onehouse.ai/blog/onetable-is-now-open-source)
marks a big leap forward for Hudi in terms of interoperability, with its integration into [OneTable](https://onetable.dev/).
This development not only emphasizes Hudi's commitment to openness but also enables a wider range of users to experience
the technological advantages offered by Hudi.

### Stay tuned for 2024

The File Group Reader APIs are poised for widespread adoption, promising benefits for numerous query 
engines. We also anticipate broad adoption for Non-Blocking Concurrency Control. And there's more on 
the horizon, including innovations like infinite timeline, secondary indexes, multi-table transactions, 
and support for unstructured data. For the latest updates and detailed insights, I encourage you to 
visit the [roadmap page](https://hudi.apache.org/roadmap).

## Content Spotlight

<img src="/assets/images/blog/2023-12-28-a-year-in-review-2023/02.contentspotlight.png" alt="drawing" style={{width:'80%', display:'block', marginLeft:'auto', marginRight:'auto'}} />


The vibrant and diverse Hudi community has been a rich source of exceptional content. Below is a 
curated list highlighting 10 noteworthy pieces of content from this dynamic group.

- [Hudi-based near-real-time transactional data lake on AWS](https://aws.amazon.com/blogs/big-data/create-an-apache-hudi-based-near-real-time-transactional-data-lake-using-aws-dms-amazon-kinesis-aws-glue-streaming-etl-and-data-visualization-using-amazon-quicksight/) - AWS
- [Automate schema evolution at scale with Apache Hudi](https://aws.amazon.com/blogs/big-data/automate-schema-evolution-at-scale-with-apache-hudi-in-aws-glue/) - AWS
- [Lakehouse at Fortune 1 scale](https://medium.com/walmartglobaltech/lakehouse-at-fortune-1-scale-480bcb10391b) - Walmart
- [Transactional Data Lake with Incremental ETL using Apache Hudi](https://www.uber.com/blog/ubers-lakehouse-architecture/) - Uber
- [Feature comparison: Hudi vs Delta vs Iceberg](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison) - Kyle Weller, Onehouse
- [Delta, Hudi, Iceberg - A Benchmark Compilation](https://medium.com/@kywe665/delta-hudi-iceberg-a-benchmark-compilation-a5630c69cffc) - Kyle Weller, Onehouse
- [Hudi Metafields demystified](https://www.onehouse.ai/blog/hudi-metafields-demystified) - Bhavani Sudha Saktheeswaran, Hudi PMC
- [Knowing your data partitioning vices](https://www.onehouse.ai/blog/knowing-your-data-partitioning-vices-on-the-data-lakehouse) - Bhavani Sudha Saktheeswaran, Hudi PMC
- [Record Level Index: blazing fast indexing for large-scale datasets](https://hudi.apache.org/blog/2023/11/01/record-level-index/) - Shiyan Xu & Sivabalan Narayanan, Hudi PMC
- [Apache Hudi from zero to one: a 10-post blog series](https://blog.datumagic.com/p/apache-hudi-from-zero-to-one-110) - Shiyan Xu, Hudi PMC

Additionally, the official Hudi website is a treasure trove of valuable learning materials. Begin your
journey on [the documentation page](https://hudi.apache.org/docs/overview), and then explore a wealth of 
[talks](https://hudi.apache.org/talks), [videos](https://hudi.apache.org/videos), 
and [blogs](https://hudi.apache.org/blog) to deepen your understanding and knowledge of Hudi.

## Engage with the Community

Throughout 2023, the Hudi community has maintained its remarkable dynamism through various engagements, 
including virtual syncs, live events, meet-ups, and conference presentations. We marked our 
presence at several events, listed here in no particular order: Re:Invent, PrestoCon, 
Trino Fest, Current, the Data & AI Summit, Flink Forward, the Open-source Data Summit, ApacheCon, 
AI.dev, QCon, OSA Con, and DEWCon.

<img src="/assets/images/blog/2023-12-28-a-year-in-review-2023/02.events.png" alt="drawing" style={{width:'80%', display:'block', marginLeft:'auto', marginRight:'auto'}} />

As we reflect on an eventful 2023, the Hudi community continues to thrive and welcomes diverse forms 
of engagement. For those looking to connect, our 
[Slack group](https://join.slack.com/t/apache-hudi/shared_invite/zt-20r833rxh-627NWYDUyR8jRtMa2mZ~gg) 
is an excellent place for general inquiries. 
You can also participate in our [weekly office hours](https://hudi.apache.org/community/office_hours) 
and [monthly community syncs](https://hudi.apache.org/community/syncs) 
to stay updated and involved. To keep abreast of the latest developments, follow Hudi's 
[LinkedIn page](https://www.linkedin.com/company/apache-hudi/), 
[X (Twitter) account](https://twitter.com/apachehudi),
and [YouTube channel](https://www.youtube.com/@apachehudi).

If you encounter any issues or have feature requests, we encourage you to file them through 
[GitHub issues](https://github.com/apache/hudi/issues) or 
[JIRA tickets](https://issues.apache.org/jira/projects/HUDI/summary). 
For more in-depth discussions and contributions to the ongoing development of Hudi, 
subscribing (by sending an empty email) to 
[our dev mailing list](mailto:dev-subscribe@hudi.apache.org) is a great option.

And for those inspired to contribute directly to the project, 
[our contribution guide](https://hudi.apache.org/contribute/how-to-contribute) is your 
starting point. Your involvement, whether it's by contributing code, sharing ideas, or simply giving 
[our repository](https://github.com/apache/hudi/) a star, is greatly valued. Together, let's continue to shape Hudi's future and 
drive innovation in the open-source community. Here's to an even more vibrant and successful year ahead!