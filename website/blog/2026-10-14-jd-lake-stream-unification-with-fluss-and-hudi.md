---
title: "Unifying Lake and Stream at JD.com with Apache Fluss and Apache Hudi"
excerpt: "How JD.com connects real-time streams with its Hudi data lake through Apache Fluss, cutting offline compute cost by 80%+ in recommendation ranking, and the Hudi tiering, consumer offset management, and Coordinator HA work behind it."
description: "How JD.com connects real-time streams with its Hudi data lake through Apache Fluss, cutting offline compute cost by 80%+ in recommendation ranking, and the Hudi tiering, consumer offset management, and Coordinator HA work behind it."
author: "Hou Zhong, Technical Expert at JD.com"
category: case-study
image: /assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/02-fluss-lake-stream-architecture.jpg
tags:
- apache fluss
- streaming
- data lakehouse
- apache flink
---

---

_This blog was translated from the [original blog in Chinese](https://mp.weixin.qq.com/s/uVv9maXyDnObbfjTRoqrzg), published by the Apache Fluss community and based on a talk at Flink Forward Asia 2026._

---

This post covers JD.com's exploration and practice of lake-stream unification based on Apache Fluss, in four parts:

- **Background and challenges**: the resource and architecture problems of JD's data lake and data streaming systems at large scale.
- **Business practice**: the redesign and its results, using recommendation ranking as the example.
- **In-house features**: the Tiering Service to Hudi, consumer offset management, and Coordinator high availability.
- **Future plans**: community contributions and the next steps in the technical roadmap.

## Background and Challenges of Lake-Stream Unification at JD

After years of building, JD's data lake and data streaming systems support a large business. On the lake side, built on the Apache Hudi table format, there are **more than 5,000 lake tables**, **more than 700 PB** of data migrated into the lake, and a single job peaks at **more than 600 million records per minute**. On the streaming side, built on Apache Kafka, the production clusters run **more than 8,000 nodes**, with peak writes of **500 million records per second** and peak egress of **2 TB/s**.

![JD's data lake and data streaming scale](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/01-jd-lake-stream-scale.jpg)
_Figure 1: Scale of JD's data lake and data streaming_

These numbers mean two things. First, JD's data infrastructure already carries production load with very high throughput, high concurrency, and complex workloads. Second, at this scale every local inefficiency is amplified. Storing an extra copy of the data, reading a few unneeded fields, or maintaining one more pipeline turns directly into cost, stability, and engineering-efficiency problems.

### Bandwidth and Compute Overhead of Row-Based Consumption

The first challenge is the resource waste of row-oriented message queues. Take JD's traffic exposure topic as an example: consuming all of it reads about **195 GB/min**, but the business actually needs only **6 GB/min**. Many downstream jobs care about only a few fields, yet a traditional message queue is organized by rows, so consumers must pull the full message first and then parse, filter, and prune fields in the compute job.

The waste is significant. First, network bandwidth: the business needs only a few fields but must pull the full message. Second, compute: deserializing and parsing irrelevant fields consumes CPU and memory. Third, pipeline pressure: the larger the topic and the more consumers it has, the heavier the repeated consumption and parsing. In the end, the data users actually need is small, but the platform pays a high price for the full volume.

### Duplicate Pipelines and Inconsistent Definitions from a Split Lake and Stream

The second challenge is the architectural complexity caused by keeping the lake and the stream separate. Take sample generation: many search and recommendation businesses need training samples and have traditionally maintained a real-time pipeline and an offline pipeline side by side. The real-time pipeline quickly processes online behavior to serve real-time samples and features; the offline pipeline handles backfill, correction, training, and analysis. This meets different freshness requirements, but it causes three problems:

- **Duplicate storage.** Similar data is stored multiple times, in the real-time message queue and in offline lake tables.
- **Duplicate compute.** Both the real-time and offline jobs do cleansing, joins, feature assembly, and sample generation; only the execution style differs.
- **Inconsistent definitions.** The two pipelines evolve separately, so field definitions, filter conditions, error handling, and backfill logic can drift apart. When the business investigates a metric fluctuation, it often has to first confirm how the real-time and offline definitions differ.

### Introducing the Fluss Lake-Stream Unified Architecture

So we began exploring how to connect streaming data and lake table data under one architecture, unifying the write, storage, consumption, and lake ingestion flows. This is the background for introducing the Fluss lake-stream unified architecture.

The core goal of lake-stream unification is to gradually unify real-time streams and the data lake in **data model, service interface, and consumption model**. For the business, one copy of data can serve real-time consumption through Fluss streaming and batch reads, and can also land in Hudi tables for long-term analysis, training, and backtracking with engines such as Apache Spark. When a job needs to read lake data and stream data together, it can use Union Read.

For the platform, this architecture reduces duplicate pipelines and resource waste, and puts table metadata, schema definitions, metric definitions, lake ingestion, and operations under unified control and governance.

![Fluss lake-stream unified architecture](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/02-fluss-lake-stream-architecture.jpg)
_Figure 2: Fluss lake-stream unified architecture_

## Lake-Stream Unification in Practice at JD with Fluss

We chose recommendation ranking as a key production scenario. Ranking sits at the core of the recommendation pipeline: it scores candidate items using user behavior, context, and a large number of features, and it directly affects click-through and order conversion rates. The quality, freshness, and consistency of ranking samples are therefore critical to model training and iteration.

### Business Background and Pain Points

Before the redesign, recommendation ranking had several typical pain points:

- **Scattered sample sources, hard to develop and operate.** Offline and real-time samples came from different pipelines. Teams had to master two tech stacks and keep two pipelines accurate and stable, and any change to processing logic had to be made in both. Offline and real-time definitions easily diverged between upstream and downstream teams.
- **High storage and compute cost for samples.** The same feature and label data was stored and computed multiple times across systems, and neither the real-time nor the offline pipeline could update samples incrementally.
- **Hard to balance sample quality and freshness.** The real-time pipeline had a short join window, inefficient row-based reads, and no shuffle capability, which limited sample quality. The offline pipeline had poor freshness and was hard to backfill, yet backfilling and correcting real-time samples still depended on it.

![Recommendation ranking background and pain points](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/03-ranking-before.jpg)
_Figure 3: Recommendation ranking background and pain points_

### Redesigned Architecture

The idea behind the Fluss-based redesign is to move sample generation from "multiple parallel pipelines" to "**one copy of data, many ways to consume it**". Real-time features and labels first stream into Fluss, supporting low-latency writes and minute-level online learning. At the same time, the Tiering Service lands the data in Hudi tables, where Hudi's efficient backfill supports offline analysis, base model training, and catch-up training. The real-time and offline sides now consume the same data in different ways.

In this architecture the business gets real-time samples from Fluss and offline training data from Hudi tables. More importantly, the two gradually share the same source and definitions. Field definitions, write paths, and lifecycle management are all managed through Fluss. Engineers no longer have to keep checking "does real-time have this field, does offline have this field, do the two agree", and can spend more time on model quality and business strategy.

![Recommendation ranking redesigned on Fluss](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/04-ranking-on-fluss.jpg)
_Figure 4: Recommendation ranking redesigned on Fluss_

### Results

The redesign delivered the following results for recommendation ranking:

- **Resource cost**: offline compute cost **down more than 80%**, storage cost **down more than 50%**.
- **Pipeline freshness**: end-to-end pipeline freshness **improved by more than 30%**.
- **Development and maintenance**: three sample sources (offline, catch-up, and real-time) merged into one, and two APIs (offline and real-time) merged into one.
- **Business impact**: core recommendation metrics, including user click-through rate (UCTR) and order conversion rate (UCVR), improved in A/B tests.

The ranking deployment validated the feasibility and benefits of the Fluss lake-stream unified architecture for this scenario. A simpler sample pipeline, unified definitions, and better freshness also shorten the cycle for algorithm experiments and validation.

![Recommendation ranking results](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/05-ranking-results.jpg)
_Figure 5: Recommendation ranking results_

## In-House Features Built on Fluss at JD

### Tiering Service to Hudi

Fluss provides the core lake-stream capabilities, and the open source project already integrates Apache Paimon, Apache Iceberg, and Lance. JD's data lake runs on Hudi, so we needed to adapt and extend Fluss around the existing Hudi ecosystem. To do this we built the Tiering Service to Hudi, which plugs Hudi in as a plugin and mainly adapts the processing flow of the SourceEnumerator, SourceReader, and Committer.

The Tiering Service framework has three layers:

- **Runtime layer**: TieringSourceEnumerator splits the tiering work and assigns the splits; TieringSourceReader reads Fluss data and writes it to the lake table; TieringCommitter coordinates the lake table commit and reports the snapshot back to Fluss.
- **Abstraction layer**: a unified lake storage interface hides the differences between lake formats.
- **Implementation layer**: Hudi plugs in as a LakeStorage plugin. HudiLakeStorage is the entry point, and HudiLakeTieringFactory creates the HudiLakeWriter and HudiLakeCommitter, packaging write and commit capabilities into an extensible plugin.

HudiLakeWriter converts data from the Fluss format to Hudi and writes it in batches; HudiLakeCommitter handles the commit logic on the Hudi side. The runtime then reports the lake snapshot and the corresponding Fluss offsets back to Fluss, so that the progress of the Fluss table and the Hudi table stays aligned.

![Tiering Service to Hudi architecture](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/06-tiering-service-hudi-plugin.jpg)
_Figure 6: Overall architecture of the Tiering Service to Hudi_

**Split assignment: inject metadata into each round of splits.** TieringSourceEnumerator divides and assigns source read tasks. After generating the splits, it injects the round metadata into each one: the total number of splits in the round, the index of the current split, and the round's generation timestamp. This metadata travels with the splits to each TieringSourceReader and is used in the later read, write, and commit stages.

![Hudi TieringSourceEnumerator](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/07-tiering-source-enumerator.jpg)
_Figure 7: Hudi TieringSourceEnumerator_

**Instant coordination: one round of writes shares one Hudi instant.** After TieringSourceReader receives a split, it passes the round metadata to HudiLakeWriter. Because several parallel splits must write into the same Hudi instant, we built a round-level instant coordination mechanism:

The writer gets its own splitIndex from the metadata. The split with index 0 acts as the coordinator for the round: it creates the Hudi instant, marks it INFLIGHT, and records the relevant fields in the checkpoint metadata. The other writers reuse that instant instead of creating their own, so all writers in the round share the same INFLIGHT instant.

![Hudi TieringSourceReader](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/08-tiering-source-reader.jpg)
_Figure 8: Hudi TieringSourceReader_

**Writing data: format conversion, batch buffering, and parallel writes.** HudiLakeWriter is the entry point of the Hudi write path. It writes data but does not do the final commit. Internally it runs a sequential pipeline: after the split with index 0 creates the instant, it wraps the Fluss record and converts the schema and fields; RecordWriter then maps Fluss buckets and fields to Hudi and puts the record into RecordWriteBuffer for batch buffering; finally it calls the Hudi client write API and collects the write result.

Every split runs the same write flow, and thanks to the round-level coordination described above, multiple writers can write into the same Hudi instant in parallel.

![HudiLakeWriter](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/09-hudi-lake-writer.jpg)
_Figure 9: HudiLakeWriter_

**Committing results: aggregate write results and report the lake snapshot.** TieringCommitter first aggregates the results of all writers and checks that they belong to the same round and the same Hudi instant. Once the check passes, it prepares the Fluss bucket offsets file. After the Hudi timeline and the checkpoint metadata both reach COMPLETED, it commits the snapshot, including offsets and manifests, to the Fluss Coordinator.

The Hudi instant time serves as the Fluss lake snapshot ID and is reported back to Fluss together with the corresponding bucket offsets. Committing the Hudi timeline once makes the whole round take effect as a single commit, and reporting the lake snapshot gives Fluss the basis for tracking lake ingestion progress, failure recovery, and consistency management.

![Hudi TieringCommitter](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/10-tiering-committer.jpg)
_Figure 10: Hudi TieringCommitter_

### Consumer Offset Management

Consumer offset management is a basic capability of any messaging system. The community version JD used at the time did not yet store consumer offsets on the Fluss server side. Flink jobs relied on checkpoints to record offsets, so restarting without state meant losing the consumption progress. Non-Flink clients such as Java, Python, and Rust also needed offset persistence independent of Flink checkpoints. We therefore implemented consumer offset management on the Fluss server side.

In the Fluss engine we designed a primary key system table, `consumer_offset`. Its composite primary key has four fields: group, table_id, partition_id, and bucket_num. An offset record holds the offset, leader_epoch, metadata, and commit timestamp; group metadata holds the coordination protocol, generation, consumer leader, members, and so on.

The system table distinguishes two kinds of records by the values of the partition and bucket fields:

- **Consumer offsets**: when bucket is not -1, the record stores the offsets of that bucket. This works for both partitioned and non-partitioned tables.
- **Consumer group metadata**: when both partition and bucket are -1, the record stores the group's protocol, leader, members, and other information at a point in time.

![Consumer offset system table](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/11-consumer-offset-system-table.jpg)
_Figure 11: Consumer offset system table_

With the system table as the store for offsets and group metadata, we implemented the RPCs between client and server, mainly for joining a group, leaving a group, committing offsets, and querying offsets.

**JoinGroup: join the group and assign partitions.** A consumer hashes the groupId over the number of buckets to find the Fluss node hosting its Consumer Coordinator, then talks to that node. Taking Consumer1 in the figure as an example:

Consumer1 sends a JoinGroup request to the ConsumerCoordinator. The server triggers a rebalance through the heartbeat responses to existing members and hands the member information to the consumer leader. The consumer leader assigns the partitions and submits the assignment through a SyncGroup request; the server then returns the result to each member in its SyncGroup response.

![Join Group flow](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/12-join-group-flow.jpg)
_Figure 12: Join Group flow_

**LeaveGroup: reassign after a member leaves.** Consumer2 sends a LeaveGroup request. After the Fluss server processes the departure, it triggers a rebalance for the remaining members. The rest follows the JoinGroup flow: the remaining members rejoin, the elected consumer leader assigns partitions, and the result goes back to each member.

![Leave Group flow](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/13-leave-group-flow.jpg)
_Figure 13: Leave Group flow_

**CommitOffset: persist consumption progress.** The consumer client organizes the partition, bucket, and offset information to match the system table schema and submits it to the Fluss server in a CommitOffset request. The server writes it into the `consumer_offset` system table. Offset commits run asynchronously with heartbeat, JoinGroup, and other requests.

![Commit Offset flow](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/14-commit-offset-flow.jpg)
_Figure 14: Commit Offset flow_

**Describe Offset: query consumption progress and lag.** Querying a group returns, for each bucket it consumes, the current offset, the log end offset, the lag, and client metadata. This makes it easy to find lagging consumer groups and the clients involved.

![Describe Offset output](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/15-describe-offset.jpg)
_Figure 15: Querying consumer offsets and lag_

### Coordinator High Availability

JD started development on community version 0.8, which did not yet have Coordinator high availability, so the cluster had a single point of failure. The Tiering Service reports lake ingestion progress to the Coordinator, so its stability also depends on the Coordinator being available. We therefore first built a high availability solution internally.

The solution runs on JD's internal Kubernetes platform and deploys a Controller daemon as a StatefulSet. In each pod, the Controller runs as PID 1. On startup it registers with ZooKeeper; the pod that registers successfully starts the Fluss Coordinator process, and the other pods act as cold standbys, watching the corresponding ephemeral znode.

There are two layers of supervision: the StatefulSet keeps the pods running and restarts them, and the Controller manages the Fluss Coordinator process inside the pod. Coordinating at both the pod level and the process level makes the Coordinator highly available.

![Coordinator high availability design](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/16-coordinator-ha.jpg)
_Figure 16: Overall design of Coordinator high availability_

### Other In-House Features

Beyond these three areas, we built other features as well. For example, dynamic configuration lets key parameters be changed at runtime, and rebalance throttling reduces the impact of rebalancing on the cluster.

![Other in-house features](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/17-other-features.jpg)
_Figure 17: Other in-house features_

## Future Plans

On the community side, we will keep pushing forward FIP-24: Tiering Service to Hudi, contributing back what we learned about Hudi integration, writes, commits, type conversion, and production rollout. We hope this work serves not only JD but also helps more Hudi users adopt the Fluss lake-stream unified architecture.

![Community contributions and future plans](/assets/images/blog/2026-10-14-jd-lake-stream-unification-with-fluss-and-hudi/18-community-and-roadmap.jpg)
_Figure 18: Community contributions and future plans_

Our next steps fall into three areas:

- **Quota throttling.** Introduce user-level quotas to control resource usage per tenant and per business more finely, so that a local traffic spike does not cause cluster-wide jitter.
- **Consumer offset management improvements.** Focus on reducing rebalance time when there are many buckets.
- **Lake-stream feature parity.** Keep aligning with capabilities in JD's Hudi stack, such as schema evolution and dynamic buckets, to round out the unified experience. The end goal is for the business to use lake and stream data through one data object, one access method, and one set of governance capabilities.

To learn more and connect with other developers, join the [Apache Fluss community](https://fluss.apache.org/community/welcome/).
