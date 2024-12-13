---
title: Design & Concepts
keywords: [hudi, writing, reading]
---
# Design & Concepts FAQ

### How does Hudi ensure atomicity?

Hudi writers atomically move an inflight write operation to a "completed" state by writing an object/file to the [timeline](timeline) folder, identifying the write operation with an instant time that denotes the time the action is deemed to have occurred. This is achieved on the underlying DFS (in the case of S3/Cloud Storage, by an atomic PUT operation) and can be observed by files of the pattern `<instant>.<action>.<state>` in Hudi’s timeline.

### Does Hudi extend the Hive table layout?

Hudi is very different from Hive in important aspects described below. However, based on practical considerations, it chooses to be compatible with Hive table layout by adopting partitioning, schema evolution and being queryable through Hive query engine. Here are the key aspect where Hudi differs:

*   Unlike Hive, Hudi does not remove the partition columns from the data files. Hudi in fact adds record level [meta fields](/tech-specs#meta-fields) including instant time, primary record key, and partition path to the data to support efficient upserts and [incremental queries/ETL](/learn/use_cases/#incremental-processing-pipelines).  Hudi tables can be non-partitioned and the Hudi metadata table adds rich indexes on Hudi tables which are beyond simple Hive extensions.
*   Hive advocates partitioning as the main remedy for most performance-based issues. Features like partition evolution and hidden partitioning are primarily based on this Hive based principle of partitioning and aim to tackle the metadata problem partially.  Whereas, Hudi biases to coarse-grained partitioning and emphasizes [clustering](/docs/clustering) for more fine-grained partitioning. Further, users can strategize and evolve the clustering asynchronously which “actually” help users experiencing performance issues with too granular partitions.
*   Hudi considers partition evolution as an anti-pattern and avoids such schemes due to the inconsistent performance of queries that goes to depend on which part of the table is being queried. Hudi’s design favors consistent performance and is aware of the need to redesign to partitioning/tables to achieve the same.

### What concurrency control approaches does Hudi adopt?

Hudi provides snapshot isolation between all three types of processes - writers, readers, and table services, meaning they all operate on a consistent snapshot of the table. Hudi provides optimistic concurrency control (OCC) between writers, while providing lock-free, non-blocking MVCC-based concurrency control between writers and table-services and between different table services. Widely accepted database literature like “[Architecture of a database system, pg 81](https://dsf.berkeley.edu/papers/fntdb07-architecture.pdf)” clearly lays out 2Phase Locking, OCC and MVCC as the different concurrency control approaches. Purely OCC-based approaches assume conflicts rarely occur and suffer from significant retries and penalties for any continuous/incremental workloads which are normal for modern lake based workloads. Hudi has been cognizant about this, and has a less enthusiastic view on [OCC](/blog/2021/12/16/lakehouse-concurrency-control-are-we-too-optimistic/), built out things like MVCC-based non-blocking async compaction (the commit time decision significantly aids this), that can have writers working non-stop with table services like compactions running in the background.

### Hudi’s commits are based on transaction start time instead of completed time. Does this cause data loss or inconsistency in case of incremental and time travel queries?

Let’s take a closer look at the scenario here: two commits C1 and C2 (with C2 starting later than C1) start with a later commit (C2) finishing first leaving the inflight transaction of the earlier commit (C1) 
before the completed write of the later transaction (C2) in Hudi’s timeline. This is not an uncommon scenario, especially with various ingestions needs such as backfilling, deleting, bootstrapping, etc 
alongside regular writes. When/Whether the first job would commit will depend on factors such as conflicts between concurrent commits, inflight compactions, other actions on the table’s timeline etc. 
If the first job fails for some reason, Hudi will abort the earlier commit inflight (c1) and the writer has to retry next time with a new instant time > c2 much similar to other OCC implementations. 
Firstly, for snapshot queries the order of commits should not matter at all, since any incomplete writes on the active timeline is ignored by queries and cause no side-effects.

In these scenarios, it might be tempting to think of data inconsistencies/data loss when using Hudi’s incremental queries. However, Hudi takes special handling 
(examples [1](https://github.com/apache/hudi/blob/aea5bb6f0ab824247f5e3498762ad94f643a2cb6/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/helpers/IncrSourceHelper.java#L76), 
[2](https://github.com/apache/hudi/blame/7a6543958368540d221ddc18e0c12b8d526b6859/hudi-hadoop-mr/src/main/java/org/apache/hudi/hadoop/utils/HoodieInputFormatUtils.java#L173)) in incremental queries to ensure that no data 
is served beyond the point there is an inflight instant in its timeline, so no data loss or drop happens. This detection is made possible because Hudi writes first request a transaction on the timeline, before planning/executing
the write, as explained in the [timeline](/docs/timeline#states) section.

In this case, on seeing C1’s inflight commit (publish to timeline is atomic), C2 data (which is > C1 in the timeline) is not served until C1 inflight transitions to a terminal state such as completed or marked as failed. 
This [test](https://github.com/apache/hudi/blob/master/hudi-utilities/src/test/java/org/apache/hudi/utilities/sources/TestHoodieIncrSource.java#L137) demonstrates how Hudi incremental source stops proceeding until C1 completes. 
Hudi favors [safety and sacrifices liveness](https://en.wikipedia.org/wiki/Safety_and_liveness_properties), in such a case. For a single writer, the start times of the transactions are the same as the order of completion of transactions, and both incremental and time-travel queries work as expected. 
In the case of multi-writer, incremental queries still work as expected but time travel queries don't. Since most time travel queries are on historical snapshots with a stable continuous timeline, this has not been implemented upto Hudi 0.13. 
However, a similar approach like above can be easily applied to failing time travel queries as well in this window.

### How does Hudi plan to address the liveness issue above for incremental queries?

Hudi 0.14 improves the liveness aspects by enabling change streams, incremental query and time-travel based on the file/object's timestamp (similar to [Delta Lake](https://docs.delta.io/latest/delta-batch.html#query-an-older-snapshot-of-a-table-time-travel)).

To expand more on the long term approach, Hudi has had a proposal to streamline/improve this experience by adding a transition-time to our timeline, which will remove the [liveness sacrifice](https://en.wikipedia.org/wiki/Safety_and_liveness_properties) and makes it easier to understand. 
This has been delayed for a few reasons 

- Large hosted query engines and users not upgrading fast enough. 
- The issues brought up - \[[1](faq_design_and_concepts#does-hudis-use-of-wall-clock-timestamp-for-instants-pose-any-clock-skew-issues),[2](faq_design_and_concepts#hudis-commits-are-based-on-transaction-start-time-instead-of-completed-time-does-this-cause-data-loss-or-inconsistency-in-case-of-incremental-and-time-travel-queries)\], 
relevant to this are not practically very important to users beyond good pedantic discussions, 
- Wanting to do it alongside [non-blocking concurrency control](https://github.com/apache/hudi/pull/7907) in Hudi version 1.x.

It's planned to be addressed in the first 1.x release.

### Does Hudi’s use of wall clock timestamp for instants pose any clock skew issues?

Theoretically speaking, a clock skew between two writers can result in different notions of time, and order the timeline differently. But, the current NTP implementations and regions standardizing on UTC make this very impractical to happen in practice. Even many popular OLTP-based systems such as DynamoDB and Cassandra use timestamps for record level conflict detection, cloud providers/OSS NTP are moving towards atomic/synchronized clocks all the time \[[1](https://aws.amazon.com/about-aws/whats-new/2017/11/introducing-the-amazon-time-sync-service/),[2](https://engineering.fb.com/2020/03/18/production-engineering/ntp-service/)\]. We haven't had these as practical issues raised over the last several years, across several large scale data lakes.

Further - Hudi’s commit time can be a logical time and need not strictly be a timestamp. If there are still uniqueness concerns over clock skew, it is easy for Hudi to further extend the timestamp implementation with salts or employ [TrueTime](https://www.cockroachlabs.com/blog/living-without-atomic-clocks/) approaches that have been proven at planet scale. In short, this is not a design issue, but more of a pragmatic implementation choice, that allows us to implement unique features like async compaction in face of updates to the same file group, by scheduling actions on discrete timestamp space.
