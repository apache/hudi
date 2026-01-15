---
title: Table Services
keywords: [hudi, writing, reading]
---
# Table Services FAQ

### What does the Hudi cleaner do?

The Hudi cleaner process often runs right after a commit and deltacommit and goes about deleting old files that are no longer needed. If you are using the incremental pull feature, then ensure you configure the cleaner to [retain sufficient amount of last commits](configurations.md#hoodiecleanercommitsretained) to rewind. Another consideration is to provide sufficient time for your long running jobs to finish running. Otherwise, the cleaner could delete a file that is being or could be read by the job and will fail the job. Typically, the default configuration of 10 allows for an ingestion running every 30 mins to retain up-to 5 hours worth of data. If you run ingestion more frequently or if you want to give more running time for a query, consider increasing the value for the config : `hoodie.cleaner.commits.retained`

### How do I run compaction for a MOR table?

Simplest way to run compaction on MOR table is to run the [compaction inline](configurations.md#hoodiecompactinline), at the cost of spending more time ingesting; This could be particularly useful, in common cases where you have small amount of late arriving data trickling into older partitions. In such a scenario, you may want to just aggressively compact the last N partitions while waiting for enough logs to accumulate for older partitions. The net effect is that you have converted most of the recent data, that is more likely to be queried to optimized columnar format.

That said, for obvious reasons of not blocking ingesting for compaction, you may want to run it asynchronously as well. This can be done either via a separate [compaction job](https://github.com/apache/hudi/blob/master/hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieCompactor.java) that is scheduled by your workflow scheduler/notebook independently. If you are using Hudi Streamer, then you can run in [continuous mode](https://github.com/apache/hudi/blob/d3edac4612bde2fa9deca9536801dbc48961fb95/hudi-utilities/src/main/java/org/apache/hudi/utilities/deltastreamer/HoodieDeltaStreamer.java#L241) where the ingestion and compaction are both managed concurrently in a single spark run time.

### What options do I have for asynchronous/offline compactions on MOR table?

There are a couple of options depending on how you write to Hudi. But first let us understand briefly what is involved. There are two parts to compaction

*   Scheduling: In this step, Hudi scans the partitions and selects file slices to be compacted. A compaction plan is finally written to Hudi timeline. Scheduling needs tighter coordination with other writers (regular ingestion is considered one of the writers). If scheduling is done inline with the ingestion job, this coordination is automatically taken care of. Else when scheduling happens asynchronously a lock provider needs to be configured for this coordination among multiple writers.
*   Execution: In this step the compaction plan is read and file slices are compacted. Execution doesnt need the same level of coordination with other writers as Scheduling step and can be decoupled from ingestion job easily.

Depending on how you write to Hudi these are the possible options currently.

*   DeltaStreamer:
  *   In Continuous mode, asynchronous compaction is achieved by default. Here scheduling is done by the ingestion job inline and compaction execution is achieved asynchronously by a separate parallel thread.
  *   In non continuous mode, only inline compaction is possible.
  *   Please note in either mode, by passing --disable-compaction compaction is completely disabled
*   Spark datasource:
  *   Async scheduling and async execution can be achieved by periodically running an offline Hudi Compactor Utility or Hudi CLI. However this needs a lock provider to be configured.
  *   Alternately, from 0.11.0, to avoid dependency on lock providers, scheduling alone can be done inline by regular writer using the config `hoodie.compact.schedule.inline` . And compaction execution can be done offline by periodically triggering the Hudi Compactor Utility or Hudi CLI.
*   Spark structured streaming:
  *   Compactions are scheduled and executed asynchronously inside the streaming job. Async Compactions are enabled by default for structured streaming jobs on Merge-On-Read table.
  *   Please note it is not possible to disable async compaction for MOR table with spark structured streaming.
*   Flink:
  *   Async compaction is enabled by default for Merge-On-Read table.
  *   Offline compaction can be achieved by setting `compaction.async.enabled` to `false` and periodically running [Flink offline Compactor](compaction/#flink-offline-compaction). When running the offline compactor, one needs to ensure there are no active writes to the table.
  *   Third option (highly recommended over the second one) is to schedule the compactions from the regular ingestion job and executing the compaction plans from an offline job. To achieve this set `compaction.async.enabled` to `false`, `compaction.schedule.enabled` to `true` and then run the [Flink offline Compactor](compaction/#flink-offline-compaction) periodically to execute the plans.

### How to disable all table services in case of multiple writers?

[hoodie.table.services.enabled](configurations.md#hoodietableservicesenabled) is an umbrella config that can be used to turn off all table services at once without having to individually disable them. This is handy in use cases where there are multiple writers doing ingestion. While one of the main pipelines can take care of the table services, other ingestion pipelines can disable them to avoid frequent trigger of cleaning/clustering etc. This does not apply to singe writer scenarios.

### Why does Hudi retain at-least one previous commit even after setting hoodie.cleaner.commits.retained': 1 ?

Hudi runs cleaner to remove old file versions as part of writing data either in inline or in asynchronous mode (0.6.0 onwards). Hudi Cleaner retains at-least one previous commit when cleaning old file versions. This is to prevent the case when concurrently running queries which are reading the latest file versions suddenly see those files getting deleted by cleaner because a new file version got added . In other words, retaining at-least one previous commit is needed for ensuring snapshot isolation for readers.

### Can I get notified when new commits happen in my Hudi table?

Yes. Hudi provides the ability to post a callback notification about a write commit. You can use a http hook or choose to

be notified via a Kafka/pulsar topic or plug in your own implementation to get notified. Please refer [here](platform_services_post_commit_callback.md)

for details
