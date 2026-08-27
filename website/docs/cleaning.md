---
title: Cleaning
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
last_modified_at: 2026-08-06T15:17:36+05:30
---
## Background
Cleaning is a table service employed by Hudi to reclaim space occupied by older versions of data and keep storage costs 
in check. Apache Hudi provides snapshot isolation between writers and readers by managing multiple versioned files with **MVCC** 
concurrency. These file versions provide history and enable time travel and rollbacks, but it is important to manage 
how much history you keep to balance your costs. Cleaning service plays a crucial role in manging the tradeoff between 
retaining long history of data and the associated storage costs.  

Hudi enables [Automatic Hudi cleaning](configurations.md#hoodiecleanautomatic) by default. Cleaning is invoked 
immediately after each commit, to delete older file slices. It's recommended to leave this enabled to ensure metadata 
and data storage growth is bounded. Cleaner can also be scheduled after every few commits instead of after every commit by 
configuring [hoodie.clean.max.commits](https://hudi.apache.org/docs/configurations#hoodiecleanmaxcommits).

### Cleaning Retention Policies 
When cleaning old files, you should be careful not to remove files that are being actively used by long running queries.

For spark based:

| Config Name                                        | Default                        | Description                                                                                                                 |
|----------------------------------------------------|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| hoodie.clean.policy                              | KEEP_LATEST_COMMITS (Optional) | org.apache.hudi.common.model.HoodieCleaningPolicy: Cleaning policy to be used. <br /><br />`Config Param: CLEANER_POLICY`   |

The corresponding config for Flink based engine is [`clean.policy`](https://hudi.apache.org/docs/configurations/#cleanpolicy).

Hudi cleaner currently supports the below cleaning policies to keep a certain number of commits or file versions:

- **KEEP_LATEST_COMMITS**: This is the default policy. This is a temporal cleaning policy that ensures the effect of
  having lookback into all the changes that happened in the last X commits. Suppose a writer is ingesting data
  into a Hudi dataset every 30 minutes and the longest running query can take 5 hours to finish, then the user should
  retain atleast the last 10 commits. With such a configuration, we ensure that the oldest version of a file is kept on
  disk for at least 5 hours, thereby preventing the longest running query from failing at any point in time. Incremental
  cleaning is also possible using this policy.
  Number of commits to retain can be configured by [`hoodie.clean.commits.retained`](https://hudi.apache.org/docs/configurations/#hoodiecleancommitsretained). 
  The corresponding Flink related config is [`clean.retain_commits`](https://hudi.apache.org/docs/configurations/#cleanretain_commits). 

- **KEEP_LATEST_FILE_VERSIONS**: This policy has the effect of keeping N number of file versions irrespective of time.
  This policy is useful when it is known how many MAX versions of the file does one want to keep at any given time.
  To achieve the same behaviour as before of preventing long running queries from failing, one should do their calculations
  based on data patterns. Alternatively, this policy is also useful if a user just wants to maintain 1 latest version of the file.
  Number of file versions to retain can be configured by [`hoodie.clean.fileversions.retained`](https://hudi.apache.org/docs/configurations/#hoodiecleanerfileversionsretained).
  The corresponding Flink related config is [`clean.retain_file_versions`](https://hudi.apache.org/docs/configurations/#cleanretain_file_versions).

- **KEEP_LATEST_BY_HOURS**: This policy clean up based on hours.It is simple and useful when knowing that you want to 
  keep files at any given time. Corresponding to commits with commit times older than the configured number of hours to 
  be retained are cleaned. Currently you can configure by parameter [`hoodie.clean.hours.retained`](https://hudi.apache.org/docs/configurations/#hoodiecleanerhoursretained).
  The corresponding Flink related config is [`clean.retain_hours`](https://hudi.apache.org/docs/configurations/#cleanretain_hours).

#### Empty Clean Commits for Append-Only Tables

Append-only tables never accumulate updates, so the cleaner's `earliest_commit_to_retain` pointer never advances —
causing the cleaner to scan the full table history on every run. Hudi 1.2.0 introduced periodic _empty clean commits_
to advance this pointer even when there is nothing to delete.

| Config Name | Default | Description |
|---|---|---|
| `hoodie.write.empty.clean.interval.hours` | `-1` (disabled) | Interval in hours at which an empty clean commit is created. `-1` disables the feature. Must be `-1` or `>= 1`. When enabled, the cleaner advances `earliest_commit_to_retain` so that subsequent clean plans only scan partitions modified after the last empty clean's pointer. |

#### Capping the Number of Commits Cleaned per Run

Since 1.2.0, you can limit how many commits are cleaned in a single clean run, which is useful for controlling job
duration on tables that have fallen significantly behind on cleaning.

| Config Name | Default | Description |
|---|---|---|
| `hoodie.clean.max.commits.to.clean` | `Long.MAX_VALUE` (unbounded) | Maximum number of commits cleaned in a single clean commit. Applicable when the cleaning policy is `KEEP_LATEST_COMMITS` or `KEEP_LATEST_BY_HOURS`. Must be `>= 1`. |

#### Full-Clean Partition Filtering

When incremental cleaning is disabled (`hoodie.clean.incremental.enabled=false`), the cleaner scans every partition on
every run. For very large tables this can cause OOM during planning. Hudi 1.2.0 added two configs to restrict which
partitions are examined.

:::note
Both configs require `hoodie.clean.incremental.enabled=false`. If both are set, `hoodie.clean.partition.filter.selected`
takes precedence over the regex.
:::

| Config Name | Default | Description |
|---|---|---|
| `hoodie.clean.partition.filter.regex` | (none) | Java regex pattern; only partitions whose path matches are cleaned. |
| `hoodie.clean.partition.filter.selected` | (none) | Comma-separated list of partition paths to clean; takes precedence over the regex when both are set. |

### Instant Times in Clean Metadata

Hudi 1.x stamps every action with both a requested instant time and a completion time, and orders actions on the
timeline by completion time — see [timeline](timeline.md). The cleaner's own plan and metadata, however, record
**instant (start) times** throughout. Keep this in mind when reading them for debugging.

| Field | Written to | Value |
|---|---|---|
| `earliestInstantToRetain.timestamp` | `HoodieCleanerPlan` (the `clean.requested` instant) | Instant time of the oldest commit this clean run retains. |
| `earliestCommitToRetain` | `HoodieCleanMetadata` (the completed `clean` instant) | Copied from the plan, so also an instant time. |
| `lastCompletedCommitTimestamp` | both | Instant time of the write that completed most recently before the clean was planned. Despite the name, this is a start time, not a completion time. |
| `startCleanTime` | `HoodieCleanMetadata` | Instant time of the clean action itself. |

Two details are easy to trip over when reading these values back:

- `lastCompletedCommitTimestamp` mixes the two orderings. The instant is taken from the end of the completed-commits
  timeline, which is ordered by completion time, but what gets recorded is that instant's start time. Concurrent writers
  can complete in a different order than they started in, so this is not always the largest instant time among the
  completed commits.
- `earliestCommitToRetain` is an empty string under the `KEEP_LATEST_FILE_VERSIONS` policy. That policy retains a fixed
  number of file versions per file group rather than a range of the timeline, so the plan carries no
  `earliestInstantToRetain` for the metadata to copy.

Incremental clean planning follows the same convention: it selects the commits whose **requested** instant time is at or
after the previous clean's `earliestCommitToRetain` and before this clean's, then scans only the partitions those
commits touched.

### Configs
For details about all possible configurations and their default values see the [configuration docs](https://hudi.apache.org/docs/next/configurations/#Clean-Configs).
For Flink related configs refer [here](https://hudi.apache.org/docs/next/configurations/#FLINK_SQL).

### Ways to trigger Cleaning

#### Inline

By default, in Spark based writing, cleaning is run inline after every commit using the default policy of `KEEP_LATEST_COMMITS`. It's recommended 
to keep this enabled, to ensure metadata and data storage growth is bounded. To enable this, users do not have to set any configs. Following are the relevant basic configs.

| Config Name                      | Default          | Description                                                                                                                                                                                                                                                                            |
|----------------------------------| -----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.clean.automatic           | true (Optional)  | When enabled, the cleaner table service is invoked immediately after each commit, to delete older file slices. It's recommended to enable this, to ensure metadata and data storage growth is bounded.<br /><br />`Config Param: AUTO_CLEAN`                                           |
| hoodie.clean.commits.retained  | 10 (Optional)    | Number of commits to retain, without cleaning. This will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much data retention the table supports for incremental queries.<br /><br />`Config Param: CLEANER_COMMITS_RETAINED` |


#### Async
In case you wish to run the cleaner service asynchronously along with writing, please enable the [`hoodie.clean.async`](https://hudi.apache.org/docs/configurations#hoodiecleanasync) as shown below:
```java
hoodie.clean.automatic=true
hoodie.clean.async=true
```

For Flink based writing, this is the default mode of cleaning. Please refer to [`clean.async.enabled`](https://hudi.apache.org/docs/configurations/#cleanasyncenabled) for details.

#### Pre-Write Cleaner Policy

By default the cleaner runs _after_ a write commits. Hudi 1.2.0 introduced `hoodie.prewrite.cleaner.policy`, which
lets you force a clean (or rollback of failed writes) _before_ each write begins. This is useful in multi-writer
deployments where you want a deterministic table state before every write — see [concurrency control](concurrency_control.md)
for related multi-writer configuration.

| Config Name | Default | Description |
|---|---|---|
| `hoodie.prewrite.cleaner.policy` | `NONE` | Pre-write cleaning action. `NONE`: no pre-write action (default). `CLEAN`: run a clean pass before each write — this also rolls back failed writes as part of the clean. `ROLLBACK_FAILED_WRITES`: only roll back any failed writes before each write, without running a full clean. |

#### Run independently
Hoodie Cleaner can also be run as a separate process. Following is the command for running the cleaner independently:
```
spark-submit --master local \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:1.0.2,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --class org.apache.hudi.utilities.HoodieCleaner `ls packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle-*.jar` --help
        Usage: <main class> [options]
        Options:
        --help, -h

        --hoodie-conf
        Any configuration that can be set in the properties file (using the CLI
        parameter "--props") can also be passed command line using this
        parameter. This can be repeated
        Default: []
        --props
        path to properties file on localfs or dfs, with configurations for
        hoodie client for cleaning
        --spark-master
        spark master to use.
        Default: local[2]
        * --target-base-path
        base path for the hoodie table to be cleaner.
```
Some examples to run the cleaner.    
Keep the latest 10 commits
```
spark-submit --master local \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:1.0.2,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --class org.apache.hudi.utilities.HoodieCleaner `ls packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle-*.jar` \
  --target-base-path /path/to/hoodie_table \
  --hoodie-conf hoodie.clean.policy=KEEP_LATEST_COMMITS \
  --hoodie-conf hoodie.clean.commits.retained=10 \
  --hoodie-conf hoodie.clean.parallelism=200
```
Keep the latest 3 file versions
```
spark-submit --master local \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:1.0.2,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --class org.apache.hudi.utilities.HoodieCleaner `ls packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle-*.jar` \
  --hoodie-conf hoodie.clean.policy=KEEP_LATEST_FILE_VERSIONS \
  --hoodie-conf hoodie.clean.fileversions.retained=3 \
  --hoodie-conf hoodie.clean.parallelism=200
```
Clean commits older than 24 hours
```
spark-submit --master local \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:1.0.2,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --class org.apache.hudi.utilities.HoodieCleaner `ls packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle-*.jar` \
  --target-base-path /path/to/hoodie_table \
  --hoodie-conf hoodie.clean.policy=KEEP_LATEST_BY_HOURS \
  --hoodie-conf hoodie.clean.hours.retained=24 \
  --hoodie-conf hoodie.clean.parallelism=200
```
Note: The parallelism takes the min value of number of partitions to clean and `hoodie.clean.parallelism`.

#### CLI
You can also use [Hudi CLI](cli.md) to run Hoodie Cleaner.

CLI provides the below commands for cleaner service:
- `cleans show`
- `clean showpartitions`
- `cleans run`

Example of cleaner keeping the latest 10 commits
```
cleans run --sparkMaster local --hoodieConfigs hoodie.clean.policy=KEEP_LATEST_COMMITS hoodie.clean.commits.retained=10 hoodie.clean.parallelism=200
```

You can find more details and the relevant code for these commands in [`org.apache.hudi.cli.commands.CleansCommand`](https://github.com/apache/hudi/blob/master/hudi-cli/src/main/java/org/apache/hudi/cli/commands/CleansCommand.java) class. 

## Partition TTL

Cleaning bounds how many *versions* of a file are kept, but it never removes a partition: an old partition whose files
have all been cleaned down to a single version still sits in the table forever. Partition TTL (time to live) is the
complementary service. It works at partition granularity, and when a partition is judged expired it deletes the whole
partition rather than trimming file versions inside it.

Because it removes data outright, TTL is off by default and stays off until you set a retention period.

### How a partition is judged expired

TTL asks a strategy which partitions have expired. Two strategies ship with Hudi, selected through
`hoodie.partition.ttl.management.strategy.type`:

| Strategy | Ages a partition against |
|---|---|
| `KEEP_BY_TIME` (default) | The partition's last commit time, taken from the newest base instant among its latest file slices. A partition that is still being written to therefore stays. |
| `KEEP_BY_CREATION_TIME` | The commit time the partition was created at, read from its partition metadata. Writing to a partition does not extend its life. |

Both compare that timestamp against `hoodie.partition.ttl.strategy.days.retain`. A custom strategy can be supplied
instead with `hoodie.partition.ttl.strategy.class`, pointing at a subclass of `PartitionTTLStrategy`; when both configs
are present the class takes precedence over the type.

:::caution
`hoodie.partition.ttl.strategy.days.retain` defaults to `-1`, and the built-in strategies treat any value of `0` or less
as "nothing expires". **TTL does nothing at all until you set a positive retention, even with TTL enabled.** This is
deliberate, so that turning the service on cannot delete data by itself, but it does mean a misconfigured job looks like
a working one: it runs, reports no expired partitions, and deletes nothing.
:::

Two other conditions make TTL a silent no-op regardless of retention: a table with no completed commit yet, and an
unpartitioned table.

### Ways to run partition TTL

**Inline.** Setting `hoodie.partition.ttl.inline=true` runs TTL immediately after each commit, alongside the other inline
table services.

**As a standalone Spark job.** `org.apache.hudi.utilities.HoodieTTLJob`, in the utilities bundle, runs TTL against an
existing table without enabling it on the writer:

```
spark-submit --master local \
  --class org.apache.hudi.utilities.HoodieTTLJob \
  hudi-utilities-bundle_2.12-1.2.0.jar \
  --base-path file:///tmp/events_table \
  --hoodie-conf hoodie.partition.ttl.strategy.days.retain=30
```

The utilities bundle is self-contained, so it is passed as the application jar and no `--packages` is needed. Download it
from Maven Central, or build it locally and point at
`packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-*.jar`.

**From Spark SQL**, with the [`run_ttl`](procedures.md#run_ttl) procedure, which is the easiest way to try TTL on a table
before committing to running it on every write:

```sql
call run_ttl(table => 'events_table', retain_days => 30);
```

However it is triggered, TTL writes a replace commit that drops the expired partitions, the same commit type used by the
`delete_partition` operation.

### Keeping a first run under control

The first TTL run on an existing table is the risky one, because every historical partition becomes a candidate at once.
Three configs bound it.

`hoodie.partition.ttl.strategy.max.delete.partitions` caps how many partitions a single run may delete, defaulting to
`1000`. The limit exists to keep one replace commit from growing unmanageably large; partitions over the cap are simply
left for the next run, so a backlog drains across several runs rather than in one commit.

`hoodie.partition.ttl.strategy.partition.selected` takes a comma-separated list of partition paths and restricts TTL to
exactly those. When it is unset, TTL considers every partition in the table. Setting it is the safest way to try a
retention policy on one partition before applying it everywhere.

`hoodie.partition.ttl.strategy.stats.max.parallelism` bounds the parallelism used to collect each candidate partition's
last commit time, defaulting to `200`; the effective value is the smaller of that and the candidate count. It matters
mainly on that first run, where a table with many historical partitions may want a higher value. This config is new in
1.3.0, so it has no effect on earlier releases and does not yet appear in the generated
[configuration reference](https://hudi.apache.org/docs/next/configurations/); the other six configs above do.

### Partition TTL configs

| Config | Default | Description |
|---|---|---|
| `hoodie.partition.ttl.inline` | `false` | Run TTL immediately after each commit |
| `hoodie.partition.ttl.management.strategy.type` | `KEEP_BY_TIME` | `KEEP_BY_TIME` or `KEEP_BY_CREATION_TIME` |
| `hoodie.partition.ttl.strategy.class` | none | A `PartitionTTLStrategy` subclass; takes precedence over the type above |
| `hoodie.partition.ttl.strategy.days.retain` | `-1` | Days to retain. Nothing expires while this is `0` or less |
| `hoodie.partition.ttl.strategy.partition.selected` | none | Comma-separated partition paths to restrict TTL to |
| `hoodie.partition.ttl.strategy.max.delete.partitions` | `1000` | Maximum partitions deleted in one run |
| `hoodie.partition.ttl.strategy.stats.max.parallelism` | `200` | Parallelism for collecting candidate partition commit times. Since 1.3.0 |

### A worked example

Retaining 30 days on a date-partitioned event table, run inline, restricted on the first pass to a single partition so
the effect can be checked before it is applied to the whole table:

```scala
val tableName = "events_table"
val basePath = "file:///tmp/events_table"

df.write.format("hudi")
  .option("hoodie.table.name", tableName)
  .option("hoodie.datasource.write.recordkey.field", "event_id")
  .option("hoodie.datasource.write.partitionpath.field", "event_date")
  // enable TTL and give it a retention, without which it does nothing
  .option("hoodie.partition.ttl.inline", "true")
  .option("hoodie.partition.ttl.management.strategy.type", "KEEP_BY_TIME")
  .option("hoodie.partition.ttl.strategy.days.retain", "30")
  // first pass: one partition only
  .option("hoodie.partition.ttl.strategy.partition.selected", "event_date=2026-01-01")
  .mode("append")
  .save(basePath)
```

Once the deleted partitions look right, drop the `partition.selected` line to let TTL consider the whole table. On a
table with a long history, expect the backlog to drain over several commits because of the
`max.delete.partitions` cap.

:::caution
Partition TTL deletes data. A partition removed by TTL is gone from the table as of that replace commit, recoverable only
for as long as the cleaner and archival have not yet removed the file versions and timeline entries a time travel query
would need. Validate a retention policy with `run_ttl`, or with `partition.selected`, before enabling it inline.
:::

## Related Resources

<h3>Blogs</h3>
* [Cleaner and Archival in Apache Hudi](https://medium.com/@simpsons/cleaner-and-archival-in-apache-hudi-9e15b08b2933)

<h3>Videos</h3>

* [Cleaner Service: Save up to 40% on data lake storage costs | Hudi Labs](https://youtu.be/mUvRhJDoO3w)
* [Efficient Data Lake Management with Apache Hudi Cleaner: Benefits of Scheduling Data Cleaning #1](https://www.youtube.com/watch?v=CEzgFtmVjx4)
* [Efficient Data Lake Management with Apache Hudi Cleaner: Benefits of Scheduling Data Cleaning #2](https://www.youtube.com/watch?v=RbBF9Ys2GqM)
