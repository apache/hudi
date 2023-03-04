---
title: Timeline
toc: true
---

A Hudi table maintains all operations happened to the table in a single timeline comprised of two parts, an active timeline and an archived timeline. The active timeline stores all the recent instants, while the archived timeline stores the older instants. An instant is a transaction where all respective partitions within a base path have been successfully updated by either a writer or a table service. Instants that get older in the active timeline are moved to archived timeline at various times.

An instant can alter one or many partitions:

-   If you have one batch ingestion, you’ll see that as one commit in the active timeline. If you ingest streaming data, you might see multiple commits in the active timeline.When you open that commit file, you’ll see a JSON object with metadata about how one or more partitions were altered.

We’ll go over some details and concepts about the active and archived timeline below. All files in the timelines are immutable.

:::caution
The user should never directly alter the timeline (i.e. manually delete the commits).
:::

## Active Timeline

The active timeline is a source of truth for all write operations: when an action (described below) happens on a table, the timeline is responsible for recording it. This guarantees a good table state, and Hudi can provide read/write isolation based on the timeline. For example, when data is being written to a Hudi table (i.e., requested, inflight), any data being written as part of the transaction is not visible to a query engine until the write transaction is completed. The query engine can still read older data, but the data inflight won’t be exposed.

The active timeline is under the `.hoodie` metadata folder. For example, when you navigate to your Hudi project directory:

```sh
cd $YOUR_HUDI_PROJECT_DIRECTORY && ls -a 
```

You’ll see the `.hoodie` metadata folder:

```sh
ls -a
.		..		.hoodie		americas	asia
```

When you navigate inside the `.hoodie` folder, you’ll see a lot of files with different suffixes and the archived timeline folder: 

```sh
cd .hoodie && ls
2023021018095339.commit
20230210180953939.commit.requested	
20230210180953939.inflight
archived
```

Before we go into what’s in the files or how the files are named, we’ll need to cover some broader concepts: 
- [actions](/docs/timeline#actions)
- [states](/docs/timeline#states)
- [instants](/docs/timeline#instants)

## Actions
An action describes what and how transactions were changed. Hudi guarantees that the actions performed on the timeline are atomic and consistent based on the instant time.
Key actions performed include:
- **COMMIT**: A commit denotes an atomic write of a batch of records into parquet base files in a table.
  -  Deltastreamer and Spark Datasource automatically support this; you do not need to set any configs. 
- **REPLACECOMMIT**: This denotes an action that adds new data files to replace existing files in a table.
  - Clustering generates a`replacecommit`, for example, when perform a clustering service, it generates a `replacecommit` action.
  - Clustering is disabled by default. Please check the [clustering documentation](https://hudi.apache.org/docs/clustering/) for more details. 
- **CLEAN**: This table service action eliminates older versions of files in the table that is no longer needed.
  - Hudi automatically supports [clean](https://hudi.apache.org/docs/basic_configurations#Clean-Configs) as the default behavior. 
  - However, the user can disable this setting and perform the clean separately. You can configure the clean through Spark SQL call procedure, Scala within a Spark Shell and the Hudi CLI tool. 
- **DELTACOMMIT**: This refers to an atomic write of a batch of records into a Merge-On-Read type table, where some/all of the data could be just written to delta logs.
  - [Deltastreamer](https://hudi.apache.org/docs/hoodie_deltastreamer) and Spark DataSource automatically support this; you don't need to set configs.
- **COMPACTION**: This is background activity to reconcile differential data structures within Hudi. For example, moving updates from row-based log files to columnar formats. Internally, compaction manifests as a unique commit on the timeline.
  - Hudi automatically supports compaction as the default behavior. 
  - However, you can disable this setting and perform the compactions separately. You can configure the [compactions](https://hudi.apache.org/docs/basic_configurations#Compaction-Configs) through the Spark SQL call procedure, Scala within a Spark Shell and the Hudi CLI tool. 
- **ROLLBACK**: This indicates that a commit/delta commit was unsuccessful & rolled back, removing any partial files produced during the writes.
  - Hudi automatically supports rollbacks as the default behavior. 
  - However, the user can disable this setting and perform rollbacks separately. You can configure the [rollbacks](https://hudi.apache.org/docs/configurations#hoodierollbackparallelism) through the Spark SQL call procedure, Scala within a Spark Shell and the Hudi CLI tool. 
- **SAVEPOINT**: This marks certain file groups as "saved," where the cleaner will not delete them. It helps restore the table to a point on the timeline for disaster/data recovery scenarios.
  - This is supported by the Spark SQL call procedure, Scala within a Spark Shell and the Hudi CLI tool. You manually set up this action. 
- **RESTORE**: This indicates an action to restore the table to a previous state.
  - This is supported by the Spark SQL call procedure, Scala within a Spark Shell and the Hudi CLI tool. You manually set up this action. 
- **INDEXING**: This denotes an action to [asynchronounsly build the index](https://hudi.apache.org/docs/metadata_indexing) in the metadata table.

## States
A state denotes if an action has been scheduled, in progress or completed. There are three different states an action can be in: 
- **REQUESTED**: Denotes an action has been has initiated, but has not been scheduled.
- **INFLIGHT**: Denotes an action is currently being performed.
- **COMPLETED**: Denotes completion of an action on the timeline.

When a transaction is initiated for an operation, we create an instant file with a `.requested` suffix that marks the transaction has been initiated. When a transaction is transitioned from requested to inflight, the writes or a table service task (i.e., cleaning, compaction, etc) will be modifying, deleting, updating or inserting a file in a Hudi table. The instant file will have a `.inflight`  suffix, and the inflight state means the transaction tasks are ongoing. When the transaction tasks are completed successfully, the transaction state transitions from inflight to complete. The instant file will **NOT HAVE** a completed suffix; instead, there will be no suffix. We’ll point this out down below. 

## Instants
At its core, Hudi maintains a timeline of all actions performed on the table at different timestamps, represented as a series of instants. The instants provide snapshot views of the table, while also efficiently supporting data retrieval in the order of arrival. A Hudi instant consists of the following components:
- **instant time**: An instant time is typically a timestamp (e.g: 20190117010349 for second-level granularity prior to 0.10.0 release or 20190117010349567 for millisecond-level granularity since 0.10.0 release). The instant time monotonically increases in the order of action's begin time. The timezone will be the same as your machine's timeszone.
- **instant action**:  An instant action is a type of action performed on the table. 
- **state suffix**: A state suffix is the current state of the instant.

Inside the `.hoodie` folder, each transaction has three metadata files. Each file is named differently depending on the state: 
- For a transaction with a **requested** or **inflight** state, the files are written like this: 
  - [instantTime.instantAction.stateSuffix]
 - For a transaction with a **completed** state, the files are written like this: 
   - [instantTime.ActionName]

Below are four instances inside the `.hoodie` folder. Let's check out two instant examples where the states are different:

```sh
cd .hoodie && ls 
20230210181040140.commit.requested     #requested state
2023021018095339.commit                #completed state
20230210180953939.commit.requested  
20230210180953939.inflight

```

- Looking at `20230210181040140.commit.requested `, we can see this instant file has the timestamp + action + state because it was in a requested state.

- Looking at `2023021018095339.commit`, we can see this instant has the timestamp + action because it is in the completed state. 

### An Instant File
An instant file will be serialized into a JSON or Avro object with metadata information about the file. 
- For files that have **COMMIT** or **DELTACOMMIT** action, you’ll see a JSON object.
- All other actions will be serialized in an Avro object.

#### A file in a requested state for a COMMIT or DELTACOMMIT action

When a COMMIT is in a requested state, it’ll be an empty file:

```sh 
.hoodie $ cat 20230210181040140.commit.requested 
.hoodie $
```

#### A COMMIT in an inflight state
When a COMMIT is in an inflight state, it’ll be have some metadata information about the file, but will largely be incomplete:

```sh 
.hoodie $ cat 20230210181040140.inflight
{
  "partitionToWriteStats" : {
    "americas/brazil/sao_paulo" : [ {
      "fileId" : "",
      "path" : null,
      "prevCommit" : "null",
      "numWrites" : 0,
      "numDeletes" : 0,
      "numUpdateWrites" : 0,
      "numInserts" : 0,
      "totalWriteBytes" : 0,
      "totalWriteErrors" : 0,
      "tempPath" : null,
      "partitionPath" : null,
      "totalLogRecords" : 0,
      "totalLogFilesCompacted" : 0,
      "totalLogSizeCompacted" : 0,
      "totalUpdatedRecordsCompacted" : 0,
      "totalLogBlocks" : 0,
      "totalCorruptLogBlock" : 0,
      "totalRollbackBlocks" : 0,
      "fileSizeInBytes" : 0,
      "minEventTime" : null,
      "maxEventTime" : null,
      "runtimeStats" : null
    }, {
      "fileId" : "384c94e2-293d-4d8e-ade7-b9b3be629f3b-0",
      "path" : null,
      "prevCommit" : "20230210181058805",
      "numWrites" : 0,
      "numDeletes" : 0,
      "numUpdateWrites" : 13221,
      "numInserts" : 0,
      "totalWriteBytes" : 0,
      "totalWriteErrors" : 0,
      "tempPath" : null,
      "partitionPath" : null,
      "totalLogRecords" : 0,
      "totalLogFilesCompacted" : 0,
      "totalLogSizeCompacted" : 0,
      "totalUpdatedRecordsCompacted" : 0,
      "totalLogBlocks" : 0,
      "totalCorruptLogBlock" : 0,
      "totalRollbackBlocks" : 0,
      "fileSizeInBytes" : 0,
      "minEventTime" : null,
      "maxEventTime" : null,
      "runtimeStats" : null
    } ],
    "americas/united_states/san_francisco" : [ {
      "fileId" : "",
      "path" : null,
      "prevCommit" : "null",
      "numWrites" : 0,
      "numDeletes" : 0,
      "numUpdateWrites" : 0,
      "numInserts" : 0,
      "totalWriteBytes" : 0,
      "totalWriteErrors" : 0,
      "tempPath" : null,
      "partitionPath" : null,
      "totalLogRecords" : 0,
      "totalLogFilesCompacted" : 0,
      "totalLogSizeCompacted" : 0,
      "totalUpdatedRecordsCompacted" : 0,
      "totalLogBlocks" : 0,
      "totalCorruptLogBlock" : 0,
      "totalRollbackBlocks" : 0,
      "fileSizeInBytes" : 0,
      "minEventTime" : null,
      "maxEventTime" : null,
      "runtimeStats" : null
    }, {
      "fileId" : "c26ac037-4e5f-492f-bfb8-fbbd3bd79848-0",
      "path" : null,
      "prevCommit" : "20230210181058805",
      "numWrites" : 0,
      "numDeletes" : 0,
      "numUpdateWrites" : 13145,
      "numInserts" : 0,
      "totalWriteBytes" : 0,
      "totalWriteErrors" : 0,
      "tempPath" : null,
      "partitionPath" : null,
      "totalLogRecords" : 0,
      "totalLogFilesCompacted" : 0,
      "totalLogSizeCompacted" : 0,
      "totalUpdatedRecordsCompacted" : 0,
      "totalLogBlocks" : 0,
      "totalCorruptLogBlock" : 0,
      "totalRollbackBlocks" : 0,
      "fileSizeInBytes" : 0,
      "minEventTime" : null,
      "maxEventTime" : null,
      "runtimeStats" : null
    } ],
    "asia/india/chennai" : [ {
      "fileId" : "",
      "path" : null,
      "prevCommit" : "null",
      "numWrites" : 0,
      "numDeletes" : 0,
      "numUpdateWrites" : 0,
      "numInserts" : 0,
      "totalWriteBytes" : 0,
      "totalWriteErrors" : 0,
      "tempPath" : null,
      "partitionPath" : null,
      "totalLogRecords" : 0,
      "totalLogFilesCompacted" : 0,
      "totalLogSizeCompacted" : 0,
      "totalUpdatedRecordsCompacted" : 0,
      "totalLogBlocks" : 0,
      "totalCorruptLogBlock" : 0,
      "totalRollbackBlocks" : 0,
      "fileSizeInBytes" : 0,
      "minEventTime" : null,
      "maxEventTime" : null,
      "runtimeStats" : null
    }, {
      "fileId" : "85eb99c7-9e85-4b3c-835a-6dcc85e26839-0",
      "path" : null,
      "prevCommit" : "20230210181058805",
      "numWrites" : 0,
      "numDeletes" : 0,
      "numUpdateWrites" : 13029,
      "numInserts" : 0,
      "totalWriteBytes" : 0,
      "totalWriteErrors" : 0,
      "tempPath" : null,
      "partitionPath" : null,
      "totalLogRecords" : 0,
      "totalLogFilesCompacted" : 0,
      "totalLogSizeCompacted" : 0,
      "totalUpdatedRecordsCompacted" : 0,
      "totalLogBlocks" : 0,
      "totalCorruptLogBlock" : 0,
      "totalRollbackBlocks" : 0,
      "fileSizeInBytes" : 0,
      "minEventTime" : null,
      "maxEventTime" : null,
      "runtimeStats" : null
    } ]
  },
  "compacted" : false,
  "extraMetadata" : { },
  "operationType" : "UPSERT"
}%
```

#### A COMMIT in an completed state
When a COMMIT is in a completed state, it’ll be have all the metadata information, including the schema: 

```sh
 .hoodie $ cat 20230210181040140.commit
{
  "partitionToWriteStats" : {
    "americas/brazil/sao_paulo" : [ {
      "fileId" : "384c94e2-293d-4d8e-ade7-b9b3be629f3b-0",
      "path" : "americas/brazil/sao_paulo/384c94e2-293d-4d8e-ade7-b9b3be629f3b-0_0-166-228_20230210181105202.parquet",
      "prevCommit" : "20230210181058805",
      "numWrites" : 33503,
      "numDeletes" : 0,
      "numUpdateWrites" : 13221,
      "numInserts" : 0,
      "totalWriteBytes" : 3431623,
      "totalWriteErrors" : 0,
      "tempPath" : null,
      "partitionPath" : "americas/brazil/sao_paulo",
      "totalLogRecords" : 0,
      "totalLogFilesCompacted" : 0,
      "totalLogSizeCompacted" : 0,
      "totalUpdatedRecordsCompacted" : 0,
      "totalLogBlocks" : 0,
      "totalCorruptLogBlock" : 0,
      "totalRollbackBlocks" : 0,
      "fileSizeInBytes" : 3431623,
      "minEventTime" : null,
      "maxEventTime" : null,
      "runtimeStats" : {
        "totalScanTime" : 0,
        "totalUpsertTime" : 1109,
        "totalCreateTime" : 0
      }
    } ],
    "americas/united_states/san_francisco" : [ {
      "fileId" : "c26ac037-4e5f-492f-bfb8-fbbd3bd79848-0",
      "path" : "americas/united_states/san_francisco/c26ac037-4e5f-492f-bfb8-fbbd3bd79848-0_1-166-229_20230210181105202.parquet",
      "prevCommit" : "20230210181058805",
      "numWrites" : 33299,
      "numDeletes" : 0,
      "numUpdateWrites" : 13145,
      "numInserts" : 0,
      "totalWriteBytes" : 3413828,
      "totalWriteErrors" : 0,
      "tempPath" : null,
      "partitionPath" : "americas/united_states/san_francisco",
      "totalLogRecords" : 0,
      "totalLogFilesCompacted" : 0,
      "totalLogSizeCompacted" : 0,
      "totalUpdatedRecordsCompacted" : 0,
      "totalLogBlocks" : 0,
      "totalCorruptLogBlock" : 0,
      "totalRollbackBlocks" : 0,
      "fileSizeInBytes" : 3413828,
      "minEventTime" : null,
      "maxEventTime" : null,
      "runtimeStats" : {
        "totalScanTime" : 0,
        "totalUpsertTime" : 1104,
        "totalCreateTime" : 0
      }
    } ],
    "asia/india/chennai" : [ {
      "fileId" : "85eb99c7-9e85-4b3c-835a-6dcc85e26839-0",
      "path" : "asia/india/chennai/85eb99c7-9e85-4b3c-835a-6dcc85e26839-0_2-166-230_20230210181105202.parquet",
      "prevCommit" : "20230210181058805",
      "numWrites" : 33198,
      "numDeletes" : 0,
      "numUpdateWrites" : 13029,
      "numInserts" : 0,
      "totalWriteBytes" : 3404928,
      "totalWriteErrors" : 0,
      "tempPath" : null,
      "partitionPath" : "asia/india/chennai",
      "totalLogRecords" : 0,
      "totalLogFilesCompacted" : 0,
      "totalLogSizeCompacted" : 0,
      "totalUpdatedRecordsCompacted" : 0,
      "totalLogBlocks" : 0,
      "totalCorruptLogBlock" : 0,
      "totalRollbackBlocks" : 0,
      "fileSizeInBytes" : 3404928,
      "minEventTime" : null,
      "maxEventTime" : null,
      "runtimeStats" : {
        "totalScanTime" : 0,
        "totalUpsertTime" : 1095,
        "totalCreateTime" : 0
      }
    } ]
  },
  "compacted" : false,
  "extraMetadata" : {
    "schema" : "{\"type\":\"record\",\"name\":\"hudi_trips_cow_record\",\"namespace\":\"hoodie.hudi_trips_cow\",\"fields\":[{\"name\":\"begin_lat\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"begin_lon\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"driver\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"end_lat\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"end_lon\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"fare\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"partitionpath\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"rider\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ts\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"uuid\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  },
  "operationType" : "UPSERT"
}%
```

## Archived Timeline 
The archived timeline directory is used to store successful and older instants that we don’t touch on the regular basis. This helps keep the active timeline with fresher data so we can easily access it with low latency. You can almost think of the archived timeline as a cold timeline storage for older instants. The older instants from the active timeline are batched and merged together into a log file and then sent to the archive timeline directory. The instants don't get modified as they moved to the archived timeline: the action, state, and instant time are the same as they were in the active timeline. The only difference between the active and archived timeline are the file formats.

The active timeline can be configured to contain a designated amount of files before they are transitioned to the archived timeline (see the configs below):

| Parameter Name | Default  | Description | Scope | Since Version                          |
|----------------|--------|----------|---------------|--------------------------------------|
| `hoodie.keep.min.commits` | 20 | This archiving service moves older entries from the active timeline into an archived log after each write. This keeps the metadata overhead constant, even as the table size grows. This property controls the minimum number of instants to retain in the active timeline.        | Archival         | 0.4.0                 |
| `hoodie.keep.max.commits` | 30 | This config controls the maximum number of instants to retain in the active timeline.  | Archival           | 0.4.0 |

