# Apache Hudi Technical Specification 1.0

| **Syntax** | **Description** |
| ---|-----------------|
| Last Updated | Aug 2026        |
| [Table Version](https://github.com/apache/hudi/blob/master/hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableVersion.java) | 9               |
| Reflects release | Hudi 1.2.0 (May 2026) |

:::note
This document specifies the current storage format — **table version 9**, as shipped through Hudi 1.2.0.
Table version 9 was introduced in Hudi 1.1.0 and remains current through the 1.x release line, layering
additions on top of the version 8 format that was introduced in Hudi 1.0.0. For the older format that
predates 1.0 see [here](/learn/tech-specs-0-point-x). Version 9 readers can correctly read any version-8 subset of a
table; version-8 readers cannot in general read version-9 features (new column types, V2 secondary index
encoding, Lance base files).
:::

Hudi brings database capabilities (tables, transactions, mutability, indexes, storage layouts) along with an incremental stream processing model (incremental merges, change streams, out-of-order data) 
over very large collections of files/objects, turning immutable cloud/file storage systems into transactional data lakes.

## Overview
This document is a specification for the Hudi's storage format along with protocols for correctly implementing readers and writers to accomplish the following goals.

*   **Unified Computation Model** - a unified way to combine large batch style operations and frequent near real time incremental operations over large datasets.
*   **Self-Optimizing Storage** - automatically handle all the table maintenance and optimizations such as compaction, clustering, cleaning asynchronously and in most cases non-blocking to actual data changes.
*   **Cloud Native Database** - abstracts Table/Schema from actual storage and ensures up-to-date metadata and indexes unlocking multi-fold read and write performance optimizations.
*   **Cross-Engine Compatibility** - designed to be neutral and compatible with different computation engines. Hudi will manage metadata, and provide common abstractions and 
    pluggable interfaces to most/all common compute/query engines.

This document is intended as reference guide for any compute engines, that aim to write/read Hudi tables, by interacting with the storage format directly.

## Storage Layout
Hudi organizes a table as a collection of files (objects in cloud storage) that can be spread across different paths on **_storage_** which can be local filesystem, distributed filesystem or object storage. 
These different paths that contain a table's files are called **_partitions_**. Some common ways of organizing files can be as follows

* **Hierarchical folder tree:** files are organized under a folder path structure like conventional filesystems, for ease of access and navigation. Hive-style partitioning is a special case of 
    this organization, where the folder path names indicate the field values that partition the data. However, note that, unlike Hive style partitioning, partition columns are not removed from 
    data files and partitioning is a mere organizational construct.

* **Cloud-optimized with random-prefixes**: files are distributed across different paths (of even varying depth) across cloud storage, to circumvent scaling/throttling issues that plague 
    cloud storage systems, at the cost of losing easy navigation of storage folder tree using standard UI/CLI tools.
  
* **Unpartitioned flat structure**: tables can also be totally unpartitioned, where a single folder contains all the files that constitute a table.

Metadata about the table is stored at a location on storage, referred to as **_basepath_**, which contains a special reserved _.hoodie_ directory under the base path 
is used to store transaction logs, metadata and indexes. A special file [`hoodie.properties`](http://hoodie.properties/) under basepath persists table level configurations, shared by writers 
and readers of the table. These configurations are explained [here](https://github.com/apache/hudi/blob/master/hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableConfig.java), and any config without a default value needs to be specified during table creation.

```plain
/data/hudi_trips/                         <-- Base Path
├── .hoodie/                              <-- Meta Path
|   └── hoodie.properties                 <-- Table Configs
│   └── metadata/                         <-- Metadata table (a Hudi MoR table itself)
|       └── files/                        <-- Partition-path -> file listing index
|       └── column_stats/                 <-- Per-column min/max/null-count statistics per file
|       └── partition_stats/              <-- Per-column statistics aggregated per partition
|       └── bloom_filters/                <-- Per-file bloom filters
|       └── record_index/                 <-- Record-key -> file group id index (RLI)
|       └── secondary_index_<name>/       <-- User-defined secondary indexes
|       └── expr_index_<name>/            <-- User-defined expression indexes
|   └── .index_defs/index.json            <-- Index definitions (path is configurable via hoodie.table.index.defs.path)
|   └── .temp/                            <-- Marker files for in-flight writes (see Appendix)
│   └── timeline/                         <-- Active timeline (per-instant files)
|       └── history/                      <-- LSM timeline history (compacted parquet)
├── americas/                             <-- Data stored as folder tree
│   ├── brazil/
│   │   └── sao_paulo                     <-- Partition Path 
│   │       ├── [data_files]
│   └── united_states/
│       └── san_francisco/
│           ├── [data_files]
└── asia/
    └── india/
        └── chennai/
            ├── [data_files]
```

Data files can be either **_base files_** or **_log files_**. Base files contain records stored in columnar or SST table like file formats depending on use-cases. 
Log files store deltas (partial or complete), deletes, change logs and other record level operations performed on the records in the base file. Data files are organized 
into logical concepts called **_file groups_**, uniquely identified by a **_file id_**. Each record in the table is identified by an unique key and mapped to a single file group 
at any given time. Within a file group, data files are further split into **_file slices_**, where each file slice contains an optional base file and a list of log files, that 
constitute the state of all records in the file group at a given time. These constructs allows Hudi to efficiently support incremental operations, as will be evident later.

## Timeline

Hudi stores all actions performed on a table into a Log Structured Merge ([LSM](https://en.wikipedia.org/wiki/Log-structured_merge-tree)) tree structure called the **_Timeline_**. 
Unlike typical LSM implementations, the memory component and the write-ahead-log are at once replaced by [avro](https://avro.apache.org/) serialized files containing individual 
actions (**_active timeline_**) for high durability and inter-process co-ordination. Every action on the Hudi table creates a new entry (**_instant_**) in the active timeline and periodically, 
actions move from the active timeline to the LSM structure. Each new actions and state changes need to be atomically published to the timeline (see Appendix for storage specific guidelines to achieve that).

### Time

Each action on the timeline is stamped with a time at which it began and completed. The notion of time can be logical or physical timestamps, but it's required that each timestamp 
generated by a process is monotonically increasing with respect to the previous time generated by the same or another process. This requirement can be satisfied by implementing 
a [TrueTime](https://research.google/pubs/pub45855/) generator with an external time generator or rely on system epoch times with assumed bounds on clock skews. See appendix for more.

### Actions

Actions have a plan (optional) and completion metadata associated with them that capture how the action alters the table state. The metadata is serialized as Avro, and the schema for 
each of these actions is described in avro [here](https://github.com/apache/hudi/tree/master/hudi-common/src/main/avro). The following are the key actions on the Hudi timeline.

| **Action type** | **Description** | **Action Metadata** |
| ---| ---| --- |
| commit | A write operation that produces new base files containing either new records or modified values of existing records. | HoodieCommitMetadata |
| deltacommit | A write operation that produces new log files contains either new records or deltas/deletes for existing records. Optionally, it can also produce new log files. | HoodieCommitMetadata |
| replacecommit | A write operation that replaces a set of file groups with another atomically. It can be used to cluster the data for better query performance or rewrite files to enforce time-to-live. The requirement for this action is that the table state before and after are logically equivalent. | HoodieRequestedReplaceMetadata  HoodieReplaceCommitMetadata |
| clean | Management activity that cleans up older versions of data files that no longer will be accessed, to keep the storage size of the table in check. | HoodieCleanerPlan  HoodieCleanMetadata |
| compaction | Management activity that applies deltas/deletes from a set of log files to records on a base file and produces new base files. This amortizes the update costs, re-optimizes the file group to ultimately improve query performance. | HoodieCompactionOperation  HoodieCompactionMetadata |
| logcompaction | Management activity that consolidates a set of (typically smaller) log files into another log file within the same file group, to improve query performance between compaction actions. | HoodieCompactionOperation  HoodieCompactionMetadata |
| indexing | Management activity to build a new index from the table state. This action is used to update/build the index asynchronously even as write actions above happen on the table. | HoodieIndexPlan  HoodieIndexCommitMetadata |
| rollback | Denotes that the changes made by the corresponding commit/delta commit were unsuccessful & hence rolled back, removing any partial files produced during such a write. | HoodieRollbackPlan  HoodieRollbackMetadata |
| savepoint | Savepoint is a special marker action to ensure a particular commit is not automatically cleaned. It helps restore the table to a point on the timeline, in case of disaster/data recovery scenarios | HoodieSavepointMetadata |
| restore | Restore denotes that the table was restored to a particular savepoint, physically removing data written after that savepoint. | HoodieRestorePlan  HoodieRestoreMetadata |

### States

An action can be in any one of the following states on the active timeline.

| **State** | **Description** |
| ---| --- |
| requested | begin time for the action is generated and the action is requested along with any metadata to "plan" the action.  Stored in the active timeline as  **_\[begin\_instant\_time\].\[action\_type\].\[state\]_** |
| inflight | A process has attempted to execute a requested action. Note that this process could be still executing or failed midway. Actions are idempotent, and could fail many times in this state.  Stored in the active timeline as  **_\[begin\_instant\_time\].\[action\_type\].\[state\]_** |
| completed | completion time is generated and the action has completed successfully by publishing a file with both begin and completion time on the timeline. |

All the actions in requested/inflight states are stored in the active timeline as files named \[**_begin\_instant\_time\].\[action\_type\].\[requested|inflight\]_**. Completed actions are stored along 
with a time that denotes when the action was completed, in a file named \[**_begin\_instant\_time\]\_\[completion\_instant\_time\].\[action\_type\]._**

### LSM Timeline
Completed actions, their plans and completion metadata are stored in a more scalable LSM tree based timeline organized in an **_timeline/history_** storage location under the .hoodie metadata path. 
It consists of Apache Parquet files with action instant data and bookkeeping metadata files, in the following manner.

```bash
/.hoodie/timeline/history 					
├── _version_                               <-- stores the manifest version that is current
├── manifest_1                              <-- manifests store list of files in timeline
├── manifest_2                              <-- compactions, cleaning, writes produce new manifest files
├── ...                                      
├── manifest_[N]                            <-- there can be many manifest files at any given time
├── [min_time]_[max_time]_[level].parquet   <-- files storing actual action details
```

The schema of the individual files are as follows.

| **File type** | **File naming** | **Description** |
| ---| ---| --- |
| version | \_version\_ | UTF-8 encoded string representing the manifest version to read. Version file should be atomically updated on storage when new manifests are created. |
| manifests | manifest\_\[N\] | Contains a json string, with a list of file name and file length for all LSM files part of the timeline. |
| LSM files | \[min\_time\]\_\[max\_time\]\_\[level\].parquet | where :  **_min\_time_** is the minimum begin time of all actions in the file  **_max\_time_** is the maximum completion time of all actions in the file.  **_level_** is an integer starting from 0, indicating the level in the LSM tree.   |

The actual parquet file [schema](https://github.com/apache/hudi/blob/master/hudi-common/src/main/avro/HoodieLSMTimelineInstant.avsc) is:

| **Field name** | **Type** | **Description** |
| ---| ---| --- |
| instantTime | string | the begin time of the action |
| completionTime | string | the completion time of the action |
| action | string | the action type |
| metadata | bytes | json string representing the completed metadata of the action |
| plan | bytes | Optional, avro serialized plan for the action, same as its requested/plan metadata |

## Table Types

Hudi storage format supports two table types offering different trade-offs between write and query performance (see appendix for more details) and records are stored differently based on the chosen table type.

| **Table Type** | **Description** |
| ---| --- |
| Copy-on-Write (CoW) | Data is stored entirely in base files, optimized for read performance and ideal for slow changing datasets. Any updates, inserts, deletes accordingly produce new base files for each write operation. Change data is still stored as log files associated with the base files. |
| Merge-on-Read (MoR) | Data is stored in a combination of base and log files, optimized to [Balancing Write and Read Performance](#balancing-write-and-read-performance) and ideal for frequently changing datasets |

Readers need to then satisfy different query types on these tables.

*   **Snapshot queries** - query the table for latest committed state of each record.
*   **Time-Travel queries** - snapshot query performed as of a point in timeline or equivalent on an older snapshot of the table.
*   **Incremental queries** - obtain the latest merged state of all records that have been updated/inserted between two points in the timeline.
*   **Change Data Capture** - obtain a change log of all modifications (updates, inserts, deletes) with before/after images for each record, between two points in the timeline.

## Data Files

Data Files have naming conventions that allows to easily track history of files with respect to the timeline as well as serving practical operational needs. 
For e.g it's quite possible to recover the table to a known good state, when operational accidents cause the timeline and other metadata are deleted.

### Base Files

Base files are standard well-known file formats like Apache Parquet, Apache Orc and Apache HBase's HFile, named as \[**_file\_id\]\_\[write\_token\]\_\[requested\_time\].\[extension\]._**

where,

*   **file\_id** \- Id of the file group that the base file belongs to.
*   **write\_token** - Monotonically increasing token for every attempt to write the base file within a given transaction. This helps uniquely identify the base file when there are
    failures and retries. Cleaning can remove partial/uncommitted base files by comparing with the successful write token.
*   **requested\_time** - Time when this action was requested on the timeline.
*   **extension** - base file extension matching the file format such as .parquet, .orc, .hfile, .lance.

Note that a single file group can contain base files with different extensions when `hoodie.table.multiple.base.file.formats.enable` is set on the table.

**Base file formats supported in table version 9**: Apache Parquet (`.parquet`), Apache ORC (`.orc`), Hudi's own HFile (`.hfile`),
and [Lance](https://lancedb.com/) (`.lance`). Lance is currently supported for base file reads and Copy-on-Write writes only;
column stats and partition stats are not populated for Lance base files (readers should not rely on these indexes for Lance-backed
file groups).

### Types

The user table schema is expressed in Avro. In addition to the primitive Avro types and the standard Avro logical types
(`decimal`, `date`, `time-millis`, `time-micros`, `timestamp-millis`, `timestamp-micros`, `local-timestamp-*`, `uuid`),
table version 9 recognizes the following Hudi-specific logical types. Each is identified by the `logicalType` attribute
on the Avro schema and layered on a physical Avro type.

| Type       | Physical Avro type | `logicalType` name | Description                                                                                                                                                     |
|------------|--------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| VARIANT    | `record`           | `variant`          | Unshredded VARIANT — a self-describing value that carries its own type metadata. Encoded as a two-field record (`metadata`, `value`) of `bytes` per the Spark VARIANT spec. |
| VECTOR     | `fixed`            | `vector`           | Fixed-length dense vector of numeric elements (int8/float32/float64), physically stored as a `fixed`-sized byte buffer plus schema properties describing element type and dimensionality. |
| BLOB       | `record`           | `blob`             | Binary large object — either an inline byte payload or a reference to a range within an external file. Controlled at write time by `blob.inline.mode` (`CONTENT` vs `REFERENCE`). |

These types are transparent to writers/readers that do not understand them (they see the underlying physical Avro type),
but Hudi's V1 column-stats and bloom-filter writers deliberately skip columns of these types since min/max bounds are not
meaningful for them.

### Log Files

The log files contain different type of blocks, that encode delta updates, deletes or change logs against the base file. They are named with the convention

**_.\[file\_id\]\_\[requested\_instant\_time\].log.\[version\]\_\[write\_token\]_**.



*   **file\_id** - File Id of the base file in the slice
*   **requested\_instant\_time** \- Time at which the write operation that produced this log file was requested.
*   **version** - Current version of the log file format, to order deltas against the base file.
*   **write\_token** - Monotonically increasing token for every attempt to write the log file. This should help uniquely identifying the log file when there are failures and retries. 
    Cleaner can clean-up partial log files if the write token is not the latest in the file slice.

### Log Format

The Log file format structure is a Hudi native format. The actual content bytes are serialized into one of Apache Avro, Apache Parquet or Apache HFile file formats based 
on configuration and the other metadata in the block is serialized using primitive types and byte arrays.

Hudi Log format specification is as follows.

| Section | #Bytes | Description |
| ---| ---| --- |
| **magic** | 6 | 6 Characters '#HUDI#' stored as a byte array. Sanity check for block corruption to assert start 6 bytes matches the magic byte\[\]. |
| **LogBlock length** | 8 | Length of the block excluding the magic. |
| **version** | 4 | Version of the Log file format, monotonically increasing to support backwards compatibility |
| **type** | 4 | Represents the type of the log block. Id of the type is serialized as an Integer. |
| **header length** | 8 | Length of the header section to follow |
| **header** | variable | Custom serialized map of header metadata entries. 4 bytes of map size that denotes number of entries, then for each entry 4 bytes of metadata type, followed by length/bytearray of variable length utf-8 string. |
| **content length** | 8 | Length of the actual content serialized |
| **content** | variable | The content contains the serialized records in one of the supported file formats (Apache Avro, Apache Parquet or Apache HFile) |
| **footer length** | 8 | Length of the footer section to follow |
| **footer** | variable | Similar to Header. Map of footer metadata entries. |
| **total block length** | 8 | Total size of the block including the magic bytes. This is used to determine if a block is corrupt by comparing to the block size in the header. Each log block assumes that the block size will be last data written in a block. Any data if written after is just ignored. |

### Versioning

Log file format versioning refers to a set of feature flags associated with a log format. The current version is `1`. Versions are changed when the log format changes.
The feature flags determine the behavior of the log file reader. The following are the feature flags associated with the log file format.

| Flag              | Version 0 | Version 1 | Default |
|-------------------|-----------|-----------|---------|
| hasMagicHeader    | True      | True      | True    |
| hasContent        | True      | True      | True    |
| hasContentLength  | True      | True      | True    |
| hasOrdinal        | True      | True      | True    |
| hasHeader         | False     | True      | True    |
| hasFooter         | False     | True      | False   |
| hasLogBlockLength | False     | True      | False   |


### Headers

Metadata key mapping from Integer to actual metadata is as follows:

| Header Metadata                                  | Encoding ID | Description                                                                                                                                                                                                                                              |
|--------------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| INSTANT\_TIME                                    | 0           | Instant time corresponding to commit when the log block is added.                                                                                                                                                                                        |
| TARGET\_INSTANT\_TIME                            | 1           | Target instant to rollback, used in rollback command log block.                                                                                                                                                                                          |
| SCHEMA                                           | 2           | Schema corresponding to data in the data block.                                                                                                                                                                                                          |
| COMMAND\_BLOCK\_TYPE                             | 3           | Command block type for the command block. Currently, `ROLLBACK_BLOCK` is the only command block type.                                                                                                                                                    |
| COMPACTED\_BLOCK\_TIMES                          | 4           | Instant times corresponding to log blocks compacted due to minor log compaction.                                                                                                                                                                         |
| RECORD\_POSITIONS                                | 5           | Record positions of the records in the base file that were updated (data block) or deleted (delete block). Record position is a long type, however, the list of record positions is Base64-encoded bytes of a serialized `Roaring64NavigableMap` bitmap. |
| BLOCK\_IDENTIFIER                                | 6           | Block sequence number used to detect duplicate log blocks (e.g. due to task retries).                                                                                                                                                                    |
| IS\_PARTIAL                                      | 7           | Boolean indicating whether the data block encodes partial updates.                                                                                                                                                                                       |
| BASE\_FILE\_INSTANT\_TIME\_OF\_RECORD\_POSITIONS | 8           | Begin (requested) instant time of the base file that the `RECORD_POSITIONS` bitmap is relative to. Used by the reader to validate that positions still refer to the same base file when reconstructing a file slice.                                     |


### Block types

The following are the possible block types used in Hudi Log Format:

### Command Block (Id: 0)

Encodes a command to the log reader. A Command block has a 0-byte content section and only populates the `COMMAND_BLOCK_TYPE` header.
The only command type currently defined is `ROLLBACK_BLOCK` (ordinal 0), which instructs the reader to undo the previous block written in the log file.
This denotes that the previous action that wrote the log block was unsuccessful.


### Delete Block (Id: 1)

| Section | #bytes | Description |
| ---| ---| --- |
| format version | 4 | version of the log file format |
| length | 8 | length of the deleted keys section to follow |
| deleted keys | variable | Tombstone of the record to encode a delete. The following 3 fields are serialized using the KryoSerializer. **Record Key** - Unique record key within the partition to deleted **Partition Path** - Partition path of the record deleted **Ordering Value** - In a particular batch of updates, the delete block is always written after the data (Avro/HFile/Parquet) block. This field would preserve the ordering of deletes and inserts within the same batch. |


### Corrupted Block (Id: 2)

This block type is never written to persistent storage. While reading a log block, if the block is corrupted, then the reader gets an instance of the
Corrupted Block instead of a Data block. It is a reserved ID for handling cases of corrupt or partially written blocks, for example on HDFS.

### Avro Block (Id: 3)

Data block serializes the actual records written into the log file

| Section | #bytes | Description |
| ---| ---| --- |
| format version | 4 | version of the log file format |
| record count | 4 | total number of records in this block |
| record length | 8 | length of the record content to follow |
| record content | variable | Record represented as an Avro record serialized using BinaryEncoder |


### HFile Block (Id: 4)

The HFile data block serializes the records using the HFile file format. The HFile data model is a key-value pair and both are encoded as byte arrays. The Hudi record key is encoded
as an Avro string and the Avro record serialized using `BinaryEncoder` is stored as the value. HFile stores records in sorted order with an internal index, enabling quick point reads and range scans.

### Parquet Block (Id: 5)

The Parquet Block serializes the records using the Apache Parquet file format. The serialization layout is similar to the Avro block except that the byte-array content is encoded in
columnar Parquet format. This log block type enables efficient columnar scans and better compression. Different data block types offer different trade-offs; picking the right block based on the workload is critical for merge and read performance.

### CDC Block (Id: 6)

The CDC block is used for change data capture and it encodes the before and after image of the record as an Avro data block as follows:

| Field              | Description                                              |
|--------------------|----------------------------------------------------------|
| HoodieCDCOperation | Type of the CDC operation. Can be INSERT, UPSERT, DELETE |
| recordKey          | Key of the record being changed                          |
| oldRecord          | This is the before image                                 |
| newRecord          | This is the after image                                  |

## Table Properties

As mentioned in the [storage layout](#storage-layout) section, the table properties are stored in the `hoodie.properties` file under the `.hoodie` directory in the table base path.
Below is the list of most commonly persisted properties that are stored in this file.

| Table Config                                       | Description                                                                                                                                               |
|----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.database.name                               | Database name under which tables will be created.                                                                                                         |
| hoodie.table.name                                  | Table name.                                                                                                                                               |
| hoodie.table.type                                  | Table type - `COPY_ON_WRITE` or `MERGE_ON_READ`.                                                                                                          |
| hoodie.table.version                               | Current table format version. Drives upgrade/downgrade behavior and the set of supported features.                                                        |
| hoodie.table.initial.version                       | Table format version at table creation time. Useful for diagnosing upgrade history.                                                                       |
| hoodie.table.create.schema                         | Schema (Avro JSON) the table was originally created with. Used as a fallback when no commit yet exists on the timeline.                                   |
| hoodie.table.recordkey.fields                      | Comma-separated list of fields used for record keys. Optional.                                                                                            |
| hoodie.table.partition.fields                      | Comma-separated list of fields used for partitioning the table. Optional.                                                                                 |
| hoodie.table.ordering.fields                       | Comma-separated list of fields used to break ties when two records have the same value for the record key. Optional. Replaces the legacy single `hoodie.table.precombine.field`. |
| hoodie.table.base.file.format                      | Base file format used by the table (e.g. `PARQUET`, `ORC`, `HFILE`).                                                                                      |
| hoodie.table.log.file.format                       | Log file format used by the table.                                                                                                                        |
| hoodie.table.cdc.enabled                           | Whether change-data-capture is enabled on the table.                                                                                                      |
| hoodie.table.cdc.supplemental.logging.mode         | Supplemental logging mode for CDC (`OP_KEY_ONLY`, `DATA_BEFORE`, `DATA_BEFORE_AFTER`) when CDC is enabled.                                                |
| hoodie.table.partial.update.mode                   | Whether partial updates are accepted, and if so how they are merged (`NONE`, `IGNORE_DEFAULTS`, `FILL_DEFAULTS`).                                         |
| hoodie.table.metadata.partitions                   | Comma-separated list of metadata table partitions that readers may use (e.g. `files`, `column_stats`, `partition_stats`, `bloom_filters`, `record_index`, `secondary_index_<name>`). |
| hoodie.table.metadata.partitions.inflight          | Metadata partitions currently being built (visible but not yet ready for query-time use).                                                                 |
| hoodie.table.index.defs.path                       | Relative path under the base path where index definitions are stored (default: `.hoodie/.index_defs/index.json`). Optional.                               |
| hoodie.table.timeline.path                         | Relative path under `.hoodie` for the active timeline (default: `timeline`).                                                                              |
| hoodie.table.timeline.history.path                 | Relative path under `.hoodie` for the LSM timeline history (default: `timeline/history`). Note: prior to table version 8 this was `archived`.             |
| hoodie.table.archivelog.folder                     | Legacy archived timeline folder, retained for compatibility with older table versions.                                                                    |
| hoodie.table.timeline.timezone                     | Timezone (`UTC` or `LOCAL`) used to interpret physical instant timestamps generated by writers.                                                           |
| hoodie.timeline.layout.version                     | Active timeline layout version. Table version 8 uses layout version 2.                                                                                    |
| hoodie.table.checksum                              | Auto-generated CRC checksum used to detect partial/corrupt writes of `hoodie.properties` itself.                                                          |
| hoodie.datasource.write.slash.separated.date.partitioning | Optional flag (advanced). When `true` on tables with a single date-typed partition field, partition paths use `/` as the separator (`2026/06/28`) instead of `-` (`2026-06-28`). |

The record key, ordering and partition fields are optional but play an important role in modeling data stored in Hudi
table.

| Field               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Partitioning key(s) | Value of this field defines the directory hierarchy within the table base path. This essentially provides an hierarchy isolation for managing data and related metadata                                                                                                                                                                                                                                                                                                                                      |
| Record key(s)       | Record keys uniquely identify a record within each partition.                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Ordering field(s)   | Hudi guarantees the uniqueness constraint of record key and the conflict resolution configuration manages strategies on how to disambiguate when multiple records with the same keys are to be merged into the table. The resolution logic can be based on an ordering field or can be custom, specific to the table. To ensure consistent behaviour dealing with duplicate records, the resolution logic should be commutative, associative and idempotent. This is also referred to as ‘precombine field’. |

## Metadata

Hudi automatically extracts the physical data statistics and stores the metadata along with the data to improve write and query performance. Hudi Metadata is an internally-managed
table which organizes the table metadata under the base path `.hoodie/metadata`. The metadata is itself a Hudi table, organized with the Hudi merge-on-read storage format.
Every record stored in the metadata table is a Hudi record and hence has a partition path and record key. The full Avro schema for every metadata-table payload type is defined in
[`HoodieMetadata.avsc`](https://github.com/apache/hudi/blob/master/hudi-common/src/main/avro/HoodieMetadata.avsc); the per-partition payload variants are summarized below.

*   **files** - Partition-path-to-file-name index. The record key is the partition path and the payload is a map of file name to `HoodieMetadataFileInfo`.
    Additionally, a special key `__all_partitions__` holds the list of all partitions.
*   **column\_stats** - Per-column min/max/null-count and additional statistics per file. Payload is `HoodieMetadataColumnStats`.
*   **partition\_stats** - Per-column statistics aggregated to the partition level. Payload reuses `HoodieMetadataColumnStats`.
*   **bloom\_filters** - Per-file bloom filters for accelerating presence checks. Payload is `HoodieMetadataBloomFilter`.
*   **record\_index** - Record-key-to-file-group-id mapping (RLI). Payload is `HoodieRecordIndexInfo`.
*   **secondary\_index\_&lt;name&gt;** - User-defined secondary indexes on non-key columns. Payload is `HoodieSecondaryIndexInfo`.
*   **expr\_index\_&lt;name&gt;** - Expression indexes (indexes on a function of a column). Payload depends on the function (column-stats-shaped or bloom-filter-shaped).

Apache Hudi employs the HFile format to store metadata-table base files, to ensure high performance for point lookups, though different implementations are free to choose their own
base file format.

### File Listings

File listing is an index of partition path to file name stored under the partition `files` in the metadata table. 
The files index can be used to do file listing and do filter based pruning of the scanset during query.
Key for the Hudi record is the partition path and the actual record is a map of file name to an instance of `HoodieMetadataFileInfo` that encodes file group ID and instant time along with file metadata. 
Additionally, a special key `__all_partitions__` holds the list of all partitions.

### Column Statistics

Column statistics are stored under the partition `column_stats` in the metadata table. The record key is derived as
`str_concat(hash(column name), str_concat(hash(partition name), hash(file name)))` and the payload is `HoodieMetadataColumnStats`
with the following fields:

| Field                  | Description                                                                                                                                                                                  |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fileName               | Name of the file these statistics apply to.                                                                                                                                                  |
| columnName             | Name of the column these statistics apply to.                                                                                                                                                |
| minValue               | Minimum value in the range, wrapped in a typed Avro record (`BooleanWrapper`, `IntWrapper`, `LongWrapper`, `FloatWrapper`, `DoubleWrapper`, `StringWrapper`, `BytesWrapper`, `DateWrapper`, `DecimalWrapper`, `TimeMicrosWrapper`, `TimestampMicrosWrapper`). |
| maxValue               | Maximum value in the range, using the same wrapper types as `minValue`.                                                                                                                      |
| valueCount             | Total count of values (including nulls) covered by this entry.                                                                                                                               |
| nullCount              | Total count of null values.                                                                                                                                                                  |
| totalSize              | Total compressed/on-disk storage size in bytes.                                                                                                                                              |
| totalUncompressedSize  | Total uncompressed storage size in bytes.                                                                                                                                                    |
| isDeleted              | Tombstone flag: when `true`, this entry has been logically deleted.                                                                                                                          |
| isTightBound           | When `true`, `minValue`/`maxValue` are guaranteed to be a tight bound on the values present in the file. With updates/deletes the range can become wider than the actual values; compaction and clustering can re-tighten it. Defaults to `false`. |
| valueType              | Optional `HoodieValueTypeInfo` record (`typeOrdinal` + optional `additionalInfo` string) describing the logical type of the value, allowing readers to round-trip wider logical types than the wrapper set above. |

Note: this is the V1 column-stats schema, which is what table version 8 writes. A V2 column-stats encoding exists in the
code base but is gated behind table version 9 and is out of scope for this document. Logical types that cannot be ordered
in a stable cross-format way (e.g. VARIANT, BLOB, VECTOR — all added in 1.1) are skipped by the V1 writer.

### Partition Statistics

Partition statistics are stored under the partition `partition_stats` in the metadata table. They are structurally identical
to column statistics but aggregated to the partition level rather than per-file, so the payload reuses the
`HoodieMetadataColumnStats` schema with `fileName` set to a partition-level identifier. This index is used by query engines
to skip entire partitions during pruning before even consulting per-file statistics, and is especially impactful for tables
with many small files per partition. Partition-stats entries are not written for columns whose logical type does not produce
meaningful min/max bounds.

### Meta Fields

In addition to the fields specified by the table's schema, the following meta fields are added to each record, to unlock incremental processing and ease of debugging. These meta fields are part of the table schema and
stored with the actual record to avoid re-computation.


| Hudi meta-fields | Description |
| ---| --- |
| \_hoodie\_commit\_time | This field contains the commit timestamp in the timeline that created this record. This enables granular, record-level history tracking on the table, much like database change-data-capture. |
| \_hoodie\_commit\_seqno | This field contains a unique sequence number for each record within each transaction. This serves much like offsets in Apache Kafka topics, to enable generating streams out of tables. |
| \_hoodie\_record\_key | Unique record key identifying the record within the partition. Key is materialized to avoid changes to key field(s) resulting in violating unique constraints maintained within a table. |
| \_hoodie\_partition\_path | Partition path under which the record is organized into. |
| \_hoodie\_file\_name | The data file name this record belongs to. |


Within a given file, all records share the same values for `_hoodie_partition_path` and `_hoodie_file_name`, thus easily compressed away without any overheads with columnar file formats. 
The other fields can also be optional for writers depending on whether protection against key field changes or incremental processing is desired. More on how to populate these fields in the sections below.

## Indexes

### Naming
Indexes are stored under `.hoodie/metadata` storage path, with separate partitions of the  form `<index_type>_<index_name>`.

### Index Definitions

Every index carries a definition, serialized to JSON under the path in `hoodie.table.index.defs.path`
(default `.hoodie/.index_defs/index.json`). All definitions for a table live in a single file, keyed by the
metadata-table partition name:

```json
{
  "indexDefinitions": {
    "<partition_name>": {
      "indexName": "<partition_name>",
      "indexType": "<index_type>",
      "indexFunction": "<index_function>",
      "version": "<index_version>",
      "sourceFields": ["<column_1>", "<column_2>"],
      "indexOptions": {}
    }
  }
}
```

*   `indexType` is one of `files`, `column_stats`, `partition_stats`, `bloom_filters`, `record_index`,
    `secondary_index` or `expr_index`.
*   `indexFunction` carries the transform for an expression index, and defaults to `identity` when an expression index
    is created without an explicit function. For every other index type the field is not meaningful, and the value it
    ends up with depends on the code path that registered the definition: empty when the built-in initialisation path
    registers it, `identity` for a secondary index created through SQL `CREATE INDEX`, and the partition name for the
    column-stats registration path. Use `indexType` to identify an index, not this field.
*   `sourceFields` are the data-table columns the index is derived from. Metadata columns are permitted.
*   `indexOptions` carries index-type-specific options, such as the expression-index function arguments.
*   `version` is the storage-layout version of the index, described below.

#### Index Versions

`version` holds a `HoodieIndexVersion`, an index-level attribute introduced in table version 9. It allows the physical
layout of an index to change without forcing a table-version upgrade or downgrade, so that readers and writers built
against different releases agree on how to interpret a given index partition. Values take the form `V1`, `V2` and so on.

A table version 8 definition may omit `version` entirely. From table version 9 onward a definition without a version is
rejected as invalid, and secondary indexes on a table still at version 8 must be `V1`.

Which version a newly created index gets depends on the index type and the table version:

| Index type | Table version 8 | Table version 9 |
|---|---|---|
| `secondary_index` | `V1` | **`V2`** |
| `column_stats` | `V1` | **`V2`** |
| `partition_stats` | `V1` | **`V2`** |
| `expr_index` | `V1` | **`V2`** |
| `record_index` | `V1` | `V1` |
| `bloom_filters` | `V1` | `V1` |
| `files` | `V1` | `V1` |

Only the layout changes; the logical contents of an index are unaffected by its version.

#### Table Upgrade and Downgrade

Index versions are reconciled when the table version changes.

*   **Upgrading from table version 8 to 9** does not rebuild or drop any index. Definitions that carry no `version` are
    stamped `V1`, so existing indexes keep their on-disk layout and continue to be read as `V1`. Indexes created after
    the upgrade pick up the table version 9 defaults above.
*   **Downgrading from table version 9 to 8** drops every metadata partition whose index version is newer than `V1`,
    because table version 8 readers cannot interpret those layouts. Additionally, if a `V2` `column_stats` partition is
    dropped, `partition_stats` is dropped with it, since partition stats may have no definition of their own to inspect.

Dropped indexes have to be rebuilt after the downgrade.

### Bloom Filter Index

The bloom filter index is used to accelerate 'presence checks' — validating whether a particular record is present in a file — which is used during merging, hash-based joins, point-lookup queries, etc.
The bloom filter index is stored in the Hudi metadata table under the partition `bloom_filters`.
The Hudi record key is `str_concat(hash(partition name), hash(file name))` and the payload is `HoodieMetadataBloomFilter` with the following schema:

| Field       | Description                                                                                                                                                                                    |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type        | Bloom filter type. Either `SIMPLE` (fixed-size filter based on a configured number of entries) or `DYNAMIC` (auto-resized as the key count grows).                                             |
| bloomFilter | Serialized byte array of the bloom filter content.                                                                                                                                             |
| timestamp   | Instant timestamp at which this bloom filter metadata record was created/updated.                                                                                                              |
| isDeleted   | Tombstone flag: when `true`, this bloom filter entry has been logically deleted.                                                                                                               |

### Record Index

The record index (RLI) maps individual record keys to the file group that currently holds the record.
It dramatically boosts write efficiency (faster index tagging) and improves read efficiency for keyed lookups.
The record index is stored in the Hudi metadata table under the partition `record_index`. The Hudi record key is the data-table record key
and the payload is `HoodieRecordIndexInfo` with the following schema:

| Field          | Description                                                                                                                                                                                                                                          |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| partitionName  | Data-table partition the record belongs to.                                                                                                                                                                                                          |
| fileIdHighBits | High 64 bits when the fileId is in UUID form. A UUID-based fileId is stored as 3 pieces (`fileIdHighBits`, `fileIdLowBits`, `fileIndex`). FileID format is `{UUID}-{fileIndex}`.                                                                     |
| fileIdLowBits  | Low 64 bits when the fileId is in UUID form.                                                                                                                                                                                                         |
| fileIndex      | Integer index used (together with the UUID halves above) to reconstruct a UUID-based fileId. Applicable when the fileId is in UUID form.                                                                                                             |
| fileIdEncoding | FileId encoding. `0` = UUID-based fileId (reader reconstructs from `fileIdHighBits`, `fileIdLowBits`, `fileIndex`); `1` = raw string fileId (read from `fileId`).                                                                                    |
| fileId         | Raw string fileId of the file group the record belongs to. Populated when `fileIdEncoding = 1`.                                                                                                                                                      |
| instantTime    | Long: epoch milliseconds for the commit time at which the record was added.                                                                                                                                                                          |
| position       | Optional row position of the record within the file group. When present, callers can perform positional lookups instead of full-record scans.                                                                                                        |

The RLI may be organized in either a non-partitioned or **partitioned** layout. In the partitioned layout the data-table partition
name is encoded into the metadata-table file id (`record-index-<escaped-partition>-<index>-0`), so the metadata-table file group
the record lives in is implied by the data-table partition. Readers detect which layout is in use by inspecting the file id
shape (`HoodieTableMetadataUtil#verifyRLIFile`) and use the partition prefix when sharding keys to file groups.

### Secondary Index

Just like in databases, a secondary index is a way to accelerate queries by columns other than the record (primary) key.
Hudi supports near-standard [SQL syntax](/docs/sql_ddl#create-index) for creating/dropping indexes on different columns
via Spark SQL, along with an asynchronous indexing table service that builds indexes without interrupting writers.

A secondary index definition is serialized to JSON and saved at the path specified by `hoodie.table.index.defs.path`
(see [Index Definitions](#index-definitions) for the on-disk shape).
The index itself is stored in the Hudi metadata table under the partition `secondary_index_<index_name>`. As with other
metadata partitions the entry is a key/value, but the encoding is a little more nuanced.

**Key** is the encoded pair of the **secondary column value** and the **primary (record) key**. The encoding ensures:

1. **Uniqueness**: each composite key is distinct.
2. **Safety**: occurrences of the delimiter or escape character within the data itself are handled correctly to avoid ambiguity.
3. **Efficiency**: encoding and decoding are constant-time per character and preserve order.

Format:
```
<escaped-secondary-key>$<escaped-primary-key>
```
Where:
  - `$` is the delimiter separating the secondary key and primary key.
  - Backslash (`\`) is the escape character.
  - Any occurrence of `$`, `\` or the null character (`\0`) inside the secondary key or primary key is prefixed with `\`.

**Value** contains metadata about the record — specifically, an `isDeleted` flag (`HoodieSecondaryIndexInfo`) indicating whether the entry is valid or has been logically deleted.

For example, a secondary index on the `city` column, for a record with `city = Chennai` and `id = id1`, produces:
```
chennai$id1 -> {"isDeleted": false}
```

**Partitioning** decides which file group of the index partition an entry is written to. Hudi hashes a portion of the
record key and takes that value modulo the number of file groups. Which portion is hashed is governed by the
[`HoodieIndexVersion`](#index-versions) recorded on the index's `HoodieIndexDefinition`:

*   **`V1`**, the default for table version 8, hashes the whole `<escaped-secondary-key>$<escaped-primary-key>` key.
    Entries sharing a secondary value are distributed across all file groups, so resolving a secondary value to its
    records reads every file group unless the primary key is already known.
*   **`V2`**, the default for table version 9, hashes only the leading `<escaped-secondary-key>$` portion. All entries
    sharing a secondary value therefore reside in one file group, and a lookup by secondary value alone reads that
    single file group.

The strategy is selected per partition from its recorded version, so `V1` partitions on an upgraded table continue to be
read as `V1` while indexes created afterwards on the same table use `V2`.

#### Limitations

*   A secondary index may be defined on **exactly one column**. Attempting more fails with
    `Only one column can be indexed for functional or secondary index.`
*   The indexed column must resolve to one of the schema types `string`, `int`, `long`, `float`, `double`, `date`,
    `time`, or a **UTC-adjusted** `timestamp`. In Spark SQL terms that covers `string`, `tinyint`, `smallint`, `int`,
    `bigint`, `float`, `double`, `date` and `timestamp`, since `tinyint` and `smallint` are represented as `int`.
    Rejected are `decimal`, `boolean`, `binary`, the nested types `array`, `map` and `struct`, and local
    (non-UTC-adjusted) timestamps. A nullable column is supported when its non-null branch is a supported type.
*   The **type of an indexed column cannot be changed** while the index exists. Both a SQL
    `ALTER TABLE ... ALTER COLUMN ... TYPE ...` and a write that evolves the column through schema-on-read fail with
    `Column '<column>' has secondary index '<index>' and cannot evolve from schema '<old>' to '<new>'`. Drop the index,
    change the type, then rebuild the index. Changing only a column's nullability is permitted and does not require
    dropping the index.

### Expression Indexes

A [expression index](https://github.com/apache/hudi/blob/00ece7bce0a4a8d0019721a28049723821e01842/rfc/rfc-63/rfc-63.md) is an index on a function of a column.
Hudi supports creating expression indexes for certain unary string and timestamp functions supported.
The index definition specified by the user is serialized to JSON format and saved at a path specified by `hoodie.table.index.defs.path` in [table properties](#table-properties).
Index itself is stored in Hudi metadata table under the partition `expr_index_<user_specified_index_name>`.

We covered different [storage layouts](#storage-layout) earlier. Functional index aggregates stats by storage partitions and, as such, partitioning can be absorbed into functional indexes.
From that perspective, some useful functions that can also be applied as transforms on a field to extract and index partitions are listed below.

| Function        | Description                                                                       |
|-----------------|-----------------------------------------------------------------------------------|
| `identity`      | Identity function, unmodified value                                               |
| `year`          | Year of the timestamp                                                             |
| `month`         | Month of the timestamp                                                            |
| `day`           | Day of the timestamp                                                              |
| `hour`          | Hour of the timestamp                                                             |
| `lower`         | Lower case of the string                                                          |
| `from_unixtime` | Convert unix epoch to a string representing the timestamp in the specified format |

## Relational Model

This section describes how to implement a traditional relational model, with concurrent writers on top of the storage format described so far. 
Specifically, it aims to employ optimistic concurrency control between writers to guarantee serializable execution of write operations. To achieve this, 
Hudi assumes the availability of a distributed lock service (see appendix for lock provider implementations) to acquire an exclusive table level lock.

There are three types of processes at play at any given time :

*   Writers that are modifying the state of the table, with updates, deletes and inserts.
*   Table services which do not logically change the state of the table.
*   Readers that are querying the state of the table.

### Writer Expectations

Writers generate a begin time for their actions, proceed to create new base and log files, and finally transition the action state to completed atomically on the timeline, as follows.

1. Writer requests a begin time for the write action as defined in the Timeline section above (this may involve a lock, depending on the TrueTime generation mechanism used).
   Writer also records the latest completion time on the timeline (let's call this the **_snapshot write time_**).
2. Writer produces new base and log files with updated, deleted, and inserted records, while ensuring updates/deletes reach the correct file group by looking up an index as
   of the snapshot write time (see appendix for an alternative encoding that treats updates as deletes to an existing file group plus inserts into a new file group, with the associated performance trade-offs).
3. Writer then grabs the distributed lock and performs the following steps within the critical section to finalize the write or fail/abort:
   1. Obtain metadata about all actions that have completed with completion time greater than the snapshot write time. This defines the **_concurrent set_** of
      actions against which the writer must check for conflicting concurrent writes or table-service actions.
   2. Writer checks for any overlapping file groups that have been written to in the concurrent set and decides to abort if any are found.
   3. Writer checks for any compactions planned or clustering actions completed on overlapping file groups and decides to abort if any are found.
   4. Writer reads out all record keys written by the concurrent set and compares them against keys from files it is about to commit. If there are any overlaps,
      the writer decides to abort (implementations may omit this check for performance reasons, accepting the resulting trade-offs/limitations).
   5. Writer also refreshes the timeline once more inside the lock to pick up any actions that completed between snapshot write time and lock acquisition, repeating the checks above against this refreshed view.
   6. If any of the checks above flag a conflict, the writer releases the lock and aborts the write.
4. If the checks in step (3) pass, the writer proceeds to finalize the write while still holding the lock:
   1. Writer updates the metadata table (file listings and any enabled indexes) based on the new files/records added to the table, and commits to the metadata-table timeline.
   2. Writer generates a completion time for the write and atomically publishes the completed instant file on the timeline along with the necessary metadata.
   3. Optionally, the writer plans any table services and may even run them in-process. This is an implementation choice.
   4. Writer releases the lock.

Note that specific writer implementations can choose to abort early to improve efficiency and reduce resource wastage,
if they can detect conflicts using mechanisms like marker files described in the appendix.

### Table Service/Writer Concurrency

All table services acquire the same exclusive lock as writers above to plan and finalize action, including updating the metadata/indexes. 
Table services plans are serialized within the lock and once requested on the timeline are idempotent and expected to complete eventually with 
potential retries on failures. External table service management and built-in optional table services within writer processes can seamlessly co-exist and merely a deployment convenience.

Table services should follow these expectations to ensure they don't block each other.

| **Table Service** | **Expectation** |
| ---| --- |
| Cleaning | planning or finalizing does not conflict with any table service by definition. |
| Clustering | Should abort if concurrent updates to the same file groups being clustered are detected when finalizing. |
| Compaction | Should exclude file groups already planned for clustering from its plan. May abort if file groups being compacted have since been replaced/clustered (correctness is not affected either way). |
| Indexing | Indexing can plan and finalize asynchronously with any table service or writer, once planned as above within the lock. |

Thus, Hudi does not treat table service actions as opaque modifications to the table and thus supports running compaction without blocking writers helpful for high update/concurrency workloads.

### Concurrent readers
The model supports snapshot isolation for concurrent readers. A reader constructs the state of the table based on the action with the greatest completion time and proceeds 
to construct file slices out of file groups that are of interest to the read, based on the type of query. Two concurrent readers are never in contention even in the presence of concurrent writes happening.

### Reader Expectations
Readers need to determine the correct file slice (an optional base file plus an ordered list of log files) to read in order to construct the snapshot state, using the following steps:

1. Reader picks an instant in the timeline — the latest completion time on the timeline or a specific time explicitly specified. Call this the **_snapshot read time_**.
2. Reader computes all file groups in the table by listing all files that are part of the table, grouping them by `file_id`, and eliminating files that don't belong to any completed write action on the timeline.
3. Reader then eliminates replaced file groups, by removing any file groups that are part of any **_replacecommit_** actions completed on or before the snapshot read time.
4. For each remaining file group, the reader determines the correct file slice as follows:
   1. Find the base file with the greatest requested instant time less than or equal to the snapshot read time.
   2. Obtain all log files with completion time less than or equal to the snapshot read time, sorted by completion time.
5. For each file slice obtained, the reader merges the base and log files:
   1. Scan log blocks in order. Each block carries an `INSTANT_TIME` header; a `COMMAND_BLOCK` of type `ROLLBACK_BLOCK`
      causes the immediately preceding block to be discarded (this is how partial/aborted appends are masked out without rewriting the log file).
      Blocks whose `INSTANT_TIME` no longer corresponds to a completed instant on the timeline are also skipped. `BLOCK_IDENTIFIER`
      is used to deduplicate blocks produced by task retries within the same instant.
   2. When the base file is scanned, for every record the reader has to look up whether there is a newer version of the record available in the surviving log blocks and merge it into the record iterator.
      When `RECORD_POSITIONS` is present, this merge can be done positionally against the base file identified by `BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS` rather than via key lookup.
   3. Compaction-produced blocks may set the `COMPACTED_BLOCK_TIMES` header listing the instant times of the (minor-compacted) blocks they replace; the reader skips any earlier blocks whose instant time is covered.

Obtaining the listings from storage can be slow or inefficient. It can be further optimized by caching in memory, by consulting the `files` partition of the metadata table, or with the support of an external timeline-serving system.

## Incremental Streaming Model

Optimistic concurrency control yields poor performance with long running transactions and even moderate levels of writer contention. While the relation model is well-understood, 
serializability based on arbitrary system time (e.g completion times in Hudi or SCNs in databases) may be an overkill and even inadequate for many scenarios. Hudi storage also supports 
an alternative model based on stream processing techniques, with the following changes.

1. **Event-time based ordering**: Instead of merging base and log files based on completion times, latest value for a record is picked based on 
    a user-specified event field that denotes the latest version of the record.
2. **Relaxed expectations to block/abort**: By tracking the span of actions using both begin and completion times on the timeline, processes can 
   detect conflicting actions and adjust file slicing accordingly. For e.g this model allows compaction to be even planned without blocking writer processes.
3. **Custom merging** : Support implementation of custom merges using RecordPayload and RecordMerger APIs.

### Non-blocking Concurrency Control
Please refer to [RFC-66](https://github.com/apache/hudi/blob/master/rfc/rfc-66/rfc-66.md) for more and a proof of correctness.

## Table Management

All table services can be run synchronous with the Table client that merges modifications to the data or can be run asynchronously to the table client. 
Asynchronous is default mode in the Apache Hudi platform. Any client can trigger table management by registering a 'requested' state action in the Hudi timeline. 
Process in charge of running the table management tasks asynchronously looks for the presence of this trigger in the timeline.

### Compaction

Compaction is the process that efficiently updates a file slice (base and log files) for efficient querying. It applies all the batched up updates 
in the log files and writes a new file slice. The logic to apply the updates to the base file follows the same set of rules listed in the Reader expectations.

### Log Compaction

Log compaction is a minor compaction operation that stitches together log files into a single large log file, thus
reducing write amplification. This process involves introducing a new action in Hudi called `logcompaction`, on the
timeline. The log-compacted file is written to the same file group as the log files being compacted. Additionally, the
log-compacted log block header contains the `COMPACTED_BLOCK_TIMES` and the log file reader can skip log blocks that
have been compacted, thus reducing read amplification as well.
See [RFC-48](https://github.com/apache/hudi/blob/master/rfc/rfc-48/rfc-48.md) for more details.

### Re-writing

If the natural ingestion ordering does not match the query patterns, then data skipping does not work efficiently. It is important for query efficiency 
to be able to skip as much data on filter and join predicates with column level statistics. Clustering columns need to be specified on the Hudi table. 
The goal of the clustering table service, is to group data often accessed together and consolidate small files to the optimum target file size for the table.

1. Identify file groups that are eligible for clustering - this is chosen based on the clustering strategy (file size based, time based etc)
2. Identify clustering groups (file groups that should be clustered together) and each group should expect data sizes in multiples of the target file size.
3. Persist the clustering plan in the Hudi timeline as a replacecommit, when clustering is requested.
4. Clustering execution can then read the individual clustering groups, write back new file groups with target size with base files sorted by the specified clustering columns.

### Cleaning

Cleaning is a process to free up storage space. Apache Hudi maintains a timeline and multiple versions of the files written as file slices. 
It is important to specify a cleaning protocol which deletes older versions and reclaims the storage space. Cleaner cannot delete versions that 
are currently in use or will be required in future. Snapshot reconstruction on a commit instant which has been cleaned is not possible.

For e.g, there are a couple of retention policies supported in Apache Hudi platform

*   **keep\_latest\_commits**: This is a temporal cleaning policy that ensures the effect of having look-back into all the changes that happened in the last X commits.
*   **keep\_latest\_file\_versions**: This policy has the effect of keeping a maximum of N number of file versions irrespective of time.

Apache Hudi provides snapshot isolation between writers and readers by managing multiple files with MVCC concurrency. These file versions provide history
and enable time travel and rollbacks, but it is important to manage how much history you keep to balance your storage costs.

Cleaning is planned and tracked in terms of **instant (start) times**, even though actions on the timeline are ordered
by completion time. The fields below all hold the requested instant time of the action they refer to.

| Field | Description |
|---|---|
| earliestInstantToRetain | Instant time, action and state of the oldest commit the clean run retains. Held in `HoodieCleanerPlan`, and copied to `HoodieCleanMetadata` as `earliestCommitToRetain`. Absent under `KEEP_LATEST_FILE_VERSIONS`, which retains a fixed number of file versions per file group rather than a range of the timeline; `earliestCommitToRetain` is then an empty string |
| lastCompletedCommitTimestamp | Instant time of the write that completed most recently before the clean was planned. Despite the name, this is a start time. The instant is chosen by completion order, so with concurrent writers it is not always the largest instant time among completed commits |
| startCleanTime | Instant time of the clean action itself, in `HoodieCleanMetadata` |

Incremental clean planning selects commits whose requested instant time is at or after the previous clean's
`earliestCommitToRetain` and before this clean's, and scans only the partitions those commits touched.


### Indexing

Indexing is an asynchronous process to create indexes on the table without blocking ingestion writers. The indexing is
divided into two phases: scheduling and execution. During scheduling, the indexer takes a lock for a short duration and
generates an indexing plan for data files based off of a snapshot. Index plan is serialized as avro bytes and stored in `[instant].indexing.requested` file in the timeline.
Here's the schema for index plan

| Field               | Description                                                                                                                                                                      |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| version             | Index plan version. Current version is 1. It is updated whenever the index plan format changes                                                                                   |
| indexPartitionInfos | An array of `HoodieIndexPartitionInfo`s. Each array element consists of metadata partition path, base instant off of which indexer started and an optional map of extra metadata |

During execution, the indexer executes the plan, writing index base files into the metadata table for the targeted
partition. Any ingestion commit that completes concurrently updates the same index via log files appended under the same
file groups. After writing the base file, the indexer checks all commits that completed after the plan's base instant
time (`baseInstantTime` carried in `HoodieIndexPartitionInfo`) to confirm each one wrote the expected entries per the
indexing plan; if any did not, the indexer aborts gracefully. Finally, on success, the indexer publishes
`[instant].indexing` (completed state) to the timeline. The indexer only takes the table lock while adding events to the
timeline, not while writing index files.
See [RFC-45](https://github.com/apache/hudi/blob/master/rfc/rfc-45/rfc-45.md) for the original design.

## Compatibility

Compatibility between different readers and writers is enforced through the `hoodie.table.version` table config. Hudi storage format evolves in a backwards 
compatible way for readers, where newer readers can read older table versions correctly. However older readers may be required to upgrade in order to read higher table versions. 
Hence, we recommend upgrading readers first, before upgrading writers when the table version evolves.

## Change Log

### version 9 — current (Hudi 1.1.0 → 1.2.0)

Version 9 is a non-breaking evolution of version 8 in the framing/timeline layer, but it introduces new column types,
new base-file-format support, and a new secondary-index encoding. Version 9 readers can correctly read the version 8 subset
of a table; version 8 readers cannot read version 9 features.

Introduced in **Hudi 1.1.0** (Nov 2025):

* New secondary-index encoding **V2** (see [Secondary Index](#secondary-index)). V1 remains the encoding used by existing v8 partitions on upgraded tables.
* Timeline layout version remains at 2; LSM-history compaction behavior refined.

Introduced in **Hudi 1.2.0** (May 2026):

* New base file format: **Lance** (`.lance`), for Spark DataSource read and Copy-on-Write writes (see [Base Files](#base-files)). Column stats and partition stats are not populated for Lance base files.
* New Hudi logical column types in the user table schema: **VARIANT**, **VECTOR**, **BLOB** (see [Types](#types)).
* Single-date-field tables can use slash-separated partition paths (e.g. `2026/07/03`) via `hoodie.datasource.write.slash.separated.date.partitioning`.

### version 8 (Hudi 1.0)

Breaking format version. Users had to migrate completely to the new timeline before resuming normal table operations.
Notable additions vs. version 7:

* New LSM-tree timeline history (`timeline/history`) replacing the old `archived/` layout (`hoodie.timeline.layout.version` advances to 2).
* New header metadata `IS_PARTIAL` (data block was produced with partial updates enabled) and `BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS` (anchors `RECORD_POSITIONS` to a specific base file).
* `isTightBound` field added to `HoodieMetadataColumnStats` so tighter min/max bounds can be re-established by compaction/clustering.
* First-class metadata partitions for `partition_stats`, `record_index`, `secondary_index_<name>`, and `expr_index_<name>`.

### version 6 & earlier

Please refer to the previous tech specs [document](/learn/tech-specs-0-point-x).

## APIs

### RecordMerger API

Hudi data model ensures record key uniqueness constraint, to maintain this constraint every single record merged into the table needs to be checked if the 
same record key already exists in the table. If it does exist, the conflict resolution strategy is applied to create a new merged record to be persisted. 
This check is done at the file group level and every record merged needs to be tagged to a single file group. By default, record merging is done during the merge which 
makes it efficient for queries but for certain real-time streaming requirements, it can also be deferred to the query as long as there is a consistent way to mapping 
the record key to a certain file group using consistent hashing techniques.

### Indexing Functions

See [RFC-63](https://github.com/apache/hudi/blob/master/rfc/rfc-63/rfc-63.md) for design and direction.


## Appendix

### Lock Provider Implementations

Apache Hudi platform uses optimistic locking and provides a pluggable LockProvider interface and multiple implementations are available out of the box (Apache Zookeeper, DynamoDB, Apache Hive 
and JVM Level in-process lock). It is also worth noting that, if multiple writers originate from the same JVM client, a simple locking at the client level would serialize the writes 
and no external locking needs to be configured.

### Balancing write and read performance

A critical design choice for any table is to pick the right trade-offs in the data freshness and query performance spectrum. Hudi storage format lets the users decide 
on this trade-off by picking the table type, record merging and file sizing.

|  | Merge Efficiency | Query Efficiency |
| ---| ---| --- |
| Copy on Write (COW) | **Tunable:** COW table type creates a new File slice in the file-group for every batch of updates. Write amplification can be quite high <br/>when the update is spread across multiple file groups. <br/>The cost involved can be high over a time period especially on tables with low data latency requirements. | **Optimal:** COW table types create whole readable data files in open source columnar file formats on each merge batch, there is minimal overhead per record in the query engine. Query engines are fairly optimized for accessing files directly in cloud storage. |
| Merge on Read (MOR) | **Optimal:** MOR table type batches the updates to the file slice in a separate optimized Log file, write amplification is amortized over time <br/>when sufficient updates are batched. The merge cost involved will be lower than COW since the churn on the records re-written for every update is much lower. | **Tunable:** MOR Table type required record level merging during query. Although there are techniques to make this merge as efficient as possible, there is still a record level overhead to apply the updates batched up for the file slice. The merge cost applies on every query until the compaction applies the updates and creates a new file slice. |


> Interesting observation on the MOR table format is that, by providing a special view of the table which only serves the base files in the file slice (read optimized query of MOR table), query can pick between query efficiency and data freshness dynamically during query time. Compaction frequency determines the data freshness of the read optimized view. With this, the MOR has all the levers required to balance the merge and query performance dynamically.

Sizing the file group is extremely critical to balance the merge and query performance. Larger the file size, the more the write amplification when new file slices are being created. So to balance the merge cost, compaction or merge frequency should be tuned accordingly and this has an impact on the query performance or data freshness.


### Optimistic concurrency efficiency

The efficiency of Optimistic concurrency is inversely proportional to the possibility of a conflict, which in turn depends on the running time and the files overlapping between the concurrent writers. 
Apache Hudi storage format makes design choices that make it possible to configure the system to have a low possibility of conflict with regular workloads

*   All records with the same record key are present in a single file group. In other words, there is a 1-1 mapping between a record key and a file group id, at all times.
*   Unit of concurrency is a single file group and this file group size is configurable. If the table needs to be optimized for concurrent updates, the file group size can be 
    smaller than default which could mean lower collision rates.
*   Merge-on-read storage engine has the option to store the contents in record oriented file formats which reduces write latencies (often up to 10 times compared to columnar storage) 
    which results in less collision with other concurrent writers
*   Merge-on-read storage engine combined with scalable metadata table ensures that the system can handle frequent updates efficiently which means ingest jobs can be frequent and quick, reducing the chance of conflicts


### Marker mechanism to remove uncommitted data

See [this](https://hudi.apache.org/blog/2021/08/18/improving-marker-mechanism/) and [RFC-56](https://github.com/apache/hudi/blob/master/rfc/rfc-56/rfc-56.md).

  
