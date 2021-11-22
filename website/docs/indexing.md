---
title: Indexing
toc: true
---

## Index
Hudi provides efficient upserts, by mapping a given hoodie key (record key + partition path) consistently to a file id, via an indexing mechanism.
This mapping between record key and file group/file id, never changes once the first version of a record has been written to a file. In short, the
mapped file group contains all versions of a group of records.

## File Layout (TODO: find home for this?)
Hudi organizes a table into a directory structure under a `basepath` on DFS. Table is broken up into partitions, which are folders containing data files for that partition,
very similar to Hive tables. Each partition is uniquely identified by its `partitionpath`, which is relative to the basepath.

Within each partition, files are organized into `file groups`, uniquely identified by a `file id`. Each file group contains several
`file slices`, where each slice contains a base file (`*.parquet`) produced at a certain commit/compaction instant time,
along with set of log files (`*.log.*`) that contain inserts/updates to the base file since the base file was produced.
Hudi adopts a MVCC design, where compaction action merges logs and base files to produce new file slices and cleaning action gets rid of
unused/older file slices to reclaim space on DFS.