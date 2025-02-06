<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-77: Secondary Indexes

## Proposers

- @codope

## Approvers
 - @vinothchandar
 - @nsivabalan

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-7146

> Please keep the status updated in `rfc/README.md`.

## Abstract

In this RFC, we propose implementing Secondary Indexes (SI), a new capability in Hudi's metadata table (MDT) based indexing 
system.  SI are indexes defined on user specified columns of the table. Similar to record level indexes,
SI will improve query performance when the query predicate involves those secondary columns. The number of files
that a query needs to scan can be pruned down using secondary indexes.

## Background

Hudi supports different indexes through its MDT. These indexes help to improve query performance by
pruning down the set of files that need to be scanned to build the result set (of the query). 

One of the supported index in Hudi is the Record Level Index (RLI). RLI acts as a unique-key index and can be used to 
locate a FileGroup of a record based on its RecordKey. A query having an EQUAL or IN predicate on the RecordKey will 
have a performance boost as the RLI can accurately give a subset of FileGroups that contain the rows matching the 
predicate.

Many workloads have queries with predicates that are not based on RecordKey. Such queries cannot use RLI for data
skipping. Traditional databases have a notion of building indexes (called Secondary Index or SI) on user specified 
columns to aid such queries. This RFC proposes implementing SI in Hudi. Users can build SI on columns which are 
frequently used as filtering columns (i.e columns on which query predicate is based on). As with any other index, 
building and maintaining SI adds overhead on the write path. Users should choose wisely based 
on their workload. Tools can be built to provide guidance on the usefulness of indexing a specific column, but it is 
not in the scope of this RFC.

## Design and Implementation
This section discusses briefly the goals, design, implementation details of supporting SI in Hudi. At a high level,
the design principle and goals are as follows:

1. User specifies SI to be built on a given column of a table. A given SI can be built on only one column of the table
(i.e composite keys are not allowed). Any number of SI can be built on a Hudi table. The indexes to be built are 
specified using regular SQL statements.
2. Metadata of a SI will be tracked through the index metadata file under `<base_path>/.hoodie/.index` (this path can be configurable).
3. Each SI will be a partition inside Hudi MDT. Index data will not be materialized with the base table's data files.
4. Logical plan of a query will be used to efficiently filter FileGroups based on the query predicate and the available
indexes.

### SQL
SI can be created using the regular `CREATE INDEX` SQL statement.
```
-- PROPOSED SYNTAX WITH `secondary_index` as the index type --
CREATE INDEX [IF NOT EXISTS] index_name ON [TABLE] table_name [USING index_type](index_column)
-- Examples --
CREATE INDEX idx_city on hudi_table USING bloom_filters(city)
-- Default is to create a hash based record level index mapping secondary column to RLI entries.
CREATE INDEX idx_last_name on hudi_table (last_name)

-- NO CHANGE IN DROP INDEX --
DROP INDEX idx_city;
```

`index_name` - Required and validated by parser. `index_name` will be used to derive the name of the physical partition
in MDT by prefixing `secondary_index_`. If the `index_name` is `idx_city`, then the MDT partition will be 
`secondary_index_idx_city`

The index_type will be `secondary_index`. This will be used to distinguish SI from other Functional Indexes.

### Secondary Index Metadata
Secondary index metadata will be managed the same way as Expression Index metadata. If the SI does not have any function
to be applied on each row, the `function_name` will be NULL.

### Index in Metadata Table (MDT)
Each SI will be stored as a physical partition in the MDT. The partition name is derived from the `index_name` by 
prefixing `secondary_index_`. Each entry in the SI partition will be a mapping of the form 
`secondary_key -> record_key`. `secondary_key` will form the "record key" for the record of the SI partition. Note that
an important design consideration here is that users may choose to build SI on a non-unique column of the table.

#### Index Initialization
Initial build of the secondary index will scan all file slices (of the base table) to extract 
`secondary-key -> record-key` tuple and write it into the secondary index partition in the metadata table. 
This is similar to how RLI is initialised.

#### Index Maintenance
The index needs to be updated on inserts, updates and deletes to the base table. Considering that secondary-keys in 
the base table could be non-unique, this process differs significantly compared to RLI.

##### Inserts (on the base table)
Newly inserted row's record-key and secondary-key is required to build the secondary-index entry. The commit metadata has the files affected by that commit. 
The metadata writer will extract the newly written records based on the commit metadata and generate the secondary-key values (for all
those columns on which secondary index is defined) to the secondary index partition. [1]

##### Updates (on the base table)
Similar to inserts, the `secondary-key -> record-key` tuples are extracted from the WriteStatus. However, additional 
information needs to be emitted to aid compaction and merging (of the MDT partition hosting the SI). 

Let's take an example where a row's `secondary-key` column is updated from its old value of 
`old-secondary-key -> record-key` to `new-secondary-key -> record-key`. Note that as the secondary keys are updated, 
the `new-secondary-key` could get mapped to a different FileGroup (in MDT partition hosting the SI) as compared to 
`old-secondary-key`. There needs to be a mechanism to clean up the `old-secondary-key -> record_key` mapping (from the 
MDT partition hosting SI). The proposal is to emit a tombstone entry `old-secondary-key -> (record-key ,deleted)`. This 
is then followed by writing the `new-secondary-key -> record-key`. The log record reader (merger) and compactor should 
correctly use the tombstone marker to remove stale entries from the old FileGroup. Note that since the mapping of a 
record (of the MDT) to a FileGroup is based on the `secondary-key`, it is guaranteed that 
`old-secondary-key -> (record-key, deleted)` tombstone record will get mapped to the FileGroup containing the 
`old-secondary-key, record-key` entry.

Another key observation here is that `old-secondary-key` is required to construct the tombstone record. Unlike some other 
data systems, Hudi does not read the old-image of a row on updates until a merge is executed. It detects that a row is getting updated by simply 
reading the index and appending the updates in log files. Hence, there needs to be a mechanism to extract `old-secondary-key`. We propose
`old-secondary-key` to be extracted by scanning the MDT partition (hosting the SI) and doing a reverse lookup based 
on the `record-key` of the row being updated. It should be noted that this might be an expensive operation as the 
base table grows in size (which inherently means that SI will grow in size) in terms of number of rows. One way to 
optimize this is to build a reverse mapping `record-key -> secondary-key` in a different MDT partition. This is 
left as a TBD (as of this writing).

##### Deletes (on the base table)
This is handled the same way updates are handled. Only `old-secondary-key -> (record-key, deleted)` tuple is emitted 
as a tombstone record to the FileGroup containing the `old-secondary-key -> record-key` entry.

### Query Execution
Secondary index will be used to prune the candidate files (to scan) if the query has  an "EQUAL TO" or "IN" predicate
on `secondary-key` columns. The initial implementation will follow the same logic (and constraints) used by RLI based 
data skipping(like single column keys, rule-based index selection etc.). In the first step, we scan the SI partition
(in MDT) and filter all entries that match the secondary keys specified in the query predicate. We collect all the 
record keys (after filtering). Subsequently, these record keys are used to look up the record location using RLI.

### Merging of secondary index records (on the read-path or during compaction)
Record mergers are used in MOR tables by the readers to merge log files and base files. Similarity of the 'keys' in 
records is used to identify candidate records that need to be merged. The 'key' for RLI entry is the 
`record-key` and by definition it is unique. But, the keys for secondary index entries are the `secondary-keys` which 
can be non-unique. Hence, the merging of SI entries will make use of the  payload i.e `record-key` in the
`secondary-key -> record-key` tuple to identify candidate records that need to be merged. It will also be guided by the 
tombstone record emitted during update or deletes. An example is provided here on how the different log files are merged 
and how the merged log records are finally merged with the base file to obtain the merged records (of the MDT partition hosting SI).

Consider the following table, `trips_table`. Note that this table is only used to illustrate the merging logic and not 
to be used as a definitive table for other considertaion (for example, the performance aspect of some of the algorithm 
chosen will depend on the cardinality of the `record-key` and `secondary-key` columns).

| Column Name | Column Type |
|-------------|-------------|
| uuid        | string      |
| ts          | long        |
| rider       | string      |
| city        | string      |

`uuid` is the `record-key` column and a SI is built on the `city` column. Few details to be noted:
1. Base files in the MDT are sorted (HFile) by the 'key' for the records stored in that MDT partition. For the MDT 
partition hosting SI, records are sorted by `secondary-key`.
2. Base file will never contain a tombstone record (record mergers never write tombstone or deleted records 
into base file)

Base File Entries:
```
chennai -> c8abbe79-8d89-47ea-b4ce-4d224bae5bfa
los-angeles -> 9909a8b1-2d15-4d3d-8ec9-efc48c536a01
los-angeles -> 9809a8b1-2d15-4d3d-8ec9-efc48c536a01
sfo -> 334e26e9-8355-45cc-97c6-c31daf0df330
sfo -> 334e26e9-8355-45cc-97c6-c31daf0df329
```

Log File Entries:
```
sfo -> (334e26e9-8355-45cc-97c6-c31daf0df329, deleted)
chennai -> e3cf430c-889d-4015-bc98-59bdce1e530c
los-angeles -> (9809a8b1-2d15-4d3d-8ec9-efc48c536a01, deleted)
austin -> 9809a8b1-2d15-4d3d-8ec9-efc48c536a01
```

Merged Records (sorted):
``` 
austin -> 9809a8b1-2d15-4d3d-8ec9-efc48c536a01
chennai -> e3cf430c-889d-4015-bc98-59bdce1e530c
chennai -> c8abbe79-8d89-47ea-b4ce-4d224bae5bfa
los-angeles -> 9909a8b1-2d15-4d3d-8ec9-efc48c536a01
sfo -> 334e26e9-8355-45cc-97c6-c31daf0df330
```

An important detail to be noted here is that grouping for merging depends on the `secondary-key, record-key` tuple (`city, uuid` 
in this example). This is radically different from any other existing partitions in Hudi (MDT or base tables). Hence, 
support needs to be added to merge records from MDT partition hosting SI (with generalized approach that can be used for
any future non-unique record key based dataset).

The data structure built for holding the merged log records should enable to search similar keys efficiently with 
following consideration:
1. Should allow spilling to disk (to prevent OOM). A pure in-memory data structure will not be production ready, but 
can be used only for POC
2. Should allow for searching (and retrieving) based on two keys as efficiently as possible. First search will be based 
on `secondary-key` and second search will be based on `record-key`. Hence, a single level Map used by 
`HoodieMergedLogRecordScanner` will not work.
3. Should allow for efficient removal of records (deleted record need to be removed). Hence, any data structure that 
uses a flat array will not be efficient. 
4. Should allow for efficient insertion of records (for inserting merged record and for buffering fresh records). 

This is achieved by the following efficient encoding of the reverse mapping of secondary values to record keys in the 
MDT partition. We exploit a key observation that its enough to merge SI entries and tombstones with a tuple key 
`{secondaryKey, recordKey}` through the existing spillable/merge map implementation. We store a flattened version of the 
logical multimap as a key-value format with key : `secondaryKey+recordKey` and value `isDeleted: false|true` indicating
whether this is a tombstone or an insert of the SI entry.

### Comparing alternate design proposals

Here are some alternate options that we considered:

1. Extend Hudi's `ExternalSpillableMap` to support multi-map. More signicant refactoring is required and it would have
   leaked implementation details to the write handle layer, as the records held by `ExternalSpillableMap` is exposed to
   write handle via `HoodieMeredLogRecordScanner::getRecords`.
2. Write spillable version of [Guava's multi-map](https://github.com/google/guava/wiki/NewCollectionTypesExplained#multimap). Apart from reason
   mentioned above, we did not want to add a third-party dependency on Guava.
3. Use [Chronicle map](https://github.com/OpenHFT/Chronicle-Map). Same reasons as above.
4. Use two different spillable data structures - one is a set of `secondary-key` and the other is map of
   `record-key -> (secondary-key, HoodieRecord)`. This would have been harder to maintain and the log scanner should
   know when to use which data structure.

## Test Plan

1. The feature will be tested by functional tests and integration tests. 
2. Indexing strategy always requires query correctness testing. One way to achieve that is to run the query against 
same data set twice - once using data-skipping (based on SI) and the other based on full-table-scan - and compare the 
result set. 
3. Indexing strategy should be accompanied by performance test results showing its benefits on the query path (and optionally 
overhead on the index maintenance (write) path)

### SparkSQL Benchmark

Benchmarks of the implementation shows some impressive gains. 

Table used - web\_sales (from 10 TB tpc-ds dataset)
Total File groups - 286,603
Total Records - 7,198,162,544
Cardinality of Secondary index column ~ 1:150 (not too high or not too low)

| Run | Query latency w/o data skipping (secs) | Query latency w/ Data Skipping (leveraging SI) (secs) | Improvement |
| ---| ---| ---| --- |
| 1 | 252 | 31 | ~88% |
| 2 | 214 | 10 | ~95% |
| 3 | 204 | 9 | ~95% |

|  | Stats w/o data skipping | Stats w/ Data Skipping (leveraging SI) |
| ---| ---| --- |
| Number of Files read | 286,603 | 150 |
| size of files read | 759 GB | 593 MB |
| Total Number of Rows read | 7,198,162,544 | 5,811,187 |


## Future enhancements and roadmap

The feature can evolve to provide additional functionalities.
1. As mentioned earlier, adding SI will incur cost - CPU and memory cost on write/ingestion side and the overall 
storage cost to store the SI mapping. Tools can be built to estimate the storage cost upfront based on the current size
of the dataset and user specified growth in the dataset size.
2. Tools can be built to help users choose the right columns to build SI (based on the observed query pattern). There 
are examples of such Automated Indexing Advisors - both in academia and industry.
3. Efficient data structures to store the reverse mapping (i.e `record_key -> secondary_key`)
4. Ability to build SI on composite keys
5. Cost based index selection (on query execution)
