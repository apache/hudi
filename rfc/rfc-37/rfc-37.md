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
# RFC-37: Metadata based Bloom Index


## Proposers

- @nsivabalan
- @manojpec

## Approvers
 - @<approver1 github username>
 - @<approver2 github username>

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-2703

## Abstract
Hudi maintains indices to locate/map incoming records to file groups during writes. Most commonly 
used record index is the HoodieBloomIndex. For larger installations and for global index types, performance might be an issue
due to loading of bloom from large number of data files and due to throttling issues with some of the cloud stores. We are proposing to 
build a new Metadata index (metadata table based bloom index) to boost the performance of existing bloom index. 

## Background
HoodieBloomIndex is used to find the location of incoming records during every write. This will assist Hudi in deterministically 
routing records to a given file group and to distinguish inserts vs updates. This bloom index relies on (min, max) values 
of records keys and bloom indexes in base file footers to find the actual record location. In this RFC, we plan to 
build a new index on top of metadata table which to assist in bloom index based tagging. 

## Design
HoodieBloomIndex involves the following steps to find the right location of incoming records
1. Load all interested partitions and fetch data files. 
2. Find and filter files to keys mapping based on min max in data file footers.
3. Filter files to keys mapping based on bloom index in data file footers. 
4. Look up actual data files to find the right location of every incoming record.

As we could see from step 1 and 2, we are in need of min and max values for "_hoodie_record_key" and bloom filter for 
all data files to perform the tagging. In this design, we will add these to metadata table and the index lookup 
will look into these metadata table partitions to deduce the file to keys mapping. 

To realize this, we are adding two new partitions namely, `column_stats` and `bloom_filter` to metadata table.  

Why metadata table: 
Metadata table uses HFile to store and retrieve data. HFile is an indexed file format and supports random lookups based on 
keys. Since, we will be storing stats/bloom for every file and the index will do lookups based on files, we should be able to 
benefit from the faster lookups in HFile. 

<img src="metadata_index_1.png" alt="High Level Metadata Index Design" width="800"/>

Following sections will talk about different partitions, key formats and then dive into the data and control flows.

### Column_Stats partition:
"Column_stats" will be discussed in depth in RFC-27, but in the interest of this RFC, Column_stats partition stores 
statistics(min and max value) for `__hoodie_record_key` column for all files in the Hudi data table. 

High level requirement for this column_stats partition are:
Given a list of record keys, partition paths and file names, find the possibly matching file names based on
`__hoodie_record_key` column stats. 

To cater to this requirement, we need to ensure our keys in Hfile are such that we can do pointed lookups for a given data file.
Below picture gives a pictorial representation of Column stats partition in metadata table. 

<img src="metadata_index_col_stats.png" alt="Column Stats Partition" width="600"/>

We have to encode column names, filenames etc to IDs to save storage and to exploit compression. We will update the RFC 
once we have more data around what kind of ID we can go with. On a high level, we are looking at incremental IDs vs 
hash Ids. 

For now, lets assume that every entity will be given an ID (column name, partition path name, file name) 

```
Key in column_stats partition =
[colId][PartitionId][FileId]
```
```
Value: stats  {
  min_value: bytes
  max_value: bytes
  ...
  ...
}
```

### Bloom Filter Partition:
This will assist in storing bloom filters for all base files in the data table. This will be leveraged by metadata 
index being designed with this RFC.

<img src="metadata_index_bloom_partition.png" alt="Bloom filter partition" width="500"/>

Requirements:<br>
Given a list of FileIDs, return their bloom filters
```
Key format: [PartitionId][FileId]
```
```
Value : 
{
  serialized bloom
  bloom type code
}
```

## Implementation 

### Writer flow: 
Let's walk through the writer flow to update these partitions.

Whenever a new commit is getting applied to metadata table, we do the following.<br>
1. Files partition - prepare records for adding
2. Column_stats partition - prepare records for adding
[ColumnID][PartitionID][FileID] => ColumnStats
This involves reading the base file footers to fetch min max values for each column
3. Bloom_filter partition - prepare records for adding
[PartitionID][FileID] => BloomFilter
This involves reading the base file footers.
We can amortize the cost across (2) and (3) and just read it once and prepare/populate records for both partitions.  
4. . Commit all these records to metadata table.

We need to ensure we have all sufficient info in WriteStatus gets passed to metadata writer for every commit. 

### Reader flow:

This is actually a writer flow. When a new batch of write is ingested into Hudi, we need to tag the records with their 
original file group location. And this index will leverage both the partitions to deduce the record key => file name mappings.

```
Input: JavaRdd<HoodieKey>
Output: JavaPairRdd<HoodieKey, HoodieRecordLocation>
```

We will re-use some of the source code from existing bloom index implementation and direct the min max value filtering and 
bloom based filtering to metadata table.

The actual steps are as follows: <br>
1. Find all interested partitions
2. Fetch all files pertaining to the partitions of interest
3. Look up in column stats partition in metadata tale and find list of possible HoodieKeys against every file. 
4. Look up in bloom filter partition in metadata table and find list of possible Hoodiekeys against every file.
5. Look up actual data file to deduce the actual record location for every HoodieKey. 

## Rollout/Adoption Plan 
 
 - To be filled. 
 - What impact (if any) will there be on existing users? 
 - If we are changing behavior how will we phase out the older behavior?
 - If we need special migration tools, describe them here.
 - When will we remove the existing behavior

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.

To be filled.