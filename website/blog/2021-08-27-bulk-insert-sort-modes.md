---
title: "Bulk Insert Sort Modes with Apache Hudi"
excerpt: "Different sort modes available with BulkInsert"
author: shivnarayan
category: blog
---

Apache Hudi supports a `bulk_insert` operation called “bulk_insert” in addition to "insert" and "upsert" to ingest data into a hudi table. 
There are different sort modes that one could employ while using bulk_insert operation. This blog will talk about 
different sort modes available out of the box, and how each compares with others. 
<!--truncate-->

Apache Hudi supports an operation called “bulk_insert” to assist in initial loading to data to hudi. This is expected
to be faster when compared to using “insert” or “upsert” operation types. Bulk insert differs from insert in one
aspect. Small files optimization is not available with “bulk insert”, where as “insert” does small file management. So, 
existing records/files are never looked up with bulk_insert operation, thus making it faster compared to other write operations.

Bulk insert offers 3 different sort modes to cater to different needs of users, based on the following principles.

- Sorting will give us good compression and upsert performance if data is laid out well. Especially if your record keys have some sort of
ordering (timestamp, etc) characteristics, sorting will assist in trimming down a lot of files during upsert. If data is 
sorted by frequently queried columns, queries will leverage parquet predicate pushdown to trim down the data to ensure lower latency as well.
  
- Additionally, parquet writing is quite a memory intensive operation. When writing large volumes of data into a table that is also partitioned
into 1000s of partitions, without sorting of any kind, the writer may have to keep 1000s of parquet writers open simultaneously incurring unsustainable
  memory pressure and eventually leading to crashes.
  
- It's also desirable to start with the smallest amount of files possible when bulk importing data, as to avoid metadata overhead 
later on for writers and queries. 

3 Sort modes supported out of the box are: `PARTITION_SORT`,`GLOBAL_SORT` and `NONE`

## Configurations 
Config to set for sort mode is 
[“hoodie.bulkinsert.sort.mode”](https://hudi.apache.org/docs/configurations.html#withBulkInsertSortMode) and different 
values are: NONE, GLOBAL_SORT and PARTITION_SORT.

## Global Sort

As the name suggests, Hudi sorts the records globally across the input partitions, which maximizes the number of files 
pruned using key ranges, during index lookups done for subsequent upserts. This is because each file has non-overlapping 
min, max values for keys, which really helps when the key has some ordering characteristics such as a time based prefix.
Given we are writing to a single parquet file on a single output partition path on storage at any given time, this mode
greatly helps control memory pressure during large partitioned writes. Also due to global sorting, each small table partition path
will be written from atmost two spark partition and thus contain just 2 files. 
This is the default sort mode. 

## Partition sort
? lower sorting overhead, but may suffer from skews.
? helps with memory pressure same as above
? each table partition path is written by just one spark partition

Records are sorted within each input partition and then written to hudi. This is often faster than global sort, since 
it avoids the initial range determination phase of spark global sorting. 


<When should I use this?> Be wary of the parallelism used, since if there
are too many spark partitions assigned to write to the same hudi partition, it could end up creating a lot of small files.

## None

For CDC kind of use-cases, record keys are mostly random. So, sorting may not give any real benefit as such. For
such use-cases, you can choose to not do any sorting only.

## User defined partitioner

Users can also implement their own [partitioner](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/BulkInsertPartitioner.java)
and plug it in with bulk insert as per necessity.

## Future: Sort merge

In future, hudi also plans to add sort merge for updates/inserts going to the same data file. This will benefit users
who wants to maintain some ordering among records for faster write and query latency.

## Bulk insert with different sort modes
Here is a microbenchmark to show the performance difference between different sort modes.

![Figure showing different sort modes in bulk_insert](/assets/images/blog/bulkinsert-sort-modes/bulkinsert-sort-modes.png)
_Figure: Shows performance of different bulk insert variants_

Two set of datasets were used, one with random keys and another one with timestamp based record keys. This benchmark 
had 10M entries being bulk inserted to hudi using different sort modes. 

## Upsert followed by bulk insert
But the real impact of sorting will be realized by a following upsert as records need to be looked up during upsert. If
records are sorted nicely, upsert operation could filter out lot of files using range pruning with bloom index. If not, 
all data files need to be looked into to search for incoming records. 

![Upsert followed by bulk_insert with different sort modes](/assets/images/blog/bulkinsert-sort-modes/upsert-sort-modes.png)

As you could see, when data is globally sorted, upserts will have lower latency since lot of data files could be filtered out.

## Conclusion
Hopefully this blog has given you good insights into different sort modes in bulk insert and when to use what.  


