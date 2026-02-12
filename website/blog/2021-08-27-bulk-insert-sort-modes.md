---
title: "Bulk Insert Sort Modes with Apache Hudi"
excerpt: "Different sort modes available with BulkInsert"
author: shivnarayan
category: blog
---

Apache Hudi supports a `bulk_insert` operation in addition to "insert" and "upsert" to ingest data into a hudi table. 
There are different sort modes that one could employ while using bulk_insert. This blog will talk about 
different sort modes available out of the box, and how each compares with others. 
<!--truncate-->

Apache Hudi supports “bulk_insert” to assist in initial loading of data to a hudi table. This is expected
to be faster when compared to using “insert” or “upsert” operations. Bulk insert differs from insert in two
aspects. Existing records are never looked up with bulk_insert, and some writer side optimizations like 
small files are not managed with bulk_insert. 

Bulk insert offers 3 different sort modes to cater to different needs of users, based on the following principles.

- Sorting will give us good compression and upsert performance, if data is laid out well. Especially if your record keys 
  have some sort of ordering (timestamp, etc) characteristics, sorting will assist in trimming down a lot of files 
  during upsert. If data is sorted by frequently queried columns, queries will leverage parquet predicate pushdown 
  to trim down the data to ensure lower latency as well.
  
- Additionally, parquet writing is quite a memory intensive operation. When writing large volumes of data into a table 
  that is also partitioned into 1000s of partitions, without sorting of any kind, the writer may have to keep 1000s of 
  parquet writers open simultaneously incurring unsustainable memory pressure and eventually leading to crashes.
  
- It's also desirable to start with the smallest amount of files possible when bulk importing data, as to avoid 
  metadata overhead later on for writers and queries. 

3 Sort modes supported out of the box are: `PARTITION_SORT`,`GLOBAL_SORT` and `NONE`.

## Configurations 
One can set the config [“hoodie.bulkinsert.sort.mode”](https://hudi.apache.org/docs/configurations.html#withBulkInsertSortMode) to either 
of the three values, namely NONE, GLOBAL_SORT and PARTITION_SORT. Default sort mode is `GLOBAL_SORT`.

## Different Sort Modes

### Global Sort

As the name suggests, Hudi sorts the records globally across the input partitions, which maximizes the number of files 
pruned using key ranges, during index lookups for subsequent upserts. This is because each file has non-overlapping 
min, max values for keys, which really helps, when the key has some ordering characteristics such as a time based prefix.
Given we are writing to a single parquet file on a single output partition path on storage at any given time, this mode
greatly helps control memory pressure during large partitioned writes. Also due to global sorting, each small table 
partition path will be written from atmost two spark partition and thus contain just 2 files. 
This is the default sort mode with bulk_insert operation in Hudi. 

### Partition sort
In this sort mode, records within a given spark partition will be sorted. But there are chances that a given spark partition 
can contain records from different table partitions. And so, even though we sort within each spark partitions, this sort
mode could result in large number of files at the end of bulk_insert, since records for a given table partition could 
be spread across many spark partitions. During actual write by the writers, we may not have much open files 
simultaneously, since we close out the file before moving to next file (as records are sorted within a spark partition) 
and hence may not have much memory pressure. 

### None

In this mode, no transformation such as sorting is done to the user records and delegated to the writers as is. So, 
when writing large volumes of data into a table partitioned into 1000s of partitions, the writer may have to keep 1000s of
parquet writers open simultaneously incurring unsustainable memory pressure and eventually leading to crashes. Also, 
min max ranges for a given file could be very wide (unsorted records) and hence subsequent upserts may read 
bloom filters from lot of files during index lookup. Since records are not sorted, and each writer could get records 
across N number of table partitions, this sort mode could result in a huge number of files at the end of bulk import. 
This could also impact your upsert or query performance due to large number of small files. 

## User defined partitioner

If none of the above built-in sort modes suffice, users can also choose to implement their own 
[partitioner](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/BulkInsertPartitioner.java)
and plug it in with bulk insert as needed.

## Bulk insert with different sort modes
Here is a microbenchmark to show the performance difference between different sort modes.

![Figure showing different sort modes in bulk_insert](/assets/images/blog/bulkinsert-sort-modes/sort-modes_perf.png) <br/>
Figure: Shows performance of different bulk insert variants

This benchmark had 10M entries being bulk inserted to hudi using different sort modes. This was followed by an upsert 
with 1M entries (10% of original dataset size). As its evident, No sorting mode has the best performance for bulk importing
data as it does not involve any sorting. Global sorting has ~15% overhead for bulk importing data when compared to
No sorting. Partition sort has less overhead (~4%) when compared to No sorting, since records need to be sorted 
but only within each executor. But the interesting thing to note is on the upsert performance. As called out earlier, 
Global sorting has lot of advantages over other two sort modes. As you could see, global sort out-performs other two sort modes 
and has 40% better upsert performance when compared to No sort mode. And even though partition sort mode does sort records, 
it shows only a moderate improvement of 5% over No sort mode due to the overhead caused by large number of small files.

## Conclusion
Hopefully this blog has given you good insights into different sort modes in bulk insert and when to use what. 
Please check out [this](https://hudi.apache.org/contribute/get-involved) if you would like to start contributing to Hudi.  


