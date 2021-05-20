---
title: "Bulk Insert Sort Modes with Apache Hudi"
excerpt: "Different sort modes available with BulkInsert"
author: shivnarayan
category: blog
---

Apache Hudi supports an operation called “bulk_insert” to assist in initial loading to data to hudi. This is expected 
to be faster when compared to using “insert” or “upsert” as operation type. Bulk insert differs from insert in few 
aspects. Small files optimization is not available with “bulk insert”, where as “insert” does small file management. 
And there is no de-dup support with “bulk insert” operation. In other words, if your batch to be bulk inserted has 
duplicate entries, hudi will also have duplicate entries.

Bulk insert offers 3 different sort modes to cater to different needs of users. In general, sorting will give us 
good compression and upsert performance if data is layed out well. Especially if your record keys have some sort of 
ordering (timestamp, etc) to it, sorting will assist in trimming down a lot of files during upsert. If data is sorted 
via some of the mostly queried columns, queries will leverage parquet predicate pushdown to trim down the data to 
ensure lower latency as well.

3 Sort modes supported out of the box are: PARTITION_SORT, GLOBAL_SORT and NONE

Config to set for sort mode: [“hoodie.bulkinsert.sort.mode”](https://hudi.apache.org/docs/configurations.html#withBulkInsertSortMode)

# Partition sort

Records are sorted within each spark partition and then written to hudi. This expected to be faster than Global sort 
and if one does not need global sort, should resort to this sort mode. Be wary of the parallelism used, since if there 
are too many spark partitions assigned to write to the same hudi partition, it could end up creating a lot of small files.

# Global Sort

As the name suggests, all records are sorted globally before being written. This is the default sort mode with 
“bulk_insert” operation. Since records are globally sorted in this mode, if record keys have some ordering characteristics,
this will benefit a lot during upsert to trim down a lot of files.

# None

For CDC kind of use-cases, record keys are mostly random. So, sorting may not give any real benefit as such. For 
such use-cases, you can choose to not do any sorting only.

# User defined partitioner

Users can also implement their own [partitioner](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/BulkInsertPartitioner.java)
and plug it in with bulk insert as per necessity.

# Future: Sort merge

In future, hudi also plans to add sort merge for updates/inserts going to the same data file. This will benefit users
who wants to maintain some ordering among records for faster write and query latency.

# Direct Row writing

In general, all operations converts incoming dataframe into a Rdd<HoodieRecodPayload> and then proceed with any write 
operation in Apache Hudi. But conversion to Rdd incurs some cost and 
[hoodie.datasource.write.row.writer.enable](https://hudi.apache.org/docs/configurations.html#ENABLE_ROW_WRITER_OPT_KEY) 
config knob can assist in doing bulk insert without any conversion to Rdd. This is expected to be ~30 to 40% faster compared 
to regular bulk_insert.
Note: If row writing is enabled, different sort modes are not supported yet. Row writing will go global sorting with bulk insert.

Here is a microbenchmark to show the performance difference between different sort modes.

![Initial layout](/assets/images/blog/bulk_insert_sort_modes.png)
_Figure: Shows performance of different bulk insert variants_

Record keys had timestamp component to it, so as to show the impact of sorting. This benchmark had 10M entries being 
bulk inserted to hudi followed by 250k upserts. This was followed by a read query which did some aggregation and had a 
filtering on one of the columns in the dataset. 

As you might have guessed, sorting will incur some additional latency when compared to no sorting during bulk insert.
But the real impact of sorting will be realized by a following upsert as records need to be looked up with upsert. If 
records are well sorted, upserts could filter out lot of files using range pruning with bloom index. As you could see, 
even though Global sorting has higher latency for bulk inserts, upserts will have lower latency as records. And as 
mentioned above, enabling Row writing gives very good benefit even with global sorting. 

If record keys are random UUIDs and if your use-case may not have any upserts, then global sorting may be of much 
benefit to you. Hopefully this blog has given you good insights into different variants in bulk insert. 







