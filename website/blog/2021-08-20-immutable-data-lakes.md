---
title: "Immutable data lakes using Apache Hudi"
excerpt: "How to leverage Apache Hudi for your immutable (or) append only data use-case"
author: shivnarayan
category: blog
---

Apache Hudi helps you build and manage data lakes with different table types, config knobs to cater to everyone's need.
We strive to listen to community and build features based on the need. From our interactions with the community, we got 
to know there are quite a few use-cases where Hudi is being used for immutable or append only data. This blog will go 
over details on how to leverage Apache Hudi in building your data lake for such immutable or append only data.

# Immutable data
Often times, users route log entries to data lakes, where data is immutable. (Add some concrete 
examples here). Data once ingested won't be updated and can only be deleted. Also, most likely, deletes are issued at 
partition level (delete partitions older than 1 week) granularity.

# Immutable data lakes using Apache Hudi 
Hudi has an efficient way to ingest data into Hudi for such immutable use-cases. "Bulk_Insert" operation in Hudi is 
commonly used for initial bootstrapping of data into hudi, but also exactly fits the bill for such immutable or append 
only data. And it is known to be performant when compared to regular "insert"s or "upsert"s. 

## Bulk_insert vs regular Inserts/Upserts
With regular inserts and upserts, Hudi executes few steps before data can be written to data files. For example, 
index lookup, small file handling, etc has to be performed before actual write. But with bulk_insert, such overhead can 
be avoided since data is known to be immutable. 

Here is an illustration of steps involved in different operations of interest. 

![Inserts/Upserts](/assets/images/blog/immutable_datalakes/immutable_data_lakes1.jpeg)

_Figure: High level steps on Insert/Upsert operation with Hudi._

![Bulk_Insert](/assets/images/blog/immutable_datalakes/immutable_data_lakes2.jpeg)

_Figure: High level steps on Bulk_insert operation with Hudi._

As you could see, bulk_insert skips the unnecessary step of indexing and small file handling which could bring down 
your write latency by a large degree for append only data. And bulk_insert also supports "Row writer" path which 
is known to be performant compared to Rdd path (WriteClient). So, users can enjoy the blazing fast writes for such 
immutable data using bulk_insert operation and row writer.

:::note
There won't be any small file handling with bulk_insert. But users can choose to leverage Clustering to batch small 
files into larger ones if need be. 
:::

## Configurations
Users need to set the write operation config `hoodie.datasource.write.operation` to "bulk_insert". To leverage row 
writer, one has to enable `hoodie.datasource.write.row.writer.enable`. Default value of this config is false. 

## Supported Operations
Even though this is catered towards immutable data, all operations are supported for a hudi table in general. Once an 
issue deletes, enable metadata, add clustering etc to these tables. Just that users can leverage bulk_insert for faster 
writes compared to other operations by bypassing the additional overhead. 

Hudi is also adding Virtual key support in upcoming release, and users can also enable virtual keys for such immutable 
data to reduce storage cost as well.

## Conclusion
For immutable use-cases with occasional deletes, users can leverge bulk_insert operation in Hudi for faster write 
latencies and still enjoy all the niceities of Hudi like snapshot query, point in time query, incremental query, etc.
Hope this blog was useful for you to learn yet another feature in Apache Hudi. If you are interested in
Hudi and looking to contribute, do check out [here](https://hudi.apache.org/contribute/get-involved). 








