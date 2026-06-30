---
title: "How Hudi helps even when you dont care about mutability"
excerpt: "How to leverage Apache Hudi for your immutable (or) append only data use-case"
author: shivnarayan
category: blog
---

Apache Hudi helps you build and manage data lakes with different table types, config knobs to cater to everyone's need.
We strive to listen to community and build features based on the need. From our interactions with the community, we got 
to know there are quite a few use-cases where Hudi is being used for immutable or append only data. While a lot of our 
content/talks refer heavily to mutability in the context of incremental data processing, Hudi also excels with such immutable
use-cases. This blog will go over details on how to leverage Apache Hudi in building your data lake for such immutable or append only data.
<!--truncate-->

# Immutable data
Often times, users route log entries to data lakes, where data is immutable. E.g sensor values streamed out of devices. Data once ingested won't be updated and can only be deleted. Also, most likely, deletes are issued at 
partition level (delete partitions older than 1 week) granularity.

# Immutable data lakes using Apache Hudi 
Hudi has an efficient way to ingest data into Hudi for such immutable use-cases. "bulk_insert" operation in Hudi is 
commonly used for initial loading of data into hudi, but also exactly fits the bill for such immutable or append 
only data. And it is also to be performant when compared to regular "insert"s or "upsert"s, since avoids a bunch of write optimizations done primarily for incremental updates. 

## Bulk_insert vs regular Inserts/Upserts
With regular inserts and upserts, Hudi executes few steps before data can be written to data files. For example, 
index lookup, small file handling, etc has to be performed before actual write. But with bulk_insert, such overhead can 
be avoided since data is known to be immutable. 

Here is an illustration of steps involved in different operations of interest. 

![Inserts/Upserts](/assets/images/blog/immutable_datalakes/immutable_data_lakes1.png)

_Figure: High level steps on Insert/Upsert operation with Hudi._

![Bulk_Insert](/assets/images/blog/immutable_datalakes/immutable_data_lakes2.png)

_Figure: High level steps on Bulk_insert operation with Hudi._

As you could see, bulk_insert skips the unnecessary step of indexing and small file handling which could bring down 
your write latency by a large degree for append only data.

:::note
Users can choose to leverage Clustering to batch small files into larger ones if need be, even for the bulk_insert path.
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








