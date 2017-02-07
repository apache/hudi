---
title: Comparison
keywords: usecases
sidebar: mydoc_sidebar
permalink: comparison.html
---

It would be useful to understand how Hoodie fits into the current big data ecosystem, contrasting it with a few related projects
and bring out the different tradeoffs these systems have accepted in their design.

## Kudu

[Apache Kudu]() is a storage system that has similar goals as Hoodie, which is to bring real-time analytics on petabytes of data.
Kudu diverges from a distributed file system abstraction and HDFS altogether, with its own set of storage servers talking to each
 other via RAFT. Hoodie, on the other hand, is designed to work with an underlying Hadoop compatible filesystem (HDFS,S3 or Ceph)
and does not have its own fleet of storage servers, instead relying on Apache Spark to do the heavy-lifting. Hoodie can be scaled
easily, just like other Spark jobs, while Kudu would require specialized hardware & operational support.

We


## Hive Transactions

## HBase

## RocksDB State Stores
