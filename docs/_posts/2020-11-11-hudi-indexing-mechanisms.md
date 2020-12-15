---
title: "Apache Hudi Indexing mechanisms"
excerpt: "Detailing different indexing mechanisms in Hudi and when to use each of them"
author: sivabalan
category: blog
---


## Introduction
Hudi employs index to find and update the location of incoming records during write operations. To be specific, index assist in differentiating 
inserts vs updates. This blog talks about different indices and when to each of them.

Hudi dataset can be of two types in general, partitioned and non-partitioned. So, most index has two implementations, one for partitioned dataset 
and another for non-partitioned called as global index.

These are the types of index supported by Hudi as of now.

- InMemory
- Bloom
- Simple
- Hbase

You could use “hoodie.index.type” to choose any of these indices.

## Different workloads
Since data comes in at different volumes, velocity and has different access patterns, different indices could be used for different workloads. 
Let’s walk through some of the typical workloads and see how to leverage Hudi index for such use-cases.

### Fact table
These are typical primary table in a dimensional model. It contains measures or quantitative figures and is used for analysis and decision making. 
For eg, trip tables in case of ride-sharing, user buying and selling of shares, or any other similar use-case can be categorized as fact tables. 
These tables are usually ever growing with random updates on most recent data with long tail of older data. In other words, most updates go into 
the latest partitions with few updates going to older ones.

![Fact table](/assets/images/blog/hudi-indexes/Hudi_Index_Blog_Fact_table.png)
Figure showing the spread of updates for Fact table.

Hudi "BLOOM" index is the way to go for these kinds of tables, since index look-up will prune a lot of data files. So, effectively actual look up will 
happen only in a very few data files where the records are most likely present. This bloom index will also benefit a lot for use-cases where record 
keys have some kind of ordering (timestamp) among them. File pruning will cut down a lot of data files to be looked up resulting in very fast look-up times.
On a high level, bloom index does pruning based on ranges of data files, followed by bloom filter look up. Depending on the workload, this could 
result in a lot of shuffling depending on the amount of data touched. Hudi is planning to support [record level indexing](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+08+%3A+Record+level+indexing+mechanisms+for+Hudi+datasets?src=contextnavpagetreemode) 
which would support record level index look-ups. This will boost the index look up performance as it avoids these shuffling.

Many a times, users query using non-partitioned columns, and Hudi is looking to add support for [secondary indexes](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+and+Query+Planning+Improvements?src=contextnavpagetreemode) to cater 
to such use-cases. Once this is available, queries based on secondary index should see a bump in their perf numbers.

### Event table
Events coming from kafka or similar message bus or event streams which is typically time series data and is huge in size. For eg, IoT event stream, click 
streams, impressions etc. Inserts and updates span the last few partitions as these are mostly append only data. Also deduping is a common requirement 
since events could come in from different sources. 

![Event table](/assets/images/blog/hudi-indexes/Hudi_Index_Blog_Event_table.png)
Figure showing the spread of updates for Event table.

As you might have guessed, Hoodie "BLOOM" index is a good fit here as it could assist in pruning lot of data files to be looked up. And since hudi does support 
deduping, such use-cases should find it beneficial to onboard to Hudi. As mentioned above, Hudi bloom index will do range lookup followed up bloom filter 
lookup. As range lookup does incur latency to read all data file footers, Hudi is looking to leverage range information stored in [optimized 
metadata](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+and+Query+Planning+Improvements) at O(1). Once this is integrated, 
we expect the index lookups to be much more fast.

### Dimensions table
These types of tables usually contain high dimensional data and hold reference data. These are high fidelity data and the updates are often small but also spread 
across a lot of partitions and data files ranging across the dataset from old to new.

![Dimensions table](/assets/images/blog/hudi-indexes/Hudi_Index_Blog_dimensions_table.png)
Figure showing the spread of updates for Dimensions table.

Since user information will be spread across different partitions and data files, "Simple" Index will be a good fit as it does not do any upfront pruning based 
on index, but directly joins with interested fields from every data file. Hbase index could also be used as it supports record level lookups. As seen above, 
Hudi has plans to support [record level indexing](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+08+%3A+Record+level+indexing+mechanisms+for+Hudi+datasets?src=contextnavpagetreemode)
which will improve the index look-up time and will also avoid additional overhead of maintaining an external system like hbase. 

If your record keys are completely random, then most likely range filtering will be ineffective and can’t say much about the efficacy of HudiBloomIndex. 
So, "SIMPLE" index could fit the bill in these cases too since it avoids making an attempt to prune with ranges which anyway could be in vain and does 
direct join with interested fields.

Hopefully we were able to give you good enough context on what indices are supported by Hudi and which one to use depending on your use-case. Please feel free 
to reach out to us if you have doubts on deciding which index to pick for your use-case.

