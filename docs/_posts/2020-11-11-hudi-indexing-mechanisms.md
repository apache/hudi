---
title: "Apache Hudi Indexing mechanisms"
excerpt: "Detailing different indexing mechanisms in Hudi and when to use each of them"
author: sivabalan
category: blog
---


## 1. Introduction
Hoodie employs index to find and update the location of incoming records during write operations. Hoodie index is a very critical piece in Hoodie as 
it gives record level lookup support to Hudi for efficient write operations. This blog talks about different indices and when to use which one. 

Hoodie dataset can be of two types in general, partitioned and non-partitioned. So, most index has two implementations one for partitioned 
dataset and another for non-partitioned called as global index. 

These are the types of index supported by Hoodie as of now. 

- InMemory
- Bloom
- Simple
- Hbase 

You could use “hoodie.index.type” to choose any of these indices. 

### 1.1 Motivation
Different workloads have different access patterns. Hudi supports different indexing schemes to cater to the needs of different workloads. 
So depending on one’s use-case, indexing schema can be chosen.
Each index goes over the use-case that it caters.

Let's take a brief look at each of these indices.

## 2. InMemory
Stores an in memory hashmap of records to location mapping. Intended to be used for local testing. May no scale in a cluster as mapping is 
stored in memory.

## 3. Bloom
Leverages bloom index stored with data files to find the location for the incoming records. This is the most commonly used Index in Hudi and is 
the default one. On a high level, this does a range pruning followed by bloom look up. So, if the record keys are laid out such that it follows 
some type of ordering like timestamps, then this will essentially cut down a lot of files to be looked up as bloom would have filtered out most 
of the files. 

For instance, consider a list of file slices in a partition with the following key ranges.
```
F1 : key_t0 to key_t10000
F2 : key_t10001 to key_t20000
F3 : key_t20001 to key_t30000
F4 : key_t30001 to key_t40000
F5 : key_t40001 to key_t50000
```

So, when looking up records ranging from key_t25000 to key_t28000, bloom will filter every file slice except F3 with range pruning. 
First range pruning is done to cut down on files to be looked up, following which actual bloom look up is done. By default this is the index 
type chosen.

It might be worth noting that range pruning is optional depending on your use-case. If your write batch is such that the records have no strict ordering in them 
, but the pattern is such that mostly the recent partitions are updated with a long tail of updates/deletes to the older partitions, 
then still bloom index speed up your write perf. So, better to turn off range pruning as it incurs the cost of checking w/o much benefit. 

## 4. Simple Index
For a decent sized dataset with random ordering, Simple index comes in handy. In the bloom index discussed above, hoodie reads the file twice. 
Once to load the file 
range info and again to load the bloom filter. So, this simple index simplifies if the data is within reasonable size. 

Baisc idea here is that, all file slices will be read for interested fiedls (record key, partition path and location) and joined 
with incoming records to find the tag location. 

Since we load only interested fields from files and join directly w/ incoming records, this works pretty well for small scale data even when 
compared to bloom index. Since in bloom, we had to load the files twice. But at larger scale, this may deteriorate since all files are touched 
w/o any upfront trimming. 

## 5. HBase
Both bloom and simple index are implicit index. In other words, there is no explicit or external index files created/stored. But Hbase is an 
external index where record locations are stored and retrieved. This is straightforward as fetch location will do a get on hbase table and 
update location will update the records in hbase. 

// talk about hbase configs? 

## 6. UserDefinedIndex
Hoodie also supports user defined index. All you need to do is to implement “org.apache.hudi.index.SparkHoodieIndex”. You can use this config 
to set the user defined class name. If this value is set, this will take precedence over “hoodie.index.type”.

## 7. Global versions 
// Talk about Global versions ? 

// Talk about Simple vs Dynamic Bloom Filter ?? 

## 8. Bloom index
As far as actual bloom filter is concerned (which is stored along with data file), Hoodie has two types, namely Simple and Dynamic. This can be 
configured using “hoodie.bloom.index.filter.type” config. 

### 8.1. Simple
Simple bloom filter is just the regular bloom filter as you might have seen elsewhere. Based on the input values set for "num of entries" and 
"false positive probability", bloom allocates the bit size and proceeds accordingly. Configs of interest are “hoodie.index.bloom.num_entries” 
and “hoodie.index.bloom.fpp”(fpp). You can check the formula used to determine the size and hash functions [here](https://github.com/apache/hudi/blob/master/hudi-common/src/main/java/org/apache/hudi/common/bloom/BloomFilterUtils.java). 
This bloom is static in the sense that the configured fpp will be honored if the entries added to bloom do not surpass the num entries set. 
But if you keep adding more entries than what was configured, then fpp may not be honored since more entries fill up more buckets. 

### 8.2. Dynamic
Compared to simple, dynamic bloom as the name suggests is dynamic in nature. It grows relatively as the number of entries increases. Basically 
users are expected to set two configs, namely “hoodie.index.bloom.num_entries” and “hoodie.bloom.index.filter.dynamic.max.entries” apart from 
the fpp. Initially bloom is allocated only for “hoodie.index.bloom.num_entries”, but as the number of entries reaches this value, the bloom 
grows to increase to 2x. This proceeds until “hoodie.bloom.index.filter.dynamic.max.entries” is reached. So until the max value is reached 
fpp is guaranteed in this bloom type. Beyond that, fpp is not guaranteed similar to Simple bloom. In general this will be beneficial compared 
to Simple as it may not allocate a larger sized bloom unless or otherwise required. Especially if you don’t have control over your incoming 
traffic it may be an unnecessary overhead to allocate a larger sized bloom upfront and never get to add so many entries as configured. 
Because, reading a larger sized bloom will have some impact on your index look up performance. 

 
// should we talk about hoodie.bloom.index.keys.per.bucket,  hoodie.bloom.index.bucketized.checking, etc. 


