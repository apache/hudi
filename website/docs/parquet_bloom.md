---
title: Parquet Bloom Filters
keywords: [ hudi, index ]
summary: This section offers an overview of parquet bloom in Hudi
toc: true
last_modified_at: 2023-06-26T15:59:57-04:00
---

Since Hudi 0.14.0, for engines based on Parquet 1.12, parquet bloom filters feature has been included. In this section, we will show a guide on how to enable parquet blooms in Hudi tables.

## Various bloom support in hudi

From almost the beginning hudi has supported dynamic bloom indexing on the record key, stored in the parquet footers - aka hudi blooms. 

Since 0.11 Hudi, and the multimodal indexing feature, the hudi blooms can be stored in the metadata table. Also arbitrary columns can be indexed. Storing the blooms within the metadata allow to skip reading the footers, and increase performances in huge tables scenarios.

Until now, hudi blooms are used at write time only. They are leveraged during the write operation to identify the files to be later merged.

In parallel parquet 1.12 came with it's own bloom filters - aka "parquet blooms". Those are also stored in the parquet footers, when enabled before writing the parquet files. Then at read time, if bloom matches the query predicates, the parquet engine will transparently use the blooms to skip reading data.

Now hudi supports both kind of blooms, which help in complementary contexts. All COW operations are supported, bulk_insert, insert, upsert, delete, clustering...

The current page describes how to enable parquet blooms in spark 3.x and starting from hudi 1.14.0, on a COW table.

## How parquet bloom can speedup read queries ?

Parquet has various statistics to speedup read queries (min/max, dictionaries, nulls ...). Dictionaries, already covers
cases when the column contains duplicates and has less than 40 000 unique values. In this case it stores the list of
unique values and makes blooms useless.

So bloom would be useful in either case (at the parquet file level) :

- the column has no duplicates
- the column number of unique values is more than 40k

## How should I choose the NDV

NDV is the number of distinct values and is used by parquet to size the bloom. Bloom precision (in order to limit the
false positive) is a tradeoff on its size. Then you should choose a NDV representing your own data.

## Setup parquet blooms before writes

First, make sure Hudi Spark 3.x bundle jar and hudi 0.14.0 are used.

Here is an example.

```java
JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
// create a parquet bloom on rider column
jsc.hadoopConfiguration().set("parquet.bloom.filter.enabled#rider", "true")
jsc.hadoopConfiguration().set("parquet.bloom.filter.expected.ndv#rider", "20")

QuickstartUtils.DataGenerator dataGen = new QuickstartUtils.DataGenerator();
List<String> inserts = convertToStringList(dataGen.generateInserts(3));
Dataset<Row> inputDF1 = spark.read().json(jsc.parallelize(inserts, 1));
inputDF1.write().format("org.apache.hudi")
	.option("hoodie.table.name", "bloom_table")
    .option("hoodie.upsert.shuffle.parallelism","2")
    .option("hoodie.insert.shuffle.parallelism","2")
    .option("hoodie.delete.shuffle.parallelism","2")
    .option("hoodie.bulkinsert.shuffle.parallelism","2")
    .mode(SaveMode.Overwrite)
    .save("path");

spark.read().format("org.apache.hudi").load("path").filter("rider = 'easy'").count();
```

Then the rider column parquet blooms will allow to skip reading a high number of parquet file, depending on the bloom tuning.

Read more from [Parquet docs](https://github.com/apache/parquet-mr/tree/parquet-1.12.x/parquet-hadoop).

### Note

This feature is currently only available for COW tables due to only Parquet base files present there.