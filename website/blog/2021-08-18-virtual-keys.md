---
title: "Adding support for Virtual Keys in Hudi"
excerpt: "Supporting Virtual keys in Hudi for reducing storage overhead"
author: shivnarayan
category: blog
tags:
- design
- metadata
- apache hudi
---

Apache Hudi helps you build and manage data lakes with different table types, config knobs to cater to everyone's need.
Hudi adds per record metadata fields like `_hoodie_record_key`, `_hoodie_partition path`, `_hoodie_commit_time` which serves multiple purposes. 
They assist in avoiding re-computing the record key, partition path during merges, compaction and other table operations 
and also assists in supporting [record-level](/blog/2021/07/21/streaming-data-lake-platform#readers) incremental queries (in comparison to other table formats, that merely track files).
In addition, it ensures data quality by ensuring unique key constraints are enforced even if the key field changes for a given table, during its lifetime.
But one of the repeated asks from the community is to leverage existing fields and not to add additional meta fields, for simple use-cases where such benefits are not desired or key changes are very rare.  
<!--truncate-->

## Virtual Key support
Hudi now supports virtual keys, where Hudi meta fields can be computed on demand from the data fields. Currently, the meta fields are 
computed once and stored as per record metadata and re-used across various operations. If one does not need incremental query support, 
they can start leveraging Hudi's Virtual key support and still go about using Hudi to build and manage their data lake to reduce the storage 
overhead due to per record metadata. 

### Configurations
Virtual keys can be enabled for a given table using the below config. When set to `hoodie.populate.meta.fields=false`, 
Hudi will use virtual keys for the corresponding table. Default value for this config is `true`, which means, all  meta fields will be added by default.

Once virtual keys are enabled, it can't be disabled for a given hudi table, because already stored records may not have 
the meta fields populated. But if you have an existing table from an older version of hudi, virtual keys can be enabled. 
Another constraint w.r.t virtual key support is that, Key generator properties for a given table cannot be changed through
the course of the lifecycle of a given hudi table. In this model, the user also shares responsibility of ensuring uniqueness 
of key within a table. For instance, if you configure record key to point to `field_5` for few batches of write and later switch to `field_10`, 
Hudi cannot guarantee uniqueness of key, since older writes could have had duplicates for `field_10`. 

With virtual keys, keys will have to be re-computed everytime when in need (merges, compaction, MOR snapshot read). Hence we 
support virtual keys for all built-in key generators on Copy-On-Write tables. Supporting all key generators on Merge-On-Read table 
would entail reading all fields out of base and delta logs, sacrificing core columnar query performance, which will be prohibitively expensive 
for users. Thus, we support only simple key generators (the default key generator, where both record key and partition path refer
to an existing field ) for now.

#### Supported Key Generators with CopyOnWrite(COW) table:
SimpleKeyGenerator, ComplexKeyGenerator, CustomKeyGenerator, TimestampBasedKeyGenerator and NonpartitionedKeyGenerator. 

#### Supported Key Generators with MergeOnRead(MOR) table:
SimpleKeyGenerator

#### Supported Index types: 
Only "SIMPLE" and "GLOBAL_SIMPLE" index types are supported in the first cut. We plan to add support for other index 
(BLOOM, etc) in future releases. 

### Supported Operations
All existing features are supported for a hudi table with virtual keys, except the incremental 
queries. Which means, cleaning, archiving, metadata table, clustering, etc can be enabled for a hudi table with 
virtual keys enabled. So, you are able to merely use Hudi as a transactional table format with all the awesome 
table service runtimes and platform services, if you wish to do so, without incurring any overheads associated with 
support for incremental data processing.

### Sample Output
As called out earlier, one has to set `hoodie.populate.meta.fields=false` to enable virtual keys. Let's see the 
difference between records of a hudi table with and without virtual keys.

Here are some sample records for a regular hudi table (virtual keys disabled)

```
+--------------------+--------------------------------------+--------------------------------------+---------+---------+-------------------+
|_hoodie_commit_time |           _hoodie_record_key         |        _hoodie_partition_path        |  rider  | driver  |        fare       |
+--------------------+--------------------------------------+--------------------------------------+---------+---------+-------------------+
|   20210825154123   | eb7819f1-6f04-429d-8371-df77620b9527 | americas/united_states/san_francisco |rider-284|driver-284|98.3428192817987  |
|   20210825154123   | 37ea44f1-fda7-4ec4-84de-f43f5b5a4d84 | americas/united_states/san_francisco |rider-213|driver-213|19.179139106643607|
|   20210825154123   | aa601d6b-7cc5-4b82-9687-675d0081616e | americas/united_states/san_francisco |rider-213|driver-213|93.56018115236618 |
|   20210825154123   | 494bc080-881c-48be-8f8a-8f1739781816 | americas/united_states/san_francisco |rider-284|driver-284|90.9053809533154  |
|   20210825154123   | 09573277-e1c1-4cdd-9b45-57176f184d4d | americas/united_states/san_francisco |rider-284|driver-284|49.527694252432056|
|   20210825154123   | c9b055ed-cd28-4397-9704-93da8b2e601f | americas/brazil/sao_paulo            |rider-213|driver-213|43.4923811219014  |
|   20210825154123   | e707355a-b8c0-432d-a80f-723b93dc13a8 | americas/brazil/sao_paulo            |rider-284|driver-284|63.72504913279929 |
|   20210825154123   | d3c39c9e-d128-497a-bf3e-368882f45c28 | americas/brazil/sao_paulo            |rider-284|driver-284|91.99515909032544 |
|   20210825154123   | 159441b0-545b-460a-b671-7cc2d509f47b | asia/india/chennai                   |rider-284|driver-284|9.384124531808036 |
|   20210825154123   | 16031faf-ad8d-4968-90ff-16cead211d3c | asia/india/chennai                   |rider-284|driver-284|90.25710109008239 |
+--------------------+--------------------------------------+--------------------------------------+---------+----------+------------------+
```

And here are some sample records for a hudi table with virtual keys enabled.

```
+--------------------+------------------------+-------------------------+---------+---------+-------------------+
|_hoodie_commit_time |    _hoodie_record_key  |  _hoodie_partition_path |  rider  | driver  |        fare       |
+--------------------+------------------------+-------------------------+---------+---------+-------------------+
|        null        |            null        |          null           |rider-284|driver-284|98.3428192817987  |
|        null        |            null        |          null           |rider-213|driver-213|19.179139106643607|
|        null        |            null        |          null           |rider-213|driver-213|93.56018115236618 |
|        null        |            null        |          null           |rider-284|driver-284|90.9053809533154  |
|        null        |            null        |          null           |rider-284|driver-284|49.527694252432056|
|        null        |            null        |          null           |rider-213|driver-213|43.4923811219014  |
|        null        |            null        |          null           |rider-284|driver-284|63.72504913279929 |
|        null        |            null        |          null           |rider-284|driver-284|91.99515909032544 |
|        null        |            null        |          null           |rider-284|driver-284|9.384124531808036 |
|        null        |            null        |          null           |rider-284|driver-284|90.25710109008239 |
+--------------------+------------------------+-------------------------+---------+----------+------------------+
```

:::note
As you could see, all meta fields are null in storage, but all users fields remain intact similar to a regular table.
:::

### Incremental Queries
Since hudi does not maintain any metadata (like commit time at a record level) for a table with virtual keys enabled,  
incremental queries are not supported. An exception will be thrown as below when an incremental query is triggered for such
a table.

```
scala> val tripsIncrementalDF = spark.read.format("hudi").
     |   option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
     |   option(BEGIN_INSTANTTIME_OPT_KEY, "20210827180901").load(basePath)
org.apache.hudi.exception.HoodieException: Incremental queries are not supported when meta fields are disabled
  at org.apache.hudi.IncrementalRelation.<init>(IncrementalRelation.scala:69)
  at org.apache.hudi.DefaultSource.createRelation(DefaultSource.scala:120)
  at org.apache.hudi.DefaultSource.createRelation(DefaultSource.scala:67)
  at org.apache.spark.sql.execution.datasources.DataSource.resolveRelation(DataSource.scala:344)
  at org.apache.spark.sql.DataFrameReader.loadV1Source(DataFrameReader.scala:297)
  at org.apache.spark.sql.DataFrameReader.$anonfun$load$2(DataFrameReader.scala:286)
  at scala.Option.getOrElse(Option.scala:189)
  at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:286)
  at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:232)
  ... 61 elided
```

### Conclusion 
Hope this blog was useful for you to learn yet another feature in Apache Hudi. If you are interested in 
Hudi and looking to contribute, do check out [here](https://hudi.apache.org/contribute/get-involved). 








