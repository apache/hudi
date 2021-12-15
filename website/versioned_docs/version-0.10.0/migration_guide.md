---
title: Bootstrapping
keywords: [ hudi, migration, use case]
summary: In this page, we will discuss some available tools for migrating your existing table into a Hudi table
last_modified_at: 2019-12-30T15:59:57-04:00
---

Hudi maintains metadata such as commit timeline and indexes to manage a table. The commit timelines helps to understand the actions happening on a table as well as the current state of a table. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently locate a record. At the moment, Hudi supports writing only parquet columnar formats.
To be able to start using Hudi for your existing table, you will need to migrate your existing table into a Hudi managed table. There are a couple of ways to achieve this.


## Approaches


### Use Hudi for new partitions alone

Hudi can be used to manage an existing table without affecting/altering the historical data already present in the
table. Hudi has been implemented to be compatible with such a mixed table with a caveat that either the complete
Hive partition is Hudi managed or not. Thus the lowest granularity at which Hudi manages a table is a Hive
partition. Start using the datasource API or the WriteClient to write to the table and make sure you start writing
to a new partition or convert your last N partitions into Hudi instead of the entire table. Note, since the historical
 partitions are not managed by HUDI, none of the primitives provided by HUDI work on the data in those partitions. More concretely, one cannot perform upserts or incremental pull on such older partitions not managed by the HUDI table.
Take this approach if your table is an append only type of table and you do not expect to perform any updates to existing (or non Hudi managed) partitions.


### Convert existing table to Hudi

Import your existing table into a Hudi managed table. Since all the data is Hudi managed, none of the limitations
 of Approach 1 apply here. Updates spanning any partitions can be applied to this table and Hudi will efficiently
 make the update available to queries. Note that not only do you get to use all Hudi primitives on this table,
 there are other additional advantages of doing this. Hudi automatically manages file sizes of a Hudi managed table
 . You can define the desired file size when converting this table and Hudi will ensure it writes out files
 adhering to the config. It will also ensure that smaller files later get corrected by routing some new inserts into
 small files rather than writing new small ones thus maintaining the health of your cluster.

There are a few options when choosing this approach.

**Option 1**
Use the HDFSParquetImporter tool. As the name suggests, this only works if your existing table is in parquet file format.
This tool essentially starts a Spark Job to read the existing parquet table and converts it into a HUDI managed table by re-writing all the data.

**Option 2**
For huge tables, this could be as simple as : 
```java
for partition in [list of partitions in source table] {
        val inputDF = spark.read.format("any_input_format").load("partition_path")
        inputDF.write.format("org.apache.hudi").option()....save("basePath")
}
```  

**Option 3**
Write your own custom logic of how to load an existing table into a Hudi managed one. Please read about the RDD API
 [here](/docs/quick-start-guide). Using the HDFSParquetImporter Tool. Once hudi has been built via `mvn clean install -DskipTests`, the shell can be
fired by via `cd hudi-cli && ./hudi-cli.sh`.

```java
hudi->hdfsparquetimport
        --upsert false
        --srcPath /user/parquet/table/basepath
        --targetPath /user/hoodie/table/basepath
        --tableName hoodie_table
        --tableType COPY_ON_WRITE
        --rowKeyField _row_key
        --partitionPathField partitionStr
        --parallelism 1500
        --schemaFilePath /user/table/schema
        --format parquet
        --sparkMemory 6g
        --retry 2
```
