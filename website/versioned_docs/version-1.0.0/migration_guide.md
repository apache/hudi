---
title: Bootstrapping
keywords: [ hudi, migration, use case]
summary: In this page, we will discuss some available tools for migrating your existing table into a Hudi table
last_modified_at: 2019-12-30T15:59:57-04:00
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
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

#### Using Hudi Streamer

Use the [Hudi Streamer](hoodie_streaming_ingestion.md#hudi-streamer) tool. HoodieStreamer supports bootstrap with 
--run-bootstrap command line option. There are two types of bootstrap, METADATA_ONLY and FULL_RECORD. METADATA_ONLY will
generate just skeleton base files with keys/footers, avoiding full cost of rewriting the dataset. FULL_RECORD will 
perform a full copy/rewrite of the data as a Hudi table.  Additionally, once can choose selective partitions using regex
patterns to apply one of the above bootstrap modes. 

Here is an example for running FULL_RECORD bootstrap on all partitions that match the regex pattern `.*` and keeping 
hive style partition with HoodieStreamer. This example configures 
[hoodie.bootstrap.mode.selector](https://hudi.apache.org/docs/configurations#hoodiebootstrapmodeselector) to 
`org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector`  which allows applying `FULL_RECORD` bootstrap 
mode to selective partitions based on the regex pattern [hoodie.bootstrap.mode.selector.regex](https://hudi.apache.org/docs/configurations#hoodiebootstrapmodeselectorregex)

```
spark-submit --master local \
--jars "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.0.jar,packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.0.jar" \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--class org.apache.hudi.utilities.streamer.HoodieStreamer `ls packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle-*.jar` \
--run-bootstrap \
--target-base-path /tmp/hoodie/bootstrap_table \
--target-table bootstrap_table \
--table-type COPY_ON_WRITE \
--hoodie-conf hoodie.bootstrap.base.path=/tmp/source_table \
--hoodie-conf hoodie.datasource.write.recordkey.field=${KEY_FIELD} \
--hoodie-conf hoodie.datasource.write.partitionpath.field=${PARTITION_FIELD} \
--hoodie-conf hoodie.datasource.write.precombine.field=${PRECOMBINE_FILED} \
--hoodie-conf hoodie.bootstrap.keygen.class=org.apache.hudi.keygen.SimpleKeyGenerator \
--hoodie-conf hoodie.bootstrap.mode.selector=org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector \
--hoodie-conf hoodie.bootstrap.mode.selector.regex='.*' \
--hoodie-conf hoodie.bootstrap.mode.selector.regex.mode=FULL_RECORD \
--hoodie-conf hoodie.datasource.write.hive_style_partitioning=true
``` 

#### Using Spark Datasource Writer

For huge tables, this could be as simple as : 
```java
for partition in [list of partitions in source table] {
        val inputDF = spark.read.format("any_input_format").load("partition_path")
        inputDF.write.format("org.apache.hudi").option()....save("basePath")
}
```  

#### Using Spark SQL CALL Procedure

Refer to [Bootstrap procedure](https://hudi.apache.org/docs/next/procedures#bootstrap) for more details. 

#### Using Hudi CLI

Write your own custom logic of how to load an existing table into a Hudi managed one. Please read about the RDD API
[here](quick-start-guide.md). Using the bootstrap run CLI. Once hudi has been built via `mvn clean install -DskipTests`, the shell can be
fired by via `cd hudi-cli && ./hudi-cli.sh`.

```java
hudi->bootstrap run --srcPath /tmp/source_table --targetPath /tmp/hoodie/bootstrap_table --tableName bootstrap_table --tableType COPY_ON_WRITE --rowKeyField ${KEY_FIELD} --partitionPathField ${PARTITION_FIELD} --sparkMaster local --hoodieConfigs hoodie.datasource.write.hive_style_partitioning=true --selectorClass org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector
```
Unlike Hudi Streamer, FULL_RECORD or METADATA_ONLY is set with --selectorClass, see details with help "bootstrap run".


## Configs

Here are the basic configs that control bootstrapping.

| Config Name                                         | Default            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| --------------------------------------------------- | ------------------ |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.bootstrap.base.path | N/A **(Required)** | Base path of the dataset that needs to be bootstrapped as a Hudi table<br /><br />`Config Param: BASE_PATH`<br />`Since Version: 0.6.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| hoodie.bootstrap.mode.selector                  | org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector (Optional)          | Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped<br />Possible values:<ul><li>`org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector`: In this mode, the full record data is not copied into Hudi therefore it avoids full cost of rewriting the dataset. Instead, 'skeleton' files containing just the corresponding metadata columns are added to the Hudi table. Hudi relies on the data in the original table and will face data-loss or corruption if files in the original table location are deleted or modified.</li><li>`org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector`: In this mode, the full record data is copied into hudi and metadata columns are added. A full record bootstrap is functionally equivalent to a bulk-insert. After a full record bootstrap, Hudi will function properly even if the original table is modified or deleted.</li><li>`org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector`: A bootstrap selector which employs bootstrap mode by specified partitions.</li></ul><br />`Config Param: MODE_SELECTOR_CLASS_NAME`<br />`Since Version: 0.6.0` |
| hoodie.bootstrap.mode.selector.regex                   | .* (Optional)                                                                                   | Matches each bootstrap dataset partition against this regex and applies the mode below to it. This is **applicable only when** `hoodie.bootstrap.mode.selector` equals `org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector`<br /><br />`Config Param: PARTITION_SELECTOR_REGEX_PATTERN`<br />`Since Version: 0.6.0`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| hoodie.bootstrap.mode.selector.regex.mode             | METADATA_ONLY (Optional)                                                                        | When specified, applies one of the possible <u>[Bootstrap Modes](https://github.com/apache/hudi/blob/bc583b4158684c23f35d787de5afda13c2865ad4/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/bootstrap/BootstrapMode.java)</u> to the partitions that match the regex provided as part of the `hoodie.bootstrap.mode.selector.regex`. For unmatched partitions the other Bootstrap Mode is applied. This is **applicable only when** `hoodie.bootstrap.mode.selector` equals `org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector`.<br />Possible values: <ul><li><u>[FULL_RECORD](https://github.com/apache/hudi/blob/bc583b4158684c23f35d787de5afda13c2865ad4/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/bootstrap/BootstrapMode.java#L36C5-L36C5)</u></li><li><u>[METADATA_ONLY](https://github.com/apache/hudi/blob/bc583b4158684c23f35d787de5afda13c2865ad4/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/bootstrap/BootstrapMode.java#L44C4-L44C4)</u></li></ul><br />`Config Param: PARTITION_SELECTOR_REGEX_MODE`<br />`Since Version: 0.6.0`                                                              |

By default, with only `hoodie.bootstrap.base.path` being provided METADATA_ONLY mode is selected. For other options, please refer [bootstrap configs](https://hudi.apache.org/docs/next/configurations#Bootstrap-Configs) for more details.

## Related Resources
<h3>Videos</h3>

* [Bootstrapping in Apache Hudi on EMR Serverless with Lab](https://www.youtube.com/watch?v=iTNLqbW3YYA)
  