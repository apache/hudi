---
title: Exporter
keywords: [hudi, snapshotexporter, export]
toc: true
---

## Introduction
HoodieSnapshotExporter allows you to copy data from one location to another for backups or other purposes. 
You can write data as Hudi, Json, Orc, or Parquet file formats. In addition to copying data, you can also repartition data 
with a provided field or implement custom repartitioning by extending a class shown in detail below.

## Arguments
HoodieSnapshotExporter accepts a reference to a source path and a destination path. The utility will issue a 
query, perform any repartitioning if required and will write the data as Hudi, parquet, or json format.

|Argument|Description|Required|Note|
|------------|--------|-----------|--|
|--source-base-path|Base path for the source Hudi dataset to be snapshotted|required||
|--target-output-path|Output path for storing a particular snapshot|required||
|--output-format|Output format for the exported dataset; accept these values: json,parquet,hudi|required||
|--output-partition-field|A field to be used by Spark repartitioning|optional|Ignored when "Hudi" or when --output-partitioner is specified.The output dataset's default partition field will inherent from the source Hudi dataset.|
|--output-partitioner|A class to facilitate custom repartitioning|optional|Ignored when using output-format "Hudi"|

## Examples

### Copy a Hudi dataset

Exporter scans the source dataset and then makes a copy of it to the target output path.
```bash
spark-submit \
  --jars "packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.11-0.6.0-SNAPSHOT.jar" \
  --deploy-mode "client" \
  --class "org.apache.hudi.utilities.HoodieSnapshotExporter" \
      packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.6.0-SNAPSHOT.jar \
  --source-base-path "/tmp/" \
  --target-output-path "/tmp/exported/hudi/" \
  --output-format "hudi"
```

### Export to json or parquet dataset
The Exporter can also convert the source dataset into other formats. Currently only "json" and "parquet" are supported.

```bash
spark-submit \
  --jars "packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.11-0.6.0-SNAPSHOT.jar" \
  --deploy-mode "client" \
  --class "org.apache.hudi.utilities.HoodieSnapshotExporter" \
      packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.6.0-SNAPSHOT.jar \
  --source-base-path "/tmp/" \
  --target-output-path "/tmp/exported/json/" \
  --output-format "json"  # or "parquet"
```

### Re-partitioning
When exporting to a different format, the Exporter takes the `--output-partition-field` parameter to do some custom re-partitioning.
Note: All `_hoodie_*` metadata fields will be stripped during export, so make sure to use an existing non-metadata field as the output partitions.

By default, if no partitioning parameters are given, the output dataset will have no partition.

Example:
```bash
spark-submit \
  --jars "packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.11-0.6.0-SNAPSHOT.jar" \
  --deploy-mode "client" \
  --class "org.apache.hudi.utilities.HoodieSnapshotExporter" \
      packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.6.0-SNAPSHOT.jar \  
  --source-base-path "/tmp/" \
  --target-output-path "/tmp/exported/json/" \
  --output-format "json" \
  --output-partition-field "symbol"  # assume the source dataset contains a field `symbol`
```

The output directory will look like this

```bash
`_SUCCESS symbol=AMRS symbol=AYX symbol=CDMO symbol=CRC symbol=DRNA ...`
```

### Custom Re-partitioning
`--output-partitioner` parameter takes in a fully-qualified name of a class that implements `HoodieSnapshotExporter.Partitioner`. 
This parameter takes higher precedence than `--output-partition-field`, which will be ignored if this is provided.

An example implementation is shown below:

**MyPartitioner.java**
```java
package com.foo.bar;
public class MyPartitioner implements HoodieSnapshotExporter.Partitioner {

  private static final String PARTITION_NAME = "date";
 
  @Override
  public DataFrameWriter<Row> partition(Dataset<Row> source) {
    // use the current hoodie partition path as the output partition
    return source
        .withColumnRenamed(HoodieRecord.PARTITION_PATH_METADATA_FIELD, PARTITION_NAME)
        .repartition(new Column(PARTITION_NAME))
        .write()
        .partitionBy(PARTITION_NAME);
  }
}
```

After putting this class in `my-custom.jar`, which is then placed on the job classpath, the submit command will look like this:

```bash
spark-submit \
  --jars "packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.11-0.6.0-SNAPSHOT.jar,my-custom.jar" \
  --deploy-mode "client" \
  --class "org.apache.hudi.utilities.HoodieSnapshotExporter" \
      packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.6.0-SNAPSHOT.jar \
  --source-base-path "/tmp/" \
  --target-output-path "/tmp/exported/json/" \
  --output-format "json" \
  --output-partitioner "com.foo.bar.MyPartitioner"