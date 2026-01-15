---
title: "Export Hudi datasets as a copy or as different formats"
excerpt: "Learn how to copy or export HUDI dataset in various formats."
authors: [xushiyan]
category: blog
tags:
- how-to
- snapshot exporter
- apache hudi
---

### Copy to Hudi dataset

Similar to the existing  `HoodieSnapshotCopier`, the Exporter scans the source dataset and then makes a copy of it to the target output path.
<!--truncate-->
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

When export to a different format, the Exporter takes parameters to do some custom re-partitioning. By default, if neither of the 2 parameters below is given, the output dataset will have no partition.

#### `--output-partition-field`

This parameter uses an existing non-metadata field as the output partitions. All  `_hoodie_*`  metadata field will be stripped during export.

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

#### `--output-partitioner`

This parameter takes in a fully-qualified name of a class that implements  `HoodieSnapshotExporter.Partitioner`. This parameter takes higher precedence than  `--output-partition-field`, which will be ignored if this is provided.

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
```

