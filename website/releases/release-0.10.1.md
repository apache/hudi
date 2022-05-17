---
title: "Release 0.10.1"
sidebar_position: 3
layout: releases
toc: true
last_modified_at: 2022-01-27T22:07:00+08:00
---
# [Release 0.10.1](https://github.com/apache/hudi/releases/tag/release-0.10.1) ([docs](/docs/quick-start-guide))

## Migration Guide

* This release (0.10.1) does not introduce any new table version, hence no migration needed if you are on 0.10.0.
* If migrating from an older release, please check the migration guide from the previous release notes, specifically the upgrade instructions in 0.6.0, 0.9.0 and 0.10.0.

## Release Highlights

### Explicit Spark 3 bundle names

In the previous release (0.10.0), we added Spark 3.1.x support and made it the default Spark 3 version to build with. In 0.10.1,
we made the Spark 3 version explicit in the bundle name and published a new bundle for Spark 3.0.x. Specifically, these 2 bundles
are available in the public maven repository.

* `hudi-spark3.1.2-bundle_2.12-0.10.1.jar`
* `hudi-spark3.0.3-bundle_2.12-0.10.1.jar`

### Repair Utility

We added a new repair utility `org.apache.hudi.utilities.HoodieRepairTool` to clean up dangling base and log files. The utility
can be run as a separate Spark job as below.

```
spark-submit \
--class org.apache.hudi.utilities.HoodieRepairTool \
--driver-memory 4g \
--executor-memory 1g \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.catalogImplementation=hive \
--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
--packages org.apache.spark:spark-avro_2.12:3.1.2 \
$HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.11.0-SNAPSHOT.jar \
--mode dry_run \
--base-path base_path \
--assume-date-partitioning
```

Check out the javadoc in `HoodieRepairTool` for more instructions and examples.

### Bug fixes

0.10.1 is mainly intended for bug fixes and stability. The fixes span across many components, including

* HoodieDeltaStreamer
* Timeline related fixes
* Table services
* Metadata table
* Spark SQL support
* Timestamp-based key generator
* Hive Sync
* Flink and Java engines
* Kafka Connect

## Raw Release Notes

The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12351135)