
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Apache Hudi

Apache Hudi is an open data lakehouse platform, built on a high-performance open table format 
to ingest, index, store, serve, transform and manage your data across multiple cloud data environments.

<img src="https://hudi.apache.org/assets/images/hudi-logo-medium.png" alt="Hudi logo" height="80px" align="right" />

<https://hudi.apache.org/>

[![Build](https://github.com/apache/hudi/actions/workflows/bot.yml/badge.svg)](https://github.com/apache/hudi/actions/workflows/bot.yml)
[![Test](https://dev.azure.com/apachehudi/hudi-oss-ci/_apis/build/status/apachehudi-ci.hudi-mirror?branchName=master)](https://dev.azure.com/apachehudi/hudi-oss-ci/_build/latest?definitionId=5&branchName=master)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.hudi/hudi/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.hudi%22)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/apache/hudi)
[![Join on Slack](https://img.shields.io/badge/slack-%23hudi-72eff8?logo=slack&color=48c628&label=Join%20on%20Slack)](https://join.slack.com/t/apache-hudi/shared_invite/zt-33fabmxb7-Q7QSUtNOHYCwUdYM8LbauA)
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheHudi)](https://twitter.com/apachehudi)
[![Follow Linkedin](https://img.shields.io/badge/apache%E2%80%93hudi-0077B5?style=for-the-badge&logo=linkedin&logoColor=white&label=Follow)](https://www.linkedin.com/company/apache-hudi/?viewAsMember=true)

## Features

Hudi stores all data and metadata on cloud storage in open formats, providing the following features across different aspects.

### Ingestion

* Built-in ingestion tools for Apache Spark/Apache Flink users.
* Supports half-dozen file formats, database change logs and streaming data systems.
* Connect sink for Apache Kafka, to bring external data sources.

### Storage

* Optimized storage format, supporting row & columnar data.
* Timeline metadata to track history of changes
* Automatically manages file sizes, layout using statistics
* Savepoints for data versioning and recovery
* Schema tracking and evolution.

### Indexing

* Scalable indexing subsystem to speed up snapshot queries, maintained automatically by writes.
* Tracks file listings, column-level and partition-level statistics to help plan queries efficiently.
* Record-level indexing mechanisms built on row-oriented file formats and bloom filters.
* Logical partitioning on tables, using expression indexes to decouple from physical partitioning on storage.

### Writing

* Atomically commit data with rollback/restore support.
* Fast upsert/delete support leveraging record-level indexes.
* Snapshot isolation between writer & queries.
* Optimistic concurrency control to implement relational data model, with Read-Modify-Write style consistent writes.
* Non-blocking concurrency control, to implement streaming data model, with support for out-of-order, late data handling.

### Queries

Hudi supports different types of queries, on top of a single table. 

* **Snapshot Query** - Provides a view of the table, as of the latest committed state, accelerated with indexes as applicable.
* **Incremental Query** - Provides latest value of records inserted/updated, since a given point in time of the table. Can be used to "diff" table states between two points in time.
* **Change-Data-Capture Query** - Provides a change stream with records inserted or updated or deleted since a point in time or between two points in time. Provides both before and after images for each change record.
* **Time-Travel Query** - Provides a view of the table, as of a given point in time.
* **Read Optimized Query** - Provides excellent snapshot query performance via purely columnar storage (e.g. [Parquet](https://parquet.apache.org/)), when used with a compaction policy to provide a transaction boundary.

### Table Management

* Automatic, hands-free table services runtime integrated into Spark/Flink writers or operated independently. 
* Configurable scheduling strategies with built-in failure handling, for all table services.
* Cleaning older versions and time-to-live management to expire older data, reclaim storage space.
* Clustering and space-filling curve algorithms to optimize data layout with pluggable scheduling strategies.
* Asynchronous compaction of row oriented data into columnar formats, for efficient streaming writers.
* Consistent index building in face of ongoing queries or writers.
* Catalog sync with Apache Hive Metastore, AWS Glue, Google BigQuery, Apache XTable and more.

Learn more about Hudi at [https://hudi.apache.org](https://hudi.apache.org)

## Building Apache Hudi from source

Prerequisites for building Apache Hudi:

* Unix-like system (like Linux, Mac OS X)
* Java 8, 11 or 17
* Git
* Maven (>=3.6.0)

```
# Checkout code and build
git clone https://github.com/apache/hudi.git && cd hudi
mvn clean package -DskipTests

# Start command
spark-3.5.0-bin-hadoop3/bin/spark-shell \
  --jars `ls packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-*.*.*-SNAPSHOT.jar` \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```

To build for integration tests that include `hudi-integ-test-bundle`, use `-Dintegration-tests`.

To build the Javadoc for all Java and Scala classes (project should be already compiled):
```
# Javadoc generated under target/site/apidocs
mvn javadoc:aggregate -Pjavadocs
```

### Build with different Spark versions

The default Spark 3.x version, corresponding to `spark3` profile is
3.5.3. The default Scala version is 2.12. Scala 2.13 is supported for Spark 3.5 and above.

Refer to the table below for building with different Spark and Scala versions.

| Maven build options       | Expected Spark bundle jar name               | Notes                                            |
|:--------------------------|:---------------------------------------------|:-------------------------------------------------|
| (empty)                   | hudi-spark3.5-bundle_2.12                    | For Spark 3.5.x and Scala 2.12 (default options) |
| `-Dspark3.3`              | hudi-spark3.3-bundle_2.12                    | For Spark 3.3.2+ and Scala 2.12                  |
| `-Dspark3.4`              | hudi-spark3.4-bundle_2.12                    | For Spark 3.4.x and Scala 2.12                   |
| `-Dspark3.5 -Dscala-2.12` | hudi-spark3.5-bundle_2.12                    | For Spark 3.5.x and Scala 2.12 (same as default) |
| `-Dspark3.5 -Dscala-2.13` | hudi-spark3.5-bundle_2.13                    | For Spark 3.5.x and Scala 2.13                   |
| `-Dspark4.0`              | hudi-spark4.0-bundle_2.13                    | For Spark 4.0 and Scala 2.13 (Needs java 17)     |
| `-Dspark3`                | hudi-spark3-bundle_2.12 (legacy bundle name) | For Spark 3.5.x and Scala 2.12                   |

Please note that only Spark-related bundles, i.e., `hudi-spark-bundle`, `hudi-utilities-bundle`,
`hudi-utilities-slim-bundle`, can be built using `scala-2.13` profile. Hudi Flink bundle cannot be built
using `scala-2.13` profile. To build these bundles on Scala 2.13, use the following command:

```
# Build against Spark 3.5.x and Scala 2.13
mvn clean package -DskipTests -Dspark3.5 -Dscala-2.13 -pl packaging/hudi-spark-bundle,packaging/hudi-utilities-bundle,packaging/hudi-utilities-slim-bundle -am
```

For example,
```
# Build against Spark 3.5.x
mvn clean package -DskipTests

# Build against Spark 3.4.x
mvn clean package -DskipTests -Dspark3.4
```

#### What about "spark-avro" module?

Starting from versions 0.11, Hudi no longer requires `spark-avro` to be specified using `--packages`

### Build with different Flink versions

The default Flink version supported is 1.20. The default Flink 1.20.x version, corresponding to `flink1.20` profile is 1.20.1.
Flink is Scala-free since 1.15.x, there is no need to specify the Scala version for Flink 1.15.x and above versions.
Refer to the table below for building with different Flink and Scala versions. Besides, Flink 2.x do not support Java 8 
anymore, so it's not set as the default Flink version since Java 8 is the default Java version for Hudi now.

| Maven build options | Expected Flink bundle jar name | Notes                            |
|:--------------------|:-------------------------------|:---------------------------------|
| (empty)             | hudi-flink1.20-bundle          | For Flink 1.20 (default options) |
| `-Dflink2.1`        | hudi-flink2.1-bundle           | For Flink 2.1                    |
| `-Dflink2.0`        | hudi-flink2.0-bundle           | For Flink 2.0                    |
| `-Dflink1.20`       | hudi-flink1.20-bundle          | For Flink 1.20 (same as default) |
| `-Dflink1.19`       | hudi-flink1.19-bundle          | For Flink 1.19                   |
| `-Dflink1.18`       | hudi-flink1.18-bundle          | For Flink 1.18                   |
| `-Dflink1.17`       | hudi-flink1.17-bundle          | For Flink 1.17                   |

For example,
```
# Build against Flink 1.17.x
mvn clean package -DskipTests -Dflink1.17
```

## Running Tests

Unit tests can be run with maven profile `unit-tests`.
```
mvn -Punit-tests test
```

Functional tests, which are tagged with `@Tag("functional")`, can be run with maven profile `functional-tests`.
```
mvn -Pfunctional-tests test
```

Integration tests can be run with maven profile `integration-tests`.
```
mvn -Pintegration-tests verify
```

To run tests with spark event logging enabled, define the Spark event log directory. This allows visualizing test DAG and stages using Spark History Server UI.
```
mvn -Punit-tests test -DSPARK_EVLOG_DIR=/path/for/spark/event/log
```

## Quickstart

Please visit [https://hudi.apache.org/docs/quick-start-guide.html](https://hudi.apache.org/docs/quick-start-guide.html) to quickly explore Hudi's capabilities using spark-shell. 

## Contributing

Please check out our [contribution guide](https://hudi.apache.org/contribute/how-to-contribute) to learn more about how to contribute.
For code contributions, please refer to the [developer setup](https://hudi.apache.org/contribute/developer-setup).
