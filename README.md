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
Apache Hudi (pronounced Hoodie) stands for `Hadoop Upserts Deletes and Incrementals`. 
Hudi manages the storage of large analytical datasets on DFS (Cloud stores, HDFS or any Hadoop FileSystem compatible storage).

<https://hudi.apache.org/>

[![Build](https://github.com/apache/hudi/actions/workflows/bot.yml/badge.svg)](https://github.com/apache/hudi/actions/workflows/bot.yml)
[![Test](https://dev.azure.com/apache-hudi-ci-org/apache-hudi-ci/_apis/build/status/apachehudi-ci.hudi-mirror?branchName=master)](https://dev.azure.com/apache-hudi-ci-org/apache-hudi-ci/_build/latest?definitionId=3&branchName=master)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.hudi/hudi/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.hudi%22)
[![Join on Slack](https://img.shields.io/badge/slack-%23hudi-72eff8?logo=slack&color=48c628&label=Join%20on%20Slack)](https://join.slack.com/t/apache-hudi/shared_invite/enQtODYyNDAxNzc5MTg2LTE5OTBlYmVhYjM0N2ZhOTJjOWM4YzBmMWU2MjZjMGE4NDc5ZDFiOGQ2N2VkYTVkNzU3ZDQ4OTI1NmFmYWQ0NzE)

## Features
* Upsert support with fast, pluggable indexing
* Atomically publish data with rollback support
* Snapshot isolation between writer & queries 
* Savepoints for data recovery
* Manages file sizes, layout using statistics
* Async compaction of row & columnar data
* Timeline metadata to track lineage
* Optimize data lake layout with clustering
 
Hudi supports three types of queries:
 * **Snapshot Query** - Provides snapshot queries on real-time data, using a combination of columnar & row-based storage (e.g [Parquet](https://parquet.apache.org/) + [Avro](https://avro.apache.org/docs/current/mr.html)).
 * **Incremental Query** - Provides a change stream with records inserted or updated after a point in time.
 * **Read Optimized Query** - Provides excellent snapshot query performance via purely columnar storage (e.g. [Parquet](https://parquet.apache.org/)).

Learn more about Hudi at [https://hudi.apache.org](https://hudi.apache.org)

## Building Apache Hudi from source

Prerequisites for building Apache Hudi:

* Unix-like system (like Linux, Mac OS X)
* Java 8 (Java 9 or 10 may work)
* Git
* Maven (>=3.3.1)

```
# Checkout code and build
git clone https://github.com/apache/hudi.git && cd hudi
mvn clean package -DskipTests

# Start command
spark-2.4.4-bin-hadoop2.7/bin/spark-shell \
  --jars `ls packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.11-*.*.*-SNAPSHOT.jar` \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
```

To build the Javadoc for all Java and Scala classes:
```
# Javadoc generated under target/site/apidocs
mvn clean javadoc:aggregate -Pjavadocs
```

### Build with Scala 2.12

The default Scala version supported is 2.11. To build for Scala 2.12 version, build using `scala-2.12` profile

```
mvn clean package -DskipTests -Dscala-2.12
```

### Build with Spark 3.0.0

The default Spark version supported is 2.4.4. To build for Spark 3.0.0 version, build using `spark3` profile

```
mvn clean package -DskipTests -Dspark3
```

### Build without spark-avro module

The default hudi-jar bundles spark-avro module. To build without spark-avro module, build using `spark-shade-unbundle-avro` profile

```
# Checkout code and build
git clone https://github.com/apache/hudi.git && cd hudi
mvn clean package -DskipTests -Pspark-shade-unbundle-avro

# Start command
spark-2.4.4-bin-hadoop2.7/bin/spark-shell \
  --packages org.apache.spark:spark-avro_2.11:2.4.4 \
  --jars `ls packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.11-*.*.*-SNAPSHOT.jar` \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
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

To run tests with spark event logging enabled, define the Spark event log directory. This allows visualizing test DAG and stages using Spark History Server UI.
```
mvn -Punit-tests test -DSPARK_EVLOG_DIR=/path/for/spark/event/log
```

## Quickstart

Please visit [https://hudi.apache.org/docs/quick-start-guide.html](https://hudi.apache.org/docs/quick-start-guide.html) to quickly explore Hudi's capabilities using spark-shell. 
