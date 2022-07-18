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

# Usage of hudi-utilities-slim-bundle

Starting from versions 0.11, Hudi provides hudi-utilities-slim-bundle which excludes hudi-spark-datasource modules. This new bundle is intended to be used with Hudi Spark bundle together, if using
hudi-utilities-bundle solely introduces problems for a specific Spark version.

## Example with Spark 2.4.7

* Build Hudi: `mvn clean install -DskipTests`
* Run deltastreamer

```
bin/spark-submit \
  --driver-memory 4g --executor-memory 2g --num-executors 3 --executor-cores 1 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.driver.maxResultSize=1g \
  --conf spark.ui.port=6679 \
  --packages org.apache.spark:spark-avro_2.11:2.4.7 \
  --jars /path/to/hudi/packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.11-0.12.0-SNAPSHOT.jar \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls /path/to/hudi/packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.11-0.12.0-SNAPSHOT.jar` \
  --props `ls /path/to/hudi/dfs-source.properties` \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource  \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-ordering-field tpep_dropoff_datetime   \
  --table-type COPY_ON_WRITE \
  --target-base-path file:\/\/\/tmp/hudi-ny-taxi-spark24/   \
  --target-table ny_hudi_tbl  \
  --op UPSERT  \
  --continuous \
  --source-limit 5000000 \
  --min-sync-interval-seconds 60
```

## Example with Spark 3.1.3

* Build Hudi: `mvn clean install -DskipTests -Dspark3.1 -Dscala-2.12`
* Run deltastreamer

```
bin/spark-submit \
  --driver-memory 4g --executor-memory 2g --num-executors 3 --executor-cores 1 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.driver.maxResultSize=1g \
  --conf spark.ui.port=6679 \
  --packages org.apache.spark:spark-avro_2.12:3.1.3 \
  --jars /path/to/hudi/packaging/hudi-spark-bundle/target/hudi-spark3.1-bundle_2.12-0.12.0-SNAPSHOT.jar \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls /path/to/hudi/packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-0.12.0-SNAPSHOT.jar` \
  --props `ls /path/to/hudi/dfs-source.properties` \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource  \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-ordering-field tpep_dropoff_datetime   \
  --table-type COPY_ON_WRITE \
  --target-base-path file:\/\/\/tmp/hudi-ny-taxi-spark31/   \
  --target-table ny_hudi_tbl  \
  --op UPSERT  \
  --continuous \
  --source-limit 5000000 \
  --min-sync-interval-seconds 60
```

## Example with Spark 3.2.2

* Build Hudi: `mvn clean install -DskipTests -Dspark3.2 -Dscala-2.12`
* Run deltastreamer

```
bin/spark-submit \
  --driver-memory 4g --executor-memory 2g --num-executors 3 --executor-cores 1 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.driver.maxResultSize=1g \
  --conf spark.ui.port=6679 \
  --packages org.apache.spark:spark-avro_2.12:3.2.2 \
  --jars /path/to/hudi/packaging/hudi-spark-bundle/target/hudi-spark3.2-bundle_2.12-0.12.0-SNAPSHOT.jar \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls /path/to/hudi/packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-0.12.0-SNAPSHOT.jar` \
  --props `ls /path/to/hudi/dfs-source.properties` \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource  \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-ordering-field tpep_dropoff_datetime   \
  --table-type COPY_ON_WRITE \
  --target-base-path file:\/\/\/tmp/hudi-ny-taxi-spark32/   \
  --target-table ny_hudi_tbl  \
  --op UPSERT  \
  --continuous \
  --source-limit 5000000 \
  --min-sync-interval-seconds 60
```
