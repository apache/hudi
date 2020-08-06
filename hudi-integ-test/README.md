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

This page describes in detail how to run end to end tests on a hudi dataset that helps in improving our confidence 
in a release as well as perform large scale performance benchmarks.  

# Objectives

1. Test with different versions of core libraries and components such as `hdfs`, `parquet`, `spark`, 
`hive` and `avro`.
2. Generate different types of workloads across different dimensions such as `payload size`, `number of updates`, 
`number of inserts`, `number of partitions`
3. Perform multiple types of operations such as `insert`, `bulk_insert`, `upsert`, `compact`, `query`
4. Support custom post process actions and validations

# High Level Design

The Hudi test suite runs as a long running spark job. The suite is divided into the following high level components : 

## Workload Generation

This component does the work of generating the workload; `inserts`, `upserts` etc.

## Workload Scheduling

Depending on the type of workload generated, data is either ingested into the target hudi 
dataset or the corresponding workload operation is executed. For example compaction does not necessarily need a workload
to be generated/ingested but can require an execution.

## Other actions/operatons

The test suite supports different types of operations besides ingestion such as Hive Query execution, Clean action etc.

# Usage instructions


## Entry class to the test suite

```
org.apache.hudi.testsuite.HoodieTestSuiteJob.java - Entry Point of the hudi test suite job. This 
class wraps all the functionalities required to run a configurable integration suite.
```

## Configurations required to run the job
```
org.apache.hudi.testsuite.HoodieTestSuiteJob.HoodieTestSuiteConfig - Config class that drives the behavior of the 
integration test suite. This class extends from com.uber.hoodie.utilities.DeltaStreamerConfig. Look at 
link#HudiDeltaStreamer page to learn about all the available configs applicable to your test suite.
```

## Generating a custom Workload Pattern

There are 2 ways to generate a workload pattern

 1.Programatically

Choose to write up the entire DAG of operations programatically, take a look at `WorkflowDagGenerator` class.
Once you're ready with the DAG you want to execute, simply pass the class name as follows:

```
spark-submit
...
...
--class org.apache.hudi.testsuite.HoodieTestSuiteJob 
--workload-generator-classname org.apache.hudi.testsuite.dag.scheduler.<your_workflowdaggenerator>
...
```

 2.YAML file

Choose to write up the entire DAG of operations in YAML, take a look at `complex-workload-dag-cow.yaml` or 
`complex-workload-dag-mor.yaml`.
Once you're ready with the DAG you want to execute, simply pass the yaml file path as follows:

```
spark-submit
...
...
--class org.apache.hudi.testsuite.HoodieTestSuiteJob 
--workload-yaml-path /path/to/your-workflow-dag.yaml
...
```

## Building the test suite

The test suite can be found in the `hudi-integ-test` module. Use the `prepare_integration_suite.sh` script to 
build 
the test suite, you can provide different parameters to the script.

```
shell$ ./prepare_integration_suite.sh --help
Usage: prepare_integration_suite.sh
   --spark-command, prints the spark command
   -h, hdfs-version
   -s, spark version
   -p, parquet version
   -a, avro version
   -s, hive version
```

```
shell$ ./prepare_integration_suite.sh
....
....
Final command : mvn clean install -DskipTests
```

## Running on the cluster or in your local machine
Copy over the necessary files and jars that are required to your cluster and then run the following spark-submit 
command after replacing the correct values for the parameters. 
NOTE : The properties-file should have all the necessary information required to ingest into a Hudi dataset. For more
 information on what properties need to be set, take a look at the test suite section under demo steps.
```
shell$ ./prepare_integration_suite.sh --spark-command
spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 --master prepare_integration_suite.sh --deploy-mode
--properties-file  --class org.apache.hudi.integ.testsuite.HoodieTestSuiteJob target/hudi-integ-test-0.6
.0-SNAPSHOT.jar --source-class  --source-ordering-field  --input-base-path  --target-base-path  --target-table  --props  --storage-type  --payload-class  --workload-yaml-path  --input-file-size  --<deltastreamer-ingest>
```

## Running through a test-case (local)
Take a look at the `TestHoodieTestSuiteJob` to check how you can run the entire suite using JUnit.

## Running an end to end test suite in Local Docker environment

```
docker exec -it adhoc-2 /bin/bash
# COPY_ON_WRITE tables
=========================
## Run the following command to start the test suite
spark-submit \ 
--packages com.databricks:spark-avro_2.11:4.0.0 \ 
--conf spark.task.cpus=1 \ 
--conf spark.executor.cores=1 \ 
--conf spark.task.maxFailures=100 \ 
--conf spark.memory.fraction=0.4 \ 
--conf spark.rdd.compress=true \ 
--conf spark.kryoserializer.buffer.max=2000m \ 
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \ 
--conf spark.memory.storageFraction=0.1 \ 
--conf spark.shuffle.service.enabled=true \ 
--conf spark.sql.hive.convertMetastoreParquet=false \ 
--conf spark.ui.port=5555 \ 
--conf spark.driver.maxResultSize=12g \ 
--conf spark.executor.heartbeatInterval=120s \ 
--conf spark.network.timeout=600s \ 
--conf spark.eventLog.overwrite=true \ 
--conf spark.eventLog.enabled=true \ 
--conf spark.yarn.max.executor.failures=10 \ 
--conf spark.sql.catalogImplementation=hive \ 
--conf spark.sql.shuffle.partitions=1000 \ 
--class org.apache.hudi.testsuite.HoodieTestSuiteJob $HUDI_TEST_SUITE_BUNDLE \
--source-ordering-field timestamp \ 
--target-base-path /user/hive/warehouse/hudi-integ-test-suite/output \ 
--input-base-path /user/hive/warehouse/hudi-integ-test-suite/input \ 
--target-table test_table \ 
--props /var/hoodie/ws/docker/demo/config/testsuite/test-source.properties \ 
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \ 
--source-limit 300000 \ 
--source-class org.apache.hudi.utilities.sources.AvroDFSSource \ 
--input-file-size 125829120 \ 
--workload-yaml-path /var/hoodie/ws/docker/demo/config/testsuite/complex-workflow-dag-cow.yaml \ 
--storage-type COPY_ON_WRITE \ 
--compact-scheduling-minshare 1 \ 
--hoodie-conf "hoodie.deltastreamer.source.test.num_partitions=100" \ 
--hoodie-conf "hoodie.deltastreamer.source.test.datagen.use_rocksdb_for_storing_existing_keys=false" \ 
--hoodie-conf "hoodie.deltastreamer.source.test.max_unique_records=100000000" \ 
--hoodie-conf "hoodie.embed.timeline.server=false" \ 
--hoodie-conf "hoodie.datasource.write.recordkey.field=_row_key" \ 
--hoodie-conf "hoodie.deltastreamer.source.dfs.root=/user/hive/warehouse/hudi-integ-test-suite/input" \ 
--hoodie-conf "hoodie.datasource.write.keygenerator.class=org.apache.hudi.ComplexKeyGenerator" \ 
--hoodie-conf "hoodie.datasource.write.partitionpath.field=timestamp" \ 
--hoodie-conf "hoodie.deltastreamer.schemaprovider.source.schema.file=/var/hoodie/ws/docker/demo/config/testsuite/source.avsc" \ 
--hoodie-conf "hoodie.datasource.hive_sync.assume_date_partitioning=false" \ 
--hoodie-conf "hoodie.datasource.hive_sync.jdbcurl=jdbc:hive2://hiveserver:10000/" \ 
--hoodie-conf "hoodie.datasource.hive_sync.database=testdb" \ 
--hoodie-conf "hoodie.datasource.hive_sync.table=test_table" \ 
--hoodie-conf "hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.NonPartitionedExtractor" \ 
--hoodie-conf "hoodie.datasource.hive_sync.assume_date_partitioning=true" \ 
--hoodie-conf "hoodie.datasource.write.keytranslator.class=org.apache.hudi.DayBasedPartitionPathKeyTranslator" \ 
--hoodie-conf "hoodie.deltastreamer.schemaprovider.target.schema.file=/var/hoodie/ws/docker/demo/config/testsuite/source.avsc"
...
...
2019-11-03 05:44:47 INFO  DagScheduler:69 - ----------- Finished workloads ----------
2019-11-03 05:44:47 INFO  HoodieTestSuiteJob:138 - Finished scheduling all tasks
...
2019-11-03 05:44:48 INFO  SparkContext:54 - Successfully stopped SparkContext
# MERGE_ON_READ tables
=========================
## Run the following command to start the test suite
spark-submit \ 
--packages com.databricks:spark-avro_2.11:4.0.0 \ 
--conf spark.task.cpus=1 \ 
--conf spark.executor.cores=1 \ 
--conf spark.task.maxFailures=100 \ 
--conf spark.memory.fraction=0.4 \ 
--conf spark.rdd.compress=true \ 
--conf spark.kryoserializer.buffer.max=2000m \ 
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \ 
--conf spark.memory.storageFraction=0.1 \ 
--conf spark.shuffle.service.enabled=true \ 
--conf spark.sql.hive.convertMetastoreParquet=false \ 
--conf spark.ui.port=5555 \ 
--conf spark.driver.maxResultSize=12g \ 
--conf spark.executor.heartbeatInterval=120s \ 
--conf spark.network.timeout=600s \ 
--conf spark.eventLog.overwrite=true \ 
--conf spark.eventLog.enabled=true \ 
--conf spark.yarn.max.executor.failures=10 \ 
--conf spark.sql.catalogImplementation=hive \ 
--conf spark.sql.shuffle.partitions=1000 \ 
--class org.apache.hudi.testsuite.HoodieTestSuiteJob $HUDI_TEST_SUITE_BUNDLE \
--source-ordering-field timestamp \ 
--target-base-path /user/hive/warehouse/hudi-integ-test-suite/output \ 
--input-base-path /user/hive/warehouse/hudi-integ-test-suite/input \ 
--target-table test_table \ 
--props /var/hoodie/ws/docker/demo/config/testsuite/test-source.properties \ 
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \ 
--source-limit 300000 \ 
--source-class org.apache.hudi.utilities.sources.AvroDFSSource \ 
--input-file-size 125829120 \ 
--workload-yaml-path /var/hoodie/ws/docker/demo/config/testsuite/complex-workflow-dag-mor.yaml \ 
--storage-type MERGE_ON_READ \ 
--compact-scheduling-minshare 1 \ 
--hoodie-conf "hoodie.deltastreamer.source.test.num_partitions=100" \ 
--hoodie-conf "hoodie.deltastreamer.source.test.datagen.use_rocksdb_for_storing_existing_keys=false" \ 
--hoodie-conf "hoodie.deltastreamer.source.test.max_unique_records=100000000" \ 
--hoodie-conf "hoodie.embed.timeline.server=false" \ 
--hoodie-conf "hoodie.datasource.write.recordkey.field=_row_key" \ 
--hoodie-conf "hoodie.deltastreamer.source.dfs.root=/user/hive/warehouse/hudi-integ-test-suite/input" \ 
--hoodie-conf "hoodie.datasource.write.keygenerator.class=org.apache.hudi.ComplexKeyGenerator" \ 
--hoodie-conf "hoodie.datasource.write.partitionpath.field=timestamp" \ 
--hoodie-conf "hoodie.deltastreamer.schemaprovider.source.schema.file=/var/hoodie/ws/docker/demo/config/testsuite/source.avsc" \ 
--hoodie-conf "hoodie.datasource.hive_sync.assume_date_partitioning=false" \ 
--hoodie-conf "hoodie.datasource.hive_sync.jdbcurl=jdbc:hive2://hiveserver:10000/" \ 
--hoodie-conf "hoodie.datasource.hive_sync.database=testdb" \ 
--hoodie-conf "hoodie.datasource.hive_sync.table=test_table" \ 
--hoodie-conf "hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.NonPartitionedExtractor" \ 
--hoodie-conf "hoodie.datasource.hive_sync.assume_date_partitioning=true" \ 
--hoodie-conf "hoodie.datasource.write.keytranslator.class=org.apache.hudi.DayBasedPartitionPathKeyTranslator" \ 
--hoodie-conf "hoodie.deltastreamer.schemaprovider.target.schema.file=/var/hoodie/ws/docker/demo/config/testsuite/source.avsc"
...
...
2019-11-03 05:44:47 INFO  DagScheduler:69 - ----------- Finished workloads ----------
2019-11-03 05:44:47 INFO  HoodieTestSuiteJob:138 - Finished scheduling all tasks
...
2019-11-03 05:44:48 INFO  SparkContext:54 - Successfully stopped SparkContext
```
 