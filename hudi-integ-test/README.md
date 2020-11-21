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

## Other actions/operations

The test suite supports different types of operations besides ingestion such as Hive Query execution, Clean action etc.

# Usage instructions


## Entry class to the test suite

```
org.apache.hudi.integ.testsuite.HoodieTestSuiteJob.java - Entry Point of the hudi test suite job. This  
class wraps all the functionalities required to run a configurable integration suite.
```

## Configurations required to run the job
```
org.apache.hudi.integ.testsuite.HoodieTestSuiteJob.HoodieTestSuiteConfig - Config class that drives the behavior of the  
integration test suite. This class extends from com.uber.hoodie.utilities.DeltaStreamerConfig. Look at 
link#HudiDeltaStreamer page to learn about all the available configs applicable to your test suite.
```

## Generating a custom Workload Pattern

There are 2 ways to generate a workload pattern

 1.Programmatically

You can create a DAG of operations programmatically - take a look at `WorkflowDagGenerator` class.
Once you're ready with the DAG you want to execute, simply pass the class name as follows:

```
spark-submit
...
...
--class org.apache.hudi.integ.testsuite.HoodieTestSuiteJob 
--workload-generator-classname org.apache.hudi.integ.testsuite.dag.scheduler.<your_workflowdaggenerator>
...
```

 2.YAML file

Choose to write up the entire DAG of operations in YAML, take a look at `complex-dag-cow.yaml` or 
`complex-dag-mor.yaml`.
Once you're ready with the DAG you want to execute, simply pass the yaml file path as follows:

```
spark-submit
...
...
--class org.apache.hudi.integ.testsuite.HoodieTestSuiteJob  
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

Start the Hudi Docker demo:

```
docker/setup_demo.sh
```

NOTE: We need to make a couple of environment changes for Hive 2.x support. This will be fixed once Hudi moves to Spark 3.x.
Execute below if you are using Hudi query node in your dag. If not, below section is not required. 
Also, for longer running tests, go to next section. 

```
docker exec -it adhoc-2 bash

cd /opt/spark/jars
rm /opt/spark/jars/hive*
rm spark-hive-thriftserver_2.11-2.4.4.jar

wget https://repo1.maven.org/maven2/org/apache/spark/spark-hive-thriftserver_2.12/3.0.0-preview2/spark-hive-thriftserver_2.12-3.0.0-preview2.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-common/2.3.1/hive-common-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.1/hive-exec-2.3.1-core.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/2.3.1/hive-jdbc-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-llap-common/2.3.1/hive-llap-common-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/2.3.1/hive-metastore-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-serde/2.3.1/hive-serde-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-service/2.3.1/hive-service-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-service-rpc/2.3.1/hive-service-rpc-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/shims/hive-shims-0.23/2.3.1/hive-shims-0.23-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/shims/hive-shims-common/2.3.1/hive-shims-common-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-storage-api/2.3.1/hive-storage-api-2.3.1.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-shims/2.3.1/hive-shims-2.3.1.jar
wget https://repo1.maven.org/maven2/org/json/json/20090211/json-20090211.jar
cp /opt/hive/lib/log* /opt/spark/jars/
rm log4j-slf4j-impl-2.6.2.jar

cd /opt

```

Copy the integration tests jar into the docker container

```
docker cp packaging/hudi-integ-test-bundle/target/hudi-integ-test-bundle-0.6.1-SNAPSHOT.jar adhoc-2:/opt
```

```
docker exec -it adhoc-2 /bin/bash
```

Clean the working directories before starting a new test:

```
hdfs dfs -rm -r /user/hive/warehouse/hudi-integ-test-suite/output/
hdfs dfs -rm -r /user/hive/warehouse/hudi-integ-test-suite/input/
```

Launch a Copy-on-Write job:

```
# COPY_ON_WRITE tables
=========================
## Run the following command to start the test suite
spark-submit \
--packages org.apache.spark:spark-avro_2.11:2.4.0 \
--conf spark.task.cpus=1 \
--conf spark.executor.cores=1 \
--conf spark.task.maxFailures=100 \
--conf spark.memory.fraction=0.4  \
--conf spark.rdd.compress=true  \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.memory.storageFraction=0.1 \
--conf spark.shuffle.service.enabled=true  \
--conf spark.sql.hive.convertMetastoreParquet=false  \
--conf spark.driver.maxResultSize=12g \
--conf spark.executor.heartbeatInterval=120s \
--conf spark.network.timeout=600s \
--conf spark.yarn.max.executor.failures=10 \
--conf spark.sql.catalogImplementation=hive \
--class org.apache.hudi.integ.testsuite.HoodieTestSuiteJob \
/opt/hudi-integ-test-bundle-0.6.1-SNAPSHOT.jar \
--source-ordering-field test_suite_source_ordering_field \
--use-deltastreamer \
--target-base-path /user/hive/warehouse/hudi-integ-test-suite/output \
--input-base-path /user/hive/warehouse/hudi-integ-test-suite/input \
--target-table table1 \
--props file:/var/hoodie/ws/docker/demo/config/test-suite/test.properties \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-class org.apache.hudi.utilities.sources.AvroDFSSource \
--input-file-size 125829120 \
--workload-yaml-path file:/var/hoodie/ws/docker/demo/config/test-suite/complex-dag-cow.yaml \
--workload-generator-classname org.apache.hudi.integ.testsuite.dag.WorkflowDagGenerator \
--table-type COPY_ON_WRITE \
--compact-scheduling-minshare 1
```

Or a Merge-on-Read job:
```
# MERGE_ON_READ tables
=========================
## Run the following command to start the test suite
spark-submit \
--packages org.apache.spark:spark-avro_2.11:2.4.0 \
--conf spark.task.cpus=1 \
--conf spark.executor.cores=1 \
--conf spark.task.maxFailures=100 \
--conf spark.memory.fraction=0.4  \
--conf spark.rdd.compress=true  \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.memory.storageFraction=0.1 \
--conf spark.shuffle.service.enabled=true  \
--conf spark.sql.hive.convertMetastoreParquet=false  \
--conf spark.driver.maxResultSize=12g \
--conf spark.executor.heartbeatInterval=120s \
--conf spark.network.timeout=600s \
--conf spark.yarn.max.executor.failures=10 \
--conf spark.sql.catalogImplementation=hive \
--class org.apache.hudi.integ.testsuite.HoodieTestSuiteJob \
/opt/hudi-integ-test-bundle-0.6.1-SNAPSHOT.jar \
--source-ordering-field test_suite_source_ordering_field \
--use-deltastreamer \
--target-base-path /user/hive/warehouse/hudi-integ-test-suite/output \
--input-base-path /user/hive/warehouse/hudi-integ-test-suite/input \
--target-table table1 \
--props file:/var/hoodie/ws/docker/demo/config/test-suite/test.properties \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-class org.apache.hudi.utilities.sources.AvroDFSSource \
--input-file-size 125829120 \
--workload-yaml-path file:/var/hoodie/ws/docker/demo/config/test-suite/complex-dag-mor.yaml \
--workload-generator-classname org.apache.hudi.integ.testsuite.dag.WorkflowDagGenerator \
--table-type MERGE_ON_READ \
--compact-scheduling-minshare 1
``` 

For long running test suite, validation has to be done differently. Idea is to run same dag in a repeated manner. 
Hence "ValidateDatasetNode" is introduced which will read entire input data and compare it with hudi contents both via 
spark datasource and hive table via spark sql engine.

If you have "ValidateDatasetNode" in your dag, do not replace hive jars as instructed above. Spark sql engine does not 
go well w/ hive2* jars. So, after running docker setup, just copy test.properties and your dag of interest and you are 
good to go ahead. 

For repeated runs, two additional configs need to be set. "dag_rounds" and "dag_intermittent_delay_mins". 
This means that your dag will be repeated for N times w/ a delay of Y mins between each round.

Also, ValidateDatasetNode can be configured in two ways. Either with "delete_input_data: true" set or not set. 
When "delete_input_data" is set for ValidateDatasetNode, once validation is complete, entire input data will be deleted. 
So, suggestion is to use this ValidateDatasetNode as the last node in the dag with "delete_input_data". 
Example dag: 
```
     Insert
     Upsert
     ValidateDatasetNode with delete_input_data = true
```

If above dag is run with "dag_rounds" = 10 and "dag_intermittent_delay_mins" = 10, then this dag will run for 10 times 
with 10 mins delay between every run. At the end of every run, records written as part of this round will be validated. 
At the end of each validation, all contents of input are deleted.   
For eg: incase of above dag, 
```
Round1: 
    insert => inputPath/batch1
    upsert -> inputPath/batch2
    Validate with delete_input_data = true
              Validates contents from batch1 and batch2 are in hudi and ensures Row equality
              Since "delete_input_data" is set, deletes contents from batch1 and batch2.
Round2:    
    insert => inputPath/batch3
    upsert -> inputPath/batch4
    Validate with delete_input_data = true
              Validates contents from batch3 and batch4 are in hudi and ensures Row equality
              Since "delete_input_data" is set, deletes contents from batch3 and batch4.
Round3:    
    insert => inputPath/batch5
    upsert -> inputPath/batch6
    Validate with delete_input_data = true
              Validates contents from batch5 and batch6 are in hudi and ensures Row equality
              Since "delete_input_data" is set, deletes contents from batch5 and batch6.   
.
.
```
If you wish to do a cumulative validation, do not set delete_input_data in ValidateDatasetNode. But remember that this 
may not scale beyond certain point since input data as well as hudi content's keeps occupying the disk and grows for 
every cycle.

Lets see an example where you don't set "delete_input_data" as part of Validation. 
```
Round1: 
    insert => inputPath/batch1
    upsert -> inputPath/batch2
    Validate: validates contents from batch1 and batch2 are in hudi and ensures Row equality
Round2:    
    insert => inputPath/batch3
    upsert -> inputPath/batch4
    Validate: validates contents from batch1 to batch4 are in hudi and ensures Row equality
Round3:    
    insert => inputPath/batch5
    upsert -> inputPath/batch6
    Validate: validates contents from batch1 and batch6 are in hudi and ensures Row equality
.
.
```

You could also have validations in the middle of your dag and not set the "delete_input_data". But set it only in the 
last node in the dag.
```
Round1: 
    insert => inputPath/batch1
    upsert -> inputPath/batch2
    Validate: validates contents from batch1 and batch2 are in hudi and ensures Row equality
    insert => inputPath/batch3
    upsert -> inputPath/batch4
    Validate with delete_input_data = true
             Validates contents from batch1 to batch4 are in hudi and ensures Row equality
             since "delete_input_data" is set to true, this node deletes contents from batch1 and batch4.
Round2: 
    insert => inputPath/batch5
    upsert -> inputPath/batch6
    Validate: validates contents from batch5 and batch6 are in hudi and ensures Row equality
    insert => inputPath/batch7
    upsert -> inputPath/batch8
    Validate: validates contents from batch5 to batch8 are in hudi and ensures Row equality
             since "delete_input_data" is set to true, this node deletes contents from batch5 to batch8.
Round3: 
    insert => inputPath/batch9
    upsert -> inputPath/batch10
    Validate: validates contents from batch9 and batch10 are in hudi and ensures Row equality
    insert => inputPath/batch11
    upsert -> inputPath/batch12
     Validate with delete_input_data = true
             Validates contents from batch9 to batch12 are in hudi and ensures Row equality
             Set "delete_input_data" to true. so this node deletes contents from batch9 to batch12. 
.
.
```
Above dag was just an example for illustration purposes. But you can make it complex as per your needs.
```
    Insert
    Upsert
    Delete
    Validate w/o deleting
    Insert
    Rollback
    Validate w/o deleting
    Upsert
    Validate w/ deletion
```                        
With this dag, you can set the two additional configs "dag_rounds" and "dag_intermittent_delay_mins" and have a long 
running test suite. 

```
dag_rounds: 1
dag_intermittent_delay_mins: 10
dag_content:
    Insert
    Upsert
    Delete
    Validate w/o deleting
    Insert
    Rollback
    Validate w/o deleting
    Upsert
    Validate w/ deletion

```

Sample COW command with repeated runs. 
```
spark-submit \
--packages org.apache.spark:spark-avro_2.11:2.4.0 \
--conf spark.task.cpus=1 \
--conf spark.executor.cores=1 \
--conf spark.task.maxFailures=100 \
--conf spark.memory.fraction=0.4  \
--conf spark.rdd.compress=true  \
--conf spark.kryoserializer.buffer.max=2000m \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.memory.storageFraction=0.1 \
--conf spark.shuffle.service.enabled=true  \
--conf spark.sql.hive.convertMetastoreParquet=false  \
--conf spark.driver.maxResultSize=12g \
--conf spark.executor.heartbeatInterval=120s \
--conf spark.network.timeout=600s \
--conf spark.yarn.max.executor.failures=10 \
--conf spark.sql.catalogImplementation=hive \
--conf spark.driver.extraClassPath=/var/demo/jars/* \
--conf spark.executor.extraClassPath=/var/demo/jars/* \
--class org.apache.hudi.integ.testsuite.HoodieTestSuiteJob \
/opt/hudi-integ-test-bundle-0.6.1-SNAPSHOT.jar \
--source-ordering-field test_suite_source_ordering_field \
--use-deltastreamer \
--target-base-path /user/hive/warehouse/hudi-integ-test-suite/output \
--input-base-path /user/hive/warehouse/hudi-integ-test-suite/input \
--target-table table1 \
--props test.properties \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-class org.apache.hudi.utilities.sources.AvroDFSSource \
--input-file-size 125829120 \
--workload-yaml-path file:/var/hoodie/ws/docker/demo/config/test-suite/complex-dag-cow.yaml \
--workload-generator-classname org.apache.hudi.integ.testsuite.dag.WorkflowDagGenerator \
--table-type COPY_ON_WRITE \
--compact-scheduling-minshare 1 
```

A ready to use dag is available under docker/demo/config/test-suite/ that could give you an idea for long running 
dags.
cow-per-round-mixed-validate.yaml

As of now, "ValidateDatasetNode" uses spark data source and hive tables for comparison. Hence COW and real time view in 
MOR can be tested.
              
                        