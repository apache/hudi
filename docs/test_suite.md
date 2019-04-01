---
title: Test Suite
keywords: test suite
sidebar: mydoc_sidebar
permalink: test_suite.html
toc: false
summary: In this page, we will discuss the Hudi Test suite to perform end to end tests

This page describes in detail how to run end to end tests on a hudi dataset that helps in improving our confidence 
in a release as well as perform large scale performance benchmarks.  

### Objectives

1. Test with different versions of core libraries and components such as `hdfs`, `parquet`, `spark`, 
`hive` and `avro`.
2. Generate different types of workloads across different dimensions such as `payload size`, `number of updates`, 
`number of inserts`, `number of partitions`
3. Perform multiple types of operations such as `insert`, `bulk_insert`, `upsert`, `compact`, `query`
4. Support custom post process actions and validations

### High Level Design

The Hudi test suite runs as a long running spark job. The suite is divided into the following high level components : 

##### Workload Generation

This component does the work of generating the workload; `inserts`, `upserts` etc.

##### Workload Scheduling

Depending on the type of workload generated, data is either ingested into the target hudi 
dataset or the corresponding workload operation is executed. For example compaction does not necessarily need a workload
to be generated/ingested but can require an execution.

#### Other actions/operatons

The test suite supports different types of operations besides ingestion such as Hive Query execution, Clean action etc.

### Usage instructions


##### Entry class to the test suite

```
org.apache.hudi.bench.job.HudiTestSuiteJob.java - Entry Point of the hudi test suite job. This 
class wraps all the functionalities required to run a configurable integration suite.
```

##### Configurations required to run the job
```
org.apache.hudi.bench.job.HudiTestSuiteConfig - Config class that drives the behavior of the 
integration test suite. This class extends from com.uber.hoodie.utilities.DeltaStreamerConfig. Look at 
link#HudiDeltaStreamer page to learn about all the available configs applicable to your test suite.
```

##### Generating a custom Workload Pattern
```
There are 2 ways to generate a workload pattern

1. Programatically
Choose to write up the entire DAG of operations programatically, take a look at WorkflowDagGenerator class.
Once you're ready with the DAG you want to execute, simply pass the class name as follows
spark-submit
...
...
--class org.apache.hudi.bench.job.HudiTestSuiteJob 
--workload-generator-classname org.apache.hudi.bench.dag.scheduler.<your_workflowdaggenerator>
...
2. YAML file
Choose to write up the entire DAG of operations in YAML, take a look at complex-workload-dag-cow.yaml or 
complex-workload-dag-mor.yaml.
Once you're ready with the DAG you want to execute, simply pass the yaml file path as follows
spark-submit
...
...
--class org.apache.hudi.bench.job.HudiTestSuiteJob 
--workload-yaml-path /path/to/your-workflow-dag.yaml
...
```

#### Building the test suite

The test suite can be found in the `hudi-bench` module. Use the `prepare_integration_suite.sh` script to build 
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
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO]
[INFO] Hudi ............................................... SUCCESS [  2.749 s]
[INFO] hudi-common ........................................ SUCCESS [ 12.711 s]
[INFO] hudi-timeline-service .............................. SUCCESS [  1.924 s]
[INFO] hudi-hadoop-mr ..................................... SUCCESS [  7.203 s]
[INFO] hudi-client ........................................ SUCCESS [ 10.486 s]
[INFO] hudi-hive .......................................... SUCCESS [  5.159 s]
[INFO] hudi-spark ......................................... SUCCESS [ 34.499 s]
[INFO] hudi-utilities ..................................... SUCCESS [  8.626 s]
[INFO] hudi-cli ........................................... SUCCESS [ 14.921 s]
[INFO] hudi-bench ......................................... SUCCESS [  7.706 s]
[INFO] hudi-hadoop-mr-bundle .............................. SUCCESS [  1.873 s]
[INFO] hudi-hive-bundle ................................... SUCCESS [  1.508 s]
[INFO] hudi-spark-bundle .................................. SUCCESS [ 17.432 s]
[INFO] hudi-presto-bundle ................................. SUCCESS [  1.309 s]
[INFO] hudi-utilities-bundle .............................. SUCCESS [ 18.386 s]
[INFO] hudi-timeline-server-bundle ........................ SUCCESS [  8.600 s]
[INFO] hudi-bench-bundle .................................. SUCCESS [ 38.348 s]
[INFO] hudi-hadoop-docker ................................. SUCCESS [  2.053 s]
[INFO] hudi-hadoop-base-docker ............................ SUCCESS [  0.806 s]
[INFO] hudi-hadoop-namenode-docker ........................ SUCCESS [  0.302 s]
[INFO] hudi-hadoop-datanode-docker ........................ SUCCESS [  0.403 s]
[INFO] hudi-hadoop-history-docker ......................... SUCCESS [  0.447 s]
[INFO] hudi-hadoop-hive-docker ............................ SUCCESS [  1.534 s]
[INFO] hudi-hadoop-sparkbase-docker ....................... SUCCESS [  0.315 s]
[INFO] hudi-hadoop-sparkmaster-docker ..................... SUCCESS [  0.407 s]
[INFO] hudi-hadoop-sparkworker-docker ..................... SUCCESS [  0.447 s]
[INFO] hudi-hadoop-sparkadhoc-docker ...................... SUCCESS [  0.410 s]
[INFO] hudi-hadoop-presto-docker .......................... SUCCESS [  0.697 s]
[INFO] hudi-integ-test .................................... SUCCESS [01:02 min]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 04:23 min
[INFO] Finished at: 2019-11-02T23:56:48-07:00
[INFO] Final Memory: 234M/1582M
[INFO] ------------------------------------------------------------------------

```

##### Running on the cluster or in your local machine
Copy over the necessary files and jars that are required to your cluster and then run the following spark-submit 
command after replacing the correct values for the parameters. 
NOTE : The properties-file should have all the necessary information required to ingest into a Hudi dataset. For more
 information on what properties need to be set, take a look at the test suite section under demo steps.
```
shell$ ./prepare_integration_suite.sh --spark-command
spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 --master prepare_integration_suite.sh --deploy-mode  
--properties-file  --class org.apache.hudi.bench.job.HudiTestSuiteJob target/hudi-bench-0.5.1-SNAPSHOT.jar 
--source-class  --source-ordering-field  --input-base-path  --target-base-path  --target-table  --props  --storage-type  --payload-class  --workload-yaml-path  --input-file-size  --<deltastreamer-ingest>
```

##### Running through a test-case (local)
Take a look at the TestHudiTestSuiteJob to check how you can run the entire suite using JUnit. 
