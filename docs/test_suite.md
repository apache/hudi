---
title: Test Suite
keywords: test suite
sidebar: mydoc_sidebar
permalink: test_suite.html
toc: false
summary: In this page, we will discuss the Hudi Test suite to perform integration tests

Hudi uses unit tests to certify the correctness of a particular class or module. This page describes in detail 
how to run integration tests on a hudi dataset that help improve our confidence in a release as well as 
perform large scale performance benchmarks.  

### Objectives

1. Test a hoodie job with different versions of `hdfs`, `parquet`, `spark`, `hive` and `avro`.
2. Generate different types of workloads across different dimensions : `payload size`, `number of updates`, `number of 
inserts`, `number of partitions`
3. Perform all types of ingest operations on workloads : `insert`, `bulk_insert`, `upsert`, `compact`
4. Flexibility to perform actions beyond ingest operations such as : `rollback`, `schedule_compaction`
5. Support custom post process actions and validations

### High Level Design

{% include image.html file="hudi_test_suite_design.png" alt="hudi_test_suite_design.png" %}

The Hudi test suite runs as a long running spark job. The suite is divided into the following high level components : 
##### Workload Generation

This component does the work of generating the workload; `inserts`, `upserts` etc.

##### Workload Execution

Depending on the type of workload generated, data is either ingested into the target hudi 
dataset or the corresponding workload operation is executed. For example compaction does not necessarily need a workload
to be generated/ingested but can require an execution.

##### Execution of Actions associated with a workload

Once the workload is executed, there are multiple knobs 
available to perform parallel of post-process actions with. For eg, performing validations on the output result or 
the data on disk.

### Usage instructions


##### Entry class to the test suite

```
com.uber.hoodie.utilities.integrationsuite.job.HudiTestSuiteJob.java - Entry Point of the hudi test suite job. This 
class wraps all the functionalities required to run a configurable integration suite.
```

##### Configurations required to run the job
```
com.uber.hoodie.utilities.integrationsuite.job.HudiTestSuiteConfig - Config class that drives the behavior of the 
integration test suite. This class extends from com.uber.hoodie.utilities.DeltaStreamerConfig. Look at 
link#HudiDeltaStreamer page to learn about all the available configs applicable to your test suite.
```

##### Generating a custom Workload Pattern
```
com.uber.hoodie.utilities.integrationsuite.configuration.WorkloadOperationSequenceGenerator - This class holds the 
definition for a workload sequence. This class provides a default implementation and should be extended and overriden 
for your own custom workload. This of this as a more interactive config to build the desired workload pattern and 
actions.
```

#### Building the test suite

The test suite can be found in the `hoodie-utilities` module. Use the `prepareIntegrationSuite.sh` script to build 
the test suite, you can provide different parameters to the script.

```
shell$ ./prepareIntegrationSuite.sh --help
Usage: prepareIntegrationSuite.sh
   --spark-command, prints the spark command
   -h, hdfs-version
   -s, spark version
   -p, parquet version
   -a, avro version
   -s, hive version
```

```
shell$ ./prepareIntegrationSuite.sh
....
....
Final command : mvn clean install -DskipTests
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO]
[INFO] Hoodie ............................................. SUCCESS [  1.493 s]
[INFO] hoodie-common ...................................... SUCCESS [  6.636 s]
[INFO] hoodie-hadoop-mr ................................... SUCCESS [  1.341 s]
[INFO] hoodie-client ...................................... SUCCESS [  4.170 s]
[INFO] hoodie-hive ........................................ SUCCESS [  0.978 s]
[INFO] hoodie-spark ....................................... SUCCESS [ 23.819 s]
[INFO] hoodie-utilities ................................... SUCCESS [ 13.487 s]
[INFO] hoodie-cli ......................................... SUCCESS [  8.099 s]
[INFO] hoodie-hadoop-mr-bundle ............................ SUCCESS [  1.592 s]
[INFO] hoodie-hive-bundle ................................. SUCCESS [ 10.571 s]
[INFO] hoodie-spark-bundle ................................ SUCCESS [ 37.000 s]
[INFO] hoodie-presto-bundle ............................... SUCCESS [  9.694 s]
[INFO] hoodie-hadoop-docker ............................... SUCCESS [  0.444 s]
[INFO] hoodie-hadoop-base-docker .......................... SUCCESS [  0.377 s]
[INFO] hoodie-hadoop-namenode-docker ...................... SUCCESS [  0.051 s]
[INFO] hoodie-hadoop-datanode-docker ...................... SUCCESS [  0.045 s]
[INFO] hoodie-hadoop-history-docker ....................... SUCCESS [  0.039 s]
[INFO] hoodie-hadoop-hive-docker .......................... SUCCESS [  0.771 s]
[INFO] hoodie-hadoop-sparkbase-docker ..................... SUCCESS [  0.223 s]
[INFO] hoodie-hadoop-sparkmaster-docker ................... SUCCESS [  0.042 s]
[INFO] hoodie-hadoop-sparkworker-docker ................... SUCCESS [  0.049 s]
[INFO] hoodie-hadoop-sparkadhoc-docker .................... SUCCESS [  0.040 s]
[INFO] hoodie-integ-test .................................. SUCCESS [  1.710 s]

```

```
shell$ ./prepareIntegrationSuite.sh --spark-command
spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 --master prepareIntegrationSuite.sh --deploy-mode 
 --properties-file  --class com.uber.hoodie.utilities.integrationsuite.job.HudiTestSuiteJob 
 target/hoodie-utilities-0.4.6-SNAPSHOT.jar --source-class  --source-ordering-field  --input-base-path  
 --target-base-path  --target-table  --props  --storage-type  --payload-class  --workload-generator-classname  
 --input-file-size  --key-generator-class  --<deltastreamer-ingest> --schemaprovider-class
```

##### Running on the cluster
Copy over the necessary files and jars that are required for the above spark command to run to your cluster, provide 
the correct paths to them in the above command and then run the spark-submit command. 

##### Running on the laptop (local)
Take a look at the TestHudiTestSuiteJob to see how can you do this programmatically. 
