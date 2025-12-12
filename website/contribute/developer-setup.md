---
title: Developer Setup
sidebar_position: 3
keywords: [ hudi, ide, developer, setup]
toc: true
last_modified_at: 2024-08-12T10:47:57-07:00
---

## Pre-requisites

To contribute code, you need

 - a GitHub account
 - a Linux (or) macOS development environment with Java JDK 8, Apache Maven (3.x+) installed
 - [Docker](https://www.docker.com/) installed for running demo, integ tests or building website
 - for large contributions, a signed [Individual Contributor License Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA) to the Apache Software Foundation (ASF).
 - (Recommended) Join our dev mailing list & slack channel, listed on [community](/community/get-involved) page.


## IntelliJ Setup

IntelliJ is the recommended IDE for developing Hudi. To contribute, you would need to do the following
 
- Fork the Hudi code on Github & then clone your own fork locally. Once cloned, we recommend building as per instructions on [spark quickstart](/docs/quick-start-guide) or [flink quickstart](/docs/flink-quick-start-guide).

- In IntelliJ, select `File` > `New` > `Project from Existing Sources...` and select the `pom.xml` file under your local Hudi source folder.

- In `Project Structure`, select Java 1.8 as the Project SDK.

  ![IDE setup java](/assets/images/contributing/IDE_setup_java.png)

- Make the following configuration in `Preferences` or `Settings` in newer IntelliJ so the Hudi code can compile in the IDE:
  * Enable annotation processing in compiler.

    ![IDE setup annotation processing](/assets/images/contributing/IDE_setup_annotation.png)
  * Configure Maven *NOT* to delegate IDE build/run actions to Maven so you can run tests in IntelliJ directly.

    ![IDE setup maven 1](/assets/images/contributing/IDE_setup_maven_1.png)
    ![IDE setup maven 2](/assets/images/contributing/IDE_setup_maven_2.png)

- If you switch maven build profile, e.g., from Spark 3.4 to Spark 3.5, you need to first build Hudi in the command line first and `Reload All Maven Projects` in IntelliJ like below,
so that IntelliJ re-indexes the code.

   ![IDE setup reload](/assets/images/contributing/IDE_setup_reload.png)

- \[Recommended\] We have embraced the code style largely based on [google format](https://google.github.io/styleguide/javaguide.html). Please set up your IDE with style files from [\<project root\>/style/](https://github.com/apache/hudi/tree/master/style). These instructions have been tested on IntelliJ.
    * Open `Settings` in IntelliJ
    * Install and activate CheckStyle plugin

      ![IDE_setup_checkstyle_1](/assets/images/contributing/IDE_setup_checkstyle_1.png)
    * In `Settings` > `Tools` > `Checkstyle`, use a recent version, e.g., 12.1.0

      ![IDE_setup_checkstyle_2](/assets/images/contributing/IDE_setup_checkstyle_2.png)
    * Click on `+`, add the style/checkstyle.xml file, and name the configuration as "Hudi Checks"

      ![IDE_setup_checkstyle_3](/assets/images/contributing/IDE_setup_checkstyle_3.png)
    * Activate the checkstyle configuration by checking `Active`

      ![IDE_setup_checkstyle_4](/assets/images/contributing/IDE_setup_checkstyle_4.png)
    * Open `Settings` > `Editor` > `Code Style` > `Java`
    * Select "Project" as the "Scheme".  Then, go to the settings, open `Import Scheme` > `CheckStyle Configuration`, select `style/checkstyle.xml` to load

      ![IDE_setup_code_style_java_before](/assets/images/contributing/IDE_setup_code_style_java_before.png)
    * After loading the configuration, you should see that the `Indent` and `Continuation indent` become 2 and 4, from 4 and 8, respectively

      ![IDE_setup_code_style_java_after](/assets/images/contributing/IDE_setup_code_style_java_after.png)
    * Apply/Save the changes
- \[Recommended\] Set up the [Save Action Plugin](https://plugins.jetbrains.com/plugin/7642-save-actions) to auto format & organize imports on save. The Maven Compilation life-cycle will fail if there are checkstyle violations.

- \[Recommended\] As it is required to add [Apache License header](https://www.apache.org/legal/src-headers#headers) to all source files, configuring "Copyright" settings as shown below will come in handy.

![IDE setup copyright 1](/assets/images/contributing/IDE_setup_copyright_1.png)



![IDE setup copyright 2](/assets/images/contributing/IDE_setup_copyright_2.png)


## Useful Maven commands for developers. 
Listing out some of the maven commands that could be useful for developers. 

- Compile/build entire project 

```shell
mvn clean package -DskipTests 
```
Default profile is spark3.5 and scala2.12

- For continuous development, you may want to build only the modules of interest. for eg, if you have been working with 
Hudi Streamer, you can build using this command instead of entire project. Majority of time goes into building all different bundles we have 
like flink bundle, presto bundle, trino bundle etc. But if you are developing something confined to hudi-utilties, you can achieve faster 
build times.

```shell
mvn package -DskipTests -pl packaging/hudi-utilities-bundle/ -am
```

To enable multi-threaded building, you can add -T. 
```shell
mvn -T 2C package -DskipTests -pl packaging/hudi-utilities-bundle/ -am
```
This command will use 2 parallel threads to build. 

You can also confine the build to just one module if need be. 
```shell
mvn -T 2C package -DskipTests -pl hudi-spark-datasource/hudi-spark -am
```
Note: "-am" will build all dependent modules as well. 
In local laptop, entire project build can take somewhere close to 7 to 10 mins. While buildig just hudi-spark-datasource/hudi-spark
with multi-threaded, could get your compilation in 1.5 to 2 mins. 

If you wish to run any single test class in java. 
```shell
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark/ -am -B -DfailIfNoTests=false -Dtest=TestCleaner -Dspark3.5
```

If you wish to run a single test method in java. 
```shell
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark/ -am -B -DfailIfNoTests=false -Dtest=TestCleaner#testKeepLatestCommitsMOR -Dspark3.5
```

To filter particular scala test:
```shell
mvn -Dsuites="org.apache.spark.sql.hudi.ddl.TestSpark3DDL @Test Chinese table " -Dtest=abc -DfailIfNoTests=false test -pl packaging/hudi-spark-bundle -am -Dspark3.5
```
-Dtest=abc will assist in skipping all java tests.
-Dsuites="org.apache.spark.sql.hudi.TestSpark3DDL @Test Chinese table " filters for a single scala test.

- Run an Integration Test

```shell
mvn -T 2C -Pintegration-tests -DfailIfNoTests=false -Dit.test=ITTestHoodieSanity#testRunHoodieJavaAppOnMultiPartitionKeysMORTable verify -Dspark3.5
```

`verify` phase runs the integration test and cleans up the docker cluster after execution. To retain the docker cluster use
`integration-test` phase instead.

**Note:** If you encounter `unknown shorthand flag: 'H' in -H`, this error occurs when local environment has docker-compose version >= 2.0.
The latest docker-compose is accessible using `docker-compose` whereas v1 version is accessible using `docker-compose-v1` locally.<br/>
You can use `alt def` command to define different docker-compose versions. Refer https://github.com/dotboris/alt. <br/>
Use `alt use` to use v1 version of docker-compose while running integration test locally.


## Code & Project Structure

  * `docker` : Docker containers used by demo and integration tests. Brings up a mini data ecosystem locally
  * `hudi-cli` : CLI to inspect, manage and administer datasets
  * `hudi-client` : Spark client library to take a bunch of inserts + updates and apply them to a Hoodie table
  * `hudi-common` : Common classes used across modules
  * `hudi-hadoop-mr` : InputFormat implementations for ReadOptimized, Incremental, Realtime views
  * `hudi-hive` : Manage hive tables off Hudi datasets and houses the HiveSyncTool
  * `hudi-integ-test` : Longer running integration test processes
  * `hudi-spark` : Spark datasource for writing and reading Hudi datasets. Streaming sink.
  * `hudi-utilities` : Houses tools like Hudi streamer, snapshot exporter, etc
  * `packaging` : Poms for building out bundles for easier drop in to Spark, Hive, Presto, Utilities
  * `style`  : Code formatting, checkstyle files

## Code WalkThrough

This Quick Video will give a code walkthrough to start with [watch](https://www.youtube.com/watch?v=N2eDfU_rQ_U).

## Running unit tests and local debugger via Intellij IDE

:::note Important reminder
When submitting a PR please make sure to NOT commit the changes mentioned in these steps, instead once testing is done make sure to revert the changes and then submit a pr.
:::

0. Build the project with the intended profiles via the `mvn` cli, for example for spark 3.5 use `mvn clean package -Dspark3.5 -Dscala-2.12 -DskipTests`. 
1. Install the "Maven Helper" plugin from the Intellij IDE.
2. Make sure IDEA uses Maven to build/run tests:
   * You need to select the intended Maven profiles (using Maven tool pane in IDEA): select profiles you are targeting for example `spark3.4` and `scala-2.2` or `spark3.5`, `scala-2.12` etc. 
   * Add `.mvn/maven.config` file at the root of the repo w/ the the profiles you selected in the pane: `-Dspark3.5` `-Dscala-2.12`
   * Add `.mvn/` to the `.gitignore` file located in the root of the project. 
3. Make sure you change (temporarily) the `scala.binary.version` in the root `pom.xml` to the intended scala profile version. For example if running with spark3 `scala.binary.version` should be `2.12`
4. Finally right click on the unit test's method signature you are trying to run, there should be an option with a mvn symbol that allows you to `run <test-name>`, as well as an option to `debug <test-name>`.
    * For debugging make sure to first set breakpoints in the src code see (https://www.jetbrains.com/help/idea/debugging-code.html)

## Docker Setup

We encourage you to test your code on docker cluster please follow this for [docker setup](https://hudi.apache.org/docs/docker_demo).

## Remote Debugging 

if your code fails on docker cluster you can remotely debug your code please follow the below steps.

Step 1 :- Run your Hudi Streamer Job with --conf as defined this will ensure to wait till you attach your intellij with Remote Debugging on port 4044

```scala
spark-submit \
  --conf spark.driver.extraJavaOptions="-Dconfig.resource=myapp.conf  -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4044" \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts  \
  --base-file-format parquet \
  --target-base-path /user/hive/warehouse/stock_ticks_cow \
  --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider
```

Step 2 :- Attaching Intellij (tested on Intellij Version > 2019. this steps may change acc. to intellij version)

- Come to Intellij --> Edit Configurations -> Remote -> Add Remote - > Put Below Configs -> Apply & Save -> Put Debug Point -> Start. <br/>
- Name : Hudi Remote <br/>
- Port : 4044 <br/>
- Command Line Args for Remote JVM : -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4044 <br/>
- Use Module ClassPath : select hudi <br/>
 
## Website

[Apache Hudi site](https://hudi.apache.org) is hosted on a special `asf-site` branch. Please follow the `README` file under `docs` on that branch for
instructions on making changes to the website.
