## Apache Hudi Demo using Docker containers

Let’s dive into a real-world scenario to understand Hudi’s capabilities from start to finish. To provide a self-contained environment, we've set up a local Docker cluster on your workstation.

These steps have been verified on a Mac ARM laptop, ensuring compatibility and ease of use.

### Prerequisites

  * Docker Setup
    * Virtual disk limit: 100 GB
    * Memory limit: 8 GB
    * File Sharing: VirtioFS
    * [If using Mac ARM], Use Rosetta for x86_64/amd64 emulation on Apple Silicon: CHECKED
  * ngrok
    * Sign up for an account with ngrok, it will be used so that you can access your daemon services on the internet.  You will need an ngrok authtoken in the following steps.
  * Internet connectivity
    * Maven repositories like https://mvnrepository.com
    * Docker Hub
    * Others
    
Also, this has not been tested on some environments like Docker on Windows.

## Why a data lakehouse and Why Apache Hudi

Here are some resources to help you understand data lakehouses and Apache Hudi:

* [What is a Data Lakehouse & How does it Work?](https://hudi.apache.org/blog/2024/07/11/what-is-a-data-lakehouse)
* [It’s Time for the Universal Data Lakehouse‍](https://www.onehouse.ai/blog/its-time-for-the-universal-data-lakehouse)
* [Apache Hudi vs Delta Lake vs Apache Iceberg - Data Lakehouse Feature Comparison](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison)
* [Comparing Apache Hudi's MOR (read and streaming write workload pattern) and COW Tables (read heavy workload pattern)](https://www.onehouse.ai/blog/comparing-apache-hudis-mor-and-cow-tables-use-cases-from-uber-and-shopee)

## Talking about Apache Hudi demo

Apache Hudi is compatible with JDK 8 and can be compiled and run on this version. While you might not need to compile the code yourself, we've provided the source code in the /opt/hudi directory. You can use `git pull` to fetch the latest updates or `git checkout release-0.15.0` to switch to a specific version like Hudi 0.15.

For this demonstration, we'll be using Hudi 0.15 and Spark 3.4. You can easily adapt these instructions to other versions by modifying the libraries you download. In addition, we will be deploying Apache Hudi in a S3-based data lakehouse environment and using Spark and SQL query engine tools like Trino to query the data in the data lakehouse.

To ensure clarity and understanding, we'll provide a detailed explanation of each step involved in the process.

### Spark 3 Support Matrix
| Hudi            | Supported Spark 3 version                                |
| --------------- | -------------------------------------------------------- |
| 0.15.x          | 3.5.x (default build), 3.4.x, 3.3.x, 3.2.x, 3.1.x, 3.0.x |
| 0.14.x          | 3.4.x (default build), 3.3.x, 3.2.x, 3.1.x, 3.0.x        |
| 0.13.x          | 3.3.x (default build), 3.2.x, 3.1.x                      |
| 0.12.x          | 3.3.x (default build), 3.2.x, 3.1.x                      |
| 0.11.x          | 3.2.x (default build, Spark bundle only), 3.1.x          |
| 0.10.x          | 3.1.x (default build), 3.0.x                             |
| 0.7.0 - 0.9.0   | 3.0.x                                                    |
| 0.6.0 and prior | not supported                                            |

### Figuring out what libraries to use

When you use a specific version of Hudi, you will sometimes need to download a version of Hadoop to match.   For instance, if you are using Apache Hudi 0.14, the [pom.xml](https://github.com/apache/hudi/blob/release-0.14.0/pom.xml) says that it was compiled with Hadoop 2.10.1.   Therefore you will need to use aws-hadoop 2.10.1 which you can get at https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.10.1 and then the corresponding aws sdk at https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle/1.11.271.  See this article on [Determine Compatibility of hadoop-aws and aws-java-sdk-bundle JARs](https://coding-stream-of-consciousness.com/2019/06/07/determine-compatibility-of-hadoop-aws-and-aws-java-sdk-bundle-jars/) for more information.

## Reset the enviroment

To improve performance, we cache the JARs needed for the demo in the spark/jars and spark/cache directories after the initial download.  When you want to switch to a different Spark or Hudi version or have class conflicts, please clear them out by typing `rm -Rf spark/jars/*.jar` and `rm -Rf spark/cache/*`

## Downloading the Environment

Clone this git repository.

```
git clone https://github.com/alberttwong/onehouse-demos/edit/main/hudi-spark-minio-trino
```

## Setting up Docker Cluster

### [Suggested] Enabling NGROK for internet routing containers within the Docker Network

Sign up for an free NGROK token at https://ngrok.com/.  NGROK will be used so that you can access your kafka cluster on the internet and on your local workstation even though it's behind the docker network.

```
export NGROK_AUTHTOKEN=XXXXXX
```

### Bringing up Demo Cluster

This should pull the Docker images from Docker hub and setup the Docker cluster.
```
docker compose up
```

At this point, the Docker cluster will be up and running. The demo cluster brings up the following services:

   * Min.IO for Object Store (s3-like service)
   * Spark Master and Worker (execution engine)
   * Hive Services (metastore along with PostgresDB, storing data about the Hudi enviornment)
   * Apache Kafka with ngrok enabled (a message service to help move data between systems) 
   * Containers for Trino setup (Trino coordinator and worker, SQL query engine)
   * ngrok proxy (allows the internet and your workstation to access services behind the docker compose network)

```output
albert@Alberts-MBP ~ % docker ps
CONTAINER ID   IMAGE                                    COMMAND                   CREATED          STATUS                            PORTS                                                                    NAMES
52a488224df2   quay.io/debezium/kafka:2.7.0.Final       "/bin/sh -c 'echo \"W…"   10 seconds ago   Up 9 seconds                      9092/tcp, 0.0.0.0:29092->29092/tcp                                       kafka
cc4e39344d20   starburstdata/hive:3.1.3-e.10            "/bin/sh -c \"/opt/bi…"   10 seconds ago   Up 9 seconds (health: starting)   0.0.0.0:9083->9083/tcp                                                   trino-prestodb-spark-minio-hive-metastore-1
2226b7caf902   minio/mc                                 "/bin/sh -c ' until …"    10 seconds ago   Up 9 seconds                                                                                               trino-prestodb-spark-minio-mc-1
d52de8d6044d   minio/minio                              "/usr/bin/docker-ent…"    10 seconds ago   Up 9 seconds                      0.0.0.0:9000-9001->9000-9001/tcp                                         trino-prestodb-spark-minio-minio-1
f3d2c4c32ab8   quay.io/debezium/zookeeper:2.7.0.Final   "/docker-entrypoint.…"    10 seconds ago   Up 9 seconds                      0.0.0.0:2181->2181/tcp, 0.0.0.0:2888->2888/tcp, 0.0.0.0:3888->3888/tcp   trino-prestodb-spark-minio-zookeeper-1
d0e5b1a7387e   trinodb/trino:418                        "/usr/lib/trino/bin/…"    10 seconds ago   Up 9 seconds (health: starting)   0.0.0.0:8080->8080/tcp                                                   trino
b695f12f9d68   ngrok/ngrok:latest                       "/nix/store/n98vsmwd…"    10 seconds ago   Up 9 seconds                      0.0.0.0:4040->4040/tcp                                                   ngrok-1
0062dc427617   postgres:11                              "docker-entrypoint.s…"    10 seconds ago   Up 9 seconds                      5432/tcp                                                                 trino-prestodb-spark-minio-metastore_db-1
```

If you did not enable NGROK, you'll see that the container will fail to start. Please ignore this and continue with the demo.

In addition, some of the example outputs will have ngrok in the URI. This is because the example outputs were from my machine. They should not be exact but similar on your machine.

### Data Lakehouse creation

Upon startup, the docker-compose.yml file creates an S3 bucket named 'warehouse' to serve as the foundation of our S3-based data lakehouse. By leveraging S3 for data storage, we benefit from its virtually limitless capacity, eliminating the need for manual resizing or disruptive downtime. Additionally, S3 storage costs on AWS are typically a fraction of EBS costs, offering significant financial advantages. These cost savings and operational efficiencies make S3 a highly beneficial choice for our data storage needs.

### [Suggested] Getting the ngrok address

```
docker logs ngrok |grep "started tunnel"
t=2024-09-07T00:05:31+0000 lvl=info msg="started tunnel" obj=tunnels name=kafka addr=//kafka:9092 url=tcp://2.tcp.us-cal-1.ngrok.io:19757
```

Your kafka URI in this situation is `tcp://2.tcp.us-cal-1.ngrok.io:19757`.  It will change every time you startup this demo environment.

## [Optional] conduktor or any other kafka toolling for kafka browsing

This an optional step.

To monitor your Kafka topics and messages in real-time, you can leverage any Kafka browser of your choice. While I've personally tested Conduktor, any compatible tool will work.

Simply follow the Conduktor quickstart guide (https://conduktor.io/get-started) and enter your Kafka ngrok URI with no username and password. You'll then be able to see the topics and messages streaming live.

## Demo

### Understanding the Scenario

Our objective is to construct a Hudi table that maintains the most recent hourly stock tracker data. To achieve this, we'll ingest two batches of minute-level stock data:

* Batch 1: Covers the initial trading hour (9:30 AM to 10:30 AM).
* Batch 2: Covers the subsequent half-hour (10:30 AM to 11:00 AM), including updates to some stocks from Batch 1.

### Leveraging Hudi's Merge-on-Read for Efficient Updates

Hudi's default write strategy, merge-on-read, proves invaluable for this use case. Here's why it outperforms copy-on-write:

* Reduced Write Amplification: Instead of creating entirely new files for each update, merge-on-read modifies existing files. This minimizes storage overhead and write operations.
* Improved Read Performance: As data is organized in a more compact manner, queries become faster.
* Simplified Upserts: Handling updates is straightforward, as Hudi efficiently merges new data with existing records.

### The Impact of Compaction

Hudi's compaction process is crucial for maintaining query performance and storage efficiency. It merges multiple small files into larger ones, reducing the number of files to scan during queries.

* Benefits of Compaction:
  * Improved query performance.
  * Reduced storage overhead.
  * Simplified data management.


### Step 1 : Publish the first batch to Kafka

Upload the first batch to Kafka topic 'stock ticks' 

```java
docker exec -it spark /bin/bash
cat /opt/demo/data/batch_1.json | kafkacat -b kafka:9092 -t stock_ticks -P
```

To check if the new topic shows up, use

```java
kafkacat -b kafka -L -J | jq .
{
  "originating_broker": {
    "id": -1,
    "name": "kafka:9092/bootstrap"
  },
  "query": {
    "topic": "*"
  },
  "controllerid": 1,
  "brokers": [
    {
      "id": 1,
      "name": "6.tcp.us-cal-1.ngrok.io:17553"
    }
  ],
  "topics": [
    {
      "topic": "stock_ticks",
      "partitions": [
        {
          "partition": 0,
          "leader": 1,
          "replicas": [
            {
              "id": 1
            }
          ],
          "isrs": [
            {
              "id": 1
            }
          ]
        }
      ]
    }
  ]
}
```

### Step 2: Incrementally ingest data from Kafka topic

Hudi offers a powerful tool called Hudi Streamer, designed to ingest data from various sources, including Kafka. It uses upsert and insert operations to efficiently apply incoming changes to Hudi tables. Compared to the Hudi Kafka Sink, Hudi Streamer is the recommended choice for data ingestion.

In this demonstration, we'll employ Hudi Streamer to retrieve JSON data from a Kafka topic and load it into both COW and M-O-R tables that we created earlier. If these tables don't already exist in the file system, Hudi Streamer will automatically initialize them.

```java
docker exec -it spark /bin/bash

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_cow table in S3
spark-submit \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer org.apache.hudi_hudi-utilities-slim-bundle_2.12-0.15.0.jar \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts  \
  --target-base-path s3a://warehouse/stock_ticks_cow \
  --target-table stock_ticks_cow \
  --props file:///opt/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_mor table in S3
  spark-submit \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer org.apache.hudi_hudi-utilities-slim-bundle_2.12-0.15.0.jar \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path s3a://warehouse/stock_ticks_mor \
  --target-table stock_ticks_mor \
  --props file:///opt/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --disable-compaction

# The configs contain mostly Kafa connectivity settings, the avro-schema to be used for ingesting along with key and partitioning fields.

exit
```

You can view the contents of the "stock_ticks_cow" table using the Min.IO browser at http://localhost:9001/browser/warehouse/stock_ticks_cow%2F. Login credentials are admin/password.

You can explore the new partition folder created in the table along with a "commit" / "deltacommit" file under .hoodie which signals a successful commit.

There will be a similar setup when you browse the M-O-R table http://localhost:9001/browser/warehouse/stock_ticks_mor%2F. Login credentials are admin/password.

Another method is to use minio's mc client tool to see the data

```
docker exec -it spark /bin/bash

# Create the connection

root@spark:/spark-3.4.3-bin-hadoop3/bin# mc alias set minio http://minio:9000 admin password

# Run the ls command to see the contents in the S3 bucket

root@spark:/spark-3.4.3-bin-hadoop3/bin# mc ls minio/warehouse
[2024-09-09 16:27:27 UTC]     0B stock_ticks_cow/
[2024-09-09 16:27:27 UTC]     0B stock_ticks_mor/
root@spark:/spark-3.4.3-bin-hadoop3/bin# mc ls minio/warehouse/stock_ticks_cow
[2024-09-09 16:27:38 UTC]     0B .hoodie/
[2024-09-09 16:27:38 UTC]     0B 2018/
root@spark:/spark-3.4.3-bin-hadoop3/bin# mc ls minio/warehouse/stock_ticks_mor/2018/08/31
[2024-09-13 17:04:51 UTC]    96B STANDARD .hoodie_partition_metadata
[2024-09-13 17:04:52 UTC] 434KiB STANDARD e7aefb82-bd77-4df7-a59e-a924e5e418a5-0_0-23-24_20240913170447603.parquet
```

### Step 3: Sync with Hive

At this step, the tables are available in S3. We need to sync with Hive to create new Hive tables and add partitions inorder to run queries against those tables.  We will use a different container because Hudi Hive Sync requires a full installation of Spark, Hadoop and Hive inorder for the utility to run.

If not older than Hudi 1.0 releases, here is a screenshot after modifications to the shell script.
![hivesync_classpath](https://github.com/user-attachments/assets/b782caaf-239c-4382-a4e4-c8d04ca21f58)

```java
docker exec -it openjdk8 /bin/bash

# If needed, we need to modify the existing run_sync_tool.sh with additional classpaths HUDI_CLASSPATH.  Here is the [before](https://github.com/apache/hudi/blob/47bdc2709566f726fa503919c87004ec26f14817/hudi-sync/hudi-hive-sync/run_sync_tool.sh#L55) and this is the [after with modifications](https://github.com/apache/hudi/blob/4e98278fdd6ff7455bd290871a542d740eaf471a/hudi-sync/hudi-hive-sync/run_sync_tool.sh#L55). Save and exit.

vi /opt/hudi/hudi-sync/hudi-hive-sync/run_sync_tool.sh

# The new java launch should look like

echo "Running Command : java -cp ${HUDI_CLASSPATH}:${HADOOP_HIVE_JARS}:${HADOOP_CONF_DIR}:$HUDI_HIVE_UBER_JAR org.apache.hudi.hive.HiveSyncTool $@"
java -cp ${HUDI_CLASSPATH}:$HUDI_HIVE_UBER_JAR:${HADOOP_HIVE_JARS}:${HADOOP_CONF_DIR} org.apache.hudi.hive.HiveSyncTool "$@"
````

```java
docker exec -it openjdk8 /bin/bash

# Let's look at what java libraries are needed to run the sync tool.

root@openjdk8:/spark-3.4.3-bin-hadoop3/bin# ls /opt/hudisync/
aws-java-sdk-bundle-1.11.271.jar  hadoop-aws-2.10.2.jar  libthrift-0.13.0.jar

# Add the sync libraries to classpath

export HUDI_CLASSPATH=/opt/hudisync/*

# This command takes in HiveServer URL and COW Hudi table location in S3 and sync the S3 state to Hive

/opt/hudi/hudi-sync/hudi-hive-sync/run_sync_tool.sh  \
--metastore-uris 'thrift://hive-metastore:9083' \
--partitioned-by dt \
--base-path 's3a://warehouse/stock_ticks_cow' \
--database default \
--table stock_ticks_cow \
--sync-mode hms \
--partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
.....
2024-09-04 12:33:27,101 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(297)) - Sync complete for stock_ticks_cow
.....

# Now run hive-sync for the second data-set in S3 using Merge-On-Read (M-O-R table type)

/opt/hudi/hudi-sync/hudi-hive-sync/run_sync_tool.sh  \
--metastore-uris 'thrift://hive-metastore:9083' \
--partitioned-by dt \
--base-path 's3a://warehouse/stock_ticks_mor' \
--database default \
--table stock_ticks_mor \
--sync-mode hms \
--partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
.....
2024-09-04 12:34:16,413 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(297)) - Sync complete for stock_ticks_mor
.....

exit
```

Upon executing the command, you'll observe the following:

* A table named stock_ticks_cow is generated, enabling Snapshot and Incremental queries using the Copy-On-Write strategy.
* Two additional tables, stock_ticks_mor_rt and stock_ticks_mor_ro, are created for the Merge-On-Read approach. The former supports both Snapshot and Incremental queries, providing near-real-time data access. The latter is optimized for Read operations, offering efficient data retrieval for analytical workloads.


### Step 4 (a): Run Queries with Spark-SQL

Run a query to find the latest timestamp ingested for stock symbol 'GOOG'. You will notice that both Snapshot (for both COW and MOR_rt table) and Read Optimized (for MOR_ro table) give the same value "10:29 a.m" as Hudi creates a parquet file for the first batch of data.

```java
docker exec -it spark /bin/bash

spark-sql --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

# List Tables

spark-sql (default)> show tables;
stock_ticks_cow
stock_ticks_mor
stock_ticks_mor_ro
stock_ticks_mor_rt
Time taken: 1.006 seconds, Fetched 4 row(s)


# Look at partitions that were added

spark-sql (default)> show partitions stock_ticks_mor_rt;
2018/08/31
Time taken: 1.191 seconds, Fetched 1 row(s)


# COPY-ON-WRITE Queries:
=========================

spark-sql (default)> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
GOOG	2018-08-31 10:29:00
Time taken: 1.701 seconds, Fetched 1 row(s)

Now, run a projection query:

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
20240904122742622	GOOG	2018-08-31 09:59:00	6330	1230.5	1230.02
20240904122742622	GOOG	2018-08-31 10:29:00	3391	1230.1899	1230.085
Time taken: 0.149 seconds, Fetched 2 row(s)

# Merge-On-Read Queries:
==========================

Lets run similar queries against M-O-R table. Lets look at both Read Optimized and Snapshot (realtime data) queries supported by M-O-R table

# Run Read Optimized Query. Notice that the latest timestamp is 10:29
spark-sql (default)> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
GOOG	2018-08-31 10:29:00
Time taken: 0.484 seconds, Fetched 1 row(s)


# Run Snapshot Query. Notice that the latest timestamp is again 10:29

spark-sql (default)> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
GOOG	2018-08-31 10:29:00
Time taken: 0.558 seconds, Fetched 1 row(s)


# Run Read Optimized and Snapshot project queries

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
20240904123001395	GOOG	2018-08-31 09:59:00	6330	1230.5	1230.02
20240904123001395	GOOG	2018-08-31 10:29:00	3391	1230.1899	1230.085
Time taken: 0.121 seconds, Fetched 2 row(s)

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
20240904123001395	GOOG	2018-08-31 09:59:00	6330	1230.5	1230.02
20240904123001395	GOOG	2018-08-31 10:29:00	3391	1230.1899	1230.085
Time taken: 0.132 seconds, Fetched 2 row(s)

spark-sql (default)> exit;

exit
```

### Step 4 (b): Run Queries with Spark-Shell

Here are the same queries running in Spark-Shell.

```java
docker exec -it spark /bin/bash

spark-shell --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

...

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.4.3
      /_/

Using Scala version 2.12.17 (OpenJDK 64-Bit Server VM, Java 11.0.24)
Type in expressions to have them evaluated.
Type :help for more information.

scala> spark.sql("show tables").show(100, false)
+---------+------------------+-----------+
|namespace|tableName         |isTemporary|
+---------+------------------+-----------+
|default  |stock_ticks_cow   |false      |
|default  |stock_ticks_mor   |false      |
|default  |stock_ticks_mor_ro|false      |
|default  |stock_ticks_mor_rt|false      |
+---------+------------------+-----------+

# Copy-On-Write Table

## Run max timestamp query against COW table

scala> spark.sql("select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+


## Projection Query

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20240904122742622  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20240904122742622  |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+

# Merge-On-Read Queries:
==========================

# Lets run similar queries against M-O-R table. Lets look at both Read Optimized and Snapshot queries supported by M-O-R table

# Run Read Optimized Query. Notice that the latest timestamp is 10:29

scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+


# Run Snapshot Query. Notice that the latest timestamp is again 10:29

scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+


# Run Read Optimized and Snapshot projection queries

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20240904123001395  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20240904123001395  |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+


scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20240904123001395  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20240904123001395  |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+

scala> :quit

exit
```

### Step 4 (c): Run Trino Queries

Here are the similar queries with Trino and the Trino configs.

```
docker exec -it trino /bin/bash
trino@trino:/$ cat /etc/trino/catalog/hudi.properties
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
connector.name=hudi
# HMS
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.aws-access-key=admin
hive.s3.aws-secret-key=password
hive.s3.path-style-access=true
hive.s3.region=us-west-2
hive.s3.endpoint=http://minio:9000

# AWS
#hive.s3.aws-access-key=XXXX
#hive.s3.aws-secret-key=YYY
#hive.s3.path-style-access=true
#hive.s3.region=us-west-2
#hive.metastore=glue
#hive.metastore.glue.region=us-west-2
#hive.metastore.glue.aws-access-key=XXXX
#hive.metastore.glue.aws-secret-key=YYYY
# AWS Glue Catalog ID is the AWS account ID
#hive.metastore.glue.catalogid=
```

```java
docker exec -it trino /bin/bash

trino@trino:/$ trino

trino> show catalogs;
 Catalog
---------
 delta
 hive
 hudi
 iceberg
 system
(5 rows)

Query 20240904_124925_00000_2hhut, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0.65 [0 rows, 0B] [0 rows/s, 0B/s]

trino:default> show schemas in hudi;
       Schema
--------------------
 default
 information_schema
(2 rows)

Query 20240904_125410_00005_dyubr, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0.17 [2 rows, 35B] [11 rows/s, 203B/s]

trino> use hudi.default;
USE

trino:default> show tables;
       Table
--------------------
 stock_ticks_cow
 stock_ticks_mor
 stock_ticks_mor_ro
 stock_ticks_mor_rt
(4 rows)

Query 20240904_125328_00004_dyubr, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0.21 [4 rows, 134B] [19 rows/s, 654B/s]



# COPY-ON-WRITE Queries:
=========================
    
trino:default> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20240904_125446_00006_dyubr, FINISHED, 1 node
Splits: 33 total, 33 done (100.00%)
2.01 [197 rows, 474KB] [98 rows/s, 236KB/s]

trino:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_cow where symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20240904122742622   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20240904122742622   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085
(2 rows)

Query 20240904_125506_00007_dyubr, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
1.08 [197 rows, 481KB] [182 rows/s, 447KB/s]

# Merge-On-Read Queries:
==========================

# Lets run similar queries against M-O-R table.

# Run Read Optimized Query. Notice that the latest timestamp is 10:29
    
trino:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20240904_125531_00008_dyubr, FINISHED, 1 node
Splits: 33 total, 33 done (100.00%)
0.95 [197 rows, 474KB] [208 rows/s, 501KB/s]

trino:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20240904123001395   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20240904123001395   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085
(2 rows)

Query 20240904_125548_00009_dyubr, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.94 [197 rows, 481KB] [209 rows/s, 512KB/s]

trino:default> exit

exit
```

### Step 5: Upload second batch to Kafka and run Hudi Streamer to ingest

Upload the second batch of data and ingest this batch using Hudi Streamer. As this batch does not bring in any new partitions, there is no need to run hive-sync.

```java
docker exec -it spark /bin/bash

cat /opt/demo/data/batch_2.json | kafkacat -b kafka:9092 -t stock_ticks -P

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_cow table in S3

spark-submit \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer org.apache.hudi_hudi-utilities-slim-bundle_2.12-0.15.0.jar \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts  \
  --target-base-path s3a://warehouse/stock_ticks_cow \
  --target-table stock_ticks_cow \
  --props file:///opt/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_mor table in S3

spark-submit \
  --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer org.apache.hudi_hudi-utilities-slim-bundle_2.12-0.15.0.jar \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path s3a://warehouse/stock_ticks_mor \
  --target-table stock_ticks_mor \
  --props file:///opt/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --disable-compaction

exit
```

With Copy-On-Write table, the second ingestion by Hudi Streamer resulted in a new version of Parquet file getting created. See http://localhost:9001/browser/warehouse/stock_ticks_cow%2F2018%2F08%2F31%2F.

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file. Take a look at the S3 filesystem to get an idea: http://localhost:9001/browser/warehouse/stock_ticks_mor%2F2018%2F08%2F31%2F.

### Step 6 (a): Run Queries

With Copy-On-Write table, the Snapshot query immediately sees the changes as part of second batch once the batch got committed as each ingestion creates newer versions of parquet files.

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file. This is the time, when Read Optimized and Snapshot queries will provide different results. Read Optimized query will still return "10:29 am" as it will only read from the Parquet file. Snapshot query will do on-the-fly merge and return latest committed data which is "10:59 a.m".

```java
docker exec -it spark /bin/bash

spark-sql --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'


# Copy On Write Table:

spark-sql (default)> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
GOOG	2018-08-31 10:59:00
Time taken: 3.263 seconds, Fetched 1 row(s)

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
20240904122742622	GOOG	2018-08-31 09:59:00	6330	1230.5	1230.02
20240904130113388	GOOG	2018-08-31 10:59:00	9021	1227.1993	1227.215
Time taken: 0.155 seconds, Fetched 2 row(s)

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.


# Merge On Read Table:

# Read Optimized Query
spark-sql (default)> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
GOOG	2018-08-31 10:29:00
Time taken: 0.452 seconds, Fetched 1 row(s)

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
20240904123001395	GOOG	2018-08-31 09:59:00	6330	1230.5	1230.02
20240904123001395	GOOG	2018-08-31 10:29:00	3391	1230.1899	1230.085
Time taken: 0.112 seconds, Fetched 2 row(s)

# Snapshot Query
spark-sql (default)> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
GOOG	2018-08-31 10:59:00
Time taken: 0.978 seconds, Fetched 1 row(s)

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
20240904123001395	GOOG	2018-08-31 09:59:00	6330	1230.5	1230.02
20240904130127262	GOOG	2018-08-31 10:59:00	9021	1227.1993	1227.215
Time taken: 0.215 seconds, Fetched 2 row(s)

spark-sql (default)> exit;

exit
```

### Step 6 (b): Run Spark Shell Queries

Running the same queries in Spark-Shell.

```java
docker exec -it spark /bin/bash

spark-shell --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

# Copy On Write Table:

scala> spark.sql("select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:59:00|
+------+-------------------+


scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20240904122742622  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20240904130113388  |GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|
+-------------------+------+-------------------+------+---------+--------+

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.


# Merge On Read Table:

# Read Optimized Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20240904123001395  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20240904123001395  |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+


# Snapshot Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
org.apache.hudi.org.openjdk.jol.vm.sa.SASupportException: Sense failed., org.apache.hudi.org.openjdk.jol.vm.sa.SASupportException: Sense failed.]
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:59:00|
+------+-------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20240904123001395  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20240904130127262  |GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|
+-------------------+------+-------------------+------+---------+--------+

scala> :quit

exit
```

### Step 6 (c): Run Trino Queries

Running the same queries on Trino for Read Optimized queries.

```java
docker exec -it trino /bin/bash

trino@trino:/$ trino

trino> use hudi.default;
USE
    
# Copy On Write Table:

trino:default> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:59:00
(1 row)

Query 20240904_132409_00014_dyubr, FINISHED, 1 node
Splits: 33 total, 33 done (100.00%)
1.14 [197 rows, 474KB] [173 rows/s, 417KB/s]

trino:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20240904122742622   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20240904130113388   | GOOG   | 2018-08-31 10:59:00 |   9021 | 1227.1993 | 1227.215
(2 rows)

Query 20240904_132423_00015_dyubr, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.88 [197 rows, 481KB] [223 rows/s, 546KB/s]

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.

# Merge On Read Table:

# Read Optimized Query
    
trino:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20240904_132439_00016_dyubr, FINISHED, 1 node
Splits: 33 total, 33 done (100.00%)
0.88 [197 rows, 474KB] [223 rows/s, 538KB/s]

trino:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20240904123001395   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20240904123001395   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085
(2 rows)

Query 20240904_132451_00017_dyubr, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.87 [197 rows, 481KB] [225 rows/s, 552KB/s]

trino:default> exit

exit
```

### Step 7 (a): Incremental Query for COPY-ON-WRITE Table

With 2 batches of data ingested, lets showcase the support for incremental queries in Hudi Copy-On-Write tables

Lets take the same projection query example

```java
docker exec -it spark /bin/bash

spark-sql --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
20240904122742622	GOOG	2018-08-31 09:59:00	6330	1230.5	1230.02
20240904130113388	GOOG	2018-08-31 10:59:00	9021	1227.1993	1227.215
Time taken: 2.913 seconds, Fetched 2 row(s)

spark-sql (default)> exit;

exit
```

As you notice from the above queries, there are 2 commits - 20240904122742622 and 20240904130113388 in timeline order. When you follow the steps, you will be getting different timestamps for commits. Substitute them in place of the above timestamps.

To show the effects of incremental-query, let us assume that a reader has already seen the changes as part of ingesting first batch. Now, for the reader to see effect of the second batch, he/she has to keep the start timestamp to the commit time of the first batch (20240904122742622) and run incremental query.

Hudi incremental mode provides efficient scanning for incremental queries by filtering out files that do not have any candidate rows using hudi-managed metadata.

```java
docker exec -it spark /bin/bash

spark-sql --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'


spark-sql (default)> set hoodie.stock_ticks_cow.consume.mode=INCREMENTAL;
hoodie.stock_ticks_cow.consume.mode	INCREMENTAL
Time taken: 0.042 seconds, Fetched 1 row(s)

spark-sql (default)> set hoodie.stock_ticks_cow.consume.max.commits=3;
hoodie.stock_ticks_cow.consume.max.commits	3
Time taken: 0.028 seconds, Fetched 1 row(s)

spark-sql (default)> set hoodie.stock_ticks_cow.consume.start.timestamp=20240904122742622;
hoodie.stock_ticks_cow.consume.start.timestamp	20240904122742622
Time taken: 0.029 seconds, Fetched 1 row(s)
```

With the above setting, file-ids that do not have any updates from the commit 20240904130113388 is filtered out without scanning. Here is the incremental query :

```java
spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG' and `_hoodie_commit_time` > '20240904122742622';
20240904130113388	GOOG	2018-08-31 10:59:00	9021	1227.1993	1227.215
Time taken: 0.199 seconds, Fetched 1 row(s)

spark-sql (default)> exit;

exit
```

### Step 7 (b): Incremental Query with Spark Shell:

```java
docker exec -it spark /bin/bash

spark-shell --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'


Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.4.3
      /_/

Using Scala version 2.12.17 (OpenJDK 64-Bit Server VM, Java 11.0.24)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceReadOptions

scala> val hoodieIncViewDF =  spark.read.format("org.apache.hudi").option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL).option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20240904122742622").load("s3a://warehouse/stock_ticks_cow")
24/09/04 13:34:10 WARN MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-s3a-file-system.properties,hadoop-metrics2.properties
24/09/04 13:34:10 WARN DFSPropertiesConfiguration: Cannot find HUDI_CONF_DIR, please set it as the dir of hudi-defaults.conf
hoodieIncViewDF: org.apache.spark.sql.DataFrame = [_hoodie_commit_time: string, _hoodie_commit_seqno: string ... 15 more fields]

scala> hoodieIncViewDF.registerTempTable("stock_ticks_cow_incr_tmp1")
warning: one deprecation (since 2.0.0); for details, enable `:setting -deprecation' or `:replay -deprecation'

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow_incr_tmp1 where  symbol = 'GOOG'").show(100, false);
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20240904130113388  |GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|
+-------------------+------+-------------------+------+---------+--------+

scala> :quit

exit
```

### Step 8: Schedule and Run Compaction for Merge-On-Read table

Lets schedule and run a compaction to create a new version of columnar  file so that Read Optimized readers will see fresher data.
Again, You can use Hudi CLI to manually schedule and run compaction

```java
docker exec -it spark /bin/bash

# Let's execute some environment variables and move java libraries to the right locations

export HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key=admin
export HOODIE_ENV_fs_DOT_s3a_DOT_secret_DOT_key=password
export HOODIE_ENV_fs_DOT_s3a_DOT_endpoint=http://minio:9000
export HOODIE_ENV_fs_DOT_s3a_DOT_aws_DOT_credentials_DOT_provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
export CLIENT_JAR=/opt/hudicli/hadoop-aws-2.10.2.jar:/opt/hudicli/aws-java-sdk-bundle-1.11.271.jar
export SPARK_BUNDLE_JAR=/opt/hudicli/hudi-spark3.4-bundle_2.12-0.15.0.jar
export CLI_BUNDLE_JAR=/opt/hudicli/hudi-cli-bundle_2.12-0.15.0.jar
cp /opt/hudicli/hadoop-aws-2.10.2.jar /spark/jars
cp /opt/hudicli/aws-java-sdk-bundle-1.11.271.jar /spark/jars
mc alias set minio http://minio:9000 admin password
mc cp /opt/demo/config/schema.avsc minio/warehouse

# Let's check the libraries needed to run Hudi ci

root@spark:/spark-3.4.3-bin-hadoop3/bin# ls /opt/hudicli/
aws-java-sdk-bundle-1.11.271.jar  hadoop-aws-2.10.2.jar  hudi-cli-bundle_2.12-0.15.0.jar  hudi-spark3.4-bundle_2.12-0.15.0.jar

# Run the Hudi cli

root@spark:/spark-3.4.3-bin-hadoop3/bin# cd /opt/hudicli && /opt/hudi/packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh
DIR is /opt/hudicli
Inferring CLI_BUNDLE_JAR path assuming this script is under Hudi repo
Inferring SPARK_BUNDLE_JAR path assuming this script is under Hudi repo
CLI_BUNDLE_JAR: /opt/hudicli/hudi-cli-bundle_2.12-0.15.0.jar
SPARK_BUNDLE_JAR: /opt/hudicli/hudi-spark3.4-bundle_2.12-0.15.0.jar
Downloading necessary auxiliary jars for Hudi CLI
--2024-09-04 18:07:34--  https://repo1.maven.org/maven2/org/glassfish/jakarta.el/3.0.3/jakarta.el-3.0.3.jar
Resolving repo1.maven.org (repo1.maven.org)... 199.232.196.209, 199.232.192.209, 2a04:4e42:4c::209, ...
Connecting to repo1.maven.org (repo1.maven.org)|199.232.196.209|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 237826 (232K) [application/java-archive]
Saving to: ‘auxlib/jakarta.el-3.0.3.jar’

jakarta.el-3.0.3.jar                         100%[===========================================================================================>] 232.25K  --.-KB/s    in 0.03s

2024-09-04 18:07:35 (7.35 MB/s) - ‘auxlib/jakarta.el-3.0.3.jar’ saved [237826/237826]

--2024-09-04 18:07:35--  https://repo1.maven.org/maven2/jakarta/el/jakarta.el-api/3.0.3/jakarta.el-api-3.0.3.jar
Resolving repo1.maven.org (repo1.maven.org)... 199.232.196.209, 199.232.192.209, 2a04:4e42:4c::209, ...
Connecting to repo1.maven.org (repo1.maven.org)|199.232.196.209|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 79816 (78K) [application/java-archive]
Saving to: ‘auxlib/jakarta.el-api-3.0.3.jar’

jakarta.el-api-3.0.3.jar                     100%[===========================================================================================>]  77.95K  --.-KB/s    in 0.02s

2024-09-04 18:07:35 (4.47 MB/s) - ‘auxlib/jakarta.el-api-3.0.3.jar’ saved [79816/79816]

Client jar location not set, please set it in conf/hudi-env.sh
Running : java -cp /opt/hudi/packaging/hudi-cli-bundle/conf:/opt/hudi/packaging/hudi-cli-bundle/auxlib/*:/spark/*:/spark/jars/*:/etc/hadoop/conf:/etc/spark/conf:/opt/hudi/packaging/hudi-cli-bundle/target/hudi-cli-bundle_2.12-0.15.0.jar:/opt/hudi/packaging/hudi-cli-bundle/../hudi-spark-bundle/target/hudi-spark-bundle_2.12-0.15.0.jar: -DSPARK_CONF_DIR=/etc/spark/conf -DHADOOP_CONF_DIR=/etc/hadoop/conf org.apache.hudi.cli.Main
Main called
===================================================================
*         ___                          ___                        *
*        /\__\          ___           /\  \           ___         *
*       / /  /         /\__\         /  \  \         /\  \        *
*      / /__/         / /  /        / /\ \  \        \ \  \       *
*     /  \  \ ___    / /  /        / /  \ \__\       /  \__\      *
*    / /\ \  /\__\  / /__/  ___   / /__/ \ |__|     / /\/__/      *
*    \/  \ \/ /  /  \ \  \ /\__\  \ \  \ / /  /  /\/ /  /         *
*         \  /  /    \ \  / /  /   \ \  / /  /   \  /__/          *
*         / /  /      \ \/ /  /     \ \/ /  /     \ \__\          *
*        / /  /        \  /  /       \  /  /       \/__/          *
*        \/__/          \/__/         \/__/    Apache Hudi CLI    *
*                                                                 *
===================================================================
733  [main] INFO  org.apache.hudi.cli.Main [] - Starting Main v0.15.0 using Java 1.8.0_422 on openjdk8 with PID 34 (/opt/hudi/packaging/hudi-cli-bundle/target/hudi-cli-bundle_2.12-0.15.0.jar started by root in /spark-3.4.3-bin-hadoop3/bin)
740  [main] INFO  org.apache.hudi.cli.Main [] - No active profile set, falling back to 1 default profile: "default"
Table command getting loaded
Sep 04, 2024 6:07:36 PM org.jline.utils.Log logr
WARNING: The Parser of class org.springframework.shell.jline.ExtendedDefaultParser does not support the CompletingParsedLine interface. Completion with escaped or quoted words won't work correctly.
1486 [main] INFO  org.apache.hudi.cli.Main [] - Started Main in 0.907 seconds (JVM running for 1.517)

hudi->connect --path s3a://warehouse/stock_ticks_mor
17945 [main] WARN  org.apache.hadoop.util.NativeCodeLoader [] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18394 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Loading HoodieTableMetaClient from s3a://warehouse/stock_ticks_mor
18429 [main] INFO  org.apache.hudi.common.table.HoodieTableConfig [] - Loading table properties from s3a://warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18441 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from s3a://warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor loaded

hudi:stock_ticks_mor->compactions show all
42012 [main] INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline [] - Loaded instants upto : Option{val=[20240905011929870__deltacommit__COMPLETED__20240905011934536]}
╔═════════════════════════╤═══════╤═══════════════════════════════╗
║ Compaction Instant Time │ State │ Total FileIds to be Compacted ║
╠═════════════════════════╧═══════╧═══════════════════════════════╣
║ (empty)                                                         ║
╚═════════════════════════════════════════════════════════════════╝

# Schedule a compaction. This will use Spark Launcher to schedule compaction
hoodie:stock_ticks_mor->compaction schedule --hoodieConfigs hoodie.compact.inline.max.delta.commits=1
....
Attempted to schedule compaction for 20240907005719894

# Now refresh and check again. You will see that there is a new compaction requested

hudi:stock_ticks_mor->refresh
221115 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Loading HoodieTableMetaClient from s3a://warehouse/stock_ticks_mor
221121 [main] INFO  org.apache.hudi.common.table.HoodieTableConfig [] - Loading table properties from s3a://warehouse/stock_ticks_mor/.hoodie/hoodie.properties
221125 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from s3a://warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor refreshed.

hudi:stock_ticks_mor->compactions show all
75780 [main] INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline [] - Loaded instants upto : Option{val=[==>20240907005719894__compaction__REQUESTED__20240907005724527]}
╔═════════════════════════╤═══════════╤═══════════════════════════════╗
║ Compaction Instant Time │ State     │ Total FileIds to be Compacted ║
╠═════════════════════════╪═══════════╪═══════════════════════════════╣
║ 20240907005719894       │ REQUESTED │ 1                             ║
╚═════════════════════════╧═══════════╧═══════════════════════════════╝


# Execute the compaction. The compaction instant value passed below must be the one displayed in the above "compactions show all" query

hoodie:stock_ticks_mor->compaction run --compactionInstant  20240907005719894 --parallelism 2 --sparkMemory 1G  --schemaFilePath s3://warehouse/schema.avsc --retry 1
....
Compaction successfully completed for 20240907005719894

## Now check if compaction is completed

hudi:stock_ticks_mor->refresh
258485 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Loading HoodieTableMetaClient from s3a://warehouse/stock_ticks_mor
258493 [main] INFO  org.apache.hudi.common.table.HoodieTableConfig [] - Loading table properties from s3a://warehouse/stock_ticks_mor/.hoodie/hoodie.properties
258497 [main] INFO  org.apache.hudi.common.table.HoodieTableMetaClient [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from s3a://warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor refreshed.

hudi:stock_ticks_mor->compactions show all
118413 [main] INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline [] - Loaded instants upto : Option{val=[20240907005719894__commit__COMPLETED__20240907005816381]}
╔═════════════════════════╤═══════════╤═══════════════════════════════╗
║ Compaction Instant Time │ State     │ Total FileIds to be Compacted ║
╠═════════════════════════╪═══════════╪═══════════════════════════════╣
║ 20240907005719894       │ COMPLETED │ 1                             ║
╚═════════════════════════╧═══════════╧═══════════════════════════════╝

hudi:stock_ticks_mor->exit

exit
```

### Step 9: Run Spark-SQL Queries including incremental queries

You will see that both Read Optimized and Snapshot queries will show the latest committed data. Lets also run the incremental query for M-O-R table. From looking at the below query output, it will be clear that the fist commit time for the M-O-R table is 20240907000722335 and the second commit time is 20240907001620314

```java
docker exec -it spark /bin/bash

spark-sql --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

# Read Optimized Query

spark-sql (default)> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
GOOG	2018-08-31 10:59:00
Time taken: 3.399 seconds, Fetched 1 row(s)

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
20240907000722335	GOOG	2018-08-31 09:59:00	6330	1230.5	1230.02
20240907001620314	GOOG	2018-08-31 10:59:00	9021	1227.1993	1227.215
Time taken: 0.135 seconds, Fetched 2 row(s)

# Snapshot Query

spark-sql (default)> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
GOOG	2018-08-31 10:59:00
Time taken: 0.654 seconds, Fetched 1 row(s)

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
20240907000722335	GOOG	2018-08-31 09:59:00	6330	1230.5	1230.02
20240907001620314	GOOG	2018-08-31 10:59:00	9021	1227.1993	1227.215
Time taken: 0.13 seconds, Fetched 2 row(s)

# Incremental Query:

spark-sql (default)> set hoodie.stock_ticks_mor.consume.mode=INCREMENTAL;
hoodie.stock_ticks_mor.consume.mode	INCREMENTAL
Time taken: 0.039 seconds, Fetched 1 row(s)

# Max-Commits covers both second batch and compaction commit

spark-sql (default)> set hoodie.stock_ticks_mor.consume.max.commits=3;
hoodie.stock_ticks_mor.consume.max.commits	3
Time taken: 0.038 seconds, Fetched 1 row(s)
spark-sql (default)> set hoodie.stock_ticks_mor.consume.start.timestamp=20240907000722335;
hoodie.stock_ticks_mor.consume.start.timestamp	20240907000722335
Time taken: 0.029 seconds, Fetched 1 row(s)

# Query:

spark-sql (default)> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG' and `_hoodie_commit_time` > '20240907000722335';
20240907001620314	GOOG	2018-08-31 10:59:00	9021	1227.1993	1227.215
Time taken: 0.195 seconds, Fetched 1 row(s)

spark-sql (default)> exit;

exit
```

### Step 10: Read Optimized and Snapshot queries for M-O-R with Spark-SQL after compaction

```java
docker exec -it spark /bin/bash

spark-shell --packages org.apache.hudi:hudi-utilities-slim-bundle_2.12:0.15.0,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

# Read Optimized Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:59:00|
+------+-------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20240907000722335  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20240907001620314  |GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|
+-------------------+------+-------------------+------+---------+--------+

# Snapshot Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:59:00|
+------+-------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20240907000722335  |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20240907001620314  |GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|
+-------------------+------+-------------------+------+---------+--------+

scala> :quit

exit
```

### Step 11:  Trino Read Optimized queries on M-O-R table after compaction

```java
docker exec -it trino /bin/bash

trino@trino:/$ trino

trino> show catalogs;
 Catalog
---------
 delta
 hive
 hudi
 iceberg
 system
(5 rows)

Query 20240907_010945_00000_94du6, FINISHED, 1 node
Splits: 11 total, 11 done (100.00%)
0.54 [0 rows, 0B] [0 rows/s, 0B/s]

trino> show schemas in hudi;
       Schema
--------------------
 default
 information_schema
(2 rows)

Query 20240907_011158_00000_dic4v, FINISHED, 1 node
Splits: 11 total, 11 done (100.00%)
0.64 [2 rows, 35B] [3 rows/s, 55B/s]

trino> use hudi.default;
USE

trino:default> show tables;
       Table
--------------------
 stock_ticks_cow
 stock_ticks_mor
 stock_ticks_mor_ro
 stock_ticks_mor_rt
(4 rows)

Query 20240907_011235_00004_dic4v, FINISHED, 1 node
Splits: 11 total, 11 done (100.00%)
0.19 [4 rows, 134B] [21 rows/s, 709B/s]

# Read Optimized Query
trino:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:59:00
(1 row)

Query 20240907_011248_00005_dic4v, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
1.18 [197 rows, 474KB] [166 rows/s, 400KB/s]

trino:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20240907000722335   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20240907001620314   | GOOG   | 2018-08-31 10:59:00 |   9021 | 1227.1993 | 1227.215
(2 rows)

Query 20240907_011306_00006_dic4v, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.19 [197 rows, 481KB] [1.01K rows/s, 2.42MB/s]
```

This concludes our demonstration. To fully appreciate Apache Hudi's capabilities, we recommend conducting a similar test with larger datasets. For example, try a 10GB table with 10MB incremental updates and perform Snapshot queries. You'll witness Hudi's exceptional performance, especially in scenarios where users demand real-time data updates regardless of the table's size or incoming data volume. This is a key advantage of Hudi's open table format (_H_ for hadoop _U_ for upserts _D_ for deletes _I_ for incrementals), designed to efficiently handle incremental updates.

Finally let's connect! Join our Slack community to share your thoughts on the Hudi Docker demo. We're eager to hear your feedback on what you enjoyed, what you didn't, and any issues you encountered. Your insights will help us improve the demo for everyone.

## Additional Demos and Resources

### Case Study: Walmart

This is an article about Walmart’s migration to a Lakehouse architecture. It discusses the challenges of using a traditional Data Lake and the benefits of using a Lakehouse. The article also details Walmart’s evaluation of different Lakehouse technologies. Ultimately, Walmart chose Apache Hudi to power their Lakehouse. https://medium.com/walmartglobaltech/lakehouse-at-fortune-1-scale-480bcb10391b

### Case Study: Notion

This is an article about Notion’s journey through different stages of data scale. It discusses the challenges they faced as their data grew and how they overcame them by adopting a universal data lakehouse architecture. The article also details the benefits of using Hudi, an open source data lakehouse engine. Some of the benefits that Notion experienced include cost savings, performance improvements, and the ability to enable new product features. https://www.onehouse.ai/blog/notions-journey-through-different-stages-of-data-scale

### Case Study: Robinhood

This is an article about Robinhood's data lakehouse. It discusses what a data lakehouse is and Robinhood's use of Apache Hudi. The data lakehouse stores more than 50,000 datasets. It ingests data from various sources and makes it available to consumers for different use cases. To support the various use cases, Robinhood built a multi-tiered architecture. https://www.onehouse.ai/blog/scaling-and-governing-robinhoods-data-lakehouse

### Onehouse Lakeview

Onehouse LakeView is a free observability service designed specifically for data lakehouses. It provides data engineers with essential monitoring capabilities and insights to effectively manage and operate their tables.  Only metadata is passed to Onehouse Lakeview.

1. Request access at https://www.onehouse.ai/product/lakeview
2. Login to Onehouse Lakeview at https://cloud.onehouse.ai/ and get an API token.
3. Shell into the spark container `docker exec -it spark /bin/bash`
4. Grab the app `wget https://github.com/alberttwong/LakeView/releases/download/1.0-SNAPSHOT/LakeView-1.0-SNAPSHOT-all.jar -P /opt/lakeview`
5. Modify `hudi.yaml` with your API token and run the Metadata Extractor Tool `java -jar /opt/lakeview/LakeView-release-v0.14.0-all.jar -p /opt/lakeview/hudi.yaml`
6. View the report in Onehouse Lakeview at http://cloud.onehouse.ai

### Apache xTable

Apache XTable is an open-source project that aims to simplify data lake operations by providing a common model for table representation. It acts as a cross-table converter, facilitating interoperability between different lakehouse table formats like Apache Hudi, Apache Iceberg, and Delta Lake. You can easily add Apache xTable to this demo.   Just follow the steps in the Apache xTable Quickstart using this docker compose.  See [xtable.md](xtable.md) for instructions.

### Apache SuperSet

Apache Superset is a powerful open-source data exploration and visualization platform designed to help organizations of all sizes uncover insights from their data. It offers a user-friendly interface that allows users to create interactive dashboards, charts, and reports without extensive technical knowledge.  To get Apache Superset working, just connect Superset to the trino container in this demo environment.  

1. Add the Trino and StarRocks drivers to the Apache SuperSet docker environment
```
albert@albertonehouse superset % cat ./docker/requirements-local.txt
trino
starrocks
```

Now you have 2 options, you can either #A add superset into the same "demo" docker network or #B enable a ngrok endpoint and have Superset connect to that URI.

2A. Add Apache Superset to "demo" Docker network

Modify docker-compose-image-tag.yml and add in a new networks section like below.
```
albert@albertonehouse superset % cat docker-compose-image-tag.yml | tail -10
  superset_home:
    external: false
  db_home:
    external: false
  redis:
    external: false

networks:
  default:
     name: datalakehouse
```

Then you can just go to Trino and use the database wizard.

![Screenshot 2024-10-14 at 9 31 02 PM](https://github.com/user-attachments/assets/09d4c526-82c2-4219-bfae-33da8a860099)

2B. Expose the trino endpoint by modifying the ngrok.yml to enable ngrok on Trino. 

Here is an example of the modifications in ngrok.yml to expose trino to the internet and workstation.
```
version: 2
log: stdout
tunnels:
  trino:
    address: "trino:8080"
    proto: tcp
```

Put in the trino ngrok URI into the database connection wizard in Apache Superset
```
albert@albertonehouse hudi-spark-minio-trino % docker logs ngrok |grep "started tunnel"
t=2024-09-09T15:28:51+0000 lvl=info msg="started tunnel" obj=tunnels name=trino addr=//trino:8080 url=tcp://0.tcp.us-cal-1.ngrok.io:11058
```

### StarRocks

Experience how you connect different SQL engines to the data lakehouse created by Apache Hudi in this demo. StarRocks is a powerful and efficient open-source OLAP (Online Analytical Processing) database that is designed to handle large-scale data analytics and real-time queries. It is a massively parallel processing (MPP) database, meaning it can distribute data across multiple nodes and perform calculations simultaneously, making it ideal for handling complex analytics tasks.  Deploy an instance of StarRocks and you can connect to the data in this demo using

Replace hive.metastore.uris and aws.s3.endpoint with ngrok URI
```
docker logs ngrok |grep "started tunnel"
```

Starrocks' catalog create sql
```
CREATE EXTERNAL CATALOG hudi_catalog_hms
PROPERTIES
(
    "type" = "hudi",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "password",
    "aws.s3.region" = "us-east-1",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "http://minio:9000"
);
```

Starrocks' catalog create sql with ngrok URI
```
CREATE EXTERNAL CATALOG hudi_catalog_hms
PROPERTIES
(
    "type" = "hudi",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://8.tcp.us-cal-1.ngrok.io:12601",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "password",
    "aws.s3.region" = "us-east-1",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "http://2.tcp.us-cal-1.ngrok.io:14995"
);
```

### Onehouse Cloud

Onehouse Cloud is a platform that simplifies the process of building and managing data lakes. It offers a managed lakehouse solution, which combines the best features of data warehouses and data lakes. This means you can enjoy the scalability and flexibility of a data lake while also benefiting from the structured and query-optimized capabilities of a data warehouse. Connect this environment to Onehouse Cloud by configuring the Onehouse Cloud Kafka Source to use the ngrok kafka URL.

### Hudi on Amazon EMR

In addition to offering Apache Hudi, Amazon provides a comprehensive tutorial on working with Hudi on Amazon EMR. This tutorial covers launching the interactive Spark shell, using Spark submit, and utilizing Amazon EMR Notebooks. It even delves into alternative methods like the Hudi Streamer utility for writing to your dataset. You can find the tutorial here:   https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hudi-work-with-dataset.html

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=ab537f1a-ab7c-4cc8-ab93-81bd51b74e0b" />
