<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
-->

# Quick Start (demo) guide for Kafka Connect Sink for Hudi

This repo contains a sample project that can be used to start off your own source connector for Kafka Connect.
This is work is tracked by [HUDI-2324](https://issues.apache.org/jira/browse/HUDI-2324) 

## Building the Hudi Sink Connector

The first thing you need to do to start using this connector is building it. In order to do that, you need to install the following dependencies:

- [Java 1.8+](https://openjdk.java.net/)
- [Apache Maven](https://maven.apache.org/)
- Install [kcat](https://github.com/edenhill/kcat)
- Install jq. `brew install jq`

  
## Trying the connector

After installing these dependencies, follow steps based on your requirement.

### 1 - Starting the environment

For runtime dependencies, we encourage using the confluent HDFS connector jars. We have tested our setup with version `10.1.0`.
Either use confluent-hub to install the connector or download it from [here](https://tinyurl.com/yb472f79).

Copy the entire folder to the classpath that will be used by the Hudi Kafka Connector.

```bash
export CONFLUENT_DIR=/path/to/confluent_install_dir
mkdir -p /usr/local/share/kafka/plugins
$CONFLUENT_DIR/bin/confluent-hub install confluentinc/kafka-connect-hdfs:10.1.0
cp -r $CONFLUENT_DIR/share/confluent-hub-components/confluentinc-kafka-connect-hdfs/* /usr/local/share/kafka/plugins/
```

Now, build the packaged jar that contains all the hudi classes, including the Hudi Kafka Connector. And copy it to the
plugin path that contains all the other jars (`/usr/local/share/kafka/plugins/lib`)

```bash
cd $HUDI_DIR
mvn package -DskipTests -pl packaging/hudi-kafka-connect-bundle -am
mkdir -p /usr/local/share/kafka/plugins/lib
cp $HUDI_DIR/packaging/hudi-kafka-connect-bundle/target/hudi-kafka-connect-bundle-0.11.0-SNAPSHOT.jar /usr/local/share/kafka/plugins/lib
```

If the Hudi Sink Connector writes to a target Hudi table on [Amazon S3](https://aws.amazon.com/s3/), you need two
additional jars, `hadoop-aws-2.10.1.jar` and `aws-java-sdk-bundle-1.11.271.jar`, in the `plugins/lib` folder. You may
download them using the following commands. Note that, when you specify the target table path on S3, you need to use
`s3a://` prefix.

```bash
cd /usr/local/share/kafka/plugins/lib
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.10.1/hadoop-aws-2.10.1.jar
```

Set up a Kafka broker locally. Download the latest apache kafka from [here](https://kafka.apache.org/downloads). Once
downloaded and built, run the Zookeeper server and Kafka server using the command line tools.

```bash
export KAFKA_HOME=/path/to/kafka_install_dir
cd $KAFKA_HOME
# Run the following commands in separate terminals to keep them running
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
./bin/kafka-server-start.sh ./config/server.properties
```

Wait until the kafka cluster is up and running.

### 2 - Set up the schema registry

Hudi leverages schema registry to obtain the latest schema when writing records. While it supports most popular schema
registries, we use Confluent schema registry. Download the
latest [confluent platform](https://docs.confluent.io/platform/current/installation/index.html) and run the schema
registry service.

NOTE: You must change the port from `8081` (default) to `8082` to avoid conflict, i.e.,
using `listeners=http://0.0.0.0:8082` in the properties file `etc/schema-registry/schema-registry.properties`.

```bash
cd $CONFLUENT_DIR
./bin/kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name _schemas --alter --add-config cleanup.policy=compact
# Make sure you have changed the listener port as above
./bin/schema-registry-start etc/schema-registry/schema-registry.properties
```

### 3 - Create the Hudi Control Topic for Coordination of the transactions

The control topic should only have `1` partition, since its used to coordinate the Hudi write transactions across the multiple Connect tasks.

```bash
cd $KAFKA_HOME
# The following command is expected to throw an error if the control topic does not exist.
# "Error while executing topic command : Topic 'hudi-control-topic' does not exist as expected"
./bin/kafka-topics.sh --delete --topic hudi-control-topic --bootstrap-server localhost:9092
./bin/kafka-topics.sh --create --topic hudi-control-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

### 4 - Create the Hudi Topic for the Sink and insert data into the topic

Open a terminal to execute the following command:

```bash
cd $HUDI_DIR/hudi-kafka-connect/demo/
bash setupKafka.sh -n <total_kafka_messages>
```

To generate data for long-running tests, you can add `-b` option to specify the number of batches of data
to generate, with each batch containing a number of messages and idle time between batches, as follows:

```bash
bash setupKafka.sh -n <num_kafka_messages_per_batch> -b <num_batches>
```

### 5 - Run the Sink connector worker (multiple workers can be run)

The Kafka connect is a distributed platform, with the ability to run one or more workers (each running multiple tasks) 
that parallely process the records from the Kafka partitions for the same topic. We provide a properties file with 
default properties to start a Hudi connector. 

Note that if multiple workers need to be run, the webserver needs to be reconfigured for subsequent workers to ensure
successful running of the workers.

```bash
cd $KAFKA_HOME
./bin/connect-distributed.sh $HUDI_DIR/hudi-kafka-connect/demo/connect-distributed.properties
```

### 6 - To add the Hudi Sink to the Connector (delete it if you want to re-configure)

Once the Connector has started, it will not run the Sink, until the Hudi sink is added using the web api. The following 
curl APIs can be used to delete and add a new Hudi Sink. Again, a default configuration is provided for the Hudi Sink, 
that can be changed based on the desired properties.

```bash
curl -X DELETE http://localhost:8083/connectors/hudi-sink
curl -X POST -H "Content-Type:application/json" -d @$HUDI_DIR/hudi-kafka-connect/demo/config-sink.json http://localhost:8083/connectors
```

Now, you should see that the connector is created and tasks are running.

```bash
> curl -X GET -H "Content-Type:application/json"  http://localhost:8083/connectors
["hudi-sink"]

> curl -X GET -H "Content-Type:application/json"  http://localhost:8083/connectors/hudi-sink/status | jq
 % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   329  100   329    0     0  21096      0 --:--:-- --:--:-- --:--:-- 36555
{
  "name": "hudi-sink",
  "connector": {
    "state": "RUNNING",
    "worker_id": "127.0.0.1:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "127.0.0.1:8083"
    },
    {
      "id": 1,
      "state": "RUNNING",
      "worker_id": "127.0.0.1:8083"
    },
    {
      "id": 2,
      "state": "RUNNING",
      "worker_id": "127.0.0.1:8083"
    },
    {
      "id": 3,
      "state": "RUNNING",
      "worker_id": "127.0.0.1:8083"
    }
  ],
  "type": "sink"
}
```

And, you should see your Hudi table created, which you can query using Spark/Flink. Note: HUDI-2325 tracks Hive sync,
which will unlock pretty much every other query engine.

```bash
> ls -a /tmp/hoodie/hudi-test-topic
.		.hoodie		partition_1	partition_3
..		partition_0	partition_2	partition_4

> ls -lt /tmp/hoodie/hudi-test-topic/.hoodie
total 72
-rw-r--r--  1 user  wheel    346 Sep 14 10:32 hoodie.properties
-rw-r--r--  1 user  wheel      0 Sep 13 23:18 20210913231805.inflight
-rw-r--r--  1 user  wheel      0 Sep 13 23:18 20210913231805.commit.requested
-rw-r--r--  1 user  wheel   9438 Sep 13 21:45 20210913214351.commit
-rw-r--r--  1 user  wheel      0 Sep 13 21:43 20210913214351.inflight
-rw-r--r--  1 user  wheel      0 Sep 13 21:43 20210913214351.commit.requested
-rw-r--r--  1 user  wheel  18145 Sep 13 21:43 20210913214114.commit
-rw-r--r--  1 user  wheel      0 Sep 13 21:41 20210913214114.inflight
-rw-r--r--  1 user  wheel      0 Sep 13 21:41 20210913214114.commit.requested
drwxr-xr-x  2 user  wheel     64 Sep 13 21:41 archived

> ls -l /tmp/hoodie/hudi-test-topic/partition_0
total 5168
-rw-r--r--  1 user  wheel  439332 Sep 13 21:43 2E0E6DB44ACC8479059574A2C71C7A7E-0_0-0-0_20210913214114.parquet
-rw-r--r--  1 user  wheel  440179 Sep 13 21:42 3B56FAAAE2BDD04E480C1CBACD463D3E-0_0-0-0_20210913214114.parquet
-rw-r--r--  1 user  wheel  437097 Sep 13 21:45 3B56FAAAE2BDD04E480C1CBACD463D3E-0_0-0-0_20210913214351.parquet
-rw-r--r--  1 user  wheel  440219 Sep 13 21:42 D5AEE453699D5D9623D704C1CF399C8C-0_0-0-0_20210913214114.parquet
-rw-r--r--  1 user  wheel  437035 Sep 13 21:45 D5AEE453699D5D9623D704C1CF399C8C-0_0-0-0_20210913214351.parquet
-rw-r--r--  1 user  wheel  440214 Sep 13 21:43 E200FA75DCD1CED60BE86BCE6BF5D23A-0_0-0-0_20210913214114.parquet
```

### 7 - Run async compaction and clustering if scheduled

When using Merge-On-Read (MOR) as the table type, async compaction and clustering can be scheduled when the Sink is
running. Inline compaction and clustering are disabled by default due to performance reason. By default, async
compaction scheduling is enabled, and you can disable it by setting `hoodie.kafka.compaction.async.enable` to `false`.
Async clustering scheduling is disabled by default, and you can enable it by setting `hoodie.clustering.async.enabled`
to `true`.

The Sink only schedules the compaction and clustering if necessary and does not execute them for performance. You need
to execute the scheduled compaction and clustering using separate Spark jobs or Hudi CLI.

After the compaction is scheduled, you can see the requested compaction instant (`20211111111410.compaction.requested`)
below:

```
> ls -l /tmp/hoodie/hudi-test-topic/.hoodie
total 280
-rw-r--r--  1 user  wheel  21172 Nov 11 11:09 20211111110807.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:08 20211111110807.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:08 20211111110807.deltacommit.requested
-rw-r--r--  1 user  wheel  22458 Nov 11 11:11 20211111110940.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:09 20211111110940.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:09 20211111110940.deltacommit.requested
-rw-r--r--  1 user  wheel  21445 Nov 11 11:13 20211111111110.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:11 20211111111110.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:11 20211111111110.deltacommit.requested
-rw-r--r--  1 user  wheel  24943 Nov 11 11:14 20211111111303.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:13 20211111111303.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:13 20211111111303.deltacommit.requested
-rw-r--r--  1 user  wheel   9885 Nov 11 11:14 20211111111410.compaction.requested
-rw-r--r--  1 user  wheel  21192 Nov 11 11:15 20211111111411.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:14 20211111111411.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:14 20211111111411.deltacommit.requested
-rw-r--r--  1 user  wheel      0 Nov 11 11:15 20211111111530.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:15 20211111111530.deltacommit.requested
drwxr-xr-x  2 user  wheel     64 Nov 11 11:08 archived
-rw-r--r--  1 user  wheel    387 Nov 11 11:08 hoodie.properties
```

Then you can run async compaction job with `HoodieCompactor` and `spark-submit` by:

```
spark-submit \
  --class org.apache.hudi.utilities.HoodieCompactor \
  hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.10.0-SNAPSHOT.jar \
  --base-path /tmp/hoodie/hudi-test-topic \
  --table-name hudi-test-topic \
  --schema-file /Users/user/repo/hudi/docker/demo/config/schema.avsc \
  --instant-time 20211111111410 \
  --parallelism 2 \
  --spark-memory 1g
```

Note that you don't have to provide the instant time through `--instant-time`. In that case, the earliest scheduled
compaction is going to be executed.

Alternatively, you can use Hudi CLI to execute compaction:

```
hudi-> connect --path /tmp/hoodie/hudi-test-topic
hudi:hudi-test-topic-> compactions show all
╔═════════════════════════╤═══════════╤═══════════════════════════════╗
║ Compaction Instant Time │ State     │ Total FileIds to be Compacted ║
╠═════════════════════════╪═══════════╪═══════════════════════════════╣
║ 20211111111410          │ REQUESTED │ 12                            ║
╚═════════════════════════╧═══════════╧═══════════════════════════════╝

compaction validate --instant 20211111111410
compaction run --compactionInstant 20211111111410 --parallelism 2 --schemaFilePath /Users/user/repo/hudi/docker/demo/config/schema.avsc
```

Similarly, you can see the requested clustering instant (`20211111111813.replacecommit.requested`) after it is scheduled
by the Sink:

```
> ls -l /tmp/hoodie/hudi-test-topic/.hoodie
total 736
-rw-r--r--  1 user  wheel  24943 Nov 11 11:14 20211111111303.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:13 20211111111303.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:13 20211111111303.deltacommit.requested
-rw-r--r--  1 user  wheel  18681 Nov 11 11:17 20211111111410.commit
-rw-r--r--  1 user  wheel      0 Nov 11 11:17 20211111111410.compaction.inflight
-rw-r--r--  1 user  wheel   9885 Nov 11 11:14 20211111111410.compaction.requested
-rw-r--r--  1 user  wheel  21192 Nov 11 11:15 20211111111411.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:14 20211111111411.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:14 20211111111411.deltacommit.requested
-rw-r--r--  1 user  wheel  22460 Nov 11 11:17 20211111111530.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:15 20211111111530.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:15 20211111111530.deltacommit.requested
-rw-r--r--  1 user  wheel  21357 Nov 11 11:18 20211111111711.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:17 20211111111711.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:17 20211111111711.deltacommit.requested
-rw-r--r--  1 user  wheel   6516 Nov 11 11:18 20211111111813.replacecommit.requested
-rw-r--r--  1 user  wheel  26070 Nov 11 11:20 20211111111815.deltacommit
-rw-r--r--  1 user  wheel      0 Nov 11 11:18 20211111111815.deltacommit.inflight
-rw-r--r--  1 user  wheel      0 Nov 11 11:18 20211111111815.deltacommit.requested
```

Then you can run async clustering job with `HoodieClusteringJob` and `spark-submit` by:

```
spark-submit \
  --class org.apache.hudi.utilities.HoodieClusteringJob \
  hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.10.0-SNAPSHOT.jar \
  --props clusteringjob.properties \
  --mode execute \
  --base-path /tmp/hoodie/hudi-test-topic \
  --table-name sample_table \
  --instant-time 20211111111813 \
  --spark-memory 1g
```

Sample `clusteringjob.properties`:

```
hoodie.datasource.write.recordkey.field=volume
hoodie.datasource.write.partitionpath.field=date
hoodie.deltastreamer.schemaprovider.registry.url=http://localhost:8081/subjects/hudi-test-topic/versions/latest

hoodie.clustering.plan.strategy.target.file.max.bytes=1073741824
hoodie.clustering.plan.strategy.small.file.limit=629145600
hoodie.clustering.execution.strategy.class=org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy
hoodie.clustering.plan.strategy.sort.columns=volume

hoodie.write.concurrency.mode=single_writer
```

Note that you don't have to provide the instant time through `--instant-time`. In that case, the earliest scheduled
clustering is going to be executed.

### 8 - Querying via Hive

In this section we explain how one can test syncing of the Hudi table with Hive server/ Hive Metastore, 
that enable querying via Hive, Presto etc. 

To ease the deployment of HDFS, Hive Server, Hive Metastore etc. for testing hive sync, we use the docker
containers from the hudi docker demo. Refer to [this link for the setup](https://hudi.apache.org/docs/docker_demo).
Additionally, the docker deploys kafka and zookeeper too, so you do not need to run them explicitly in this setup.

Essentially, follow the steps listed here:

/etc/hosts : The demo references many services running in container by the hostname. Add the following settings to /etc/hosts
```bash
127.0.0.1 adhoc-1
127.0.0.1 adhoc-2
127.0.0.1 namenode
127.0.0.1 datanode1
127.0.0.1 hiveserver
127.0.0.1 hivemetastore
127.0.0.1 kafkabroker
127.0.0.1 sparkmaster
127.0.0.1 zookeeper
```

Bring up the docker containers
```bash
cd $HUDI_DIR/docker
./setup_demo.sh
```

Firstly, (re)-install a different connector that is configured to write the Hudi table to Hdfs instead of local filesystem.

```bash
curl -X DELETE http://localhost:8083/connectors/hudi-sink
curl -X POST -H "Content-Type:application/json" -d @$HUDI_DIR/hudi-kafka-connect/demo/config-sink-hive.json http://localhost:8083/connectors
```

After running the connector, you can query the hive server using the following steps:

```bash
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false

# List Tables
0: jdbc:hive2://hiveserver:10000> show tables;
+---------------------+--+
|      tab_name       |
+---------------------+--+
| huditesttopic_ro  |
| huditesttopic_rt  |
+---------------------+--+
3 rows selected (1.199 seconds)
0: jdbc:hive2://hiveserver:10000>

# Look at partitions that were added
0: jdbc:hive2://hiveserver:10000> show partitions huditesttopic_ro;
+-------------------+--+
|     partition     |
+-------------------+--+
| date=partition_0  |
| date=partition_1  |
| date=partition_2  |
| date=partition_3  |
| date=partition_4  |
+-------------------+--+
1 row selected (0.24 seconds)

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from huditesttopic_ro;
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
```

`Current Limitation:` The Hudi Kafka Connect sink uses `Merge-On-Read` by default, and inserts/ appends the kafka records 
directly to the log file(s). Asynchronously, compaction service can be executed to merge the log files into base file (Parquet format).
Generally, we support both Read-Optimized that reads only parquet base files and Snapshot queries that read and merge 
records across base and log files. However, currently there is a limitation where we are not able to read records from 
only log files. Hence, the queries for Hudi Kafka Connect will only work after compaction merges the records into base files. Alternatively,
users have the option to reconfigure the table type to `COPY_ON_WRITE` in config-sink.json.


