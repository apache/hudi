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

After installing these dependencies, execute the following commands. This will install all the Hudi dependency jars,
including the fat packaged jar that contains all the dependencies required for a functional Hudi Kafka Connect Sink.

```bash
cd $HUDI_DIR
mvn clean -DskipTests install
```

Henceforth, incremental builds can be performed as follows. 

```bash
mvn clean -pl hudi-kafka-connect install -DskipTests
mvn clean -pl packaging/hudi-kafka-connect-bundle install
```

Next, we need to make sure that the hudi sink connector bundle jar is in Kafka Connect classpath. Note that the connect
classpath should be same as the one configured in the connector configuration file.

```bash
cp $HUDI_DIR/packaging/hudi-kafka-connect-bundle/target/hudi-kafka-connect-bundle-0.10.0-SNAPSHOT.jar /usr/local/share/java/hudi-kafka-connect/
```

## Trying the connector

After building the package, we need to install the Apache Kafka

### 1 - Starting the environment

To try out the Connect Sink locally, set up a Kafka broker locally. Download the latest apache kafka from https://kafka.apache.org/downloads.
Once downloaded and built, run the Zookeeper server and Kafka server using the command line tools.

```bash
export KAFKA_HOME=/path/to/kafka_install_dir
cd $KAFKA_KAFKA_HOME
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
./bin/kafka-server-start.sh ./config/server.properties
```

Wait until the kafka cluster is up and running.

### 2 - Set up the schema registry

Hudi leverages schema registry to obtain the latest schema when writing records. While it supports most popular schema
registries, we use Confluent schema registry. Download the latest confluent platform and run the schema registry
service.

```bash
cd $CONFLUENT_DIR
./bin/schema-registry-start etc/schema-registry/schema-registry.properties
```

### 3 - Create the Hudi Control Topic for Coordination of the transactions

The control topic should only have `1` partition, since its used to coordinate the Hudi write transactions across the multiple Connect tasks.

```bash
cd $KAFKA_HOME
./bin/kafka-topics.sh --delete --topic hudi-control-topic --bootstrap-server localhost:9092
./bin/kafka-topics.sh --create --topic hudi-control-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

### 4 - Create the Hudi Topic for the Sink and insert data into the topic

Open a terminal to execute the following command:

```bash
cd $HUDI_DIR/hudi-kafka-connect/demo/
bash setupKafka.sh -n <total_kafka_messages>
```

### 4 - Run the Sink connector worker (multiple workers can be run)

The Kafka connect is a distributed platform, with the ability to run one or more workers (each running multiple tasks) 
that parallely process the records from the Kafka partitions for the same topic. We provide a properties file with 
default properties to start a Hudi connector. 

Note that if multiple workers need to be run, the webserver needs to be reconfigured for subsequent workers to ensure
successful running of the workers.

```bash
cd $KAFKA_HOME
./bin/connect-distributed.sh $HUDI_DIR/hudi-kafka-connect/demo/connect-distributed.properties
```

### 5- To add the Hudi Sink to the Connector (delete it if you want to re-configure)

Once the Connector has started, it will not run the Sink, until the Hudi sink is added using the web api. The following 
curl APIs can be used to delete and add a new Hudi Sink. Again, a default configuration is provided for the Hudi Sink, 
that can be changed based on the desired properties.

```bash
curl -X DELETE http://localhost:8083/connectors/hudi-sink
curl -X POST -H "Content-Type:application/json" -d @$HUDI_DIR/hudi-kafka-connect/demo/config-sink.json http://localhost:8083/connectors
```

Now, you should see that the connector is created and tasks are running.

```bash
curl -X GET -H "Content-Type:application/json"  http://localhost:8083/connectors
["hudi-sink"]
curl -X GET -H "Content-Type:application/json"  http://localhost:8083/connectors/hudi-sink/status | jq
```

And, you should see your Hudi table created, which you can query using Spark/Flink.
Note: HUDI-2325 tracks Hive sync, which will unlock pretty much every other query engine.

```bash
ls -a /tmp/hoodie/hudi-test-topic
.		.hoodie		partition-1	partition-3
..		partition-0	partition-2	partition-4

ls -lt /tmp/hoodie/hudi-test-topic/.hoodie
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

ls -l /tmp/hoodie/hudi-test-topic/partition-0
total 5168
-rw-r--r--  1 user  wheel  439332 Sep 13 21:43 2E0E6DB44ACC8479059574A2C71C7A7E-0_0-0-0_20210913214114.parquet
-rw-r--r--  1 user  wheel  440179 Sep 13 21:42 3B56FAAAE2BDD04E480C1CBACD463D3E-0_0-0-0_20210913214114.parquet
-rw-r--r--  1 user  wheel  437097 Sep 13 21:45 3B56FAAAE2BDD04E480C1CBACD463D3E-0_0-0-0_20210913214351.parquet
-rw-r--r--  1 user  wheel  440219 Sep 13 21:42 D5AEE453699D5D9623D704C1CF399C8C-0_0-0-0_20210913214114.parquet
-rw-r--r--  1 user  wheel  437035 Sep 13 21:45 D5AEE453699D5D9623D704C1CF399C8C-0_0-0-0_20210913214351.parquet
-rw-r--r--  1 user  wheel  440214 Sep 13 21:43 E200FA75DCD1CED60BE86BCE6BF5D23A-0_0-0-0_20210913214114.parquet
```


