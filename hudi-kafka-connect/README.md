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

# Quick Start guide for Kafka Connect Sink for Hudi

This repo contains a sample project that can be used to start off your own source connector for Kafka Connect.

## Building the connector

The first thing you need to do to start using this connector is building it. In order to do that, you need to install the following dependencies:

- [Java 1.8+](https://openjdk.java.net/)
- [Apache Maven](https://maven.apache.org/)

After installing these dependencies, execute the following command:

```bash
cd $HUDI_DIR
mvn clean package
```

## Incremental Builds

```bash
mvn clean -pl hudi-kafka-connect install -DskipTests
mvn clean -pl packaging/hudi-kafka-connect-bundle install
```

## Put hudi connector in Kafka Connet classpath

```bash
cp $HUDI_DIR/packaging/hudi-kafka-connect-bundle/target/hudi-kafka-connect-bundle-0.10.0-SNAPSHOT.jar /usr/local/share/java/hudi-kafka-connect/
```

## Trying the connector

After building the package, we need to install the Apache Kafka

### 1 - Starting the environment

Start the ZK and Kafka:

```bash
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
./bin/kafka-server-start.sh ./config/server.properties
```

Wait until the kafka cluster is up and running.

### 2 - Create the Hudi Control Topic for Coordination of the transactions

The control topic should only have `1` partition

```bash
./bin/kafka-topics.sh --delete --topic hudi-control-topic --bootstrap-server localhost:9092
./bin/kafka-topics.sh --create --topic hudi-control-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

### 3 - Create the Hudi Topic for the Sink and insert data into the topic

Open a terminal to execute the following command:

```bash
bash runKafkaTrafficGenerator.sh <total_messages>
```

### 4 - Run the Sink connector worker (multiple workers can be run)

Open a terminal to execute the following command:

```bash
./bin/connect-distributed.sh ../hudi-kafka-connect/configs/connect-distributed.properties
```

### 5- To add the Hudi Sink to the Connector (delete it if you want to re-configure)

```bash
curl -X DELETE http://localhost:8083/connectors/hudi-sink
curl -X POST -H "Content-Type:application/json" -d @$HUDI-DIR/hudi-kafka-connect/configs/config-sink.json http://localhost:8083/connectors
```
