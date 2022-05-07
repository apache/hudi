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

This directory contains examples code that uses hudi.

To run the demo: 

  1. Configure your `SPARK_MASTER` env variable, yarn-cluster mode by default.
  2. For hudi write client demo and hudi data source demo, just use spark-submit as common spark app
  3. For hudi delta streamer demo of custom source, run `bin/custom-delta-streamer-example.sh`
  4. For hudi delta streamer demo of dfs source:

      4.1 Prepare dfs data, we have provided `src/main/resources/delta-streamer-config/dfs/source-file.json` for test

      4.2 Run `bin/dfs-delta-streamer-example.sh`

  5. For hudi delta streamer demo of dfs source:

      5.1 Start Kafka server

      5.2 Configure your Kafka properties, we have provided `src/main/resources/delta-streamer-config/kafka/kafka-source.properties` for test

      5.3 Run `bin/kafka-delta-streamer-example.sh`

      5.4 Continuously write source data to the Kafka topic your configured with `hoodie.deltastreamer.source.kafka.topic` in `kafka-source.properties`

  6. Some notes delta streamer demo:

      6.1 The configuration files we provided is just the simplest demo, you can change it according to your specific needs.

      6.2 You could also use Intellij to run the example directly by configuring parameters as "Program arguments"
