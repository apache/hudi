#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

kafkaTopicName=hudi-test-topic

# Setup the schema registry
SCHEMA=$(sed 's|/\*|\n&|g;s|*/|&\n|g' /opt/bundle-validation/data/stocks/schema.avsc | sed '/\/\*/,/*\//d' | jq tostring)
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": $SCHEMA}" http://localhost:8081/subjects/${kafkaTopicName}/versions

# produce data
cat /opt/bundle-validation/data/stocks/data/batch_1.json /opt/bundle-validation/data/stocks/data/batch_2.json | $CONFLUENT_HOME/bin/kafka-console-producer \
--bootstrap-server http://localhost:9092 \
--topic ${kafkaTopicName}
