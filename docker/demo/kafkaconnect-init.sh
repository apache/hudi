#!/bin/bash

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

# Wait for Kafka Connect listener
echo "Waiting for Kafka Connect to start listening on localhost ⏳"
while :; do
  curl_status=$(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors)
  echo -e $(date) " Kafka Connect listener HTTP state: " "$curl_status" " (waiting for 200)"
  if [ "$curl_status" -eq 200 ] ; then
    break
  fi
  sleep 5
done

echo -e "\n--\n+> Creating Hudi Sink"
curl -s -X PUT -H  "Content-Type:application/json" http://localhost:8083/connectors/hudi-sink/config \
    -d '
{
  "bootstrap.servers": "kafkabroker:9092",
 	"connector.class": "org.apache.hudi.connect.HoodieSinkConnector",
 	"tasks.max": "1",
 	"key.converter": "org.apache.kafka.connect.storage.StringConverter",
 	"value.converter": "org.apache.kafka.connect.storage.StringConverter",
 	"value.converter.schemas.enable": "false",
 	"topics": "stock_ticks",
 	"hoodie.table.name": "stock_ticks_mor",
 	"hoodie.table.type": "MERGE_ON_READ",
 	"hoodie.base.path": "hdfs://namenode:8020/var/demo/tables/stock_ticks_mor",
 	"hoodie.datasource.write.recordkey.field": "volume",
 	"hoodie.datasource.write.partitionpath.field": "date",
 	"hoodie.schemaprovider.class": "org.apache.hudi.schema.FilebasedSchemaProvider",
 	"hoodie.deltastreamer.schemaprovider.source.schema.file": "hdfs://namenode:8020/var/demo/config/schema.avsc",
 	"hoodie.kafka.commit.interval.secs": 5
}'

sleep 60
