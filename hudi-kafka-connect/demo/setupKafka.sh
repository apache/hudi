# Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#!/bin/bash

#########################
# The command line help #
#########################
usage() {
  echo "Usage: $0"
  echo "   -n |--num-kafka-records, (required) number of kafka records to generate in a batch"
  echo "   -b |--num-batch, (optional) number of batches of records to generate (default is 1)"
  echo "   -t |--reuse-topic, (optional) reuses the Kafka topic (default deletes and recreate the topic)"
  echo "   -f |--raw-file, (optional) raw file for the kafka records"
  echo "   -k |--kafka-topic, (optional) Topic name for Kafka"
  echo "   -m |--num-kafka-partitions, (optional) number of kafka partitions"
  echo "   -r |--record-key, (optional) field to use as record key"
  echo "   -o |--record-key-offset, (optional) record key offset to start with (default is 0)"
  echo "   -l |--num-hudi-partitions, (optional) number of hudi partitions"
  echo "   -p |--partition-key, (optional) field to use as partition"
  echo "   -s |--schema-file, (optional) path of the file containing the schema of the records"
  exit 1
}

case "$1" in
--help)
  usage
  exit 0
  ;;
esac

if [ $# -lt 1 ]; then
  echo "Illegal number of parameters"
  usage
  exit 0
fi

## defaults
rawDataFile=${HUDI_DIR}/docker/demo/data/batch_1.json
kafkaBrokerHostname=localhost
kafkaTopicName=hudi-test-topic
numKafkaPartitions=4
recordKey=volume
numHudiPartitions=5
partitionField=date
schemaFile=${HUDI_DIR}/docker/demo/config/schema.avsc
numBatch=1
recordValue=0
recreateTopic="Y"

while getopts ":n:b:tf:k:m:r:o:l:p:s:-:" opt; do
  case $opt in
  n)
    numRecords="$OPTARG"
    printf "Argument num-kafka-records is %s\n" "$numRecords"
    ;;
  b)
    numBatch="$OPTARG"
    printf "Argument num-batch is %s\n" "$numBatch"
    ;;
  t)
    recreateTopic="N"
    printf "Argument recreate-topic is N (reuse Kafka topic) \n"
    ;;
  k)
    rawDataFile="$OPTARG"
    printf "Argument raw-file is %s\n" "$rawDataFile"
    ;;
  f)
    kafkaTopicName="$OPTARG"
    printf "Argument kafka-topic is %s\n" "$kafkaTopicName"
    ;;
  m)
    numKafkaPartitions="$OPTARG"
    printf "Argument num-kafka-partitions is %s\n" "$numKafkaPartitions"
    ;;
  r)
    recordKey="$OPTARG"
    printf "Argument record-key is %s\n" "$recordKey"
    ;;
  o)
    recordValue="$OPTARG"
    printf "Argument record-key-offset is %s\n" "$recordValue"
    ;;
  l)
    numHudiPartitions="$OPTARG"
    printf "Argument num-hudi-partitions is %s\n" "$numHudiPartitions"
    ;;
  p)
    partitionField="$OPTARG"
    printf "Argument partition-key is %s\n" "$partitionField"
    ;;
  s)
    schemaFile="$OPTARG"
    printf "Argument schema-file is %s\n" "$schemaFile"
    ;;
  -)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

if [ $recreateTopic = "Y" ]; then
  # First delete the existing topic
  echo "Delete Kafka topic $kafkaTopicName ..."
  ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${kafkaTopicName} --bootstrap-server ${kafkaBrokerHostname}:9092

  # Create the topic with 4 partitions
  echo "Create Kafka topic $kafkaTopicName ..."
  ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${kafkaTopicName} --partitions $numKafkaPartitions --replication-factor 1 --bootstrap-server ${kafkaBrokerHostname}:9092
fi

# Setup the schema registry
export SCHEMA=$(sed 's|/\*|\n&|g;s|*/|&\n|g' ${schemaFile} | sed '/\/\*/,/*\//d' | jq tostring)
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": $SCHEMA}" http://localhost:8082/subjects/${kafkaTopicName}/versions
curl -X GET http://localhost:8081/subjects/${kafkaTopicName}/versions/latest

# Generate kafka messages from raw records
# Each records with unique keys and generate equal messages across each hudi partition
partitions={}
for ((i = 0; i < ${numHudiPartitions}; i++)); do
  partitions[$i]="partition_"$i
done

events_file=/tmp/kcat-input.events
rm -f ${events_file}

totalNumRecords=$((numRecords + recordValue))

for ((i = 1;i<=numBatch;i++)); do
  rm -f ${events_file}
  date
  echo "Start batch $i ..."
  batchRecordSeq=0
  for (( ; ; )); do
    while IFS= read line; do
      for partitionValue in "${partitions[@]}"; do
        echo $line | jq --arg recordKey $recordKey --arg recordValue $recordValue --arg partitionField $partitionField --arg partitionValue $partitionValue -c '.[$recordKey] = $recordValue | .[$partitionField] = $partitionValue' >>${events_file}
        ((recordValue = recordValue + 1))
        ((batchRecordSeq = batchRecordSeq + 1))

        if [ $batchRecordSeq -eq $numRecords ]; then
          break
        fi
      done

      if [ $batchRecordSeq -eq $numRecords ]; then
        break
      fi
    done <"$rawDataFile"

    if [ $batchRecordSeq -eq $numRecords ]; then
        date
        echo " Record key until $recordValue"
        sleep 20
        break
      fi
  done

  echo "publish to Kafka ..."
  grep -v '^$' ${events_file} | kcat -P -b ${kafkaBrokerHostname}:9092 -t ${kafkaTopicName}
done
