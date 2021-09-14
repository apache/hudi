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

## Directories
HOME_DIR=~
HUDI_DIR=${HOME_DIR}/hudi
KAFKA_HOME=${HOME_DIR}/kafka

#########################
# The command line help #
#########################
usage() {
    echo "Usage: $0"
    echo "   -n |--num-kafka-records, (required) number of kafka records to generate"
    echo "   -f |--raw-file, (optional) raw file for the kafka records"
    echo "   -k |--kafka-topic, (optional) Topic name for Kafka"
    echo "   -m |--num-kafka-partitions, (optional) number of kafka partitions"
    echo "   -r |--record-key, (optional) field to use as record key"
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
kafkaTopicName=hudi-test-topic
numKafkaPartitions=4
recordKey=volume
numHudiPartitions=5
partitionField=date
schemaFile=${HUDI_DIR}/docker/demo/config/schema.avsc

while getopts ":n:f:k:m:r:l:p:s:-:" opt; do
  case $opt in
    n) num_records="$OPTARG"
    printf "Argument num-kafka-records is %s\n" "$num_records"
    ;;
    k) rawDataFile="$OPTARG"
    printf "Argument raw-file is %s\n" "$rawDataFile"
    ;;
    f) kafkaTopicName="$OPTARG"
    printf "Argument kafka-topic is %s\n" "$kafkaTopicName"
    ;;
    m) numKafkaPartitions="$OPTARG"
    printf "Argument num-kafka-partitions is %s\n" "$numKafkaPartitions"
    ;;
    r) recordKey="$OPTARG"
    printf "Argument record-key is %s\n" "$recordKey"
    ;;
    l) numHudiPartitions="$OPTARG"
    printf "Argument num-hudi-partitions is %s\n" "$numHudiPartitions"
    ;;
    p) partitionField="$OPTARG"
    printf "Argument partition-key is %s\n" "$partitionField"
    ;;
    p) schemaFile="$OPTARG"
    printf "Argument schema-file is %s\n" "$schemaFile"
    ;;
    -) echo "Invalid option -$OPTARG" >&2
    ;;
esac
done

# First delete the existing topic
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic ${kafkaTopicName} --bootstrap-server localhost:9092

# Create the topic with 4 partitions
$KAFKA_HOME/bin/kafka-topics.sh --create --topic ${kafkaTopicName} --partitions $numKafkaPartitions --replication-factor 1 --bootstrap-server localhost:9092


# Setup the schema registry
export SCHEMA=`sed 's|/\*|\n&|g;s|*/|&\n|g' ${schemaFile} | sed '/\/\*/,/*\//d' | jq tostring`
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": $SCHEMA}" http://localhost:8081/subjects/${kafkaTopicName}/versions
curl -X GET http://localhost:8081/subjects/${kafkaTopicName}/versions/latest


# Generate kafka messages from raw records
# Each records with unique keys and generate equal messages across each hudi partition
partitions={}
for ((i=0; i<${numHudiPartitions}; i++))
do
    partitions[$i]="partition-"$i;
done

for ((recordValue=0; recordValue<=${num_records}; ))
do 
    while IFS= read line 
    do
        for partitionValue in "${partitions[@]}"
        do
            echo $line | jq --arg recordKey $recordKey --arg recordValue $recordValue --arg partitionField $partitionField --arg partitionValue $partitionValue -c '.[$recordKey] = $recordValue | .[$partitionField] = $partitionValue' | kafkacat -P -b localhost:9092 -t hudi-test-topic;
            ((recordValue++));
            if [ $recordValue -gt ${num_records} ]; then
                exit 0
            fi
        done
        
        if [ $(( $recordValue % 1000 )) -eq 0 ]
            then sleep 1
        fi
    done < "$rawDataFile"
done 
