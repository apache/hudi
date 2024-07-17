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

SCRIPT_PATH=$(cd `dirname $0`; pwd)
HUDI_DEMO_ENV=$1
WS_ROOT=`dirname $SCRIPT_PATH`
#PLATFORM_TYPE_ARG=$(uname -m)
#COMPOSE_FILE_NAME="docker-compose_hadoop335_hive233_spark351_$PLATFORM_TYPE_ARG.yml"
COMPOSE_FILE_NAME="docker-compose_hadoop335_hive233_spark351.yml"
# restart cluster
HUDI_WS=${WS_ROOT} docker compose -f ${SCRIPT_PATH}/compose/${COMPOSE_FILE_NAME} down
if [ "$HUDI_DEMO_ENV" != "dev" ]; then
  echo "Pulling docker demo images ..."
  HUDI_WS=${WS_ROOT} docker compose -f ${SCRIPT_PATH}/compose/${COMPOSE_FILE_NAME} pull
fi
sleep 5
HUDI_WS=${WS_ROOT} docker compose -f ${SCRIPT_PATH}/compose/${COMPOSE_FILE_NAME} up -d
sleep 15

docker exec -it adhoc-1 /bin/bash /var/hoodie/ws/docker/demo/setup_demo_container.sh
docker exec -it adhoc-2 /bin/bash /var/hoodie/ws/docker/demo/setup_demo_container.sh

#source /Users/jon/hudi_commands.sh
#set_spark 3.5
#docker cp $(hudi_util_slim_bundle)  adhoc-2:/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-utilities-slim.jar
#docker cp $(hudi_spark_bundle) adhoc-2:/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-spark.jar
#docker cp /Users/jon/.m2/repository/org/apache/parquet/parquet-avro/1.13.1/parquet-avro-1.13.1.jar adhoc-2:/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/parquet-avro-1.13.1.jar
