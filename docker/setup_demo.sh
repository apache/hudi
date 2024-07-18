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
PLATFORM_TYPE_ARG=$(uname -m)
if [ "$PLATFORM_TYPE_ARG" = "aarch64" ]; then
  PLATFORM_TYPE_ARG="arm64"
fi
COMPOSE_FILE_NAME="docker-compose_hadoop335_hive233_spark351_$PLATFORM_TYPE_ARG.yml"
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


docker cp $WS_ROOT/packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.12-1.0.0-SNAPSHOT.jar  adhoc-2:/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-utilities-slim.jar
docker cp $WS_ROOT/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.0.0-SNAPSHOT.jar adhoc-2:/var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-spark.jar