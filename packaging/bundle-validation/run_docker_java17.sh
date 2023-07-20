#!/bin/bash

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

CONTAINER_NAME=hudi_docker
docker ps -a
docker start $CONTAINER_NAME
HADOOP_HOME=$(docker exec $CONTAINER_NAME printenv HADOOP_HOME)
echo "HADOOP_HOME: $HADOOP_HOME"
echo "pwd: $(pwd)"
ls -l
docker ps

# Upload config
docker cp packaging/bundle-validation/conf/core-site.xml $CONTAINER_NAME:$HADOOP_HOME/etc/hadoop/core-site.xml
docker cp packaging/bundle-validation/conf/hdfs-site.xml $CONTAINER_NAME:$HADOOP_HOME/etc/hadoop/hdfs-site.xml

# Start test script
docker exec $CONTAINER_NAME bash packaging/bundle-validation/docker_java17/docker_java17_tests.sh $SPARK_PROFILE $SCALA_PROFILE
