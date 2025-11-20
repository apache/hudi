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

echo "SPARK_RUNTIME: $SPARK_RUNTIME SPARK_PROFILE (optional): $SPARK_PROFILE"
echo "SCALA_PROFILE: $SCALA_PROFILE"
CONTAINER_NAME=hudi_docker
DOCKER_TEST_DIR=/opt/bundle-validation/docker-test

# choose versions based on build profiles
if [[ ${SPARK_RUNTIME} == 'spark3.3.1' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.15.3
  SPARK_VERSION=3.3.1
  SPARK_HADOOP_VERSION=2
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1153hive313spark331
elif [[ ${SPARK_RUNTIME} == 'spark3.3.2' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.17.0
  SPARK_VERSION=3.3.2
  SPARK_HADOOP_VERSION=2
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1170hive313spark332
elif [[ ${SPARK_RUNTIME} == 'spark3.4.0' ]]; then
  HADOOP_VERSION=3.3.5
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.17.0
  SPARK_VERSION=3.4.0
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1170hive313spark340
elif [[ ${SPARK_RUNTIME} == 'spark3.5.0' && ${SCALA_PROFILE} == 'scala-2.12' ]]; then
  HADOOP_VERSION=3.3.5
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.18.0
  SPARK_VERSION=3.5.0
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1180hive313spark350
elif [[ ${SPARK_RUNTIME} == 'spark3.5.0' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
  HADOOP_VERSION=3.3.5
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.18.0
  SPARK_VERSION=3.5.0
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1180hive313spark350scala213
elif [[ ${SPARK_RUNTIME} == 'spark4.0.0' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
  HADOOP_VERSION=3.4.0
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.18.0
  SPARK_VERSION=4.0.0
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1200hive313spark400scala213
fi

# build docker image
cd ${GITHUB_WORKSPACE}/packaging/bundle-validation || exit 1
docker build \
--build-arg SCALA_VERSION=$SCALA_PROFILE \
--build-arg HADOOP_VERSION=$HADOOP_VERSION \
--build-arg HIVE_VERSION=$HIVE_VERSION \
--build-arg DERBY_VERSION=$DERBY_VERSION \
--build-arg FLINK_VERSION=$FLINK_VERSION \
--build-arg SPARK_VERSION=$SPARK_VERSION \
--build-arg SPARK_HADOOP_VERSION=$SPARK_HADOOP_VERSION \
--build-arg CONFLUENT_VERSION=$CONFLUENT_VERSION \
--build-arg KAFKA_CONNECT_HDFS_VERSION=$KAFKA_CONNECT_HDFS_VERSION \
--build-arg IMAGE_TAG=$IMAGE_TAG \
-t hudi-ci-bundle-validation:$IMAGE_TAG \
.

# run Java 17 test script in docker
docker run --name $CONTAINER_NAME \
  -v ${GITHUB_WORKSPACE}:$DOCKER_TEST_DIR \
  -i hudi-ci-bundle-validation:$IMAGE_TAG bash docker_java17/docker_java17_test.sh $SPARK_PROFILE $SCALA_PROFILE
