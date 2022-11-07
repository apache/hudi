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

# Note:
#
# This script is to
#  - set the corresponding variables based on CI job's build profiles
#  - prepare Hudi bundle jars for mounting into Docker container for validation
#  - prepare test datasets for mounting into Docker container for validation
#
# This is to run by GitHub Actions CI tasks from the project root directory
# and it contains the CI environment-specific variables.

HUDI_VERSION=$1

# choose versions based on build profiles
if [[ ${SPARK_PROFILE} == 'spark2.4' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=2.3.9
  DERBY_VERSION=10.10.2.0
  SPARK_VERSION=2.4.8
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=spark248hive239
elif [[ ${SPARK_PROFILE} == 'spark3.1' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  SPARK_VERSION=3.1.3
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=spark313hive313
elif [[ ${SPARK_PROFILE} == 'spark3.2' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  SPARK_VERSION=3.2.3
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=spark323hive313
elif [[ ${SPARK_PROFILE} == 'spark3.3' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  SPARK_VERSION=3.3.1
  SPARK_HADOOP_VERSION=2
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=spark331hive313
fi

# Copy bundle jars to temp dir for mounting
TMP_JARS_DIR=/tmp/jars/$(date +%s)
mkdir -p $TMP_JARS_DIR
cp ${GITHUB_WORKSPACE}/packaging/hudi-hadoop-mr-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
cp ${GITHUB_WORKSPACE}/packaging/hudi-spark-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
cp ${GITHUB_WORKSPACE}/packaging/hudi-utilities-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
cp ${GITHUB_WORKSPACE}/packaging/hudi-utilities-slim-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
cp ${GITHUB_WORKSPACE}/packaging/hudi-kafka-connect-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
echo 'Validating jars below:'
ls -l $TMP_JARS_DIR

# Copy test dataset
TMP_DATA_DIR=/tmp/data/$(date +%s)
mkdir -p $TMP_DATA_DIR/stocks/data
cp ${GITHUB_WORKSPACE}/docker/demo/data/*.json $TMP_DATA_DIR/stocks/data/
cp ${GITHUB_WORKSPACE}/docker/demo/config/schema.avsc $TMP_DATA_DIR/stocks/

# build docker image
cd ${GITHUB_WORKSPACE}/packaging/bundle-validation || exit 1
docker build \
--build-arg HADOOP_VERSION=$HADOOP_VERSION \
--build-arg HIVE_VERSION=$HIVE_VERSION \
--build-arg DERBY_VERSION=$DERBY_VERSION \
--build-arg SPARK_VERSION=$SPARK_VERSION \
--build-arg SPARK_HADOOP_VERSION=$SPARK_HADOOP_VERSION \
--build-arg CONFLUENT_VERSION=$CONFLUENT_VERSION \
--build-arg KAFKA_CONNECT_HDFS_VERSION=$KAFKA_CONNECT_HDFS_VERSION \
--build-arg IMAGE_TAG=$IMAGE_TAG \
-t hudi-ci-bundle-validation:$IMAGE_TAG \
.

# run validation script in docker
docker run -v $TMP_JARS_DIR:/opt/bundle-validation/jars -v $TMP_DATA_DIR:/opt/bundle-validation/data \
  -i hudi-ci-bundle-validation:$IMAGE_TAG bash validate.sh
