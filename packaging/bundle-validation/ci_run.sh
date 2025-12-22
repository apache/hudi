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

CONTAINER_NAME=$1
HUDI_VERSION=$2
JAVA_RUNTIME_VERSION=$3
STAGING_REPO_NUM=$4
echo "HUDI_VERSION: $HUDI_VERSION JAVA_RUNTIME_VERSION: $JAVA_RUNTIME_VERSION"
echo "SPARK_RUNTIME: $SPARK_RUNTIME SPARK_PROFILE (optional): $SPARK_PROFILE"
echo "SCALA_PROFILE: $SCALA_PROFILE"

# choose versions based on build profiles
if [[ ${SPARK_RUNTIME} == 'spark2.4.8' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=2.3.9
  DERBY_VERSION=10.10.2.0
  FLINK_VERSION=1.14.6
  SPARK_VERSION=2.4.8
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1146hive239spark248
elif [[ ${SPARK_RUNTIME} == 'spark3.0.2' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.14.6
  SPARK_VERSION=3.0.2
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1146hive313spark302
elif [[ ${SPARK_RUNTIME} == 'spark3.1.3' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.14.6
  SPARK_VERSION=3.1.3
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1146hive313spark313
elif [[ ${SPARK_RUNTIME} == 'spark3.2.3' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.15.3
  SPARK_VERSION=3.2.3
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1153hive313spark323
elif [[ ${SPARK_RUNTIME} == 'spark3.3.1' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.16.2
  SPARK_VERSION=3.3.1
  SPARK_HADOOP_VERSION=2
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1162hive313spark331
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
  FLINK_VERSION=1.18.0
  SPARK_VERSION=3.4.0
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1180hive313spark340
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
fi

# Copy bundle jars to temp dir for mounting
TMP_JARS_DIR=/tmp/jars/$(date +%s)
mkdir -p $TMP_JARS_DIR

if [[ -z "$STAGING_REPO_NUM" ]]; then
  echo 'Adding built bundle jars for validation'
  if [[ "$SCALA_PROFILE" != 'scala-2.13' ]]; then
    # For Scala 2.13, Flink is not support, so skipping the Flink bundle validation
    cp ${GITHUB_WORKSPACE}/packaging/hudi-flink-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
    cp ${GITHUB_WORKSPACE}/packaging/hudi-kafka-connect-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
    cp ${GITHUB_WORKSPACE}/packaging/hudi-metaserver-server-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  fi
  cp ${GITHUB_WORKSPACE}/packaging/hudi-hadoop-mr-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  cp ${GITHUB_WORKSPACE}/packaging/hudi-spark-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  cp ${GITHUB_WORKSPACE}/packaging/hudi-utilities-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  cp ${GITHUB_WORKSPACE}/packaging/hudi-utilities-slim-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  echo 'Validating jars below:'
else
  echo 'Adding environment variables for bundles in the release candidate'

  HUDI_HADOOP_MR_BUNDLE_NAME=hudi-hadoop-mr-bundle
  HUDI_KAFKA_CONNECT_BUNDLE_NAME=hudi-kafka-connect-bundle
  HUDI_METASERVER_SERVER_BUNDLE_NAME=hudi-metaserver-server-bundle

  if [[ ${SPARK_PROFILE} == 'spark' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark-bundle_2.11
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.11
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.11
  elif [[ ${SPARK_PROFILE} == 'spark2.4' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark2.4-bundle_2.11
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.11
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.11
  elif [[ ${SPARK_PROFILE} == 'spark3.0' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.0-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  elif [[ ${SPARK_PROFILE} == 'spark3.1' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.1-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  elif [[ ${SPARK_PROFILE} == 'spark3.2' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.2-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  elif [[ ${SPARK_PROFILE} == 'spark3.3' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.3-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  elif [[ ${SPARK_PROFILE} == 'spark3.4' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.4-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  elif [[ ${SPARK_PROFILE} == 'spark3.5' && ${SCALA_PROFILE} == 'scala-2.12' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.5-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  elif [[ ${SPARK_PROFILE} == 'spark3.5' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.5-bundle_2.13
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.13
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.13
  elif [[ ${SPARK_PROFILE} == 'spark3' ]]; then
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  fi

  if [[ ${FLINK_PROFILE} == 'flink1.14' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink1.14-bundle
  elif [[ ${FLINK_PROFILE} == 'flink1.15' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink1.15-bundle
  elif [[ ${FLINK_PROFILE} == 'flink1.16' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink1.16-bundle
  elif [[ ${FLINK_PROFILE} == 'flink1.17' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink1.17-bundle
  elif [[ ${FLINK_PROFILE} == 'flink1.18' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink1.18-bundle
  fi

  echo "Downloading bundle jars from staging repo orgapachehudi-$STAGING_REPO_NUM ..."
  REPO_BASE_URL=https://repository.apache.org/content/repositories/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi
  wget -q $REPO_BASE_URL/$HUDI_FLINK_BUNDLE_NAME/$HUDI_VERSION/$HUDI_FLINK_BUNDLE_NAME-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
  wget -q $REPO_BASE_URL/$HUDI_HADOOP_MR_BUNDLE_NAME/$HUDI_VERSION/$HUDI_HADOOP_MR_BUNDLE_NAME-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
  wget -q $REPO_BASE_URL/$HUDI_KAFKA_CONNECT_BUNDLE_NAME/$HUDI_VERSION/$HUDI_KAFKA_CONNECT_BUNDLE_NAME-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
  wget -q $REPO_BASE_URL/$HUDI_SPARK_BUNDLE_NAME/$HUDI_VERSION/$HUDI_SPARK_BUNDLE_NAME-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
  wget -q $REPO_BASE_URL/$HUDI_UTILITIES_BUNDLE_NAME/$HUDI_VERSION/$HUDI_UTILITIES_BUNDLE_NAME-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
  wget -q $REPO_BASE_URL/$HUDI_UTILITIES_SLIM_BUNDLE_NAME/$HUDI_VERSION/$HUDI_UTILITIES_SLIM_BUNDLE_NAME-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
  wget -q $REPO_BASE_URL/$HUDI_METASERVER_SERVER_BUNDLE_NAME/$HUDI_VERSION/$HUDI_METASERVER_SERVER_BUNDLE_NAME-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
  echo "Downloaded these jars from $REPO_BASE_URL for validation:"
fi

ls -l $TMP_JARS_DIR

# Copy test dataset
TMP_DATA_DIR=/tmp/data/$(date +%s)
mkdir -p $TMP_DATA_DIR/stocks/data
cp ${GITHUB_WORKSPACE}/docker/demo/data/*.json $TMP_DATA_DIR/stocks/data/
cp ${GITHUB_WORKSPACE}/docker/demo/config/schema.avsc $TMP_DATA_DIR/stocks/

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

# run validation script in docker
docker run --name $CONTAINER_NAME \
  -v ${GITHUB_WORKSPACE}:/opt/bundle-validation/docker-test \
  -v $TMP_JARS_DIR:/opt/bundle-validation/jars \
  -v $TMP_DATA_DIR:/opt/bundle-validation/data \
  -i hudi-ci-bundle-validation:$IMAGE_TAG bash validate.sh $JAVA_RUNTIME_VERSION $SCALA_PROFILE
