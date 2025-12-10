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
MAVEN_BASE_URL=$5
echo "HUDI_VERSION: $HUDI_VERSION JAVA_RUNTIME_VERSION: $JAVA_RUNTIME_VERSION"
echo "SPARK_RUNTIME: $SPARK_RUNTIME SPARK_PROFILE (optional): $SPARK_PROFILE"
echo "FLINK_PROFILE: $FLINK_PROFILE"
echo "SCALA_PROFILE: $SCALA_PROFILE"
echo "MAVEN_BASE_URL: $MAVEN_BASE_URL"
echo "STAGING_REPO_NUM: $STAGING_REPO_NUM"

# Ensure only one of STAGING_REPO_NUM or MAVEN_BASE_URL is provided
if [[ -n "$STAGING_REPO_NUM" && -n "$MAVEN_BASE_URL" ]]; then
  echo "Error: Both STAGING_REPO_NUM and MAVEN_BASE_URL cannot be provided simultaneously."
  exit 1
fi

if [[ -n "$STAGING_REPO_NUM" ]]; then
  REPO_BASE_URL=https://repository.apache.org/content/repositories/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi
  echo "Downloading bundle jars from staging repo orgapachehudi-$REPO_BASE_URL ..."
elif [[ -n "$MAVEN_BASE_URL" ]]; then
  REPO_BASE_URL=$MAVEN_BASE_URL/org/apache/hudi
  echo "Downloading bundle jars from maven central - $REPO_BASE_URL ..."
fi

# choose versions based on build profiles
if [[ ${SPARK_RUNTIME} == 'spark3.3.4' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.17.1
  SPARK_VERSION=3.3.4
  SPARK_HADOOP_VERSION=2
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1171hive313spark334
elif [[ ${SPARK_RUNTIME} == 'spark3.4.3' ]]; then
  HADOOP_VERSION=3.3.5
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.18.1
  SPARK_VERSION=3.4.3
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1181hive313spark343
elif [[ ${SPARK_RUNTIME} == 'spark3.5.0' && ${SCALA_PROFILE} == 'scala-2.12' ]]; then
  HADOOP_VERSION=3.3.5
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.20.1
  SPARK_VERSION=3.5.0
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1200hive313spark350
elif [[ ${SPARK_RUNTIME} == 'spark3.5.0' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
  HADOOP_VERSION=3.3.5
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.20.1
  SPARK_VERSION=3.5.0
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1200hive313spark350scala213
elif [[ ${SPARK_RUNTIME} == 'spark3.5.1' && ${SCALA_PROFILE} == 'scala-2.12' ]]; then
  HADOOP_VERSION=3.3.5
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  SPARK_VERSION=3.5.1
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  if [[ ${FLINK_PROFILE} == 'flink2.0' ]]; then
    IMAGE_TAG=flink200hive313spark351
    FLINK_VERSION=2.0.0
    REPO=icshuo
  elif [[ ${FLINK_PROFILE} == 'flink2.1' ]]; then
    IMAGE_TAG=flink211hive313spark351
    FLINK_VERSION=2.1.1
    REPO=icshuo
  else
    IMAGE_TAG=flink1170hive313spark351
    FLINK_VERSION=1.17.0
  fi
elif [[ ${SPARK_RUNTIME} == 'spark3.5.1' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
  HADOOP_VERSION=3.3.5
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.20.1
  SPARK_VERSION=3.5.1
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1200hive313spark351scala213
elif [[ ${SPARK_RUNTIME} == 'spark4.0.0' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
  HADOOP_VERSION=3.4.0
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.20.1
  SPARK_VERSION=4.0.0
  SPARK_HADOOP_VERSION=3
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1200hive313spark400scala213
fi

# Copy bundle jars to temp dir for mounting
TMP_JARS_DIR=/tmp/jars/$(date +%s)
mkdir -p $TMP_JARS_DIR

if [ -z "$STAGING_REPO_NUM" ] && [ -z "$MAVEN_BASE_URL" ]; then
  echo 'Adding built bundle jars for validation'
  if [[ "$SCALA_PROFILE" != 'scala-2.13' ]]; then
    # For Scala 2.13, Flink is not support, so skipping the Flink bundle validation
    cp ${GITHUB_WORKSPACE}/packaging/hudi-flink-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
    cp ${GITHUB_WORKSPACE}/packaging/hudi-kafka-connect-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
    cp ${GITHUB_WORKSPACE}/packaging/hudi-metaserver-server-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  fi
  cp ${GITHUB_WORKSPACE}/packaging/hudi-cli-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  cp ${GITHUB_WORKSPACE}/packaging/hudi-hadoop-mr-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  cp ${GITHUB_WORKSPACE}/packaging/hudi-spark-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  cp ${GITHUB_WORKSPACE}/packaging/hudi-utilities-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  cp ${GITHUB_WORKSPACE}/packaging/hudi-utilities-slim-bundle/target/hudi-*-$HUDI_VERSION.jar $TMP_JARS_DIR/
  echo 'Validating jars below:'
else
  echo 'Adding environment variables for bundles in the release candidate or artifact'

  HUDI_HADOOP_MR_BUNDLE_NAME=hudi-hadoop-mr-bundle
  HUDI_KAFKA_CONNECT_BUNDLE_NAME=hudi-kafka-connect-bundle
  HUDI_METASERVER_SERVER_BUNDLE_NAME=hudi-metaserver-server-bundle

  if [[ ${SPARK_PROFILE} == 'spark3.3' ]]; then
    HUDI_CLI_BUNDLE_NAME=hudi-cli-bundle_2.12
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.3-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  elif [[ ${SPARK_PROFILE} == 'spark3.4' ]]; then
    HUDI_CLI_BUNDLE_NAME=hudi-cli-bundle_2.12
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.4-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  elif [[ ${SPARK_PROFILE} == 'spark3.5' && ${SCALA_PROFILE} == 'scala-2.12' ]]; then
    HUDI_CLI_BUNDLE_NAME=hudi-cli-bundle_2.12
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.5-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  elif [[ ${SPARK_PROFILE} == 'spark3.5' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
    HUDI_CLI_BUNDLE_NAME=hudi-cli-bundle_2.13
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3.5-bundle_2.13
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.13
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.13
  elif [[ ${SPARK_PROFILE} == 'spark4.0' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
    HUDI_CLI_BUNDLE_NAME=hudi-cli-bundle_2.13
    HUDI_SPARK_BUNDLE_NAME=hudi-spark4.0-bundle_2.13
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.13
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.13
  elif [[ ${SPARK_PROFILE} == 'spark3' ]]; then
    HUDI_CLI_BUNDLE_NAME=hudi-cli-bundle_2.12
    HUDI_SPARK_BUNDLE_NAME=hudi-spark3-bundle_2.12
    HUDI_UTILITIES_BUNDLE_NAME=hudi-utilities-bundle_2.12
    HUDI_UTILITIES_SLIM_BUNDLE_NAME=hudi-utilities-slim-bundle_2.12
  fi

  if [[ ${FLINK_PROFILE} == 'flink1.17' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink1.17-bundle
  elif [[ ${FLINK_PROFILE} == 'flink1.18' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink1.18-bundle
  elif [[ ${FLINK_PROFILE} == 'flink1.19' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink1.19-bundle
  elif [[ ${FLINK_PROFILE} == 'flink1.20' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink1.20-bundle
  elif [[ ${FLINK_PROFILE} == 'flink2.0' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink2.0-bundle
  elif [[ ${FLINK_PROFILE} == 'flink2.1' ]]; then
    HUDI_FLINK_BUNDLE_NAME=hudi-flink2.1-bundle
  fi

  echo "Downloading bundle jars from base URL - $REPO_BASE_URL ..."
  wget -q $REPO_BASE_URL/$HUDI_CLI_BUNDLE_NAME/$HUDI_VERSION/$HUDI_CLI_BUNDLE_NAME-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
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
--build-arg REPO=$REPO \
-t hudi-ci-bundle-validation:$IMAGE_TAG \
.

# run validation script in docker
docker run --name $CONTAINER_NAME \
  -v ${GITHUB_WORKSPACE}:/opt/bundle-validation/docker-test \
  -v $TMP_JARS_DIR:/opt/bundle-validation/jars \
  -v $TMP_DATA_DIR:/opt/bundle-validation/data \
  -i hudi-ci-bundle-validation:$IMAGE_TAG bash validate.sh $JAVA_RUNTIME_VERSION $SCALA_PROFILE $SPARK_VERSION
