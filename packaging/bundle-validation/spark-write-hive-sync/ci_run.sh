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
# this script is to run by GitHub Actions CI tasks from the project root directory
# and contains environment-specific variables

HUDI_VERSION=$1
# to store bundle jars for validation
mkdir ${GITHUB_WORKSPACE}/jars
cp packaging/hudi-spark-bundle/target/hudi-*-$HUDI_VERSION.jar ${GITHUB_WORKSPACE}/jars
echo 'Validating jars below:'
ls -l ${GITHUB_WORKSPACE}/jars

# choose versions based on build profiles
if [[ ${SPARK_PROFILE} == 'spark2.4' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=2.3.9
  DERBY_VERSION=10.10.2.0
  SPARK_VERSION=2.4.8
  SPARK_HADOOP_VERSION=2.7
  IMAGE_TAG=spark248hive239
elif [[ ${SPARK_PROFILE} == 'spark3.1' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  SPARK_VERSION=3.1.3
  SPARK_HADOOP_VERSION=2.7
  IMAGE_TAG=spark313hive313
elif [[ ${SPARK_PROFILE} == 'spark3.2' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  SPARK_VERSION=3.2.2
  SPARK_HADOOP_VERSION=2.7
  IMAGE_TAG=spark322hive313
elif [[ ${SPARK_PROFILE} == 'spark3.3' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  SPARK_VERSION=3.3.0
  SPARK_HADOOP_VERSION=2
  IMAGE_TAG=spark330hive313
fi

cd packaging/bundle-validation/spark-write-hive-sync || exit 1
docker build \
--build-arg HADOOP_VERSION=$HADOOP_VERSION \
--build-arg HIVE_VERSION=$HIVE_VERSION \
--build-arg DERBY_VERSION=$DERBY_VERSION \
--build-arg SPARK_VERSION=$SPARK_VERSION \
--build-arg SPARK_HADOOP_VERSION=$SPARK_HADOOP_VERSION \
-t hudi-ci-bundle-validation:$IMAGE_TAG \
.
docker run -v ${GITHUB_WORKSPACE}/jars:/opt/hudi-bundles/jars -i hudi-ci-bundle-validation:$IMAGE_TAG bash validate.sh
