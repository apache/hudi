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

# Copy bundle jars
BUNDLE_VALIDATION_DIR=${GITHUB_WORKSPACE}/bundle-validation
mkdir $BUNDLE_VALIDATION_DIR
JARS_DIR=${BUNDLE_VALIDATION_DIR}/jars
mkdir $JARS_DIR
cp ${GITHUB_WORKSPACE}/packaging/hudi-spark-bundle/target/hudi-*-$HUDI_VERSION.jar $JARS_DIR/
cp ${GITHUB_WORKSPACE}/packaging/hudi-utilities-bundle/target/hudi-*-$HUDI_VERSION.jar $JARS_DIR/
cp ${GITHUB_WORKSPACE}/packaging/hudi-utilities-slim-bundle/target/hudi-*-$HUDI_VERSION.jar $JARS_DIR/
echo 'Validating jars below:'
ls -l $JARS_DIR

# Copy hive data
cp -r ${GITHUB_WORKSPACE}/packaging/bundle-validation/hive ${BUNDLE_VALIDATION_DIR}/

# Copy utilities data
cp -r ${GITHUB_WORKSPACE}/packaging/bundle-validation/utilities ${BUNDLE_VALIDATION_DIR}/
cp -r ${GITHUB_WORKSPACE}/docker/demo/data ${BUNDLE_VALIDATION_DIR}/utilities/
cp  ${GITHUB_WORKSPACE}/docker/demo/config/schema.avsc ${BUNDLE_VALIDATION_DIR}/utilities/

# add shell args to utilities data
SHELL_ARGS=" --conf spark.serializer=org.apache.spark.serializer.KryoSerializer" 
if [[ $SPARK_PROFILE = "spark3.2" || $SPARK_PROFILE = "spark3.3" ]]; then
    SHELL_ARGS+=" --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog"
fi
SHELL_ARGS+=" --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
echo $SHELL_ARGS > ${BUNDLE_VALIDATION_DIR}/utilities/shell_args

# build docker image
cd ${GITHUB_WORKSPACE}/packaging/bundle-validation || exit 1
docker build \
--build-arg HADOOP_VERSION=$HADOOP_VERSION \
--build-arg HIVE_VERSION=$HIVE_VERSION \
--build-arg DERBY_VERSION=$DERBY_VERSION \
--build-arg SPARK_VERSION=$SPARK_VERSION \
--build-arg SPARK_HADOOP_VERSION=$SPARK_HADOOP_VERSION \
-t hudi-ci-bundle-validation:$IMAGE_TAG \
.

# run script in docker
docker run -v ${GITHUB_WORKSPACE}/bundle-validation:/opt/bundle-validation/data -i hudi-ci-bundle-validation:$IMAGE_TAG bash validate.sh
