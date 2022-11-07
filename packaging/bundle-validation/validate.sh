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

#################################################################################################
# NOTE: this script runs inside hudi-ci-bundle-validation container
# $WORKDIR/jars/ is to mount to a host directory where bundle jars are placed
# $WORKDIR/data/ is to mount to a host directory where test data are placed with structures like
#    - <dataset name>/schema.avsc
#    - <dataset name>/data/<data files>
#################################################################################################

WORKDIR=/opt/bundle-validation
JARS_DIR=${WORKDIR}/jars
STOCK_DATA_DIR=/opt/bundle-validation/data/stocks/data
# link the jar names to easier to use names
ln -sf $JARS_DIR/hudi-spark*.jar $JARS_DIR/spark.jar
ln -sf $JARS_DIR/hudi-utilities-bundle*.jar $JARS_DIR/utilities.jar
ln -sf $JARS_DIR/hudi-utilities-slim*.jar $JARS_DIR/utilities-slim.jar

source functions.sh

test_spark_bundle
if [ "$?" -ne 0 ]; then
    exit 1
fi

if [[ $SPARK_HOME == *"spark-2.4"* ]] || [[  $SPARK_HOME == *"spark-3.1"* ]]
then
  echo "::warning::validate.sh testing utilities bundle"
  TEST_NAME="utilities bundle"
  test_utilities_bundle $JARS_DIR/utilities.jar
  if [ "$?" -ne 0 ]; then
      exit 1
  fi
  echo "::warning::validate.sh done testing utilities bundle"
else
  echo "::warning::validate.sh skip testing utilities bundle for non-spark2.4 & non-spark3.1 build"
fi

echo "::warning::validate.sh testing utilities slim bundle"
TEST_NAME="utilities slim bundle"
test_utilities_bundle $JARS_DIR/utilities-slim.jar $JARS_DIR/spark.jar
if [ "$?" -ne 0 ]; then
    exit 1
fi
echo "::warning::validate.sh done testing utilities slim bundle"




