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

source ${PWD}/scripts/get_spark_hadoop.sh
process_spark_version
process_scala_version

VERSION_STRING="${SCALA_NUM}_${SPARK_PROFILE}"

#Set spark env vars
export SPARK_HOME=${PWD}/${SPARK_FOLDER_NAME}
export PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

#download and extract file
if [[ ! -d $SPARK_HOME ]]; then
    download_spark
fi 



SPARK_BUNDLE_NAME=${PWD}/packaging/hudi-spark-bundle/target/hudi-${SPARK_PROFILE}-bundle_${SCALA_NUM}-0.13.0-SNAPSHOT.jar
SLIM_EXAMPLE_BUNDLE=${PWD}/hudi-examples/hudi-examples-spark-slim/target/hudi-examples-spark-slim-0.13.0-SNAPSHOT.jar

SPARK_SUBMIT=${SPARK_HOME}/bin/spark-submit
OUTPUT_FOLDER_NAME="/tmp/spark-bundle-test_${VERSION_STRING}/"
SPARK_SUBMIT_COMMAND="${SPARK_SUBMIT} --driver-memory 8g --executor-memory 8g"
SPARK_SUBMIT_COMMAND+=" --class org.apache.hudi.examples.quickstartslim.HoodieSparkQuickstartSlim"
SPARK_SUBMIT_COMMAND+="  --jars ${SPARK_BUNDLE_NAME} ${SLIM_EXAMPLE_BUNDLE}"
SPARK_SUBMIT_COMMAND+=" /tmp/HoodieSparkQuickstartSlim HoodieSparkQuickstartSlim"

$SPARK_SUBMIT_COMMAND

