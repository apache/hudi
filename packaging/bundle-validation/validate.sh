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

# NOTE: this script runs inside hudi-ci-bundle-validation container
# $WORKDIR/jars/ is supposed to be mounted to a host directory where bundle jars are placed
# TODO: test_spark_bundle should use more jars and try different orders to
#   detect class loading issues

WORKDIR=/opt/bundle-validation
JARS_DIR=${WORKDIR}/jars
# link the jar names to easier to use names
ln -sf $JARS_DIR/hudi-spark*.jar $JARS_DIR/spark.jar
ln -sf $JARS_DIR/hudi-utilities-bundle*.jar $JARS_DIR/utilities.jar
ln -sf $JARS_DIR/hudi-utilities-slim*.jar $JARS_DIR/utilities-slim.jar


##
# used to test the spark bundle with hive sync
# Env Vars:
#   HIVE_HOME: path to the hive directory
#   SPARK_HOME: path to the spark directory
#   DERBY_HOME: path to the derby directory
#   JARS_DIR: path to the directory where our bundle jars to test are located
##
test_spark_bundle () {
    echo "::warning::validate.sh setting up hive metastore for spark bundle validation"

    $DERBY_HOME/bin/startNetworkServer -h 0.0.0.0 &
    $HIVE_HOME/bin/hiveserver2 &
    echo "::warning::validate.sh hive metastore setup complete. Testing"
    $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar < $WORKDIR/spark/validate.scala
    if [ "$?" -ne 0 ]; then
        echo "::error::validate.sh failed hive testing"
        exit 1
    fi
    echo "::warning::validate.sh spark bundle validation successful"
}


##
# Runs deltastreamer and then verifies that deltastreamer worked correctly
# Used to test the utilities bundle and utilities slim bundle + spark bundle
# Inputs:
#   SPARK_HOME: path to the spark directory
#   MAIN_JAR: path to the main jar to run with spark-shell or spark-submit
#   ADDITIONAL_JARS: comma seperated list of additional jars to be used
#   OUTPUT_DIR: directory where delta streamer will output to
#   COMMANDS_FILE: path to file of scala commands that we will run in
#       spark-shell to validate the delta streamer
# Modifies: OPT_JARS, OUTPUT_SIZE, SHELL_COMMAND, LOGFILE, SHELL_RESULT
##
test_utilities_bundle () {
    OPT_JARS=""
    if [[ -n $ADDITIONAL_JARS ]]; then
        OPT_JARS="--jars $ADDITIONAL_JARS"
    fi
    echo "::warning::validate.sh running deltastreamer"
    $SPARK_HOME/bin/spark-submit \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
    $OPT_JARS $MAIN_JAR \
    --props $WORKDIR/utilities/hoodieapp.properties \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
    --source-class org.apache.hudi.utilities.sources.JsonDFSSource \
    --source-ordering-field ts --table-type MERGE_ON_READ \
    --target-base-path file://${OUTPUT_DIR} \
    --target-table utilities_tbl  --op UPSERT
    if [ "$?" -ne 0 ]; then
        echo "::error::validate.sh deltastreamer failed with exit code $?"
        exit 1
    fi
    echo "::warning::validate.sh done with deltastreamer"

    OUTPUT_SIZE=$(du -s ${OUTPUT_DIR} | awk '{print $1}')
    if [[ -z $OUTPUT_SIZE || "$OUTPUT_SIZE" -lt "580" ]]; then
        echo "::error::validate.sh deltastreamer output folder ($OUTPUT_SIZE) is smaller than minimum expected (580)" 
        exit 1
    fi

    echo "::warning::validate.sh validating deltastreamer in spark shell"
    SHELL_COMMAND="$SPARK_HOME/bin/spark-shell --jars $ADDITIONAL_JARS $MAIN_JAR -i $COMMANDS_FILE"
    echo "::debug::this is the shell command: $SHELL_COMMAND"
    LOGFILE="$WORKDIR/submit.log"
    $SHELL_COMMAND >> $LOGFILE
    if [ "$?" -ne 0 ]; then
        SHELL_RESULT=$(cat $LOGFILE | grep "Counts don't match")
        echo "::error::validate.sh $SHELL_RESULT"
        exit 1
    fi
    echo "::warning::validate.sh done validating deltastreamer in spark shell"
}


test_spark_bundle
if [ "$?" -ne 0 ]; then
    exit 1
fi

if [[ $SPARK_HOME == *"spark-2.4"* ]] || [[  $SPARK_HOME == *"spark-3.1"* ]]
then
  echo "::warning::validate.sh testing utilities bundle"
  MAIN_JAR=$JARS_DIR/utilities.jar
  ADDITIONAL_JARS=""
  OUTPUT_DIR=/tmp/hudi-utilities-test/
  rm -rf $OUTPUT_DIR
  COMMANDS_FILE=$WORKDIR/utilities/commands.scala
  test_utilities_bundle
  if [ "$?" -ne 0 ]; then
      exit 1
  fi
  echo "::warning::validate.sh done testing utilities bundle"
else
  echo "::warning::validate.sh skip testing utilities bundle for non-spark2.4 & non-spark3.1 build"
fi

echo "::warning::validate.sh testing utilities slim bundle"
MAIN_JAR=$JARS_DIR/utilities-slim.jar
ADDITIONAL_JARS=$JARS_DIR/spark.jar
OUTPUT_DIR=/tmp/hudi-utilities-slim-test/
rm -rf $OUTPUT_DIR
COMMANDS_FILE=$WORKDIR/utilities/slimcommands.scala
test_utilities_bundle
if [ "$?" -ne 0 ]; then
    exit 1
fi
echo "::warning::validate.sh done testing utilities slim bundle"

