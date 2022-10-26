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
# link the jar names to easier to use names
ln -sf $JARS_DIR/hudi-spark*.jar $JARS_DIR/spark.jar
ln -sf $JARS_DIR/hudi-utilities-bundle*.jar $JARS_DIR/utilities.jar
ln -sf $JARS_DIR/hudi-utilities-slim*.jar $JARS_DIR/utilities-slim.jar


##
# Function to test the spark bundle with hive sync.
#
# env vars (defined in container):
#   HIVE_HOME: path to the hive directory
#   DERBY_HOME: path to the derby directory
#   SPARK_HOME: path to the spark directory
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
# Function to test the utilities bundle and utilities slim bundle + spark bundle.
# It runs deltastreamer and then verifies that deltastreamer worked correctly.
#
# 1st arg: main jar to run with spark-submit, usually it's the utilities(-slim) bundle
# 2nd arg and beyond: any additional jars to pass to --jars option
#
# env vars (defined in container):
#   SPARK_HOME: path to the spark directory
##
test_utilities_bundle () {
    MAIN_JAR=$1
    printf -v EXTRA_JARS '%s,' "${@:2}"
    EXTRA_JARS="${EXTRA_JARS%,}"
    OPT_JARS=""
    if [[ -n $EXTRA_JARS ]]; then
        OPT_JARS="--jars $EXTRA_JARS"
    fi
    OUTPUT_DIR=/tmp/hudi-utilities-test/
    rm -r $OUTPUT_DIR
    echo "::warning::validate.sh running deltastreamer"
    $SPARK_HOME/bin/spark-submit \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
    $OPT_JARS $MAIN_JAR \
    --props $WORKDIR/utilities/hoodieapp.properties \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
    --source-class org.apache.hudi.utilities.sources.JsonDFSSource \
    --source-ordering-field ts --table-type MERGE_ON_READ \
    --target-base-path ${OUTPUT_DIR} \
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
    SHELL_COMMAND="$SPARK_HOME/bin/spark-shell $OPT_JARS $MAIN_JAR -i $WORKDIR/utilities/validate.scala"
    echo "::debug::this is the shell command: $SHELL_COMMAND"
    LOGFILE="$WORKDIR/${FUNCNAME[0]}.log"
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
  test_utilities_bundle $JARS_DIR/utilities.jar
  if [ "$?" -ne 0 ]; then
      exit 1
  fi
  echo "::warning::validate.sh done testing utilities bundle"
else
  echo "::warning::validate.sh skip testing utilities bundle for non-spark2.4 & non-spark3.1 build"
fi

echo "::warning::validate.sh testing utilities slim bundle"
test_utilities_bundle $JARS_DIR/utilities-slim.jar $JARS_DIR/spark.jar
if [ "$?" -ne 0 ]; then
    exit 1
fi
echo "::warning::validate.sh done testing utilities slim bundle"
