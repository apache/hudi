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

##
# Function to test the spark bundle with hive sync.
#
# env vars (defined in container):
#   HIVE_HOME: path to the hive directory
#   DERBY_HOME: path to the derby directory
#   SPARK_HOME: path to the spark directory
##
test_spark_bundle () {
    echo "::warning::functions.sh setting up hive metastore for spark bundle validation"

    $DERBY_HOME/bin/startNetworkServer -h 0.0.0.0 &
    $HIVE_HOME/bin/hiveserver2 &
    echo "::warning::functions.sh hive metastore setup complete. Testing"
    $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar < $WORKDIR/spark/validate.scala
    if [ "$?" -ne 0 ]; then
        echo "::error::functions.sh failed hive testing"
        exit 1
    fi
    echo "::warning::functions.sh spark bundle validation successful"
}

##
# Helper function to test the utilities bundle and utilities slim bundle + spark bundle.
# It runs deltastreamer and then verifies that deltastreamer worked correctly.
#
# 1st arg: main jar to run with spark-submit, usually it's the utilities(-slim) bundle
# 2nd arg and beyond: any additional jars to pass to --jars option
#
# env vars (defined in container):
#   SPARK_HOME: path to the spark directory
#   EXPECTED_SIZE: threshold that output directory must be larger than to pass
#       the testing. If the directory is smaller than this size then deltastreamer
#       definitely failed 
##
test_utilities_bundle_helper () {
    MAIN_JAR=$1
    printf -v EXTRA_JARS '%s,' "${@:2}"
    EXTRA_JARS="${EXTRA_JARS%,}"
    OPT_JARS=""
    if [[ -n $EXTRA_JARS ]]; then
        OPT_JARS="--jars $EXTRA_JARS"
    fi
    echo "VEXLER doing ls 63"
    ls /opt/bundle-validation/data/stocks/data

    echo "::warning::functions.sh running deltastreamer"
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
        echo "::error::functions.sh deltastreamer failed with exit code $?"
        exit 1
    fi
    echo "::warning::functions.sh done with deltastreamer"

    OUTPUT_SIZE=$(du -s ${OUTPUT_DIR} | awk '{print $1}')
    if [[ -z $OUTPUT_SIZE || "$OUTPUT_SIZE" -lt "$EXPECTED_SIZE" ]]; then
        echo "::error::functions.sh deltastreamer output folder ($OUTPUT_SIZE) is smaller than minimum expected ($EXPECTED_SIZE)" 
        exit 1
    fi
    echo "::warning::functions.sh output size is $OUTPUT_SIZE"

    echo "::warning::functions.sh validating deltastreamer in spark shell"
    SHELL_COMMAND="$SPARK_HOME/bin/spark-shell --jars $EXTRA_JARS $MAIN_JAR -i $WORKDIR/utilities/validate.scala"
    echo "::debug::this is the shell command: $SHELL_COMMAND"
    LOGFILE="$WORKDIR/${FUNCNAME[0]}.log"
    $SHELL_COMMAND >> $LOGFILE
    if [ "$?" -ne 0 ]; then
        SHELL_RESULT=$(cat $LOGFILE | grep "Counts don't match")
        echo "::error::functions.sh $SHELL_RESULT"
        exit 1
    fi
    echo "::warning::functions.sh done validating deltastreamer in spark shell"
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
    OUTPUT_DIR=/tmp/hudi-utilities-test/
    rm -r $OUTPUT_DIR
    EXPECTED_SIZE=580
    test_utilities_bundle_helper $1 "${@:2}"
    exit $?
}

##
# Function to test the upgrading the utilities bundle and 
# utilities slim bundle + spark bundle.
# It runs deltastreamer and then verifies that deltastreamer worked correctly on
# half the data. Then, using an upgraded hudi, runs deltastreamer and verifies 
# that deltastreamer worked correctly on the rest of the data
#
#
# env vars (defined in container):
#   SPARK_HOME: path to the spark directory
#   FIRST_MAIN_ARG: what you would put as the first arg to test_utilities_bundle
#       and that is used for running deltastreamer on the first batch of data
#   FIRST_ADDITIONAL_ARG: what you would put as extra args to test_utilities_bundle
#       and that is used for running deltastreamer on the first batch of data
#   SECOND_MAIN_ARG: what you would put as the first arg to test_utilities_bundle
#       and that is used for running deltastreamer on the second batch of data
#   SECOND_ADDITIONAL_ARG: what you would put as extra args to test_utilities_bundle
#       and that is used for running deltastreamer on the second batch of data
##
test_upgrade_bundle () {
    #move batch 2 away from the data folder
    mkdir /tmp/datadir
    mv $STOCK_DATA_DIR/batch_2.json /tmp/datadir/
    OUTPUT_DIR=/tmp/hudi-utilities-test/
    rm -r $OUTPUT_DIR

    #run the deltastreamer and validate
    echo "::warning::functions.sh testing upgrade bundle on batch_1"
    EXPECTED_SIZE=500
    test_utilities_bundle_helper $FIRST_MAIN_ARG $FIRST_ADDITIONAL_ARG
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    echo "::warning::functions.sh done testing upgrade bundle on batch_1"

    #move batch 2 back to the data folder
    mv /tmp/datadir/batch_2.json $STOCK_DATA_DIR/

    #run the deltastreamer and validate
    echo "::warning::functions.sh testing upgrade bundle on batch_2"
    EXPECTED_SIZE=500
    #EXPECTED_SIZE=1000
    test_utilities_bundle_helper $SECOND_MAIN_ARG $SECOND_ADDITIONAL_ARG
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    echo "::warning::functions.sh done testing upgrade bundle on batch_2"
}