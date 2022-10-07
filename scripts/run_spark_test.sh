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


#Get name of file to download
SPARK_FOLDER_NAME=""
SPARK_PREFIX_NAME=""
SPARK_BUNDLE_NUM=""

case $SPARK_PROFILE in

    "spark2.4")
        SPARK_FOLDER_NAME="spark-2.4.7-bin-hadoop2.7"
        SPARK_PREFIX_NAME="spark-2.4.7"
        SPARK_BUNDLE_NUM="2.4"
        ;;

    "spark3.1")
        SPARK_FOLDER_NAME="spark-3.1.3-bin-hadoop3.2"
        SPARK_PREFIX_NAME="spark-3.1.3"
        SPARK_BUNDLE_NUM="3.1"
        ;;

    "spark3.2")
        SPARK_FOLDER_NAME="spark-3.2.1-bin-hadoop3.2"
        SPARK_PREFIX_NAME="spark-3.2.1"
        SPARK_BUNDLE_NUM="3.2"
        ;;

    "spark3.3")
        SPARK_FOLDER_NAME="spark-3.3.0-bin-hadoop3"
        SPARK_PREFIX_NAME="spark-3.3.0"
        SPARK_BUNDLE_NUM="3.3"
        ;;

    *)
        echo "::error::run_spark_test.sh unknown spark version: ${SPARK_PROFILE}"
        exit 1
        ;;
esac

#download and extract file
SPARK_URL=https://archive.apache.org/dist/spark/${SPARK_PREFIX_NAME}/${SPARK_FOLDER_NAME}.tgz
wget --tries=3 $SPARK_URL || { echo "::error::run_spark_test.sh failed to download ${SPARK_URL}" && exit 1; }
tar -xzvf ${SPARK_FOLDER_NAME}.tgz || { echo "::error::run_spark_test.sh failed to extract ${SPARK_FOLDER_NAME}.tgz" && exit 1; }

export SPARK_HOME=${PWD}/${SPARK_FOLDER_NAME}
export PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

#set locations in props file
TEST_DATA_FOLDER=${PWD}/scripts/test_data
PROPS_FILE=${TEST_DATA_FOLDER}/parquet-dfs-compact.props
echo hoodie.deltastreamer.source.dfs.root=${TEST_DATA_FOLDER}/parquet_src >> $PROPS_FILE
echo hoodie.deltastreamer.schemaprovider.target.schema.file=file:${TEST_DATA_FOLDER}/ny.avsc >> $PROPS_FILE
echo hoodie.deltastreamer.schemaprovider.source.schema.file=file:${TEST_DATA_FOLDER}/ny.avsc >> $PROPS_FILE


SPARK_SUBMIT=${SPARK_HOME}/bin/spark-submit

#get utilities jar name
SCALA_NUM=""
case $SCALA_PROFILE in

    "scala-2.11")
        SCALA_NUM="2.11"
        ;;

    "scala-2.12")
        SCALA_NUM="2.12"
        ;;

    *)
        echo "::error::run_spark_test.sh unknown scala version: ${SCALA_PROFILE}"
        exit 1
        ;;
esac
UTILITIES_BUNDLE_NAME=${PWD}/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_${SCALA_NUM}-0.13.0-SNAPSHOT.jar

SPARK_SUBMIT_COMMAND="${SPARK_SUBMIT} --driver-memory 8g --executor-memory 8g"
SPARK_SUBMIT_COMMAND+=" --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer"
SPARK_SUBMIT_COMMAND+=" ${UTILITIES_BUNDLE_NAME} --props ${PROPS_FILE}"
SPARK_SUBMIT_COMMAND+=" --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
SPARK_SUBMIT_COMMAND+=" --source-class org.apache.hudi.utilities.sources.ParquetDFSSource"
SPARK_SUBMIT_COMMAND+=" --source-ordering-field date_col --table-type MERGE_ON_READ"
SPARK_SUBMIT_COMMAND+=" --target-base-path file:///tmp/hudi-deltastreamer-ny/"
SPARK_SUBMIT_COMMAND+=" --target-table ny_hudi_tbl  --op UPSERT"

#run spark submit command
#$SPARK_SUBMIT_COMMAND || { echo "::error::run_spark_test.sh deltastreamer failed" && exit 1; }

SPARK_SHELL=${SPARK_HOME}/bin/spark-shell
#create spark shell command
SPARK_SHELL_COMMAND="${SPARK_SHELL} --jars ${UTILITIES_BUNDLE_NAME}" 
SPARK_SHELL_COMMAND+=" --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'"
if [[ $SPARK_PROFILE = "spark3.2" || $SPARK_PROFILE = "spark3.3" ]]; then
    SPARK_SHELL_COMMAND+=" --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'"
fi
SPARK_SHELL_COMMAND+=" --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'"
SPARK_SHELL_COMMAND+=" -i ${TEST_DATA_FOLDER}/commands.scala"
#run spark shell command
$SPARK_SHELL_COMMAND > "sparktest.log" || { SHELL_RESULT=$(cat sparktest.log | grep "Counts don't match") && echo "::error::run_spark_test.sh $SHELL_RESULT" && exit 1; }
exit 0
