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

case $SPARK_PROFILE in

    "spark2.4")
        SPARK_FOLDER_NAME="spark-2.4.7-bin-hadoop2.7"
        SPARK_PREFIX_NAME="spark-2.4.7"
        ;;

    "spark3.1")
        SPARK_FOLDER_NAME="spark-3.1.3-bin-hadoop3.2"
        SPARK_PREFIX_NAME="spark-3.1.3"
        ;;

    "spark3.2")
        SPARK_FOLDER_NAME="spark-3.2.1-bin-hadoop3.2"
        SPARK_PREFIX_NAME="spark-3.2.1"
        ;;

    "spark3.3")
        SPARK_FOLDER_NAME="spark-3.3.0-bin-hadoop3.2"
        SPARK_PREFIX_NAME="spark-3.3.0"
        ;;

    *)
        echo "unknown spark version"
        exit 1
        ;;
esac

#debug
echo "spark folder name: $SPARK_FOLDER_NAME"
echo "spark prefix name: $SPARK_PREFIX_NAME"
#debug

#download and extract file
wget --tries=3 https://archive.apache.org/dist/spark/${SPARK_PREFIX_NAME}/${SPARK_FOLDER_NAME}.tgz
tar -xzvf ${SPARK_FOLDER_NAME}.tgz

export SPARK_HOME=${PWD}/${SPARK_FOLDER_NAME}
export PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

#set locations in props file
TEST_DATA_FOLDER=${PWD}/scripts/test_data
PROPS_FILE=${TEST_DATA_FOLDER}/parquet-dfs-compact.props
echo hoodie.deltastreamer.source.dfs.root=${TEST_DATA_FOLDER}/parquet_src >> $PROPS_FILE
echo hoodie.deltastreamer.schemaprovider.target.schema.file=file:${TEST_DATA_FOLDER}/ny.avsc >> $PROPS_FILE
echo hoodie.deltastreamer.schemaprovider.source.schema.file=file:${TEST_DATA_FOLDER}/ny.avsc >> $PROPS_FILE

#run spark submit
SPARK_SUBMIT=${SPARK_HOME}/bin/spark-submit

#debug
echo "spark submit: $SPARK_SUBMIT"
#debug

$SPARK_SUBMIT --driver-memory 8g --executor-memory 8g \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
${PWD}/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.13.0-SNAPSHOT.jar \
--props $PROPS_FILE \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
--source-ordering-field date_col   --table-type MERGE_ON_READ \
--target-base-path file:\/\/\/tmp/hudi-deltastreamer-ny/ \
--target-table ny_hudi_tbl  --op UPSERT

#get utilities jar name
UTILITIES_BUNDLE_NAME=""
case $SCALA_PROFILE in

    "scala-2.11")
        UTILITIES_BUNDLE_NAME="2.11"
        ;;

    "scala-2.12")
        UTILITIES_BUNDLE_NAME="2.12"
        ;;

    *)
        echo "unknown scala version"
        exit 1
        ;;
esac
UTILITIES_BUNDLE_NAME=${PWD}/packaging/hudi-spark-bundle/target/hudi-spark-bundle_${UTILITIES_BUNDLE_NAME}-0.13.0-SNAPSHOT.jar

#debug
echo "utilities bundle name: $UTILITIES_BUNDLE_NAME"
#debug

#create spark shell command
SPARK_SHELL_COMMAND="spark-shell --jars ${UTILITIES_BUNDLE_NAME} --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' "

if [[ $SPARK_PROFILE = "spark3.2" ]] || [[ $SPARK_PROFILE = "spark3.3"]]
then
    SPARK_SHELL_COMMAND="${SPARK_SHELL_COMMAND} --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'"
fi

#debug
echo "spark shell command: $SPARK_SHELL_COMMAND"
#debug

#run spark shell command
$SPARK_SHELL_COMMAND -i ${TEST_DATA_FOLDER}/commands.scala



