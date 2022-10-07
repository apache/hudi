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

#Create props file
TEST_DATA_FOLDER=${PWD}/scripts/test_data
PROPS_FILE_TEMPLATE=${TEST_DATA_FOLDER}/parquet-dfs-compact.props
PROPS_FILE="${TEST_DATA_FOLDER}/parquet-dfs-compact_${VERSION_STRING}.props"
rm -rf $PROPS_FILE
touch $PROPS_FILE
cat $PROPS_FILE_TEMPLATE >> $PROPS_FILE
PARQUET_SOURCE="${TEST_DATA_FOLDER}/parquet_src"
echo hoodie.deltastreamer.source.dfs.root=${PARQUET_SOURCE} >> $PROPS_FILE
echo hoodie.deltastreamer.schemaprovider.target.schema.file=file:${TEST_DATA_FOLDER}/ny.avsc >> $PROPS_FILE
echo hoodie.deltastreamer.schemaprovider.source.schema.file=file:${TEST_DATA_FOLDER}/ny.avsc >> $PROPS_FILE

#Create spark submit command
SPARK_SUBMIT=${SPARK_HOME}/bin/spark-submit
OUTPUT_FOLDER_NAME="/tmp/hudi-deltastreamer-ny_${VERSION_STRING}/"
UTILITIES_BUNDLE_NAME=${PWD}/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_${SCALA_NUM}-0.13.0-SNAPSHOT.jar
SPARK_SUBMIT_COMMAND="${SPARK_SUBMIT} --driver-memory 8g --executor-memory 8g"
SPARK_SUBMIT_COMMAND+=" --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer"
SPARK_SUBMIT_COMMAND+=" ${UTILITIES_BUNDLE_NAME} --props ${PROPS_FILE}"
SPARK_SUBMIT_COMMAND+=" --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
SPARK_SUBMIT_COMMAND+=" --source-class org.apache.hudi.utilities.sources.ParquetDFSSource"
SPARK_SUBMIT_COMMAND+=" --source-ordering-field date_col --table-type MERGE_ON_READ"
SPARK_SUBMIT_COMMAND+=" --target-base-path file://${OUTPUT_FOLDER_NAME}"
SPARK_SUBMIT_COMMAND+=" --target-table ny_hudi_tbl  --op UPSERT"

#run spark submit command
rm -rf $OUTPUT_FOLDER_NAME
$SPARK_SUBMIT_COMMAND || { echo "::error::run_utilities_bundle_test.sh deltastreamer failed" && exit 1; }

#make sure that the output folder has a bunch of data in it
OUTPUT_SIZE=$(du -s ${OUTPUT_FOLDER_NAME} | awk '{print $1}')
if [[ -z $OUTPUT_SIZE || "$OUTPUT_SIZE" -lt "1000" ]]; then
    echo "::error::run_utilities_bundle_test.sh deltastreamer output folder is much smaller than expected" 
    exit 1
fi

#create scala commands file
COMMANDS_FILE="$TEST_DATA_FOLDER/commands_${VERSION_STRING}.scala"
rm -rf $COMMANDS_FILE
touch $COMMANDS_FILE
echo "val hudiDf = spark.read.format(\"org.apache.hudi\").load(\"${OUTPUT_FOLDER_NAME}\")" >> $COMMANDS_FILE
echo "val df = spark.read.format(\"parquet\").load(\"${PARQUET_SOURCE}\")" >> $COMMANDS_FILE
cat $TEST_DATA_FOLDER/commands.scala >> $COMMANDS_FILE

#create spark shell command
SPARK_SHELL=${SPARK_HOME}/bin/spark-shell
SPARK_SHELL_COMMAND="${SPARK_SHELL} --jars ${UTILITIES_BUNDLE_NAME}" 
SPARK_SHELL_COMMAND+=" --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'"
if [[ $SPARK_PROFILE = "spark3.2" || $SPARK_PROFILE = "spark3.3" ]]; then
    SPARK_SHELL_COMMAND+=" --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'"
fi
SPARK_SHELL_COMMAND+=" --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'"
SPARK_SHELL_COMMAND+=" -i ${COMMANDS_FILE}"

#run spark shell command
LOGFILE="${PWD}/sparktest_${VERSION_STRING}.log"
$SPARK_SHELL_COMMAND > $LOGFILE || { SHELL_RESULT=$(cat $LOGFILE | grep "Counts don't match") && echo "::error::run_utilities_bundle_test.sh $SHELL_RESULT" && exit 1; }

#cleanup
rm -rf $COMMANDS_FILE
rm -rf $PROPS_FILE
rm -rf $LOGFILE
exit 0
