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

SCRIPT_PATH=$(cd `dirname $0`; pwd)
# set up root directory
WS_ROOT=`dirname $SCRIPT_PATH`

echo "Preparing Spark binary file."
SPARK_VERSION=2.4.4
HADOOP_VERSION=2.8.4
SPARK_HADOOP_VERSION=2.7

SPARK_BINARY_FILE_NAME=spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz
SPARK_BINARY_CACHE_FILE_PATH=$SCRIPT_PATH/binarycache/$SPARK_BINARY_FILE_NAME

if [ -f $SPARK_BINARY_CACHE_FILE_PATH ]; then
  echo "The binary file $SPARK_BINARY_FILE_NAME has been cached in the binary cache directory!"
else
  echo "The binary file $SPARK_BINARY_FILE_NAME did not exist in the binary cache directory, try to download."
  wget http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_BINARY_FILE_NAME} -O ${SPARK_BINARY_CACHE_FILE_PATH}
fi