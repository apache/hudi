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

process_spark_version() {
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
            SPARK_FOLDER_NAME="spark-3.3.0-bin-hadoop3"
            SPARK_PREFIX_NAME="spark-3.3.0"
            ;;

        *)
            echo "::error::run_utilities_bundle_test.sh unknown spark version: ${SPARK_PROFILE}"
            exit 1
            ;;
    esac
}

process_scala_version() {
    case $SCALA_PROFILE in

        "scala-2.11")
            SCALA_NUM="2.11"
            ;;

        "scala-2.12")
            SCALA_NUM="2.12"
            ;;

        *)
            echo "::error::run_utilities_bundle_test.sh unknown scala version: ${SCALA_PROFILE}"
            exit 1
            ;;
    esac
}

download_spark() {
    #download and extract file
    SPARK_URL=https://archive.apache.org/dist/spark/${PREFIX_NAME}/${FOLDER_NAME}.tgz
    wget --tries=3 $SPARK_URL || { echo "::error::get_spark_hadoop.sh failed to download ${SPARK_URL}" && exit 1; }
    tar -xzvf ${FOLDER_NAME}.tgz || { echo "::error::get_spark_hadoop.sh failed to extract ${FOLDER_NAME}.tgz" && exit 1; }
    rm -rf ${FOLDER_NAME}.tgz
}





