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

# must set SPARK_ROOT to a directory where you will install your spark versions
# Usage set_spark [-s spark version]  [-h hadoop version]


set_spark() {
	  declare opt
    declare OPTARG
    declare OPTIND
    SPARK_VERSION="3.3"
    HADOOP_VERSION="2"
    while getopts "s:h:" opt; do
    case "${opt}" in
        s)
            SPARK_VERSION="$OPTARG"
            ;;
        h)
            HADOOP_VERSION="$OPTARG"
            ;;
    esac
    done
    shift "$(($OPTIND -1))"


	if [ ! -d "$SPARK_ROOT" ]; then
		echo "SPARK_ROOT is not set to an existing directory"
		exit 1
	fi

    case $SPARK_VERSION in
        2)
            SPARK_VERSION="2.4.7"
            ;;
        2.4)
            SPARK_VERSION="2.4.7"
            ;;
        3.1)
            SPARK_VERSION="3.1.3"
            ;;
        3.2)
            SPARK_VERSION="3.2.3"
            ;;
        3)
            SPARK_VERSION="3.3.1"
            ;;
        3.3)
            SPARK_VERSION="3.3.1"
            ;;
    esac

	USE_OLD_HADOOP="0"
	if [ "${SPARK_VERSION:0:1}" -eq "3" ] && [ "${SPARK_VERSION:2:1}" -lt "3" ]; then
		USE_OLD_HADOOP="1"
	elif [ "${SPARK_VERSION:0:1}" -lt "3" ]; then
		USE_OLD_HADOOP="1"
	fi

    if [ "$USE_OLD_HADOOP" -eq "1" ]; then
        case $HADOOP_VERSION in
            2)
                HADOOP_VERSION="2.7"
                ;;
            3)
                HADOOP_VERSION="3.2"
                ;;
        esac
    fi

	export HUDI_SPARK_VERSION="${SPARK_VERSION:0:3}"
  export SPARK_HOME=${SPARK_ROOT}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

	echo "SPARK_HOME=$SPARK_HOME"
	if [ ! -d "$SPARK_HOME" ]; then
		pushd $SPARK_ROOT
		wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
		tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
		rm -rf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
		popd
	fi

	export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
	export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
	export PYTHONPATH=$SPARK_HOME/python/lib/*.zip:$PYTHONPATH
}