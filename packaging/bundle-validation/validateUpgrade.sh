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
STOCK_DATA_DIR=/opt/bundle-validation/data/stocks/data

source functions.sh

upgrade_from_version () {
    BUNDLE_VERSION=$1
    if [[ $SPARK_HOME == *"spark-2.4"* ]] || [[  $SPARK_HOME == *"spark-3.1"* ]]
    then
        echo "::warning::validateUpgrade.sh testing upgrade with utilities version $BUNDLE_VERSION"
        wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-bundle_${SCALA_VERSION}/${BUNDLE_VERSION}/hudi-utilities-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar -P $WORKDIR
        FIRST_MAIN_ARG=${WORKDIR}/hudi-utilities-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar
        FIRST_ADDITIONAL_ARG=""
        SECOND_MAIN_ARG=$JARS_DIR/utilities.jar
        SECOND_ADDITIONAL_ARG=""
        test_upgrade_bundle
        if [ "$?" -ne 0 ]; then
            exit 1
        fi
        echo "::warning::validateUpgrade.sh done testing upgrade with utilities version $BUNDLE_VERSION"
    else
        echo "::warning::validateUpgrade.sh skip testing utilities bundle for non-spark2.4 & non-spark3.1 build"
    fi

    echo "::warning::validateUpgrade.sh testing upgrade with utilities slim version $BUNDLE_VERSION"
    wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-slim-bundle_${SCALA_VERSION}/${BUNDLE_VERSION}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar -P $WORKDIR
    FIRST_MAIN_ARG=${WORKDIR}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar
    wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-${SPARK_PROFILE}-bundle_${SCALA_VERSION}/${BUNDLE_VERSION}/hudi-${SPARK_PROFILE}-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar -P $WORKDIR
    FIRST_ADDITIONAL_ARG=${WORKDIR}/hudi-${SPARK_PROFILE}-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar
    SECOND_MAIN_ARG=$JARS_DIR/utilities-slim.jar
    SECOND_ADDITIONAL_ARG=$JARS_DIR/spark.jar
    test_upgrade_bundle
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    echo "::warning::validateUpgrade.sh done testing upgrade with utilities slim version $BUNDLE_VERSION"
}


curl https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-bundle_2.11/ | grep "<a href=\"0." | awk '{print $2}' | cut -c 7- | rev | cut -c 3- | rev |  sort -t. -k 1,1nr -k 2,2nr -k 3,3nr -k 4,4nr | grep -m 5 -v '-' | while read line ; do upgrade_from_version $line ; done


