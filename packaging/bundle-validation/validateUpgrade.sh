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
# link the jar names to easier to use names
ln -s $JARS_DIR/hudi-spark*.jar $JARS_DIR/spark.jar
ln -s $JARS_DIR/hudi-utilities-bundle*.jar $JARS_DIR/utilities.jar
ln -s $JARS_DIR/hudi-utilities-slim*.jar $JARS_DIR/utilities-slim.jar

SCALA_VERSION=$1
SPARK_PROFILE=$2
source functions.sh

##
# Function to do upgrade testing for utilities bundle
#
# 1st arg: hudi version
#
# env vars (defined in container):
#   SPARK_HOME: path to the spark directory
#   SCALA_VERSION: version of scala
#   SPARK_PROFILE: spark+the rounded version eg. spark2.4 spark3.1 spark3.3
##
upgrade_from_utilities_version () {
    BUNDLE_VERSION=$1
    TEST_NAME="upgrade utilities $BUNDLE_VERSION"
    echo "::warning::validateUpgrade.sh starting test **$TEST_NAME**"
    FIRST_MAIN_ARG=${WORKDIR}/hudi-utilities-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar
    FIRST_ADDITIONAL_ARG=""
    SECOND_MAIN_ARG=$JARS_DIR/utilities.jar
    SECOND_ADDITIONAL_ARG=""
    if [ ! -f "$FIRST_MAIN_ARG" ]; then
        DOWNLOAD_URL="https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-bundle_${SCALA_VERSION}/${BUNDLE_VERSION}/hudi-utilities-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar"
        wget $DOWNLOAD_URL -P $WORKDIR
        if [ "$?" -ne 0 ]; then
            echo "::error::validateUpgrade.sh **$TEST_NAME** failed to download $DOWNLOAD_URL"
            exit 1
        fi
    fi
    test_upgrade_bundle
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    echo "::warning::validateUpgrade.sh finished test **$TEST_NAME**"
}


##
# Function to do upgrade testing for utilities slim + spark bundle
#
# 1st arg: hudi version
#
# env vars (defined in container):
#   SPARK_HOME: path to the spark directory
#   SCALA_VERSION: version of scala
#   SPARK_PROFILE: spark+the rounded version eg. spark2.4 spark3.1 spark3.3
##
upgrade_from_slim_version () {
    BUNDLE_VERSION=$1
    TEST_NAME="upgrade utilities slim $BUNDLE_VERSION"
    echo "::warning::validateUpgrade.sh starting test **$TEST_NAME**"
    FIRST_MAIN_ARG=${WORKDIR}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar
    FIRST_ADDITIONAL_ARG=${WORKDIR}/hudi-${SPARK_PROFILE}-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar
    SECOND_MAIN_ARG=$JARS_DIR/utilities-slim.jar
    SECOND_ADDITIONAL_ARG=$JARS_DIR/spark.jar
    if [ ! -f "$FIRST_MAIN_ARG" ]; then
        DOWNLOAD_URL="https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-slim-bundle_${SCALA_VERSION}/${BUNDLE_VERSION}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar"
        wget $DOWNLOAD_URL -P $WORKDIR
        if [ "$?" -ne 0 ]; then
            echo "::error::validateUpgrade.sh **$TEST_NAME** failed to download $DOWNLOAD_URL"
            exit 1
        fi
    fi
    if [ ! -f "$FIRST_ADDITIONAL_ARG" ]; then
        DOWNLOAD_URL="https://repo1.maven.org/maven2/org/apache/hudi/hudi-${SPARK_PROFILE}-bundle_${SCALA_VERSION}/${BUNDLE_VERSION}/hudi-${SPARK_PROFILE}-bundle_${SCALA_VERSION}-${BUNDLE_VERSION}.jar"
        wget $DOWNLOAD_URL -P $WORKDIR
        if [ "$?" -ne 0 ]; then
            echo "::error::validateUpgrade.sh **$TEST_NAME** failed to download $DOWNLOAD_URL"
            exit 1
        fi
    fi
    
    test_upgrade_bundle
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    echo "::warning::validateUpgrade.sh finished test **$TEST_NAME**"
}


#need curl for getting the versions
apk add curl

if [[ $SPARK_HOME == *"spark-2.4"* ]] || [[  $SPARK_HOME == *"spark-3.1"* ]]
then
    VERSIONS=$(curl https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-bundle_${SCALA_VERSION}/ | grep "<a href=\"0." | awk '{print $2}' | cut -c 7- | rev | cut -c 3- | rev |  sort -t. -k 1,1nr -k 2,2nr -k 3,3nr -k 4,4nr | grep -m 8 -v '-') 

    #turn VERSIONS into an array because piping it messes with spark-shell
    SAVEIFS=$IFS   # Save current IFS (Internal Field Separator)
    IFS=$'\n'      # Change IFS to newline char
    VERSIONS=($VERSIONS) # split the `VERSIONS` string into an array by the same name
    IFS=$SAVEIFS   # Restore original IFS

    echo "::warning::validateUpgrade.sh testing utilties bundle upgrade with ${#VERSIONS[@]} versions"
    #test upgrading from each version
    for (( i=0; i<${#VERSIONS[@]}; i++ ))
    do
        upgrade_from_utilities_version "${VERSIONS[$i]}"
        if [ "$?" -ne 0 ]; then
            exit 1
        fi
    done
    echo "::warning::validateUpgrade.sh done testing utilities bundle upgrades"
else
  echo "::warning::validate.sh skip testing utilities bundle for non-spark2.4 & non-spark3.1 build"
fi

VERSIONS=$(curl https://repo1.maven.org/maven2/org/apache/hudi/hudi-${SPARK_PROFILE}-bundle_${SCALA_VERSION}/ | grep "<a href=\"0." | awk '{print $2}' | cut -c 7- | rev | cut -c 3- | rev |  sort -t. -k 1,1nr -k 2,2nr -k 3,3nr -k 4,4nr | grep -m 8 -v '-') 

#turn VERSIONS into an array because piping it messes with spark-shell
SAVEIFS=$IFS   # Save current IFS (Internal Field Separator)
IFS=$'\n'      # Change IFS to newline char
VERSIONS=($VERSIONS) # split the `VERSIONS` string into an array by the same name
IFS=$SAVEIFS   # Restore original IFS

echo "::warning::validateUpgrade.sh testing utilties slim + spark bundle upgrade with ${#VERSIONS[@]} versions"
#test upgrading from each version
for (( i=0; i<${#VERSIONS[@]}; i++ ))
do
    upgrade_from_slim_version "${VERSIONS[$i]}"
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    echo "::warning::validateUpgrade.sh done testing utilities slim + spark bundle upgrades"
done


