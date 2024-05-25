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

usage="
USAGE:
$(basename "$0") [--help] [--all boolen] -- Script to generate the test suite according to arguments provided and run these test suites.

where:
    --help  show this help text
    --all  set the seed value
    --execute_test_suite  flag if test need to execute (DEFAULT- true)
    --medium_num_iterations  number of medium iterations (DEFAULT- 20)
    --long_num_iterations  number of long iterations (DEFAULT- 30)
    --intermittent_delay_mins  delay after every test run (DEFAULT- 1)
    --table_type  hoodie table type to test (DEFAULT COPY_ON_WRITE)
    --include_long_test_suite_yaml  include long infra test suite (DEFAULT false)
    --include_medium_test_suite_yaml  include medium infra test suite (DEFAULT false)
    --cluster_num_itr  number of cluster iterations (DEFAULT 30)
    --include_cluster_yaml  include cluster infra test suite (DEFAULT false)
    --input_path input path for test in docker image (DEFAULT /user/hive/warehouse/hudi-integ-test-suite/input/)
    --output_path input path for test in docker image (DEFAULT /user/hive/warehouse/hudi-integ-test-suite/output/)

Example:
Note - Execute the command from within docker folder

  1. To generate and run all test suites
      ./generate_test_suite.sh --all true
  2. To only generate test suites
      ./generate_test_suite.sh --all --execute_test_suite false
  3. To run only specific test suite yaml
      ./generate_test_suite.sh --execute_test_suite true --include_medium_test_suite_yaml true
     "


MEDIUM_NUM_ITR=20
LONG_NUM_ITR=50
DELAY_MINS=1
TABLE_TYPE=COPY_ON_WRITE
INCLUDE_LONG_TEST_SUITE=false
INCLUDE_MEDIUM_TEST_SUITE=false
INCLUDE_CLUSTER_YAML=false
CLUSTER_NUM_ITR=30
CLUSTER_DELAY_MINS=1
CLUSTER_ITR_COUNT=15
EXECUTE_TEST_SUITE=true
JAR_NAME=hudi-integ-test-bundle-0.9.0-SNAPSHOT.jar
INPUT_PATH="/user/hive/warehouse/hudi-integ-test-suite/input/"
OUTPUT_PATH="/user/hive/warehouse/hudi-integ-test-suite/output/"

CUR_DIR=$(pwd)

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --help)
    echo "$usage"
    exit
    ;; 
    --all)
    INCLUDE_LONG_TEST_SUITE="$2"
    INCLUDE_MEDIUM_TEST_SUITE="$2"
    INCLUDE_CLUSTER_YAML="$2"
    shift # past argument
    shift # past value
    ;;
    --execute_test_suite)
    EXECUTE_TEST_SUITE="$2"
    shift # past argument
    shift # past value
    ;;
    --medium_num_iterations)
    MEDIUM_NUM_ITR="$2"
    shift # past argument
    shift # past value
    ;;
    --long_num_iterations)
    LONG_NUM_ITR="$2"
    shift # past argument
    shift # past value
    ;;
    --intermittent_delay_mins)
    DELAY_MINS="$2"
    shift # past argument
    shift # past value
    ;;
    --table_type)
    TABLE_TYPE="$2"
    shift # past argument
    shift # past value
    ;;
    --include_long_test_suite_yaml)
    INCLUDE_LONG_TEST_SUITE="$2"
    shift # past argument
    shift # past value
    ;;
    --include_medium_test_suite_yaml)
    INCLUDE_MEDIUM_TEST_SUITE="$2"
    shift # past argument
    shift # past value
    ;;
    --include_cluster_yaml)
    INCLUDE_CLUSTER_YAML="$2"
    shift # past argument
    shift # past value
    ;;
    --cluster_num_itr)
    CLUSTER_NUM_ITR="$2"
    shift # past argument
    shift # past value
    ;;
    --cluster_delay_mins)
    CLUSTER_DELAY_MINS="$2"
    shift # past argument
    shift # past value
    ;;
    --cluster_exec_itr_count)
    CLUSTER_ITR_COUNT="$2"
    shift # past argument
    shift # past value
    ;;
    --integ_test_jar_name)
    JAR_NAME="$2"
    shift # past argument
    shift # past value
    ;;
    --input_path)
    INPUT_PATH="$2"
    shift # past argument
    shift # past value
    ;;
    --output_path)
    OUTPUT_PATH="$2"
    shift # past argument
    shift # past value
    ;;
    --default)
    DEFAULT=YES
    shift # past argument
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    echo "Unknown argument provided - '$1'"
    echo "$usage"
    exit 0
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters
echo "$POSITIONAL"
echo "Include Medium test suite $INCLUDE_MEDIUM_TEST_SUITE"
if $INCLUDE_MEDIUM_TEST_SUITE ; then
  echo "Medium test suite iterations = ${MEDIUM_NUM_ITR}"
fi
echo "Include Long test suite $INCLUDE_LONG_TEST_SUITE"
if $INCLUDE_LONG_TEST_SUITE ; then
  echo "Long test suite iterations = ${LONG_NUM_ITR}"
fi
echo "Intermittent delay in mins     = ${DELAY_MINS}"
echo "Table type   = ${TABLE_TYPE}"

echo "Include cluster yaml $INCLUDE_CLUSTER_YAML"
if $INCLUDE_CLUSTER_YAML ; then
  echo "Cluster total itr count  $CLUSTER_NUM_ITR"
  echo "Cluster delay mins $CLUSTER_DELAY_MINS"
  echo "Cluster exec itr count $CLUSTER_ITR_COUNT"
fi
echo "Jar name $JAR_NAME"
INPUT_PATH=$(echo "$INPUT_PATH" | sed "s|\/|\\\/|g")
echo "Input path $INPUT_PATH"
OUTPUT_PATH=$(echo "$OUTPUT_PATH" | sed "s|\/|\\\/|g")
echo "Output path $OUTPUT_PATH"

if [ ! -f $CUR_DIR/../packaging/hudi-integ-test-bundle/target/$JAR_NAME ]; then
   echo "Integ test bundle not found at $CUR_DIR/../packaging/hudi-integ-test-bundle/target/$JAR_NAME"
   exit 1
fi

if [ -d "demo/config/test-suite/staging" ]; then
  echo "Cleaning up staging dir"
  rm -rf demo/config/test-suite/staging*
fi

if [ ! -d "demo/config/test-suite/staging" ]; then
  echo "Creating staging dir"
  mkdir demo/config/test-suite/staging
fi

cp demo/config/test-suite/templates/sanity.yaml.template demo/config/test-suite/staging/sanity.yaml

sed -i '' "s/NAME/$TABLE_TYPE/" demo/config/test-suite/staging/sanity.yaml

cp demo/config/test-suite/templates/test.properties.template demo/config/test-suite/staging/test.properties
sed -i '' "s/INPUT_PATH/$INPUT_PATH/" demo/config/test-suite/staging/test.properties

cp demo/config/test-suite/templates/spark_command.txt.template demo/config/test-suite/staging/sanity_spark_command.sh

sed -i '' "s/JAR_NAME/$JAR_NAME/" demo/config/test-suite/staging/sanity_spark_command.sh
sed -i '' "s/INPUT_PATH/$INPUT_PATH/" demo/config/test-suite/staging/sanity_spark_command.sh
sed -i '' "s/OUTPUT_PATH/$OUTPUT_PATH/" demo/config/test-suite/staging/sanity_spark_command.sh
sed -i '' "s/input_yaml/sanity.yaml/" demo/config/test-suite/staging/sanity_spark_command.sh
sed -i '' "s/TABLE_TYPE/$TABLE_TYPE/" demo/config/test-suite/staging/sanity_spark_command.sh

if $INCLUDE_MEDIUM_TEST_SUITE ; then

  cp demo/config/test-suite/templates/medium_test_suite.yaml.template demo/config/test-suite/staging/medium_test_suite.yaml

  sed -i '' "s/NAME/$TABLE_TYPE/" demo/config/test-suite/staging/medium_test_suite.yaml
  sed -i '' "s/medium_num_iterations/$MEDIUM_NUM_ITR/" demo/config/test-suite/staging/medium_test_suite.yaml
  sed -i '' "s/delay_in_mins/$DELAY_MINS/" demo/config/test-suite/staging/medium_test_suite.yaml

  cp demo/config/test-suite/templates/spark_command.txt.template demo/config/test-suite/staging/medium_test_suite_spark_command.sh

  sed -i '' "s/JAR_NAME/$JAR_NAME/" demo/config/test-suite/staging/medium_test_suite_spark_command.sh
  sed -i '' "s/INPUT_PATH/$INPUT_PATH/" demo/config/test-suite/staging/medium_test_suite_spark_command.sh
  sed -i '' "s/OUTPUT_PATH/$OUTPUT_PATH/" demo/config/test-suite/staging/medium_test_suite_spark_command.sh
  sed -i '' "s/input_yaml/medium_test_suite.yaml/" demo/config/test-suite/staging/medium_test_suite_spark_command.sh
  sed -i '' "s/TABLE_TYPE/$TABLE_TYPE/" demo/config/test-suite/staging/medium_test_suite_spark_command.sh

fi

if $INCLUDE_LONG_TEST_SUITE ; then

  cp demo/config/test-suite/templates/long_test_suite.yaml.template demo/config/test-suite/staging/long_test_suite.yaml

  sed -i '' "s/NAME/$TABLE_TYPE/" demo/config/test-suite/staging/long_test_suite.yaml
  sed -i '' "s/long_num_iterations/$LONG_NUM_ITR/" demo/config/test-suite/staging/long_test_suite.yaml
  sed -i '' "s/delay_in_mins/$DELAY_MINS/" demo/config/test-suite/staging/long_test_suite.yaml

  cp demo/config/test-suite/templates/spark_command.txt.template demo/config/test-suite/staging/long_test_suite_spark_command.sh

  sed -i '' "s/JAR_NAME/$JAR_NAME/" demo/config/test-suite/staging/long_test_suite_spark_command.sh
  sed -i '' "s/INPUT_PATH/$INPUT_PATH/" demo/config/test-suite/staging/long_test_suite_spark_command.sh
  sed -i '' "s/OUTPUT_PATH/$OUTPUT_PATH/" demo/config/test-suite/staging/long_test_suite_spark_command.sh
  sed -i '' "s/input_yaml/long_test_suite.yaml/" demo/config/test-suite/staging/long_test_suite_spark_command.sh
  sed -i '' "s/TABLE_TYPE/$TABLE_TYPE/" demo/config/test-suite/staging/long_test_suite_spark_command.sh

fi

if $INCLUDE_CLUSTER_YAML ; then

  cp demo/config/test-suite/templates/clustering.yaml.template demo/config/test-suite/staging/clustering.yaml

  sed -i '' "s/NAME/$TABLE_TYPE/" demo/config/test-suite/staging/clustering.yaml
  sed -i '' "s/clustering_num_iterations/$CLUSTER_NUM_ITR/" demo/config/test-suite/staging/clustering.yaml
  sed -i '' "s/delay_in_mins/$CLUSTER_DELAY_MINS/" demo/config/test-suite/staging/clustering.yaml
  sed -i '' "s/clustering_itr_count/$CLUSTER_ITR_COUNT/" demo/config/test-suite/staging/clustering.yaml

  cp demo/config/test-suite/templates/spark_command.txt.template demo/config/test-suite/staging/clustering_spark_command.sh

  sed -i '' "s/JAR_NAME/$JAR_NAME/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "s/INPUT_PATH/$INPUT_PATH/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "s/OUTPUT_PATH/$OUTPUT_PATH/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "s/input_yaml/clustering.yaml/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "s/TABLE_TYPE/$TABLE_TYPE/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "/use-deltastreamer/d" demo/config/test-suite/staging/clustering_spark_command.sh

fi

if $EXECUTE_TEST_SUITE ; then

  docker cp $CUR_DIR/../packaging/hudi-integ-test-bundle/target/"$JAR_NAME" adhoc-2:/opt/
  docker exec -it adhoc-2 /bin/bash rm -rf /opt/staging*
  docker cp demo/config/test-suite/staging/ adhoc-2:/opt/
  docker exec -it adhoc-2 /bin/bash echo "\n============================== Executing sanity test suite ============================== "
  docker exec -it adhoc-2 /bin/bash /opt/staging/sanity_spark_command.sh

  if [ -f demo/config/test-suite/staging/medium_test_suite_spark_command.sh ]; then
    docker exec -it adhoc-2 /bin/bash echo "\n\n\n============================== Executing medium test suite ============================== "
    docker exec -it adhoc-2 /bin/bash /opt/staging/medium_test_suite_spark_command.sh
  fi

  if [ -f demo/config/test-suite/staging/long_test_suite_spark_command.sh ]; then
    docker exec -it adhoc-2 /bin/bash echo "\n\n\n============================== Executing long test suite ============================== "
    docker exec -it adhoc-2 /bin/bash /opt/staging/long_test_suite_spark_command.sh
  fi

  if [ -f demo/config/test-suite/staging/clustering_spark_command.sh ]; then
    docker exec -it adhoc-2 /bin/bash echo "\n\n\n============================== Executing clustering test suite ============================== "
    docker exec -it adhoc-2 /bin/bash /opt/staging/clustering_spark_command.sh
  fi

fi
