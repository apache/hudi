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

NUM_ITR=1
DELAY_MINS=1
TABLE_TYPE=COPY_ON_WRITE
INCLUDE_CLUSTER_YAML=false
CLUSTER_NUM_ITR=2
CLUSTER_DELAY_MINS=1
CLUSTER_ITR_COUNT=1
JAR_NAME=hudi-integ-test-bundle-0.9.0-SNAPSHOT.jar
INPUT_PATH="/user/hive/warehouse/hudi-integ-test-suite/input/"
OUTPUT_PATH="/user/hive/warehouse/hudi-integ-test-suite/output/"

CUR_DIR=$(pwd)

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --num_iterations)
    NUM_ITR="$2"
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
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

echo "Num Iterations  = ${NUM_ITR}"
echo "Intermittent delay in mins     = ${DELAY_MINS}"
echo "Table type   = ${TABLE_TYPE}"
echo "Include cluster yaml $INCLUDE_CLUSTER_YAML"
echo "Cluster total itr count  $CLUSTER_NUM_ITR"
echo "Cluster delay mins $CLUSTER_DELAY_MINS"
echo "Cluster exec itr count $CLUSTER_ITR_COUNT"
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

cp demo/config/test-suite/templates/long-running.yaml.template demo/config/test-suite/staging/long-running.yaml

sed -i '' "s/NAME/$TABLE_TYPE/" demo/config/test-suite/staging/long-running.yaml
sed -i '' "s/num_iterations/$NUM_ITR/" demo/config/test-suite/staging/long-running.yaml
sed -i '' "s/delay_in_mins/$DELAY_MINS/" demo/config/test-suite/staging/long-running.yaml

cp demo/config/test-suite/templates/test.properties.template demo/config/test-suite/staging/test.properties
sed -i '' "s/INPUT_PATH/$INPUT_PATH/" demo/config/test-suite/staging/test.properties

cp demo/config/test-suite/templates/spark_command.txt.template demo/config/test-suite/staging/long_running_spark_command.sh

sed -i '' "s/JAR_NAME/$JAR_NAME/" demo/config/test-suite/staging/long_running_spark_command.sh
sed -i '' "s/INPUT_PATH/$INPUT_PATH/" demo/config/test-suite/staging/long_running_spark_command.sh
sed -i '' "s/OUTPUT_PATH/$OUTPUT_PATH/" demo/config/test-suite/staging/long_running_spark_command.sh
sed -i '' "s/input_yaml/long-running.yaml/" demo/config/test-suite/staging/long_running_spark_command.sh
sed -i '' "s/TABLE_TYPE/$TABLE_TYPE/" demo/config/test-suite/staging/long_running_spark_command.sh

if $INCLUDE_CLUSTER_YAML ; then

  cp demo/config/test-suite/templates/clustering.yaml.template demo/config/test-suite/staging/clustering.yaml

  sed -i '' "s/NAME/$TABLE_TYPE/" demo/config/test-suite/staging/clustering.yaml
  sed -i '' "s/clustering_num_iterations/$CLUSTER_NUM_ITR/" demo/config/test-suite/staging/clustering.yaml
  sed -i '' "s/clustering_delay_in_mins/$CLUSTER_DELAY_MINS/" demo/config/test-suite/staging/clustering.yaml
  sed -i '' "s/clustering_itr_count/$CLUSTER_ITR_COUNT/" demo/config/test-suite/staging/clustering.yaml

  cp demo/config/test-suite/templates/spark_command.txt.template demo/config/test-suite/staging/clustering_spark_command.sh

  sed -i '' "s/JAR_NAME/$JAR_NAME/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "s/INPUT_PATH/$INPUT_PATH/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "s/OUTPUT_PATH/$OUTPUT_PATH/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "s/input_yaml/clustering.yaml/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "s/TABLE_TYPE/$TABLE_TYPE/" demo/config/test-suite/staging/clustering_spark_command.sh
  sed -i '' "/use-deltastreamer/d" demo/config/test-suite/staging/clustering_spark_command.sh

fi

docker cp $CUR_DIR/../packaging/hudi-integ-test-bundle/target/$JAR_NAME adhoc-2:/opt/
docker exec -it adhoc-2 /bin/bash rm -rf /opt/staging*
docker cp demo/config/test-suite/staging/ adhoc-2:/opt/
docker exec -it adhoc-2 /bin/bash /opt/staging/long_running_spark_command.sh

if [ -f demo/config/test-suite/staging/clustering_spark_command.sh ]; then
  docker exec -it adhoc-2 /bin/bash /opt/staging/clustering_spark_command.sh
fi
