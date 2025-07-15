#!/bin/bash

# file:/Users/ethan/Work/tmp/20231230-0141release/cow
export CONFIG_PATH=$1
export TEST_BASE_PATH=$2
export ORDERING_FIELD=$3
export NUM_ROUNDS=$4
export TABLE_BASE_PATH="file:$2/test_table"
export TEST_LOG="$TEST_BASE_PATH/test.log"
export STREAMER_CONFIG_FILE="file:$CONFIG_PATH/streamer.properties"

# common util to get configs
config_file="$CONFIG_PATH/test_config.properties"
if [[ ! -f "$config_file" ]]; then
    echo "Config file $config_file not found!"
    exit 1
fi

while IFS='=' read -r key value; do
    if [[ -n "$key" && "$key" != \#* ]]; then
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)

        eval "${key}='${value}'"
    fi
done < "$config_file"

echo "CONFIG_PATH: $CONFIG_PATH"
echo "TEST_BASE_PATH: $TEST_BASE_PATH"
echo "MINUTES: $MINUTES"
echo "TABLE_TYPE: $TABLE_TYPE"
echo "OPERATION_TYPE: $OPERATION_TYPE"

SECONDS_TO_WAIT=$((MINUTES * 60))

start_timestamp=$(date +%s)

for ((i = 0; i < $NUM_ROUNDS ; i++)); do
  echo "Start Hudi Streamer run $i ..." >> $TEST_LOG 2>&1
  $SPARK_HOME/bin/spark-submit \
      --master local[4] \
      --driver-memory 2g --executor-memory 2g --num-executors 2 --executor-cores 1 \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.driver.maxResultSize=1g \
      --conf spark.ui.port=6680 \
      --jars $HUDI_STREAMER_JARS \
      --class org.apache.hudi.utilities.streamer.HoodieStreamer \
      $HUDI_UTILITIES_SLIM_BUNDLE \
      --props $STREAMER_CONFIG_FILE \
      --source-class org.apache.hudi.benchmarks.BenchmarkDataSource \
      --source-ordering-field $ORDERING_FIELD \
      --target-base-path $TABLE_BASE_PATH \
      --target-table test_table \
      --table-type $TABLE_TYPE \
      --op $OPERATION_TYPE --merge-mode EVENT_TIME_ORDERING --post-write-termination-strategy-class >> $TEST_LOG 2>&1
  echo "Validate Data quality $i ..." >> $TEST_LOG 2>&1
  $SPARK_HOME/bin/spark-submit \
      --master local[1] \
      --driver-memory 1g --executor-memory 1g --num-executors 1 --executor-cores 1 \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.driver.maxResultSize=1g \
      --jars $HUDI_STREAMER_JARS \
      --class org.apache.hudi.utilities.HoodieSourceDataValidator \
      $HUDI_UTILITIES_SLIM_BUNDLE \
      --base-path $TABLE_BASE_PATH \
      --source-path /tmp/trial5/ \
      --record-key-field _row_key \
      --partition-path-field partition_path \
      --ordering-field-config current_ts >> $TEST_LOG 2>&1
  current_timestamp=$(date +%s)
done
