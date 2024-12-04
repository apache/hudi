#!/bin/bash

export CONFIG_PATH=$1
export TEST_BASE_PATH=$2
export TABLE_BASE_PATH="file:$2/test_table"
export VALIDATE_LOG="$TEST_BASE_PATH/validate_mdt.log"

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
VALIDATION_MIN_TIME_SECONDS=30
start_timestamp=$(date +%s)

for ((i = 0; ; i++)); do
  batch_start_timestamp=$(date +%s)
  echo "Start validation run $i ..." >> $VALIDATE_LOG 2>&1
  echo "Validate latest file slides and base files run $i ..." >> $VALIDATE_LOG 2>&1
  $SPARK_HOME/bin/spark-submit \
      --master local[1] \
      --driver-memory 1g --executor-memory 1g --num-executors 1 --executor-cores 1 \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.eventLog.enabled=true \
      --conf spark.eventLog.dir=/Users/ethan/Work/data/hudi/spark-logs \
      --conf spark.driver.maxResultSize=1g \
      --jars $HUDI_STREAMER_JARS \
      --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
      $HUDI_UTILITIES_SLIM_BUNDLE \
      --base-path $TABLE_BASE_PATH \
      --validate-latest-file-slices \
      --validate-latest-base-files \
      --skip-data-files-for-cleaning >> $VALIDATE_LOG 2>&1
  echo "Validate all file groups run $i ..." >> $VALIDATE_LOG 2>&1
  $SPARK_HOME/bin/spark-submit \
      --master local[1] \
      --driver-memory 1g --executor-memory 1g --num-executors 1 --executor-cores 1 \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.eventLog.enabled=true \
      --conf spark.eventLog.dir=/Users/ethan/Work/data/hudi/spark-logs \
      --conf spark.driver.maxResultSize=1g \
      --jars $HUDI_STREAMER_JARS \
      --class org.apache.hudi.utilities.HoodieMetadataTableValidator \
      $HUDI_UTILITIES_SLIM_BUNDLE \
      --base-path $TABLE_BASE_PATH \
      --validate-all-file-groups \
      --skip-data-files-for-cleaning \
      --ignore-failed >> $VALIDATE_LOG 2>&1

  current_timestamp=$(date +%s)
  elapsed_time=$((current_timestamp - start_timestamp))
  echo "Elapsed time in MDT validation: $((elapsed_time / 60)) minutes."
  if (( elapsed_time >= SECONDS_TO_WAIT )); then
    echo "The specified time of $MINUTES minutes has passed. Exiting the MDT validation."
    break
  fi

  current_timestamp=$(date +%s)
  elapsed_time=$((current_timestamp - batch_start_timestamp))
  if (( elapsed_time < VALIDATION_MIN_TIME_SECONDS)); then
    echo "Sleep $((VALIDATION_MIN_TIME_SECONDS - elapsed_time))"
    sleep $((VALIDATION_MIN_TIME_SECONDS - elapsed_time))
  fi
done
