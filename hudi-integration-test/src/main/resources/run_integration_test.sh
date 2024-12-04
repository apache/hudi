#!/bin/bash

CONFIG_PATH=$(realpath "$1")
TEST_TAG=$(basename "$CONFIG_PATH")
BASE_DIR=/Users/ethan/Work/tmp/hudi-1.0.0-testing
# Generate a timestamp in yyyy-mm-dd-hh-mm-ss format
TIME_PREFIX=$(date +"%Y-%m-%d-%H-%M-%S")
BASE_FOLDER="${TIME_PREFIX}-$TEST_TAG"
TEST_BASE_PATH="$BASE_DIR/$BASE_FOLDER"

# Create a directory with the generated timestamp name
mkdir -p $TEST_BASE_PATH

export SPARK_HOME=/Users/ethan/Work/lib/spark-3.5.3-bin-hadoop3
export HUDI_UTILITIES_SLIM_BUNDLE=/Users/ethan/Work/tmp/hudi-1.0.0-testing/hudi-utilities-slim-bundle_2.12-1.0.0-rc1.jar
export HUDI_STREAMER_JARS="$HUDI_UTILITIES_SLIM_BUNDLE,/Users/ethan/Work/tmp/hudi-1.0.0-testing/hudi-spark3.5-bundle_2.12-1.0.0-rc1.jar,/Users/ethan/Work/tmp/hudi-1.0.0-testing/hudi-benchmarks-0.1-SNAPSHOT.jar"

echo "SPARK_HOME: $SPARK_HOME"
echo "HUDI_UTILITIES_SLIM_BUNDLE: $HUDI_UTILITIES_SLIM_BUNDLE"
echo "HUDI_STREAMER_JARS: $HUDI_STREAMER_JARS"
echo "CONFIG_PATH: $CONFIG_PATH"
echo "TEST_BASE_PATH: $TEST_BASE_PATH"

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

echo "TEST_SCRIPT_NAME: $TEST_SCRIPT_NAME"
echo "MDT_VALIDATION_SCRIPT_NAME: $TEST_SCRIPT_NAME"

# Start Hudi streamer
./$TEST_SCRIPT_NAME $CONFIG_PATH $TEST_BASE_PATH &
pid1=$!
echo "Test running PID: $pid1"

# Start Async validation
if [[ -n "$MDT_VALIDATION_SCRIPT_NAME" ]]; then
  echo "Start MDT validation script separately."
  ./$MDT_VALIDATION_SCRIPT_NAME $CONFIG_PATH $TEST_BASE_PATH &
  pid2=$!
  echo "MDT validation running PID: $pid2"
else
  echo "No separate MDT validation script to run."
fi

# Function to handle Ctrl+C (SIGINT)
cleanup() {
  echo "Caught Ctrl+C. Terminating process with PID $pid1..."
  kill "$pid1"  # Kill the process
  wait "$pid1"  # Wait for the process to fully terminate
  echo "Process terminated."
  if [[ -n "$pid2" ]]; then
    kill "$pid2"  # Kill the process
    wait "$pid2"  # Wait for the process to fully terminate
    echo "Process 2 terminated."
  fi
  exit 0
}

trap cleanup SIGINT
wait $pid1
if [[ -n "$pid2" ]]; then
  wait $pid2
fi
echo "Testing has finished."
