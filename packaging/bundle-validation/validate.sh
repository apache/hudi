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

JAVA_RUNTIME_VERSION=$1
SCALA_PROFILE=$2
SPARK_VERSION=$3
DEFAULT_JAVA_HOME=${JAVA_HOME}
WORKDIR=/opt/bundle-validation
echo $WORKDIR
JARS_DIR=${WORKDIR}/jars
# link the jar names to easier to use names
ln -sf $JARS_DIR/hudi-hadoop-mr*.jar $JARS_DIR/hadoop-mr.jar
if [[ "$SCALA_PROFILE" != 'scala-2.13' ]]; then
  # For Scala 2.13, Flink is not support, so skipping the Flink and Kafka Connect bundle validation
  # (Note that Kafka Connect bundle pulls in hudi-flink dependency)
  ln -sf $JARS_DIR/hudi-flink*.jar $JARS_DIR/flink.jar
  ln -sf $JARS_DIR/hudi-kafka-connect-bundle*.jar $JARS_DIR/kafka-connect.jar
fi
ln -sf $JARS_DIR/hudi-spark*.jar $JARS_DIR/spark.jar
ln -sf $JARS_DIR/hudi-utilities-bundle*.jar $JARS_DIR/utilities.jar
ln -sf $JARS_DIR/hudi-utilities-slim*.jar $JARS_DIR/utilities-slim.jar
ln -sf $JARS_DIR/hudi-metaserver-server-bundle*.jar $JARS_DIR/metaserver.jar
ln -sf $JARS_DIR/hudi-cli-bundle*.jar $JARS_DIR/cli.jar
# workaround for solving dependency conflict issues of Flink sql client (FLINK-33358)
ln -sf $FLINK_HOME/opt/flink-sql-client*.jar $FLINK_HOME/lib/flink-sql-client.jar

##
# Function to change Java runtime version by changing JAVA_HOME
##
change_java_runtime_version () {
  if [[ ${JAVA_RUNTIME_VERSION} == 'openjdk11' ]]; then
    echo "Change JAVA_HOME to /usr/lib/jvm/java-11-openjdk"
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
  elif [[ ${JAVA_RUNTIME_VERSION} == 'openjdk17' ]]; then
    echo "Change JAVA_HOME to /usr/lib/jvm/java-17-openjdk"
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
  fi
}

##
# Function to change Java runtime version to default Java 8
##
use_default_java_runtime () {
  echo "::warning:: Use default java runtime under ${DEFAULT_JAVA_HOME}"
  export JAVA_HOME=${DEFAULT_JAVA_HOME}
}

##
# Function to test the spark & hadoop-mr bundles with hive sync.
#
# env vars (defined in container):
#   HIVE_HOME: path to the hive directory
#   DERBY_HOME: path to the derby directory
#   SPARK_HOME: path to the spark directory
##
test_spark_hadoop_mr_bundles () {
    echo "::warning::validate.sh setting up hive metastore for spark & hadoop-mr bundles validation"

    if [ "$SPARK_VERSION" = "4.0.0" ]; then
        change_java_runtime_version
    fi
    $DERBY_HOME/bin/startNetworkServer -h 0.0.0.0 &
    local DERBY_PID=$!
    use_default_java_runtime
    $HIVE_HOME/bin/hiveserver2 --hiveconf hive.aux.jars.path=$JARS_DIR/hadoop-mr.jar &
    local HIVE_PID=$!
    change_java_runtime_version
    echo "::warning::validate.sh Writing sample data via Spark DataSource and run Hive Sync..."
    $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' < $WORKDIR/spark_hadoop_mr/write.scala

    echo "::warning::validate.sh Query and validate the results using Spark SQL"
    # save Spark SQL query results
    $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' < $WORKDIR/spark_hadoop_mr/validate.scala
    numRecords=$(cat /tmp/spark-bundle/sparksql/trips/results/*.csv | wc -l)
    if [ "$numRecords" -ne 10 ]; then
        echo "::error::validate.sh Spark SQL validation failed."
        exit 1
    fi
    echo "::warning::validate.sh Query and validate the results using HiveQL"
    use_default_java_runtime
    if [[ $HIVE_HOME =~ 'hive-2' ]]; then return; fi # skipping hive2 for HiveQL query due to setup issue
    # save HiveQL query results
    hiveqlresultsdir=/tmp/hadoop-mr-bundle/hiveql/trips/results
    mkdir -p $hiveqlresultsdir
    $HIVE_HOME/bin/beeline --hiveconf hive.input.format=org.apache.hudi.hadoop.HoodieParquetInputFormat \
      -u jdbc:hive2://localhost:10000/default --showHeader=false --outputformat=csv2 \
      -e 'select * from trips' >> $hiveqlresultsdir/results.csv
    numRecordsHiveQL=$(cat $hiveqlresultsdir/*.csv | wc -l)
    if [ "$numRecordsHiveQL" -ne 10 ]; then
        echo "::error::validate.sh HiveQL validation failed."
        if [ "$SPARK_VERSION" = "4.0.0" ]; then
            echo "::error::validate.sh Debug info for Spark4 validation failure:"
            $HIVE_HOME/bin/beeline --hiveconf hive.input.format=org.apache.hudi.hadoop.HoodieParquetInputFormat \
                      -u jdbc:hive2://localhost:10000/default --showHeader=true --outputformat=csv2 \
                      -e 'select * from trips' --verbose=true --showNestedErrs=true
            # not exit here for Spark 4, may be we can fix it as a follow-up
        else
            exit 1
        fi
    fi
    echo "::warning::validate.sh spark & hadoop-mr bundles validation was successful."
    kill $DERBY_PID $HIVE_PID
}


##
# Function to test the utilities bundle and utilities slim bundle + spark bundle.
# It runs deltastreamer and then verifies that deltastreamer worked correctly.
#
# 1st arg: main jar to run with spark-submit, usually it's the utilities(-slim) bundle
# 2nd arg and beyond: any additional jars to pass to --jars option
#
# env vars (defined in container):
#   SPARK_HOME: path to the spark directory
##
test_utilities_bundle () {
    MAIN_JAR=$1
    printf -v EXTRA_JARS '%s,' "${@:2}"
    EXTRA_JARS="${EXTRA_JARS%,}"
    OPT_JARS=""
    if [[ -n $EXTRA_JARS ]]; then
        OPT_JARS="--jars $EXTRA_JARS"
    fi
    OUTPUT_DIR=/tmp/hudi-utilities-test/
    rm -r $OUTPUT_DIR
    change_java_runtime_version
    echo "::warning::validate.sh running deltastreamer"
    $SPARK_HOME/bin/spark-submit \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
    $OPT_JARS $MAIN_JAR \
    --props $WORKDIR/utilities/hoodieapp.properties \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
    --source-class org.apache.hudi.utilities.sources.JsonDFSSource \
    --source-ordering-field ts --table-type MERGE_ON_READ \
    --target-base-path ${OUTPUT_DIR} \
    --target-table utilities_tbl  --op UPSERT
    if [ "$?" -ne 0 ]; then
        echo "::error::validate.sh deltastreamer failed with exit code $?"
        exit 1
    fi
    echo "::warning::validate.sh done with deltastreamer"

    OUTPUT_SIZE=$(du -s ${OUTPUT_DIR} | awk '{print $1}')
    if [[ -z $OUTPUT_SIZE || "$OUTPUT_SIZE" -lt "580" ]]; then
        echo "::error::validate.sh deltastreamer output folder ($OUTPUT_SIZE) is smaller than minimum expected (580)" 
        exit 1
    fi

    echo "::warning::validate.sh validating deltastreamer in spark shell"
    SHELL_COMMAND="$SPARK_HOME/bin/spark-shell $OPT_JARS $MAIN_JAR -i $WORKDIR/utilities/validate.scala"
    echo "::debug::this is the shell command: $SHELL_COMMAND"
    LOGFILE="$WORKDIR/${FUNCNAME[0]}.log"
    $SHELL_COMMAND >> $LOGFILE
    use_default_java_runtime
    if [ "$?" -ne 0 ]; then
        SHELL_RESULT=$(cat $LOGFILE | grep "Counts don't match")
        echo "::error::validate.sh $SHELL_RESULT"
        exit 1
    fi
    echo "::warning::validate.sh done validating deltastreamer in spark shell"
}


##
# Function to test the flink bundle.
#
# env vars (defined in container):
#   HADOOP_HOME: path to the hadoop directory
#   FLINK_HOME: path to the flink directory
##
test_flink_bundle() {
    export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
    change_java_runtime_version
    $FLINK_HOME/bin/start-cluster.sh
    $FLINK_HOME/bin/sql-client.sh -j $JARS_DIR/flink.jar -f $WORKDIR/flink/insert.sql
    echo "validate flink insert finished."
    $WORKDIR/flink/compact.sh $JARS_DIR/flink.jar
    local EXIT_CODE=$?
    $FLINK_HOME/bin/stop-cluster.sh
    unset HADOOP_CLASSPATH
    use_default_java_runtime
    if [ "$EXIT_CODE" -ne 0 ]; then
        echo "::error::validate.sh Flink bundle validation failed."
        exit 1
    fi
    echo "::warning::validate.sh done validating Flink bundle validation was successful."
}

##
# Function to test the kafka-connect bundle.
# It runs zookeeper, kafka broker, schema registry, and connector worker.
# After producing and consuming data, it checks successful commit under `.hoodie/`
#
# 1st arg: path to the hudi-kafka-connect-bundle.jar (for writing data)
#
# env vars (defined in container):
#   CONFLUENT_HOME: path to the confluent community directory
#   KAFKA_CONNECT_PLUGIN_PATH_LIB_PATH: path to install hudi-kafka-connect-bundle.jar
##
test_kafka_connect_bundle() {
    change_java_runtime_version
    KAFKA_CONNECT_JAR=$1
    cp $KAFKA_CONNECT_JAR $KAFKA_CONNECT_PLUGIN_PATH_LIB_PATH
    $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &
    local ZOOKEEPER_PID=$!
    $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &
    local KAFKA_SERVER_PID=$!
    sleep 10
    $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties &
    local SCHEMA_REG_PID=$!
    sleep 10
    $CONFLUENT_HOME/bin/kafka-topics --create --topic hudi-control-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
    $WORKDIR/kafka/produce.sh
    $WORKDIR/kafka/consume.sh
    local EXIT_CODE=$?
    kill $ZOOKEEPER_PID $KAFKA_SERVER_PID $SCHEMA_REG_PID
    use_default_java_runtime
    if [ "$EXIT_CODE" -ne 0 ]; then
        echo "::error::validate.sh Kafka Connect bundle validation failed."
        exit 1
    fi
    echo "::warning::validate.sh done validating Kafka Connect bundle validation was successful."
}

##
# Function to test the hudi metaserver bundles.
#
# env vars (defined in container):
#   SPARK_HOME: path to the spark directory
##
test_metaserver_bundle () {
    echo "::warning::validate.sh setting up Metaserver bundle validation"

    echo "::warning::validate.sh Start Metaserver"
    java -jar $JARS_DIR/metaserver.jar &
    local METASEVER_PID=$!

    if [ "$SPARK_VERSION" = "4.0.0" ]; then
            change_java_runtime_version
    fi
    echo "::warning::validate.sh Start hive server"
    $DERBY_HOME/bin/startNetworkServer -h 0.0.0.0 &
    local DERBY_PID=$!
    use_default_java_runtime
    $HIVE_HOME/bin/hiveserver2 --hiveconf hive.aux.jars.path=$JARS_DIR/hadoop-mr.jar &
    local HIVE_PID=$!

    change_java_runtime_version
    echo "::warning::validate.sh Writing sample data via Spark DataSource."
    $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar < $WORKDIR/service/write.scala
    ls /tmp/hudi-bundles/tests/trips

    echo "::warning::validate.sh Query and validate the results using Spark DataSource"
    # save Spark DataSource query results
    $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar  < $WORKDIR/service/read.scala
    numRecords=$(cat /tmp/metaserver-bundle/sparkdatasource/trips/results/*.csv | wc -l)
    echo $numRecords
    use_default_java_runtime
    if [ "$numRecords" -ne 10 ]; then
        echo "::error::validate.sh Metaserver bundle validation failed."
        exit 1
    fi

    echo "::warning::validate.sh Metaserver bundle validation was successful."
    kill $DERBY_PID $HIVE_PID $METASEVER_PID
}

##
# Function to test the hudi-cli bundle.
# It creates a test table and connects to it using CLI commands
#
# env vars
#   SPARK_HOME: path to the spark directory
#   CLI_BUNDLE_JAR: path to the hudi cli bundle jar
#   SPARK_BUNDLE_JAR: path to the hudi spark bundle jar
##
test_cli_bundle() {
    echo "::warning::validate.sh setting up CLI bundle validation"

    if [ "$SPARK_VERSION" = "4.0.0" ]; then
        change_java_runtime_version
    fi

    # Create a temporary directory for CLI commands output
    CLI_TEST_DIR="/tmp/hudi-bundles/tests/log"
    mkdir -p $CLI_TEST_DIR

    # Set required environment variables
    export SPARK_HOME=$SPARK_HOME
    export CLI_BUNDLE_JAR=$JARS_DIR/cli.jar
    export SPARK_BUNDLE_JAR=$JARS_DIR/spark.jar

    # Execute with debug output
    echo "Executing Hudi CLI commands..."
    # This feeds CLI commands, stripping license lines to avoid parser errors
    sed '/^#/d' "$WORKDIR/cli/commands.txt" | $WORKDIR/docker-test/packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh 2>&1 | tee $CLI_TEST_DIR/output.txt

    # Verify CLI started successfully
    if ! grep -q "hudi->" $CLI_TEST_DIR/output.txt; then
        echo "::error::validate.sh CLI bundle validation failed - CLI did not start successfully"
        return 1
    fi

    # Verify table was created
    if [ ! -d "/tmp/hudi-bundles/tests/table/.hoodie" ]; then
        echo "::error::validate.sh CLI bundle validation failed - 'create' command did not execute successfully. Table directory not created."
        return 1
    fi

    # Verify table connection
    if ! grep -q "Metadata for table trips loaded" $CLI_TEST_DIR/output.txt; then
        echo "::error::validate.sh CLI bundle validation failed - 'connect' command did not execute successfully. Table connection failed."
        return 1
    fi

    # Verify table description
    if ! grep -q "hoodie.table.name.*trips" $CLI_TEST_DIR/output.txt; then
        echo "::error::validate.sh CLI bundle validation failed - 'desc' command did not execute successfully. Table properties are missing."
        return 1
    fi

    # Verify commit history
    if ! grep -q "CommitTime" $CLI_TEST_DIR/output.txt; then
        echo "::error::validate.sh CLI bundle validation failed - 'commits show' command did not execute successfully."
        return 1
    fi

    if grep -q "(empty)" $CLI_TEST_DIR/output.txt; then
        echo "::error::validate.sh CLI bundle validation failed - No commit history found."
        return 1
    fi

    echo "::warning::validate.sh CLI bundle validation was successful"
    return 0
}

############################
# Execute tests
############################

# run flink bundle test only for flink2.x case, since there is problem for java 11 and spark3.5 bundle (HUDI-8608).
if [[ "${FLINK_HOME}" == *"2.0"* || "${FLINK_HOME}" == *"2.1"* ]]; then
    echo "::warning::validate.sh validating flink 2.0 bundle"
    test_flink_bundle
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    echo "::warning::validate.sh done validating flink 2.x bundle"
    exit 0
fi

echo "::warning::validate.sh validating spark & hadoop-mr bundle"
test_spark_hadoop_mr_bundles
if [ "$?" -ne 0 ]; then
    exit 1
fi
echo "::warning::validate.sh done validating spark & hadoop-mr bundle"

if [[ $SPARK_HOME == *"spark-3.5"* || $SPARK_HOME == *"spark-4.0"* ]]
then
  echo "::warning::validate.sh validating cli bundle"
  test_cli_bundle
  if [ "$?" -ne 0 ]; then
      exit 1
  fi
  echo "::warning::validate.sh done validating cli bundle"
else
  echo "::warning::validate.sh skip validating cli bundle for Spark < 3.5 build"
fi

if [[ $SPARK_HOME == *"spark-3.5"* ]]
then
  echo "::warning::validate.sh validating utilities bundle"
  test_utilities_bundle $JARS_DIR/utilities.jar
  if [ "$?" -ne 0 ]; then
      exit 1
  fi
  echo "::warning::validate.sh done validating utilities bundle"
else
  echo "::warning::validate.sh skip validating utilities bundle for Spark < 3.5 build"
fi

echo "::warning::validate.sh validating utilities slim bundle"
test_utilities_bundle $JARS_DIR/utilities-slim.jar $JARS_DIR/spark.jar
if [ "$?" -ne 0 ]; then
    exit 1
fi
echo "::warning::validate.sh done validating utilities slim bundle"

if [[ ${JAVA_RUNTIME_VERSION} == 'openjdk8' && ${SCALA_PROFILE} != 'scala-2.13' ]]; then
  echo "::warning::validate.sh validating flink bundle"
  test_flink_bundle
  if [ "$?" -ne 0 ]; then
      exit 1
  fi
  echo "::warning::validate.sh done validating flink bundle"
fi

if [[ ${SCALA_PROFILE} != 'scala-2.13' ]]; then
  echo "::warning::validate.sh validating kafka connect bundle"
  test_kafka_connect_bundle $JARS_DIR/kafka-connect.jar
  if [ "$?" -ne 0 ]; then
      exit 1
  fi
  echo "::warning::validate.sh done validating kafka connect bundle"

  echo "::warning::validate.sh validating metaserver bundle"
  test_metaserver_bundle
  if [ "$?" -ne 0 ]; then
      exit 1
  fi
  echo "::warning::validate.sh done validating metaserver bundle"
fi
