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
# link the jar names to easier to use names
ln -sf $JARS_DIR/hudi-hadoop-mr*.jar $JARS_DIR/hadoop-mr.jar
#ln -sf $JARS_DIR/hudi-flink*.jar $JARS_DIR/flink.jar
ln -sf $JARS_DIR/hudi-spark*.jar $JARS_DIR/spark.jar
#ln -sf $JARS_DIR/hudi-utilities-bundle*.jar $JARS_DIR/utilities.jar
#ln -sf $JARS_DIR/hudi-utilities-slim*.jar $JARS_DIR/utilities-slim.jar
#ln -sf $JARS_DIR/hudi-kafka-connect-bundle*.jar $JARS_DIR/kafka-connect.jar
ln -sf $JARS_DIR/hudi-metaserver-server-bundle*.jar $JARS_DIR/metaserver.jar


##
# Function to test the hudi metaserver bundles.
#
# env vars (defined in container):
#   SPARK_HOME: path to the spark directory
# --defaults-files=
##
test_hudi_metaserver_bundles () {
    echo "::warning::validate1.sh setting up hudi metaserver for hudi metaserver bundles validation"

    $MYSQL_HOME/bin/mysqld --defaults-files=service/my.cnf  --initialize
    echo "1==========="
    $MYSQL_HOME/support-files mysql.server start
    echo "2==========="
#    echo "::warning::validate.sh Start hudi metaserver"
#    java -jar $JARS_DIR/metaserver.jar
##    & local METASEVER=$!
#    ps -ef | grep metaserver

#    echo "::warning::validate.sh Start hive server"
#    $DERBY_HOME/bin/startNetworkServer -h 0.0.0.0 &
#    local DERBY_PID=$!
#    $HIVE_HOME/bin/hiveserver2 --hiveconf hive.aux.jars.path=$JARS_DIR/hadoop-mr.jar &
#    local HIVE_PID=$!
#
#    echo "::warning::validate.sh Writing sample data via Spark DataSource."
#    $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar < $WORKDIR/service/write.scala
#
#    echo "::warning::validate.sh Query and validate the results using Spark SQL"
#    # save Spark SQL query results
#    $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar  < $WORKDIR/service/read.scala
#    numRecordsSparkSQL=$(cat /tmp/sparksql/trips/results/*.csv | wc -l)
#    echo $numRecordsSparkSQL
#    if [ "$numRecordsSparkSQL" -ne 10 ]; then
#        echo "::error::validate.sh Spark SQL validation failed."
#        exit 1
#    fi
#
#    echo "::warning::validate.sh hudi metaserver validation was successful."
#    kill $DERBY_PID $HIVE_PID $METASEVER
}


############################
# Execute tests
############################

#echo "::warning::validate.sh validating spark & hadoop-mr bundle"
#test_spark_hadoop_mr_bundles
#if [ "$?" -ne 0 ]; then
#    exit 1
#fi
#echo "::warning::validate.sh done validating spark & hadoop-mr bundle"

echo "::warning::validate.sh validating hudi metaserver bundle"
test_hudi_metaserver_bundles
if [ "$?" -ne 0 ]; then
    exit 1
fi
echo "::warning::validate.sh done validating hudi metaserver bundle"
