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
SPARK_PROFILE=$2
SCALA_PROFILE=$3
DEFAULT_JAVA_HOME=${JAVA_HOME}
WORKDIR=/opt/bundle-validation
JARS_DIR=${WORKDIR}/jars
DOCKER_TEST_DIR=${WORKDIR}/docker-test

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
  echo "Use default java runtime under ${DEFAULT_JAVA_HOME}"
  export JAVA_HOME=${DEFAULT_JAVA_HOME}
}

start_datanode () {
  DN=$1

  echo "::warning::docker_test_java17.sh starting datanode:"$DN

  DN_DIR_PREFIX=$DOCKER_TEST_DIR/additional_datanode/
  PID_DIR=$DOCKER_TEST_DIR/pid/$1

  if [ -z $DN_DIR_PREFIX ]; then
    mkdir -p $DN_DIR_PREFIX
  fi

  if [ -z $PID_DIR ]; then
    mkdir -p $PID_DIR
  fi

  export HADOOP_PID_DIR=$PID_PREFIX
  DN_CONF_OPTS="\
  -Dhadoop.tmp.dir=$DN_DIR_PREFIX$DN\
  -Ddfs.datanode.address=localhost:5001$DN \
  -Ddfs.datanode.http.address=localhost:5008$DN \
  -Ddfs.datanode.ipc.address=localhost:5002$DN"
  $HADOOP_HOME/bin/hdfs --daemon start datanode $DN_CONF_OPTS
  $HADOOP_HOME/bin/hdfs dfsadmin -report
}

setup_hdfs () {
  $HADOOP_HOME/bin/hdfs namenode -format
  $HADOOP_HOME/bin/hdfs --daemon start namenode
  echo "::warning::docker_test_java17.sh starting hadoop hdfs"
  $HADOOP_HOME/sbin/start-dfs.sh

  # start 3 datanodes
  for i in 1 2 3
  do
    start_datanode $i
  done

  echo "::warning::docker_test_java17.sh starting hadoop hdfs, hdfs report"
  $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/root
  $HADOOP_HOME/bin/hdfs dfs -ls /user/
  if [ "$?" -ne 0 ]; then
    echo "::error::docker_test_java17.sh Failed setting up HDFS!"
    exit 1
  fi
}

build_hudi_java8 () {
  use_default_java_runtime
  mvn clean install -D"$SCALA_PROFILE" -D"$SPARK_PROFILE" -DskipTests=true \
    -e -ntp -B -V -Dgpg.skip -Djacoco.skip -Pwarn-log \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=warn \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.dependency=warn \
    -pl packaging/hudi-spark-bundle -am

  if [ "$?" -ne 0 ]; then
    echo "::error::docker_test_java17.sh Failed building Hudi with Java 8!"
    exit 1
  fi

  cp ./packaging/hudi-spark-bundle/target/hudi-spark*.jar $JARS_DIR/spark.jar
}

run_docker_tests() {
  echo "::warning::docker_test_java17.sh run_docker_tests Running Hudi maven tests on Docker"
  change_java_runtime_version

  mvn -e test -D$SPARK_PROFILE -D$SCALA_PROFILE -Djava17 -Drun.docker.java17.test=true \
     -Dtest=org.apache.hudi.common.functional.TestHoodieLogFormat,org.apache.hudi.common.util.TestDFSPropertiesConfiguration \
     -DfailIfNoTests=false -pl hudi-common

  if [ "$?" -ne 0 ]; then
    echo "::error::docker_test_java17.sh Hudi maven tests failed"
    exit 1
  fi
  echo "::warning::docker_test_java17.sh Hudi maven tests passed!"

  echo "::warning::docker_test_java17.sh run_docker_tests Running Hudi Scala script tests on Docker"
  $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar < $WORKDIR/spark_hadoop_mr/TestHiveClientUtils.scala
  if [ $? -ne 0 ]; then
    echo "::error::docker_test_java17.sh HiveClientUtils failed"
    exit 1
  fi
  echo "::warning::docker_test_java17.sh run_docker_tests Hudi Scala script tests passed!"

  echo "::warning::docker_test_java17.sh All Docker tests passed!"
  use_default_java_runtime
}

############################
# Execute tests
############################
cd $DOCKER_TEST_DIR

echo "::warning::docker_test_java17.sh Building Hudi with Java 8"
build_hudi_java8
echo "::warning::docker_test_java17.sh Done building Hudi with Java 8"

setup_hdfs

echo "::warning::docker_test_java17.sh Running tests with Java 17"
run_docker_tests
if [ "$?" -ne 0 ]; then
  exit 1
fi
echo "::warning::docker_test_java17.sh Done running tests with Java 17"

