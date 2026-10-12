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

SPARK_PROFILE=$1
SCALA_PROFILE=$2
JAVA_RUNTIME_VERSION=openjdk17
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
  local dn=$1
  local data_dir="$DOCKER_TEST_DIR/additional_datanode/$dn"
  local pid_dir="$DOCKER_TEST_DIR/pid/datanode-$dn"

  echo "::warning::docker_test_java17.sh starting datanode:$dn"
  mkdir -p "$data_dir" "$pid_dir" || return 1
  HADOOP_PID_DIR="$pid_dir" "$HADOOP_HOME/bin/hdfs" --daemon start datanode \
    -Dhadoop.tmp.dir="$data_dir" \
    -Ddfs.datanode.address="localhost:5001$dn" \
    -Ddfs.datanode.http.address="localhost:5008$dn" \
    -Ddfs.datanode.ipc.address="localhost:5002$dn"
}

setup_hdfs () {
  # The base image's Java 8 cgroup metrics can crash on newer CI hosts.
  # Disable container detection only for Hadoop and bound each daemon's heap explicitly.
  export HADOOP_OPTS="${HADOOP_OPTS:-} -XX:-UseContainerSupport"
  export HADOOP_HEAPSIZE_MAX=512

  echo "::warning::docker_test_java17.sh copying hadoop conf"
  cp "$WORKDIR/tmp-conf-dir/hdfs-site.xml" "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" || return 1
  cp "$WORKDIR/tmp-conf-dir/core-site.xml" "$HADOOP_HOME/etc/hadoop/core-site.xml" || return 1

  mkdir -p "$DOCKER_TEST_DIR/pid/namenode" || return 1
  "$HADOOP_HOME/bin/hdfs" namenode -format || return 1
  HADOOP_PID_DIR="$DOCKER_TEST_DIR/pid/namenode" "$HADOOP_HOME/bin/hdfs" --daemon start namenode || return 1

  # All daemons run in this container. Avoid the SSH/su worker dispatch in start-dfs.sh.
  local i
  for i in 1 2 3; do
    start_datanode "$i" || return 1
  done

  # Starting a daemon only forks it; wait for all replicas before running the tests.
  local report
  for i in $(seq 1 30); do
    if report=$("$HADOOP_HOME/bin/hdfs" dfsadmin -Dipc.client.connect.max.retries=0 -report 2>&1) \
        && printf '%s\n' "$report" | grep -q 'Live datanodes (3)'; then
      printf '%s\n' "$report"
      "$HADOOP_HOME/bin/hdfs" dfsadmin -safemode wait || return 1
      "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/root || return 1
      "$HADOOP_HOME/bin/hdfs" dfs -ls /user/
      return $?
    fi
    sleep 2
  done

  printf '%s\n' "$report"
  echo "::error::docker_test_java17.sh Failed waiting for three live HDFS datanodes!"
  tail -n 100 "$HADOOP_HOME"/logs/*.log "$HADOOP_HOME"/logs/*.out
  return 1
}

stop_hdfs() {
  use_default_java_runtime
  echo "::warning::docker_test_java17.sh stopping hadoop hdfs"
  local i
  for i in 1 2 3; do
    HADOOP_PID_DIR="$DOCKER_TEST_DIR/pid/datanode-$i" "$HADOOP_HOME/bin/hdfs" --daemon stop datanode
  done
  HADOOP_PID_DIR="$DOCKER_TEST_DIR/pid/namenode" "$HADOOP_HOME/bin/hdfs" --daemon stop namenode
}

cleanup_hdfs() {
  local exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    # Daemon startup errors are redirected to these files by Hadoop's launcher.
    tail -n 100 "$HADOOP_HOME"/logs/*.log "$HADOOP_HOME"/logs/*.out
  fi
  stop_hdfs
  return "$exit_code"
}

build_hudi () {
  if [ "$SPARK_PROFILE" = "spark4.0" ]; then
    change_java_runtime_version
  else
    use_default_java_runtime
  fi

  mvn clean install -D"$SCALA_PROFILE" -D"$SPARK_PROFILE" -DskipTests=true \
    -e -ntp -B -V -Dgpg.skip -Djacoco.skip -Pwarn-log \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=warn \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.dependency=warn \
    -pl packaging/hudi-spark-bundle -am

  if [ "$?" -ne 0 ]; then
    echo "::error::docker_test_java17.sh Failed building Hudi!"
    exit 1
  fi

  if [ ! -d $JARS_DIR ]; then
    mkdir -p $JARS_DIR
  fi

  cp ./packaging/hudi-spark-bundle/target/hudi-spark*.jar $JARS_DIR/spark.jar
}

run_docker_tests() {
  echo "::warning::docker_test_java17.sh run_docker_tests Running Hudi maven tests on Docker"
  change_java_runtime_version

  mvn -e test -D$SPARK_PROFILE -D$SCALA_PROFILE -Djava17 -Duse.external.hdfs=true \
     -Dtest=org.apache.hudi.common.functional.TestHoodieLogFormat,org.apache.hudi.common.util.TestDFSPropertiesConfiguration,org.apache.hudi.common.fs.TestHoodieWrapperFileSystem \
     -DfailIfNoTests=false -pl hudi-common -Pwarn-log

  if [ "$?" -ne 0 ]; then
    echo "::error::docker_test_java17.sh Hudi maven tests failed"
    exit 1
  fi
  echo "::warning::docker_test_java17.sh Hudi maven tests passed!"

  echo "::warning::docker_test_java17.sh run_docker_tests Running Hudi Scala script tests on Docker"
  $SPARK_HOME/bin/spark-shell --jars $JARS_DIR/spark.jar < $WORKDIR/docker_java17/TestHiveClientUtils.scala
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
echo "yxchang: $(PATH)"
export PATH=/usr/bin:$PATH
whoami
which ssh
whoami

echo "::warning::docker_test_java17.sh Building Hudi"
build_hudi
echo "::warning::docker_test_java17.sh Done building Hudi"

trap cleanup_hdfs EXIT
setup_hdfs || exit 1

echo "::warning::docker_test_java17.sh Running tests with Java 17"
run_docker_tests
if [ "$?" -ne 0 ]; then
  exit 1
fi
echo "::warning::docker_test_java17.sh Done running tests with Java 17"
