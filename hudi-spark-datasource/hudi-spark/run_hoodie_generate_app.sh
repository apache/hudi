#!/usr/bin/env bash

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

function error_exit {
    echo "$1" >&2   ## Send message to stderr. Exclude >&2 if you don't want it that way.
    exit "${2:-1}"  ## Return a code specified by $2 or 1 by default.
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HUDI_JAR=`ls -c $DIR/../../packaging/hudi-spark-bundle/target/hudi-spark*-bundle*.jar | grep -v sources | grep -v javadoc | head -1`

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="/etc/hadoop/conf"
fi

if [ -z "$CLIENT_JAR" ]; then
  echo "client jar location not set"
fi

#OTHER_JARS=`ls -1 $DIR/target/lib/*jar | grep -v 'hadoop-common-2' | grep -v 'hadoop-auth-2' | grep -v 'jackson-core-asl-1' | grep -v 'jackson-mapper-asl-1' | grep -v 'jackson-jaxrs-1' | grep -v 'jackson-xc-1' | tr '\n' ':'`
OTHER_JARS=`ls -1 $DIR/target/lib/*jar | grep -e 'slf4j' -e 'log4j' \
  -e 'lib/jcommander' -e 'lib/spark' -e 'lib/scala-' -e 'lib/hudi' -e 'lib/hadoop-common' \
  -e 'lib/common' -e 'lib/kryo' | tr '\n' ':'`
#OTHER_JARS=`ls -1 $DIR/target/lib/*jar | grep -v 'hadoop-common-2' | grep -v 'hadoop-auth-2' | \
#  grep -v 'jackson-core-asl-1' | grep -v 'jackson-mapper-asl-1' | grep -v 'jackson-jaxrs-1' | \
#  grep -v 'jackson-xc-1' | grep -v 'SNAPSHOT.jar' | tr '\n' ':'`

#TODO - Need to move TestDataGenerator and HoodieJavaApp out of tests
echo "Running command : java -cp $DIR/target/test-classes/:$DIR/../hudi-client/target/test-classes/:${HADOOP_CONF_DIR}:${CLIENT_JAR}:$OTHER_JARS:$SPARK_HOME/jars:$HUDI_JAR $@"
java -Xmx1G -cp $DIR/target/test-classes/:$DIR/../hudi-client/target/test-classes/:${HADOOP_CONF_DIR}:${CLIENT_JAR}:$OTHER_JARS:$SPARK_HOME/jars:$HUDI_JAR HoodieJavaGenerateApp "$@"
