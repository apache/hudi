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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HOODIE_JAR=`ls -c $DIR/target/hudi-timeline-server-bundle-*.jar | grep -v test | grep -v source | head -1`

if [ -z "$HADOOP_HOME" ]; then
  echo "HADOOP_HOME not set. It must be set"
  exit -1
fi

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="/etc/hadoop/conf"
fi

if [ ! -f "${DIR}/javax.servlet-api-3.1.0.jar" ]; then
 echo "Downloading runtime servlet jar first :"
 wget https://repo1.maven.org/maven2/javax/servlet/javax.servlet-api/3.1.0/javax.servlet-api-3.1.0.jar
fi

if [ ! -f "${DIR}/javax.servlet-api-3.1.0.jar" ]; then
 echo "Servlet API Jar not found. "
 exit -1
fi

HADOOP_COMMON_JARS=`ls -c ${HADOOP_HOME}/share/hadoop/common/*.jar | grep -v test | grep -v source | grep -v servlet | grep -v 'jetty-' | grep -v 'jersey-' | grep -v 'jsp-' | tr '\n' ':'`
HADOOP_COMMON_LIB_JARS=`ls -c ${HADOOP_HOME}/share/hadoop/common/lib/*.jar | grep -v test | grep -v source | grep -v servlet | grep -v 'jetty-' | grep -v 'jersey-' | grep -v 'jsp-' | tr '\n' ':'`
HADOOP_HDFS_JARS=`ls -c ${HADOOP_HOME}/share/hadoop/hdfs/*.jar | grep -v test | grep -v source | grep -v servlet | grep -v 'jetty-' | grep -v 'jersey-' | grep -v 'jsp-' | tr '\n' ':'`
HADOOP_HDFS_LIB_JARS=`ls -c ${HADOOP_HOME}/share/hadoop/hdfs/lib/*.jar | grep -v test | grep -v source | grep -v servlet | grep -v 'jetty-' | grep -v 'jersey-' | grep -v 'jsp-' | tr '\n' ':'`
HADOOP_JARS=${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*
HADOOP_JARS=${HADOOP_COMMON_JARS}:${HADOOP_COMMON_LIB_JARS}:${HADOOP_HDFS_JARS}:${HADOOP_HDFS_LIB_JARS}

SERVLET_JAR=${DIR}/javax.servlet-api-3.1.0.jar
echo "Running command : java -cp ${SERVLET_JAR}:${HADOOP_JARS}:${HADOOP_CONF_DIR}:$HOODIE_JAR org.apache.hudi.timeline.service.TimelineService $@"
java -Xmx4G -cp ${SERVLET_JAR}:${HADOOP_JARS}:${HADOOP_CONF_DIR}:$HOODIE_JAR org.apache.hudi.timeline.service.TimelineService "$@"
