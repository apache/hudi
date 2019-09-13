#!/usr/bin/env bash

################################################################################
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
################################################################################

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HOODIE_JAR=`ls $DIR/target/hudi-cli-*.jar | grep -v source | grep -v javadoc`
if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="/etc/hadoop/conf"
fi
if [ -z "$SPARK_CONF_DIR" ]; then
  echo "setting spark conf dir"
  SPARK_CONF_DIR="/etc/spark/conf"
fi
if [ -z "$CLIENT_JAR" ]; then
  echo "client jar location not set"
fi
java -cp ${HADOOP_CONF_DIR}:${SPARK_CONF_DIR}:$DIR/target/lib/*:$HOODIE_JAR:${CLIENT_JAR} -DSPARK_CONF_DIR=${SPARK_CONF_DIR} -DHADOOP_CONF_DIR=${HADOOP_CONF_DIR} org.springframework.shell.Bootstrap $@
