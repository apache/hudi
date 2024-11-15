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

HUDI_LAKEVIEW_SYNC_UBER_JAR=`ls -c $DIR/../../packaging/hudi-lakeview-sync-bundle/target/hudi-lakeview-sync-*.jar | grep -v source | head -1`

HIVE_EXEC=`ls ${HIVE_HOME}/lib/hive-exec-*.jar | tr '\n' ':'`
HIVE_SERVICE=`ls ${HIVE_HOME}/lib/hive-service-*.jar | grep -v rpc | tr '\n' ':'`
HIVE_METASTORE=`ls ${HIVE_HOME}/lib/hive-metastore-*.jar | tr '\n' ':'`
HIVE_JDBC=`ls ${HIVE_HOME}/lib/hive-jdbc-*.jar | tr '\n' ':'`
if [ -z "${HIVE_JDBC}" ]; then
  HIVE_JDBC=`ls ${HIVE_HOME}/lib/hive-jdbc-*.jar | grep -v handler | tr '\n' ':'`
fi
HIVE_JARS=$HIVE_METASTORE:$HIVE_SERVICE:$HIVE_EXEC:$HIVE_JDBC
HADOOP_HIVE_JARS=${HIVE_JARS}:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*

echo "Running Command : java -cp $HUDI_LAKEVIEW_SYNC_UBER_JAR:${HADOOP_HIVE_JARS} org.apache.hudi.sync.LakeviewSyncTool $@"
set -x
java -cp $HUDI_LAKEVIEW_SYNC_UBER_JAR:${HADOOP_HIVE_JARS}  org.apache.hudi.sync.LakeviewSyncTool "$@"
