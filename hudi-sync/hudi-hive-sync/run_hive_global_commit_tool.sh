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

# A tool to sync the hudi table to hive from different clusters. Similar to HiveSyncTool but syncs it to more
# than one hive cluster ( currently a local and remote cluster). The common timestamp that was synced is stored as a new table property
# This is most useful when we want to ensure that across different hive clusters we want ensure consistent reads. If that is not a requirement
# then it is better to run HiveSyncTool separately.
# Note:
#   The tool tries to be transactional but does not guarantee it. If the sync fails midway in one cluster it will try to roll back the committed
#   timestamp from already successful sync on other clusters but that can also fail.
#   The tool does not roll back any synced partitions but only the timestamp.

function error_exit {
    echo "$1" >&2   ## Send message to stderr. Exclude >&2 if you don't want it that way.
    exit "${2:-1}"  ## Return a code specified by $2 or 1 by default.
}

if [ -z "${HADOOP_HOME}" ]; then
  error_exit "Please make sure the environment variable HADOOP_HOME is setup"
fi

if [ -z "${HIVE_HOME}" ]; then
  error_exit "Please make sure the environment variable HIVE_HOME is setup"
fi

# Joins the non-empty arguments with ':'. Java reads an empty classpath entry as the
# current directory, so empty values (a jar that is not found) must not leave a bare ':' behind.
function join_classpath {
  local joined="" entry
  for entry in "$@"; do
    if [ -n "${entry}" ]; then
      joined="${joined:+${joined}:}${entry}"
    fi
  done
  printf '%s\n' "${joined}"
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HUDI_HIVE_UBER_JAR=`ls -c "$DIR"/../../packaging/hudi-hive-sync-bundle/target/hudi-hive-sync-*.jar | grep -v source | head -1`

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
fi

## Include only specific packages from HIVE_HOME/lib to avoid version mismatches
HIVE_EXEC=`ls "${HIVE_HOME}"/lib/hive-exec-*.jar | paste -sd ':' -`
HIVE_SERVICE=`ls "${HIVE_HOME}"/lib/hive-service-*.jar | grep -v rpc | paste -sd ':' -`
HIVE_METASTORE=`ls "${HIVE_HOME}"/lib/hive-metastore-*.jar | paste -sd ':' -`
HIVE_JDBC=`ls "${HIVE_HOME}"/lib/hive-jdbc-*.jar | paste -sd ':' -`
if [ -z "${HIVE_JDBC}" ]; then
  HIVE_JDBC=`ls "${HIVE_HOME}"/lib/hive-jdbc-*.jar | grep -v handler | paste -sd ':' -`
fi
HIVE_JACKSON=`ls "${HIVE_HOME}"/lib/jackson-*.jar | paste -sd ':' -`
HIVE_NUCLEUS=`ls "${HIVE_HOME}"/lib/datanucleus*.jar | paste -sd ':' -`
HIVE_JARS=$(join_classpath "${HIVE_METASTORE}" "${HIVE_SERVICE}" "${HIVE_EXEC}" "${HIVE_JDBC}" "${HIVE_JACKSON}" "${HIVE_NUCLEUS}")

HADOOP_HIVE_JARS=$(join_classpath "${HIVE_JARS}" "${HADOOP_HOME}/share/hadoop/common/*" "${HADOOP_HOME}/share/hadoop/mapreduce/*" "${HADOOP_HOME}/share/hadoop/hdfs/*" "${HADOOP_HOME}/share/hadoop/common/lib/*" "${HADOOP_HOME}/share/hadoop/hdfs/lib/*")

if ! [ -z "$HIVE_CONF_DIR" ]; then
  error_exit "Don't set HIVE_CONF_DIR; use config xml file"
fi

# Build the classpath once so the logged command matches the java command that actually runs.
HUDI_HIVE_GLOBAL_COMMIT_CLASSPATH=$(join_classpath "${HUDI_HIVE_UBER_JAR}" "${HADOOP_HIVE_JARS}" "${HADOOP_CONF_DIR}" "${HIVE_HOME}lib/*")
# Shell-quote the logged command so it can be copied and run as is, even with spaces in paths or arguments.
LOGGED_ARGS=""
if [ $# -gt 0 ]; then
  printf -v LOGGED_ARGS ' %q' "$@"
fi
echo "Running Command : java -cp $(printf '%q' "${HUDI_HIVE_GLOBAL_COMMIT_CLASSPATH}") org.apache.hudi.hive.replication.HiveSyncGlobalCommitTool${LOGGED_ARGS}"
java -cp "${HUDI_HIVE_GLOBAL_COMMIT_CLASSPATH}" org.apache.hudi.hive.replication.HiveSyncGlobalCommitTool "$@"
