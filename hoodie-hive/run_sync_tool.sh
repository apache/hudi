#!/usr/bin/env bash

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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HOODIE_HIVE_UBER_JAR=`ls -c $DIR/../packaging/hoodie-hive-bundle/target/hoodie-hive-*.jar | head -1`

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
fi

## Include only specific packages from HIVE_HOME/lib to avoid version mismatches
HIVE_EXEC=`ls ${HIVE_HOME}/lib/hive-exec-*.jar | tr '\n' ':'`
HIVE_SERVICE=`ls ${HIVE_HOME}/lib/hive-service-*.jar | grep -v rpc | tr '\n' ':'`
HIVE_METASTORE=`ls ${HIVE_HOME}/lib/hive-metastore-*.jar | tr '\n' ':'`
# Hive 1.x/CDH has standalone jdbc jar which is no longer available in 2.x
HIVE_JDBC=`ls ${HIVE_HOME}/lib/hive-jdbc-*standalone*.jar | tr '\n' ':'`
if [ -z "${HIVE_JDBC}" ]; then
  HIVE_JDBC=`ls ${HIVE_HOME}/lib/hive-jdbc-*.jar | grep -v handler | tr '\n' ':'`
fi
HIVE_JARS=$HIVE_METASTORE:$HIVE_SERVICE:$HIVE_EXEC:$HIVE_SERVICE:$HIVE_JDBC

HADOOP_HIVE_JARS=${HIVE_JARS}:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*

echo "Running Command : java -cp ${HADOOP_HIVE_JARS}:${HADOOP_CONF_DIR}:$HOODIE_HIVE_UBER_JAR com.uber.hoodie.hive.HiveSyncTool $@"
java -cp $HOODIE_HIVE_UBER_JAR:${HADOOP_HIVE_JARS}:${HADOOP_CONF_DIR} com.uber.hoodie.hive.HiveSyncTool "$@"
