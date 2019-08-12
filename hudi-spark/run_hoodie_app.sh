#!/usr/bin/env bash

function error_exit {
    echo "$1" >&2   ## Send message to stderr. Exclude >&2 if you don't want it that way.
    exit "${2:-1}"  ## Return a code specified by $2 or 1 by default.
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HUDI_JAR=`ls -c $DIR/../packaging/hudi-spark-bundle/target/hudi-spark-bundle-*.jar | grep -v source | head -1`

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="/etc/hadoop/conf"
fi

if [ -z "$CLIENT_JAR" ]; then
  echo "client jar location not set"
fi

OTHER_JARS=`ls -1 $DIR/target/lib/*jar | grep -v '*avro*-1.' | tr '\n' ':'`
#TODO - Need to move TestDataGenerator and HoodieJavaApp out of tests
echo "Running command : java -cp $DIR/target/test-classes/:$DIR/../hudi-client/target/test-classes/:${HADOOP_CONF_DIR}:$HUDI_JAR:${CLIENT_JAR}:$OTHER_JARS HoodieJavaApp $@"
java -Xmx1G -cp $DIR/target/test-classes/:$DIR/../hudi-client/target/test-classes/:${HADOOP_CONF_DIR}:$HUDI_JAR:${CLIENT_JAR}:$OTHER_JARS HoodieJavaApp "$@"
