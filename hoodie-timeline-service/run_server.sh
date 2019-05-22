#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HOODIE_JAR=`ls -c $DIR/target/hoodie-timeline-*.jar | grep -v test | head -1`

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="/etc/hadoop/conf"
fi

OTHER_JARS=`ls -1 $DIR/target/lib/*jar | grep -v '*avro*-1.' | tr '\n' ':'`
echo "Running command : java -cp $DIR/target/:${HADOOP_CONF_DIR}:$HOODIE_JAR:$OTHER_JARS com.uber.hoodie.timeline.service.TimelineService $@"
java -Xmx4G -cp $DIR/target/test-classes/:${HADOOP_CONF_DIR}:$HOODIE_JAR:$OTHER_JARS com.uber.hoodie.timeline.service.TimelineService "$@"
