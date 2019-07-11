#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HOODIE_JAR=`ls $DIR/target/hoodie-cli-*-SNAPSHOT.jar`
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
