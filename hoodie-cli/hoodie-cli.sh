#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HOODIE_JAR=`ls $DIR/target/hoodie-cli-*-SNAPSHOT.jar`
java -cp /etc/hadoop/conf:/etc/spark/conf:$DIR/target/lib/*:$HOODIE_JAR org.springframework.shell.Bootstrap
