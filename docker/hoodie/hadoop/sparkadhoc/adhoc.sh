#!/bin/bash

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"


export SPARK_HOME=/opt/spark

date
echo "SPARK HOME is : $SPARK_HOME"

tail -f /dev/null
