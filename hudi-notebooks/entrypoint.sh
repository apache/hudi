#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

SPARK_MASTER_HOST=$(hostname)
SPARK_MASTER_PORT=7077

echo "Starting Spark Master..."
nohup "$SPARK_HOME"/sbin/start-master.sh &
sleep 2

echo "Starting Spark Worker..."
nohup "$SPARK_HOME"/sbin/start-worker.sh "spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT" &
sleep 2

echo "Starting Spark History Server..."
nohup "$SPARK_HOME"/sbin/start-history-server.sh &
sleep 2

PY4J_ZIP=$(ls "$SPARK_HOME"/python/lib/py4j*.zip)
export PYTHONPATH="${SPARK_HOME}/python/:${PY4J_ZIP}"

echo "Starting Jupyter Notebook..."
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''