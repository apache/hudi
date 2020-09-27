#!/bin/bash

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

mode=$1
modules=$2
sparkVersion=2.4.4
hadoopVersion=2.7

if [ "$mode" = "unit" ]; then
  mvn clean install -DskipTests -q
  echo "Running Unit Tests"
  mvn test -Punit-tests -pl "$modules" -B
elif [ "$mode" = "functional" ]; then
  echo "Running Functional Tests"
  mvn test -Pfunctional-tests -B
elif [ "$mode" = "integration" ]; then
  echo "Downloading Apache Spark-${sparkVersion}-bin-hadoop${hadoopVersion}"
  wget http://archive.apache.org/dist/spark/spark-${sparkVersion}/spark-${sparkVersion}-bin-hadoop${hadoopVersion}.tgz -O /tmp/spark-${sparkVersion}.tgz
  tar -xvf /tmp/spark-${sparkVersion}.tgz
  export SPARK_HOME=$PWD/spark-${sparkVersion}-bin-hadoop${hadoopVersion}
  mkdir /tmp/spark-events/
  echo "Running Integration Tests"
  mvn verify -Pintegration-tests -B
else
  echo "Unknown mode $mode"
  exit 1
fi
