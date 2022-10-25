#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# NOTE: this script runs inside hudi-ci-bundle-validation container
# $WORKDIR/jars/ is supposed to be mounted to a host directory where bundle jars are placed
# TODO: $JAR_COMBINATIONS should have different orders for different jars to detect class loading issues

$DERBY_HOME/bin/startNetworkServer -h 0.0.0.0 &
$HIVE_HOME/bin/hiveserver2 &
WORKDIR=/opt/hudi-bundles
JAR_COMBINATIONS=$(echo $WORKDIR/jars/*.jar | tr ' ' ',')
$SPARK_HOME/bin/spark-shell --jars $JAR_COMBINATIONS < $WORKDIR/validate.scala

exit $?
