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

docker build \
 --build-arg HIVE_VERSION=3.1.3 \
 --build-arg FLINK_VERSION=1.18.0 \
 --build-arg SPARK_VERSION=3.4.0 \
 --build-arg SPARK_HADOOP_VERSION=3 \
 --build-arg HADOOP_VERSION=3.3.5 \
 -t hudi-ci-bundle-validation-base:flink1180hive313spark340 .
docker image tag hudi-ci-bundle-validation-base:flink1180hive313spark340 apachehudi/hudi-ci-bundle-validation-base:flink1180hive313spark340
