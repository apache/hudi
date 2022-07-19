#
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
#
# Trino docker-compose_hadoop284_hive233_spark313.yml setup is adapted from https://github.com/Lewuathe/docker-trino-cluster

ARG HADOOP_VERSION=2.8.4
ARG TRINO_VERSION=368
FROM apachehudi/hudi-hadoop_${HADOOP_VERSION}-trinobase_${TRINO_VERSION}:latest as trino-base

ADD etc /usr/local/trino/etc
EXPOSE 8091

WORKDIR /usr/local/trino
ENTRYPOINT [ "./scripts/trino.sh" ]
