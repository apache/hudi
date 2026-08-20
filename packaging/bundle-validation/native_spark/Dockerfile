#
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
#

# A glibc image with Java 17, which the native bundle needs and the shared bundle-validation base
# image cannot provide: that one is Alpine based, so its JVMs are musl linked and Comet's
# glibc-linked libcomet.so cannot load into them. Kept separate from the shared base image so this
# does not change how any other bundle is validated.
FROM eclipse-temurin:17-jdk

ARG SPARK_VERSION=3.5.5
ARG SPARK_HADOOP_VERSION=3

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /opt/native-bundle-validation
ENV WORKDIR=/opt/native-bundle-validation

RUN wget -q https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz -P "$WORKDIR" \
    && tar -xf $WORKDIR/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz -C $WORKDIR/ \
    && rm $WORKDIR/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz
ENV SPARK_HOME=$WORKDIR/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION
