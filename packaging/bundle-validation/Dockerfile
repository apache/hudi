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
FROM adoptopenjdk/openjdk8:alpine

RUN apk add --no-cache --upgrade bash

RUN mkdir /opt/bundle-validation
ENV WORKDIR=/opt/bundle-validation
WORKDIR $WORKDIR

ARG HADOOP_VERSION=2.7.7
ARG HIVE_VERSION=3.1.3
ARG DERBY_VERSION=10.14.1.0
ARG SPARK_VERSION=3.1.3
ARG SPARK_HADOOP_VERSION=2.7

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -P "$WORKDIR" \
    && tar -xf $WORKDIR/hadoop-$HADOOP_VERSION.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/hadoop-$HADOOP_VERSION.tar.gz
ENV HADOOP_HOME=$WORKDIR/hadoop-$HADOOP_VERSION

RUN wget https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz -P "$WORKDIR" \
    && tar -xf $WORKDIR/apache-hive-$HIVE_VERSION-bin.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/apache-hive-$HIVE_VERSION-bin.tar.gz
ENV HIVE_HOME=$WORKDIR/apache-hive-$HIVE_VERSION-bin

RUN wget https://archive.apache.org/dist/db/derby/db-derby-$DERBY_VERSION/db-derby-$DERBY_VERSION-bin.tar.gz -P "$WORKDIR" \
    && tar -xf $WORKDIR/db-derby-$DERBY_VERSION-bin.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/db-derby-$DERBY_VERSION-bin.tar.gz
ENV DERBY_HOME=$WORKDIR/db-derby-$DERBY_VERSION-bin

RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz -P "$WORKDIR" \
    && tar -xf $WORKDIR/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz -C $WORKDIR/ \
    && rm $WORKDIR/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz
ENV SPARK_HOME=$WORKDIR/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION
COPY validate.sh .

