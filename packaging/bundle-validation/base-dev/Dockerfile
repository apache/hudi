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

RUN apk add --no-cache --upgrade bash curl jq openjdk11 openjdk17 --repository=https://dl-cdn.alpinelinux.org/alpine/v3.15/community

RUN mkdir /opt/bundle-validation
ENV WORKDIR=/opt/bundle-validation
WORKDIR $WORKDIR

ARG HADOOP_VERSION=2.7.7
ARG HIVE_VERSION=3.1.3
ARG DERBY_VERSION=10.14.1.0
ARG FLINK_VERSION=1.13.6
ARG SPARK_VERSION=3.1.3
ARG SPARK_HADOOP_VERSION=2.7
ARG CONFLUENT_MAJOR_VERSION=5.5
ARG CONFLUENT_VERSION=5.5.12
ARG KAFKA_CONNECT_HDFS_VERSION=10.1.13

ADD https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz $WORKDIR
RUN cd $WORKDIR \
    && tar -xf $WORKDIR/hadoop-$HADOOP_VERSION.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/hadoop-$HADOOP_VERSION.tar.gz
ENV HADOOP_HOME=$WORKDIR/hadoop-$HADOOP_VERSION

ADD https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz $WORKDIR
RUN cd $WORKDIR \
    && tar -xf $WORKDIR/apache-hive-$HIVE_VERSION-bin.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/apache-hive-$HIVE_VERSION-bin.tar.gz
ENV HIVE_HOME=$WORKDIR/apache-hive-$HIVE_VERSION-bin

ADD https://archive.apache.org/dist/db/derby/db-derby-$DERBY_VERSION/db-derby-$DERBY_VERSION-bin.tar.gz $WORKDIR
RUN cd $WORKDIR \
    && tar -xf $WORKDIR/db-derby-$DERBY_VERSION-bin.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/db-derby-$DERBY_VERSION-bin.tar.gz
ENV DERBY_HOME=$WORKDIR/db-derby-$DERBY_VERSION-bin

ADD https://archive.apache.org/dist/flink/flink-$FLINK_VERSION/flink-$FLINK_VERSION-bin-scala_2.12.tgz $WORKDIR
RUN cd $WORKDIR \
    && tar -xf $WORKDIR/flink-$FLINK_VERSION-bin-scala_2.12.tgz -C $WORKDIR/ \
    && rm $WORKDIR/flink-$FLINK_VERSION-bin-scala_2.12.tgz
ENV FLINK_HOME=$WORKDIR/flink-$FLINK_VERSION

ADD https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz $WORKDIR
RUN cd $WORKDIR \
    && tar -xf $WORKDIR/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz -C $WORKDIR/ \
    && rm $WORKDIR/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION.tgz
ENV SPARK_HOME=$WORKDIR/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION

ADD https://packages.confluent.io/archive/${CONFLUENT_MAJOR_VERSION}/confluent-community-$CONFLUENT_VERSION-2.12.tar.gz $WORKDIR
RUN cd $WORKDIR \
    && tar -xf $WORKDIR/confluent-community-$CONFLUENT_VERSION-2.12.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/confluent-community-$CONFLUENT_VERSION-2.12.tar.gz
ENV CONFLUENT_HOME=$WORKDIR/confluent-$CONFLUENT_VERSION

ADD https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-hdfs/versions/$KAFKA_CONNECT_HDFS_VERSION/confluentinc-kafka-connect-hdfs-$KAFKA_CONNECT_HDFS_VERSION.zip $WORKDIR
RUN cd $WORKDIR \
    && mkdir $WORKDIR/kafka-connectors \
    && unzip $WORKDIR/confluentinc-kafka-connect-hdfs-$KAFKA_CONNECT_HDFS_VERSION.zip -d $WORKDIR/kafka-connectors/ \
    && rm $WORKDIR/confluentinc-kafka-connect-hdfs-$KAFKA_CONNECT_HDFS_VERSION.zip \
    && printf "\nplugin.path=$WORKDIR/kafka-connectors\n" >> $CONFLUENT_HOME/etc/kafka/connect-distributed.properties
ENV KAFKA_CONNECT_PLUGIN_PATH_LIB_PATH=$WORKDIR/kafka-connectors/confluentinc-kafka-connect-hdfs-$KAFKA_CONNECT_HDFS_VERSION/lib
