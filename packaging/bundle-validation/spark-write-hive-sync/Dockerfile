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

RUN mkdir /opt/hudi-bundles
ENV WORKDIR=/opt/hudi-bundles
WORKDIR $WORKDIR

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz -P "$WORKDIR" \
    && tar -xf $WORKDIR/hadoop-2.7.7.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/hadoop-2.7.7.tar.gz
ENV HADOOP_HOME=$WORKDIR/hadoop-2.7.7

RUN wget https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz -P "$WORKDIR" \
    && tar -xf $WORKDIR/spark-2.4.8-bin-hadoop2.7.tgz -C $WORKDIR/ \
    && rm $WORKDIR/spark-2.4.8-bin-hadoop2.7.tgz
ENV SPARK_HOME=$WORKDIR/spark-2.4.8-bin-hadoop2.7

RUN wget https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz -P "$WORKDIR" \
    && tar -xf $WORKDIR/apache-hive-2.3.9-bin.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/apache-hive-2.3.9-bin.tar.gz
ENV HIVE_HOME=$WORKDIR/apache-hive-2.3.9-bin

RUN wget https://archive.apache.org/dist/db/derby/db-derby-10.10.2.0/db-derby-10.10.2.0-bin.tar.gz -P "$WORKDIR" \
    && tar -xf $WORKDIR/db-derby-10.10.2.0-bin.tar.gz -C $WORKDIR/ \
    && rm $WORKDIR/db-derby-10.10.2.0-bin.tar.gz
ENV DERBY_HOME=$WORKDIR/db-derby-10.10.2.0-bin

RUN cp $DERBY_HOME/lib/derbyclient.jar $SPARK_HOME/jars/
COPY hive-site.xml $HIVE_HOME/conf/
RUN ln -sf $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml
COPY spark-defaults.conf $SPARK_HOME/conf/
COPY validate.scala .
COPY validate.sh .
