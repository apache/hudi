################################################################################
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
################################################################################


FROM flink:1.18

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
RUN mv -v /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop
RUN chown -R flink:flink /opt/flink/plugins/flink-s3-fs-hadoop

ENV FLINK_HADOOP_CONF /etc/hadoop/conf
RUN mkdir -p $FLINK_HADOOP_CONF
COPY config/hadoop/core-site.xml $FLINK_HADOOP_CONF
ENV HADOOP_CLASSPATH=$FLINK_HADOOP_CONF
WORKDIR /opt/hudi/examples
COPY target/hudi-examples-k8s-*-SNAPSHOT.jar streaming/hudi-examples-k8s.jar