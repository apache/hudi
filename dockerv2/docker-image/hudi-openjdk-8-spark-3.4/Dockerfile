# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License. 

ARG ARCH
FROM eclipse-temurin:8-jdk-focal

ARG SPARK_VERSION=3.4.3
ARG HADOOP_VERSION=3

ARG SPARK_ARTIFACT="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"

ENV SPARK_HOME=/spark

RUN set -ex; \ 
    apt-get update; \
    apt-get install -y python3 python3-pip git vim kafkacat jq maven; \
    rm -rf /var/lib/apt/lists/*

RUN set -xeu; \
    wget -nv "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARTIFACT}.tgz"; \
    tar -xf ${SPARK_ARTIFACT}.tgz; \
    rm ${SPARK_ARTIFACT}.tgz; \
    ln -sn /${SPARK_ARTIFACT} ${SPARK_HOME}

WORKDIR ${SPARK_HOME}/bin

ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.10.2/hadoop-aws-2.10.2.jar -P /opt/hudisync && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -P /opt/hudisync && wget https://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.13.0/libthrift-0.13.0.jar -P /opt/hudisync

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.10.2/hadoop-aws-2.10.2.jar -P /opt/hudicli && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -P /opt/hudicli && wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-cli-bundle_2.12/0.15.0/hudi-cli-bundle_2.12-0.15.0.jar -P /opt/hudicli && wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/0.15.0/hudi-spark3.4-bundle_2.12-0.15.0.jar -P /opt/hudicli

RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc && chmod +x /usr/local/bin/mc

RUN cd /opt && git clone https://github.com/apache/hudi.git

EXPOSE 10213

CMD spark-submit \
   --master "local[*]" \
   --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
   --name "Thrift JDBC/ODBC Server" \
   --conf spark.hive.server2.thrift.port=10213 \
   spark-internal
