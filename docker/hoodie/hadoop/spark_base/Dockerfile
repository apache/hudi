
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

ARG HADOOP_VERSION=2.8.4 
ARG HIVE_VERSION=2.3.3
FROM apachehudi/hudi-hadoop_${HADOOP_VERSION}-hive_${HIVE_VERSION}

ENV ENABLE_INIT_DAEMON true
ENV INIT_DAEMON_BASE_URI http://identifier/init-daemon
ENV INIT_DAEMON_STEP spark_master_init

ARG SPARK_VERSION=3.1.3
ARG SPARK_HADOOP_VERSION=2.7

ENV SPARK_VERSION ${SPARK_VERSION}
ENV HADOOP_VERSION ${SPARK_HADOOP_VERSION}

COPY wait-for-step.sh /
COPY execute-step.sh /
COPY finish-step.sh /

RUN echo "Installing Spark-version (${SPARK_VERSION})" \
      &&  wget http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && cd /

# Install python3 to enable and use pyspark shell
RUN apt-get update \
    && apt-get -yq install python3 \
    && ln -sf $(which python3) /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

#Give permission to execute scripts
RUN chmod +x /wait-for-step.sh && chmod +x /execute-step.sh && chmod +x /finish-step.sh

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV PYTHONHASHSEED 1

ENV SPARK_HOME /opt/spark
ENV SPARK_INSTALL ${SPARK_HOME}
ENV SPARK_CONF_DIR ${SPARK_HOME}/conf
ENV PATH $SPARK_INSTALL/bin:$PATH

ENV SPARK_DRIVER_PORT 5001
ENV SPARK_UI_PORT 5002
ENV SPARK_BLOCKMGR_PORT 5003

EXPOSE $SPARK_DRIVER_PORT $SPARK_UI_PORT $SPARK_BLOCKMGR_PORT

# Without this spark-shell fails - Download if it is not already there in $SPARK_INSTALL
RUN wget -nc -q -O "${SPARK_INSTALL}/jars/jersey-bundle-1.19.4.jar" "https://repo1.maven.org/maven2/com/sun/jersey/jersey-bundle/1.19.4/jersey-bundle-1.19.4.jar"

