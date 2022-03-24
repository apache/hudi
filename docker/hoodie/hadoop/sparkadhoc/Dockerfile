
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
ARG SPARK_VERSION=2.4.4
FROM apachehudi/hudi-hadoop_${HADOOP_VERSION}-hive_${HIVE_VERSION}-sparkbase_${SPARK_VERSION}

ARG PRESTO_VERSION=0.268
ARG TRINO_VERSION=368
COPY adhoc.sh /opt/spark

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"
ENV PRESTO_VERSION ${PRESTO_VERSION}
ENV TRINO_VERSION ${TRINO_VERSION}
ENV BASE_URL=https://repo1.maven.org/maven2

RUN apt-get update
RUN apt-get install -y \
    curl \
    tar \
    sudo \
    rsync \
    python \
    wget \
    python3-pip \
    python-dev \
    build-essential \
    uuid-runtime \
    less

RUN set -x \
    ## presto-client
    && wget -q -O /usr/local/bin/presto ${BASE_URL}/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar \
    && chmod +x /usr/local/bin/presto
RUN set -x \
    ## trino-cli
    && wget -q -O /usr/local/bin/trino ${BASE_URL}/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar \
    && chmod +x /usr/local/bin/trino
CMD ["/bin/bash", "/opt/spark/adhoc.sh"]
