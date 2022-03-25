
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


## Presto docker setup is based on https://github.com/smizy/docker-presto

ARG HADOOP_VERSION=3.1.0
ARG HIVE_VERSION=3.1.2
FROM apachehudi/hudi-hadoop_${HADOOP_VERSION}-base:latest as hadoop-base

ARG PRESTO_VERSION=0.271

ENV PRESTO_VERSION       ${PRESTO_VERSION}
ENV PRESTO_HOME          /opt/presto-server-${PRESTO_VERSION}
ENV PRESTO_CONF_DIR      ${PRESTO_HOME}/etc
ENV PRESTO_LOG_DIR       /var/log/presto
ENV PRESTO_JVM_MAX_HEAP  2G
ENV PRESTO_QUERY_MAX_MEMORY          1GB
ENV PRESTO_QUERY_MAX_MEMORY_PER_NODE 512MB
ENV PRESTO_DISCOVERY_URI  http://presto-coordinator-1:8090
ENV PATH        $PATH:${PRESTO_HOME}/bin

RUN set -x \
    && DEBIAN_FRONTEND=noninteractive apt-get -yq update \
    && apt-get -yq install \
        bash \
        less \
        python \
        tar \
        wget \
    ## - hadoop native dependency lib
        bzip2 \
        fts \
        fuse \
        libtirpc1 \
        libsnappy1v5 \
        zip \
        cron \
        gosu \
    && rm -rf /var/lib/apt/lists/* \
    ## presto-server
    && wget -q -O - https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz \
        | tar -xzf - -C /opt/  \
    && mkdir -p /var/hoodie/ws/docker/hoodie/hadoop/prestobase/target/ \
    ## presto-client
    && wget -q -O /usr/local/bin/presto https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar \
    && chmod +x /usr/local/bin/presto \
    ## user/dir/permmsion
    && adduser --shell /sbin/nologin --uid 1000 docker \
    && adduser --shell /sbin/nologin presto \
    && mkdir -p \
        ${PRESTO_CONF_DIR} \
        ${PRESTO_LOG_DIR} \
    && chmod -R 777 \
        ${PRESTO_HOME} \
        ${PRESTO_LOG_DIR} \
    ## cleanup
    && rm -rf /tmp/nativelib

COPY etc/  ${PRESTO_CONF_DIR}/
COPY bin/*  /usr/local/bin/
COPY lib/*  /usr/local/lib/
RUN chmod +x /usr/local/bin/entrypoint.sh

ADD target/ /var/hoodie/ws/docker/hoodie/hadoop/prestobase/target/
ENV HUDI_PRESTO_BUNDLE /var/hoodie/ws/docker/hoodie/hadoop/prestobase/target/hudi-presto-bundle.jar
RUN  cp ${HUDI_PRESTO_BUNDLE} ${PRESTO_HOME}/plugin/hive-hadoop2/
# TODO: the latest master of Presto relies on hudi-presto-bundle, while current Presto releases
# rely on hudi-common and hudi-hadoop-mr 0.9.0, which are pulled in plugin/hive-hadoop2/ in the
# docker setup, making it hard to test the latest changes in Hudi due to class conflict.
# To get around the conflicts due to older Hudi jars below, they are removed for integration tests,
# so the hudi-presto-bundle build can be used solely for testing.  This temporary logic must be
# removed once Presto has a new release depending on hudi-presto-bundle and we upgrade docker setup
# to that release version.
RUN rm ${PRESTO_HOME}/plugin/hive-hadoop2/hudi-common-*
RUN rm ${PRESTO_HOME}/plugin/hive-hadoop2/hudi-hadoop-mr-*

VOLUME ["${PRESTO_LOG_DIR}"]

WORKDIR ${PRESTO_HOME}

ENTRYPOINT ["entrypoint.sh"]
