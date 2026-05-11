
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

# --- Stage 1: fetch + extract Hadoop (throwaway) ---
FROM eclipse-temurin:17-jre-jammy AS hadoop-builder

ARG HADOOP_VERSION=3.4.0
ARG HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

RUN set -x \
    && DEBIAN_FRONTEND=noninteractive apt-get -yq update \
    && apt-get -yq install --no-install-recommends curl ca-certificates \
    && curl -fSL "${HADOOP_URL}" -o /tmp/hadoop.tar.gz \
    && mkdir -p /opt \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt/ \
    && rm /tmp/hadoop.tar.gz \
    && mkdir -p /opt/hadoop-${HADOOP_VERSION}/logs

# --- Stage 2: runtime ---
FROM eclipse-temurin:17-jre-jammy
LABEL maintainer="Hoodie"
USER root

# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

ARG HADOOP_VERSION=3.4.0
ENV HADOOP_VERSION=${HADOOP_VERSION}

RUN DEBIAN_FRONTEND=noninteractive apt-get -yq update \
    && apt-get -yq install --no-install-recommends netcat-openbsd procps \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /hadoop-data

COPY --from=hadoop-builder /opt/hadoop-${HADOOP_VERSION} /opt/hadoop-${HADOOP_VERSION}
RUN ln -s /opt/hadoop-${HADOOP_VERSION}/etc/hadoop /etc/hadoop

ENV HADOOP_PREFIX=/opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV HADOOP_HOME=${HADOOP_PREFIX}
ENV HADOOP_INSTALL=${HADOOP_HOME}
ENV USER=root
ENV PATH=/usr/bin:/bin:${HADOOP_PREFIX}/bin/:$PATH

# Exposing a union of ports across hadoop versions
# Well known ports including ssh
EXPOSE 0-1024 4040 7000-10100 5000-5100 50000-50200 58188 58088 58042

ADD entrypoint.sh /entrypoint.sh
ADD export_container_ip.sh /usr/bin/
RUN chmod a+x /usr/bin/export_container_ip.sh \
    && chmod a+x /entrypoint.sh

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
