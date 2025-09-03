
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

ARG HADOOP_VERSION=3.3.4 
ARG HADOOP_HISTORY_PORT=8188
FROM apachehudi/hudi-hadoop_${HADOOP_VERSION}-base:latest

# Install unzip and wget
RUN apt-get update && \
    apt-get install -y unzip wget && \
    apt-get clean

# Download the JAR and extract the native library
RUN wget https://repo1.maven.org/maven2/org/openlabtesting/leveldbjni/leveldbjni-linux64-aarch64/1.8/leveldbjni-linux64-aarch64-1.8.jar && \
    unzip leveldbjni-linux64-aarch64-1.8.jar 'META-INF/native/linux64/aarch64/libleveldbjni.so' && \
    cp META-INF/native/linux64/aarch64/libleveldbjni.so /usr/lib/ && \
    rm -rf leveldbjni-linux64-aarch64-1.8.jar META-INF

# Set environment variables for native lib
ENV LD_LIBRARY_PATH="/usr/lib"
ENV JAVA_LIBRARY_PATH="/usr/lib"

ENV HADOOP_HISTORY_PORT ${HADOOP_HISTORY_PORT}

ENV YARN_CONF_yarn_timeline___service_leveldb___timeline___store_path=/hadoop/yarn/timeline
RUN mkdir -p /hadoop/yarn/timeline
VOLUME /hadoop/yarn/timeline

ADD run_history.sh /run_history.sh
RUN chmod a+x /run_history.sh

CMD ["/run_history.sh"]
