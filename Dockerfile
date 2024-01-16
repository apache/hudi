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

# Use a home made image as the base, which includes:
# utuntu:latest
# git
# thrift
# maven
# java8
# Use an official Ubuntu base image
FROM ubuntu:latest

# Install necessary packages
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk maven && \
    apt-get clean

RUN apt-get update && \
    apt-get install -y \
    build-essential \
    automake \
    bison \
    flex \
    libboost-all-dev \
    libevent-dev \
    libssl-dev \
    git \
    pkg-config

WORKDIR /app

# Clone the Thrift repository from GitHub
RUN git clone --branch 0.14.2 --depth 1 https://github.com/apache/thrift.git .

RUN export PKG_PROG_PKG_CONFIG=`which pkg-config`

# Build and install Thrift
RUN ./bootstrap.sh && \
    ./configure && \
    make && \
    make install

# Cleanup unnecessary build dependencies
RUN apt-get remove -y \
    build-essential \
    automake \
    bison \
    flex \
    libboost-all-dev \
    libevent-dev \
    libssl-dev && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Set Java 8 as the default version
RUN update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java 1
RUN update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

CMD ["java", "-version"]

# Set the working directory to /app
WORKDIR /app

# Copy git repo into the working directory
COPY . /app