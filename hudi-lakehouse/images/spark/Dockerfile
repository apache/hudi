# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Spark runtime with the Hudi Spark bundle built from this repository, used by
# the local-dev environment to run Hudi write jobs on Kubernetes.
#
# Build context: this directory. scripts/build-images.sh stages the bundle jar
# (packaging/hudi-spark-bundle/target/hudi-spark*-bundle_*.jar) into ./target/
# first; the staging directory is gitignored so jars never enter version
# control.
#
# The Scala version of the base image must match the bundle:
#   apache/spark:3.5.x (Scala 2.12)  <->  hudi-spark3.5-bundle_2.12
#   apache/spark:4.1.x (Scala 2.13)  <->  hudi-spark4.1-bundle_2.13
ARG SPARK_IMAGE_TAG=3.5.7-scala2.12-java17-python3-ubuntu
FROM apache/spark:${SPARK_IMAGE_TAG}

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.772
ARG HUDI_SPARK_BUNDLE_JAR

USER root
RUN set -eux; \
    wget -q -O /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    wget -q -O /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
COPY target/${HUDI_SPARK_BUNDLE_JAR} /opt/spark/jars/
USER spark
