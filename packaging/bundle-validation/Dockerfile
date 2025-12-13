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

ARG IMAGE_TAG=spark313hive313
ARG REPO=apachehudi
FROM $REPO/hudi-ci-bundle-validation-base:$IMAGE_TAG

# configure the stack
ADD . .
ENV HUDI_CONF_DIR=$WORKDIR/conf
RUN if [[ $HIVE_HOME =~ 'hive-2' ]]; \
    then mv conf/hive-site.hive2.xml conf/hive-site.xml; fi

RUN cp conf/hive-site.xml $HIVE_HOME/conf/
RUN cp conf/hive-site.xml $SPARK_HOME/conf/
RUN cp $DERBY_HOME/lib/derbyclient.jar $SPARK_HOME/jars/
RUN cp conf/spark-defaults.conf $SPARK_HOME/conf/
RUN if [[ $SPARK_HOME =~ 'spark-3.2' ]] || [[ $SPARK_HOME =~ 'spark-3.3' ]]; \
    then printf "\nspark.sql.catalog.spark_catalog org.apache.spark.sql.hudi.catalog.HoodieCatalog\n" >> $SPARK_HOME/conf/spark-defaults.conf; fi
RUN printf "\ntaskmanager.numberOfTaskSlots: 2\n" >> $FLINK_HOME/conf/flink-conf.yaml
RUN printf "\nlocalhost\n" >> $FLINK_HOME/conf/workers

# install maven
RUN wget https://archive.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
RUN mkdir -p /usr/share/maven
RUN tar xzvf apache-maven-3.6.3-bin.tar.gz -C /usr/share/maven
ENV MAVEN_HOME=/usr/share/maven/apache-maven-3.6.3
ENV PATH=$MAVEN_HOME/bin:$PATH
RUN mvn --version

# prepare directory for docker java17 test
RUN mkdir -p /opt/bundle-validation/docker-test
RUN mkdir -p /opt/bundle-validation/tmp-conf-dir
RUN cp conf/hdfs-site.xml /opt/bundle-validation/tmp-conf-dir/hdfs-site.xml
RUN cp conf/core-site.xml /opt/bundle-validation/tmp-conf-dir/core-site.xml

# Hadoop env
ENV HDFS_NAMENODE_USER root
ENV HDFS_DATANODE_USER root
ENV HDFS_SECONDARYNAMENODE_USER root
ENV YARN_RESOURCEMANAGER_USER root
ENV YARN_NODEMANAGER_USER root

# for RocksDb
RUN apk add --no-cache libstdc++
