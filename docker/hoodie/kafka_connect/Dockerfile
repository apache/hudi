
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

ARG CP_KAFKA_VERSION=5.5.8
# work with Apache Kafka 2.5.x
FROM confluentinc/cp-kafka-connect-base:${CP_KAFKA_VERSION}

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs:10.1.0
ADD target/hudi-kafka-connect-bundle.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-hdfs/lib
ENV CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components

ADD bin/launch.sh /home/

CMD /home/launch.sh
