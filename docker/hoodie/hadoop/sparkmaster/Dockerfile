
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
ARG HIVE_VERSION=3.1.3
ARG SPARK_VERSION=3.5.3
FROM apachehudi/hudi-hadoop_${HADOOP_VERSION}-hive_${HIVE_VERSION}-sparkbase_${SPARK_VERSION}

COPY *.sh /opt/spark
RUN chmod +x /opt/spark/*.sh

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /opt/spark/logs
ENV SPARK_JUPYTER_WEBUI_PORT 8888

RUN pip install --upgrade pip && pip install jupyterlab findspark pandas

EXPOSE 8080 7077 6066 8888

WORKDIR /opt/workspace

CMD ["/opt/spark/entrypoint.sh"]
