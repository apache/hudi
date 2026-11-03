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

# Independent Spark 3.5/Hudi 1.2 writer for cross-version freshness checks.
FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu@sha256:7897b9a69efa94caccc52579e310a2fe68fb75b88a3d55b97b31568e5e233176
USER root
RUN set -eux; cd /opt/spark/jars; for spec in \
 'cc79fba00822f3a6c3a860df99375c2e3d4a034d037dbf4213a4966a706a7676 org/apache/hudi/hudi-spark3.5-bundle_2.12/1.2.0/hudi-spark3.5-bundle_2.12-1.2.0.jar' \
 '53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar' \
 'e9cb23619b5b3a222e6f7ba5a79cb18ce4f606e6cabd976cd728582ec47080fa com/amazonaws/aws-java-sdk-bundle/1.12.772/aws-java-sdk-bundle-1.12.772.jar'; do \
 set -- $spec; checksum="$1"; artifact="$2"; file="$(basename "$artifact")"; \
 wget -q --timeout=30 --tries=3 -O "$file" "https://repo.maven.apache.org/maven2/$artifact"; \
 printf '%s  %s\n' "$checksum" "$file" | sha256sum -c -; done
USER spark
