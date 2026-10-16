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

# Keep this release matrix in sync with the chart and README.
# Separate from images/spark: the existing Spark 3.5 writer is unchanged.
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu@sha256:9b0a6c2c860f5e7d18dd5270286fef32a09c7f5a7e2b0dbe5642de8a3a02ab3e

USER root
# Pin release artifacts and verify Maven Central checksums. Resolve all jars
# at build time: endpoint startup must not need Maven/network access.
RUN set -eux; \
    cd /opt/spark/jars; \
    for artifact in \
      org/apache/hudi/hudi-spark4.1-bundle_2.13/1.2.0/hudi-spark4.1-bundle_2.13-1.2.0.jar \
      org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
      software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
      org/apache/parquet/parquet-variant/1.16.0/parquet-variant-1.16.0.jar; do \
        file="$(basename "$artifact")"; \
        wget -q --timeout=30 --tries=3 -O "$file" "https://repo.maven.apache.org/maven2/$artifact"; \
        checksum="$(wget -q --timeout=30 --tries=3 -O - "https://repo.maven.apache.org/maven2/$artifact.sha1")"; \
        printf '%s  %s\n' "$checksum" "$file" | sha1sum -c -; \
    done; \
    test -f spark-connect_2.13-4.1.3.jar; \
    test -f hadoop-client-runtime-3.4.2.jar

# Pure Python Connect client for the quickstart query; no second Spark/JVM runtime.
RUN python3 -m pip install --no-cache-dir pyspark-client==4.1.3 pyarrow==18.1.0

COPY start-connect.sh /opt/spark/start-connect.sh
RUN chmod 755 /opt/spark/start-connect.sh && mkdir -p /opt/spark/conf && chown spark:spark /opt/spark/conf
USER spark
EXPOSE 15002
ENTRYPOINT ["/opt/spark/start-connect.sh"]
