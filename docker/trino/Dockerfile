# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ARG TRINO_VERSION=481
FROM trinodb/trino:${TRINO_VERSION}

USER root

# Replace the bundled hudi plugin with the locally-built trino-hudi plugin
# staged into the build context at plugin/ (see build_image.sh).
#
# The staged plugin dir carries only the connector's top-level runtime jars.
# fs.hadoop.enabled=true additionally needs the isolated HDFS loader jar set at
# <plugin>/hdfs (io.trino.filesystem.manager.HdfsFileSystemLoader). That jar set
# is only distributed inside the trino-server tarball / base image
# (io.trino:trino-hdfs:zip is not on Maven Central), so preserve the stock hudi
# plugin's version-matched copy before replacing it, and re-attach it when the
# staged plugin dir lacks one. /opt/hudi-hdfs-lib stays in the image so the
# overlay entrypoint can do the same for bind-mounted plugin overlays.
RUN cp -r /usr/lib/trino/plugin/hudi/hdfs /opt/hudi-hdfs-lib \
    && rm -rf /usr/lib/trino/plugin/hudi
COPY --chown=trino:trino plugin/ /usr/lib/trino/plugin/hudi/
RUN if [ ! -d /usr/lib/trino/plugin/hudi/hdfs ]; then \
        cp -r /opt/hudi-hdfs-lib /usr/lib/trino/plugin/hudi/hdfs; \
    fi \
    && chown -R trino:trino /usr/lib/trino/plugin/hudi /opt/hudi-hdfs-lib

# Bake the Hudi E2E Trino config (coordinator, catalog, hadoop-conf) into the image.
COPY --chown=trino:trino etc/ /etc/trino/

# Overlay-aware entrypoint: a bind-mounted plugin overlay (if present) replaces
# the baked-in plugin at container start, otherwise the baked-in plugin is used.
COPY --chown=trino:trino overlay-entrypoint.sh /opt/overlay-entrypoint.sh
RUN chmod +x /opt/overlay-entrypoint.sh

USER trino
ENTRYPOINT ["/opt/overlay-entrypoint.sh"]
