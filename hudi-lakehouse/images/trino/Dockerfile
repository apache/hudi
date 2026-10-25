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

# Trino server with the Hudi connector built from this repository.
#
# Build context: this directory. scripts/build-images.sh stages the exploded
# plugin (hudi-trino-plugin/target/trino-hudi-472/) into ./target/ first; the
# staging directory is gitignored so plugin jars never enter version control.
#
# The Trino server version is pinned to 472 because the plugin performs a
# strict SPI version match at load time (see HudiConnectorFactory).
FROM trinodb/trino:472

USER root
# Replace the hudi plugin bundled with the upstream image with the one built
# from this repository.
RUN rm -rf /usr/lib/trino/plugin/hudi
COPY target/trino-hudi-472/ /usr/lib/trino/plugin/hudi/
RUN chown -R trino:trino /usr/lib/trino/plugin/hudi
USER trino
