#!/usr/bin/env bash
#
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
#
# Overlay the locally-built trino-hudi plugin from the host bind-mount onto
# the image's /usr/lib/trino/plugin/hudi/. `cp -r SRC/. DST/` merges at file
# granularity: same-name files in the overlay overwrite the image's bundled
# versions; everything else in plugin/hudi/ (notably the bundled hdfs/
# sub-plugin which Trino 472 needs for HDFS reads) survives untouched.
#
# Mirrors docker/hoodie/hadoop/trinobase/scripts/trino.sh from the legacy
# Trino 368 setup: developers iterate on hudi-trino-plugin code via
# `mvn -f hudi-trino-plugin/pom.xml package` and just restart the container,
# never rebuilding the docker image.
set -euo pipefail

OVERLAY=/opt/hudi-plugin-overlay
if [ -d "$OVERLAY" ] && [ -n "$(ls -A "$OVERLAY" 2>/dev/null)" ]; then
  echo "Overlaying locally-built trino-hudi plugin from $OVERLAY onto /usr/lib/trino/plugin/hudi/"
  cp -r "$OVERLAY"/. /usr/lib/trino/plugin/hudi/
else
  echo "WARN: $OVERLAY is missing or empty; using the image-bundled plugin/hudi/ as-is." >&2
fi

exec /usr/lib/trino/bin/run-trino