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
# Overlay-aware Trino entrypoint. If a plugin overlay is bind-mounted at
# /opt/hudi-plugin-overlay (set TRINO_PLUGIN_DIR to the Trino repo's
# plugin/trino-hudi/target/trino-hudi-<ver> build output), fully replace the
# image's baked-in trino-hudi plugin with it (rm -rf then copy), so plugin
# iterations need only a rebuild of that dir plus a container restart, not a
# docker image rebuild. Otherwise the image-baked plugin is used as-is.
set -euo pipefail

OVERLAY=/opt/hudi-plugin-overlay
PLUGIN_DIR=/usr/lib/trino/plugin/hudi

# The overlay counts as present only if it holds at least one jar: the compose
# default mount is docker/trino/empty-overlay, whose .gitkeep must not trigger
# a wipe of the baked-in plugin.
if [ -d "$OVERLAY" ] && [ -n "$(find "$OVERLAY" -name '*.jar' -print -quit 2>/dev/null)" ]; then
  echo "Applying trino-hudi plugin overlay from $OVERLAY (fully replacing $PLUGIN_DIR)"
  rm -rf "$PLUGIN_DIR"
  mkdir -p "$PLUGIN_DIR"
  cp -r "$OVERLAY"/. "$PLUGIN_DIR"/
else
  echo "No plugin overlay found at $OVERLAY; using the image-baked trino-hudi plugin as-is."
fi

exec /usr/lib/trino/bin/run-trino
