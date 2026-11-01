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
# /opt/hudi-plugin-overlay (set TRINO_PLUGIN_DIR to the in-repo shim's
# docker/trino/shim/target/trino-hudi-<ver> build output, or to a trinodb/trino
# checkout's plugin/trino-hudi/target/trino-hudi-<ver>), fully replace the
# image's baked-in trino-hudi plugin with it (rm -rf then copy), so plugin
# iterations need only a rebuild of that dir plus a container restart, not a
# docker image rebuild. Otherwise the image-baked plugin is used as-is.
#
# This runs on released images (<= 483, which still shipped bin/run-trino) and on
# images built from the pinned trino.sha (after trinodb/trino f0d1f3c982e, which
# removed run-trino in favor of launcher in the CMD, on a hardened base image
# without grep or find). Keep it bash builtins only, apart from cp/rm/mkdir.
set -euo pipefail

OVERLAY=/opt/hudi-plugin-overlay
PLUGIN_DIR=/usr/lib/trino/plugin/hudi

# The overlay counts as present only if it holds at least one jar: the compose
# default mount is docker/trino/empty-overlay, whose .gitkeep must not trigger
# a wipe of the baked-in plugin.
# dotglob mirrors find, which also descends into hidden dirs.
OVERLAY_JARS=()
if [ -d "$OVERLAY" ]; then
  shopt -s globstar nullglob dotglob
  OVERLAY_JARS=("$OVERLAY"/**/*.jar)
  shopt -u globstar nullglob dotglob
fi
if [ "${#OVERLAY_JARS[@]}" -gt 0 ]; then
  echo "Applying trino-hudi plugin overlay from $OVERLAY (fully replacing $PLUGIN_DIR)"
  rm -rf "$PLUGIN_DIR"
  mkdir -p "$PLUGIN_DIR"
  cp -r "$OVERLAY"/. "$PLUGIN_DIR"/
else
  echo "No plugin overlay found at $OVERLAY; using the image-baked trino-hudi plugin as-is."
fi

# Overlays built from the in-repo shim (docker/trino/shim/target/trino-hudi-<ver>)
# lack the hdfs/ loader dir that fs.hadoop.enabled=true needs; restore the copy
# the image preserved from the stock plugin (see Dockerfile).
if [ ! -d "$PLUGIN_DIR/hdfs" ] && [ -d /opt/hudi-hdfs-lib ]; then
  echo "Restoring hdfs/ loader dir into $PLUGIN_DIR from /opt/hudi-hdfs-lib"
  cp -r /opt/hudi-hdfs-lib "$PLUGIN_DIR/hdfs"
fi

# Inlined from the removed bin/run-trino wrapper. The node.id check mirrors its
# `grep -s -q 'node.id' /etc/trino/node.properties`: a missing or unreadable file
# means not set.
launcher_opts=(--etc-dir /etc/trino)
NODE_ID_SET=false
if [ -r /etc/trino/node.properties ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" == *node.id* ]]; then
      NODE_ID_SET=true
      break
    fi
  done < /etc/trino/node.properties
fi
if [ "$NODE_ID_SET" != "true" ]; then
  launcher_opts+=("-Dnode.id=${HOSTNAME}")
fi
exec /usr/lib/trino/bin/launcher run "${launcher_opts[@]}" "$@"
