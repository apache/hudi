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
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Regenerates the clips derived from h264-aac-bframes-12s.mp4 (see README.md in this directory for every clip's source).
# Nothing is re-encoded, so a rerun writes the committed bytes (measured with the ffmpeg program of
# org.bytedeco:ffmpeg:7.1-1.5.11, classifier linux-arm64, the LGPL build Hudi tests with). Run it with that program:
#
#   unzip ffmpeg-7.1-1.5.11-linux-arm64.jar 'org/bytedeco/ffmpeg/linux-arm64/*' -d /tmp/lgpl
#   chmod +x /tmp/lgpl/org/bytedeco/ffmpeg/linux-arm64/ffmpeg
#   FFMPEG_DIR=/tmp/lgpl/org/bytedeco/ffmpeg/linux-arm64 ./generate-av-fixtures.sh
set -euo pipefail

: "${FFMPEG_DIR:?set FFMPEG_DIR to the directory holding the bytedeco ffmpeg program}"
export LD_LIBRARY_PATH="${FFMPEG_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
FFMPEG="${FFMPEG_DIR}/ffmpeg"
DIR="$(cd "$(dirname "$0")" && pwd)"
BASE="${DIR}/h264-aac-bframes-12s.mp4"
WORK="$(mktemp -d)"
trap 'rm -rf -- "${WORK:?}"' EXIT
# everything is made in WORK and copied next to this script only when the whole run succeeded
ff() { "$FFMPEG" -hide_banner -loglevel error -y "$@"; }

# The base clip cut at 3 s (both tracks; the video keeps its GOP through an edit list), stream copy.
ff -ss 3 -i "$BASE" -map 0 -c copy "${WORK}/h264-aac-bframes-trimmed-ss3.mp4"
# The base clip with its video track delayed 1.5 s by an empty edit, stream copy.
ff -i "$BASE" -itsoffset 1.5 -i "$BASE" -map 1:v -map 0:a -c copy "${WORK}/h264-aac-bframes-video-delayed-1500ms.mp4"


cp -- "${WORK}"/*.mp4 "$DIR"/
