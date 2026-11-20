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
# Generates h264-4slices.mp4: H.264 (baseline, libx264) in MP4, 6 s test pattern at 160x120, 10 fps, a keyframe every 10 frames,
# FOUR slices per frame, no audio. TestJcodecKeyframeVideoFrameSource decodes it to check that a multi-slice stream leaves no
# decoder thread behind. Run with the ffmpeg program of org.bytedeco:ffmpeg:7.1-1.5.11, classifier linux-arm64-gpl or
# linux-x86_64-gpl (only the GPL build has libx264; a fixture tool only):
#
#   unzip ffmpeg-7.1-1.5.11-linux-arm64-gpl.jar 'org/bytedeco/ffmpeg/linux-arm64-gpl/*' -d /tmp/gpl
#   FFMPEG_DIR=/tmp/gpl/org/bytedeco/ffmpeg/linux-arm64-gpl ./generate-h264-4slices.sh
set -euo pipefail

: "${FFMPEG_DIR:?set FFMPEG_DIR to the directory holding the bytedeco GPL ffmpeg program}"
export LD_LIBRARY_PATH="${FFMPEG_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
"${FFMPEG_DIR}/ffmpeg" -hide_banner -loglevel error -y -f lavfi -i "testsrc=size=160x120:rate=10:duration=6" \
  -c:v libx264 -pix_fmt yuv420p -profile:v baseline -x264-params "slices=4:keyint=10:min-keyint=10:scenecut=0:threads=1" \
  -an -map_metadata -1 -fflags +bitexact -flags:v +bitexact "$(cd "$(dirname "$0")" && pwd)/h264-4slices.mp4"
