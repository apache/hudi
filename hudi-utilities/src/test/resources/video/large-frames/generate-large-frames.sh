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
# Generates large-frames-720p.mp4: a 3 s 1280x720 H.264 clip whose kept frames each encode (JPEG q90, as the FFmpeg frame
# source writes them) to well above 80 KiB, while the clip itself stays small. Run it with the ffmpeg program of
# org.bytedeco:ffmpeg:7.1-1.5.11, classifier linux-arm64-gpl or linux-x86_64-gpl (the GPL build has libx264):
#
#   unzip ffmpeg-7.1-1.5.11-linux-arm64-gpl.jar 'org/bytedeco/ffmpeg/linux-arm64-gpl/*' -d /tmp/gpl
#   chmod +x /tmp/gpl/org/bytedeco/ffmpeg/linux-arm64-gpl/ffmpeg
#   FFMPEG_DIR=/tmp/gpl/org/bytedeco/ffmpeg/linux-arm64-gpl ./generate-large-frames.sh
#
# The picture is procedural (no noise): a zone plate centred on the frame, whose ring frequency steps up every second, over a
# low-frequency cosine grating whose direction turns every second, and two slow chroma gratings. Within a second the picture is
# still (the P-frames cost almost nothing); between seconds it changes, so each second's keyframe survives the dHash dedup.
# The rings are high-frequency detail a q90 JPEG keeps at great cost while H.264 at CRF 28 codes them compactly.
# 25 fps with a keyframe every 25 frames (scene-cut keyframes off): keyframes at 0, 1 and 2 s. A rerun with the same binary
# writes the same bytes (single-threaded x264, bitexact flags, no metadata).
set -euo pipefail

: "${FFMPEG_DIR:?set FFMPEG_DIR to the directory holding the bytedeco GPL ffmpeg program}"
export LD_LIBRARY_PATH="${FFMPEG_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
FFMPEG="${FFMPEG_DIR}/ffmpeg"
OUT="$(cd "$(dirname "$0")" && pwd)/large-frames-720p.mp4"

S="floor(T)"
ZONE="sin(((X-640)*(X-640)+(Y-360)*(Y-360))*(0.0002+0.00005*${S}))"
GRATING="cos(2*PI*((X/640)*cos(1.1*${S})+(Y/360)*sin(1.1*${S})))"
LUM="128+80*${ZONE}+45*${GRATING}"
CB="128+60*sin(X*0.05+Y*0.031*(1+${S}))"
CR="128+60*cos(Y*0.043-X*0.02*(1+${S}))"

"$FFMPEG" -hide_banner -loglevel error -y \
  -f lavfi -i "nullsrc=size=1280x720:rate=25:duration=3,geq=lum='${LUM}':cb='${CB}':cr='${CR}'" \
  -c:v libx264 -preset medium -crf 28 -pix_fmt yuv420p -g 25 -keyint_min 25 -sc_threshold 0 -threads 1 -an \
  -map_metadata -1 -fflags +bitexact -flags:v +bitexact -f mp4 "${OUT}"
ls -l "${OUT}"
