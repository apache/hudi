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
# Valid clips the truncation checks of FfmpegKeyframeVideoFrameSource must accept whole,
# remuxed (-c copy) from the codec matrix clips with the ffmpeg program of org.bytedeco:ffmpeg:7.1-1.5.11 (classifier
# linux-arm64-gpl; see ../codec-matrix/generate-codec-matrix.sh for how to unpack it):
#   c01-offset-1500ms.mp4   both streams start 1.5 s late (an empty edit on every track)
#   c08-offset-1500ms.mkv   both streams start 1.5 s late; FFmpeg's Matroska Duration is then the END time (start 1.5 s, duration 7.547 s)
#   c01-faststart.mp4 / c13-faststart.mov   the index (moov) at the front: a cut file still opens
#   c18-unpatched-riff.avi  written to a pipe: the RIFF size is left at its 0xFFFFFFFF placeholder and there is no idx1
# usage: FFMPEG_DIR=<dir with ffmpeg> ./generate-truncation-fixtures.sh   (run in this directory)
set -euo pipefail
F="$FFMPEG_DIR/ffmpeg"
M=../codec-matrix
export LD_LIBRARY_PATH="$FFMPEG_DIR"
"$F" -v error -y -itsoffset 1.5 -i "$M/c01-h264-aac.mp4" -c copy c01-offset-1500ms.mp4
"$F" -v error -y -i "$M/c08-h264-mp3.mkv" -c copy -output_ts_offset 1.5 c08-offset-1500ms.mkv
"$F" -v error -y -i "$M/c01-h264-aac.mp4" -c copy -movflags +faststart c01-faststart.mp4
"$F" -v error -y -i "$M/c13-h264-aac.mov" -c copy -movflags +faststart c13-faststart.mov
"$F" -v error -y -i "$M/c18-mpeg4-mp3.avi" -c copy -f avi pipe:1 > c18-unpatched-riff.avi
