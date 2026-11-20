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
# Generates the codec and container matrix clips of TestVideoCodecMatrix and their expectations
# (codec-matrix-expected.txt). Run it with the ffmpeg and ffprobe programs of org.bytedeco:ffmpeg:7.1-1.5.11, classifier
# linux-arm64-gpl or linux-x86_64-gpl: only the GPL build has an HEVC (libx265) and an H.264 (libx264) software encoder. The
# encoders are a fixture tool only; the code under test decodes with the LGPL natives the build ships.
#
#   unzip ffmpeg-7.1-1.5.11-linux-arm64-gpl.jar 'org/bytedeco/ffmpeg/linux-arm64-gpl/*' -d /tmp/gpl
#   FFMPEG_DIR=/tmp/gpl/org/bytedeco/ffmpeg/linux-arm64-gpl ./generate-codec-matrix.sh
#
# Every clip: a 6 s test pattern (small: low bitrates, it only has to decode) at 160x120, 10 fps, encoded with a keyframe every 10
# frames (scene-cut keyframes off; the AVI demuxer reports them 100 ms later from the first GOP on), and (unless
# its audio is "none") a 48 kHz audio track of a 440 Hz tone in the first half of every 2 s ([0,1), [2,3), [4,5) s) and digital
# silence otherwise. Two more clips carry the frame timeline cases: an edit list trimmed inside a GOP and a video track delayed
# by an empty edit.
#
# Expectations come from ffprobe, independent of the code under test: per clip the container start_time and the video
# packets the demuxer reports (pts and flags). The first kept frame's time: the first keyframe's PTS that is not
# flagged DISCARD, minus the container start_time, clamped at 0, in milliseconds.
#
# A rerun with the same binary writes the same bytes. TestVideoCodecMatrix's SEEK_FAILURES
# pin how the seeks of c16 and c17 land (frames kept, landing time): regenerate them only with those values re-measured.
set -euo pipefail

: "${FFMPEG_DIR:?set FFMPEG_DIR to the directory holding the bytedeco GPL ffmpeg and ffprobe programs}"
export LD_LIBRARY_PATH="${FFMPEG_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
FFMPEG="${FFMPEG_DIR}/ffmpeg"
FFPROBE="${FFMPEG_DIR}/ffprobe"
FINAL="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf -- "${WORK:?}"' EXIT
# everything is made in WORK and copied next to this script only when the whole run succeeded
OUT="${WORK}/clips"
mkdir -p "$OUT"

SECONDS_LONG=6
VIDEO_IN=(-f lavfi -i "testsrc=size=160x120:rate=10:duration=${SECONDS_LONG}")
TONE="0.25*sin(2*PI*440*t)*lt(mod(t\,2)\,1)"
AUDIO_IN=(-f lavfi -i "aevalsrc=exprs=${TONE}:sample_rate=48000:duration=${SECONDS_LONG}")
COMMON=(-hide_banner -loglevel error -y)
# output options: no encoder or creation-time tags, so a rerun writes the same bytes
OUTPUT=(-map_metadata -1 -fflags +bitexact -flags:v +bitexact -flags:a +bitexact)

video_args() {
  case "$1" in
    h264) echo "-c:v libx264 -preset veryfast -crf 36 -pix_fmt yuv420p -g 10 -keyint_min 10 -sc_threshold 0" ;;
    hevc) echo "-c:v libx265 -preset veryfast -crf 36 -pix_fmt yuv420p -x265-params keyint=10:min-keyint=10:scenecut=0:log-level=error" ;;
    mpeg4) echo "-c:v mpeg4 -q:v 20 -g 10 -sc_threshold 1000000000" ;;
    vp9) echo "-c:v libvpx-vp9 -deadline realtime -cpu-used 8 -b:v 40k -g 10 -keyint_min 10" ;;
    av1) echo "-c:v libaom-av1 -usage realtime -cpu-used 8 -b:v 40k -g 10 -keyint_min 10" ;;
    *) echo "unknown video codec $1" >&2; exit 2 ;;
  esac
}

audio_args() {
  case "$1" in
    aac) echo "-c:a aac -b:a 24k -ac 1" ;;
    mp3) echo "-c:a libmp3lame -b:a 24k -ac 1" ;;
    opus) echo "-c:a libopus -b:a 16k -ac 1" ;;
    # FFmpeg's own Vorbis encoder is experimental and stereo only
    vorbis) echo "-c:a vorbis -strict -2 -ac 2" ;;
    none) echo "-an" ;;
    *) echo "unknown audio codec $1" >&2; exit 2 ;;
  esac
}

tag_args() {
  # HEVC in an ISO BMFF container carried as hvc1, the tag Apple players require
  if [ "$1" = hevc ] && { [ "$2" = mp4 ] || [ "$2" = mov ]; }; then echo "-tag:v hvc1"; fi
}

# expect <file> <cell id> <container> <video codec> <audio codec>: one line of the expectations file
expect() {
  local file="$1"
  local start
  start="$("$FFPROBE" -v error -show_entries format=start_time -of default=noprint_wrappers=1:nokey=1 "$file")"
  [ "$start" = "N/A" ] && start=0
  # video packets: pts_time,flags (K = keyframe, D = discard); the presented keyframes are K without D
  local keyframes
  keyframes="$("$FFPROBE" -v error -select_streams v:0 -show_entries packet=pts_time,flags -of csv=p=0 "$file" \
    | awk -F, -v s="$start" '$1 != "N/A" && $2 ~ /K/ && $2 !~ /D/ { t = ($1 - s) * 1000; if (t < 0) t = 0; printf "%d\n", t + 0.5 }' \
    | sort -n | uniq | paste -sd' ' -)"
  local audio
  audio="$("$FFPROBE" -v error -select_streams a:0 -show_entries stream=codec_name -of csv=p=0 "$file" | head -1)"
  local video
  video="$("$FFPROBE" -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 "$file" | head -1)"
  local start_ms
  start_ms="$(awk -v s="$start" 'BEGIN { printf "%d", s * 1000 + (s < 0 ? -0.5 : 0.5) }')"
  echo "$(basename "$file")|$2|$3|$4:${video}|$5:${audio:-none}|${start_ms}|${keyframes}"
}

# cell <id> <container extension> <ffmpeg muxer> <video codec> <audio codec>
cell() {
  local id="$1" ext="$2" muxer="$3" vcodec="$4" acodec="$5"
  # the argument helpers run in command substitutions, where their exit would not stop the script: check the names here
  case "$vcodec" in h264|hevc|mpeg4|vp9|av1) ;; *) echo "unknown video codec $vcodec" >&2; exit 2 ;; esac
  case "$acodec" in aac|mp3|opus|vorbis|none) ;; *) echo "unknown audio codec $acodec" >&2; exit 2 ;; esac
  local name="${id}-${vcodec}-${acodec}.${ext}"
  local inputs=("${VIDEO_IN[@]}")
  local maps=(-map 0:v)
  if [ "$acodec" != none ]; then
    inputs+=("${AUDIO_IN[@]}")
    maps+=(-map 1:a)
  fi
  # shellcheck disable=SC2046
  "$FFMPEG" "${COMMON[@]}" "${inputs[@]}" "${maps[@]}" $(video_args "$vcodec") $(tag_args "$vcodec" "$ext") \
    $(audio_args "$acodec") "${OUTPUT[@]}" -f "$muxer" "${OUT}/${name}"
  expect "${OUT}/${name}" "$id" "$ext" "$vcodec" "$acodec" >> "${WORK}/expected"
}

: > "${WORK}/expected"
# the baseline: MP4 with H.264 and with MPEG-4 Part 2
cell c01 mp4 mp4 h264 aac
cell c02 mp4 mp4 mpeg4 mp3
# the codecs in MP4
cell c03 mp4 mp4 hevc aac
cell c04 mp4 mp4 vp9 opus
cell c05 mp4 mp4 av1 aac
# Matroska
cell c06 mkv matroska hevc opus
cell c07 mkv matroska av1 vorbis
cell c08 mkv matroska h264 mp3
cell c09 mkv matroska vp9 none
# WebM (VP9 or AV1 with Opus or Vorbis only)
cell c10 webm webm vp9 opus
cell c11 webm webm vp9 vorbis
cell c12 webm webm av1 opus
# QuickTime
cell c13 mov mov h264 aac
cell c14 mov mov hevc aac
cell c15 mov mov mpeg4 none
# MPEG-TS (its muxer delays the start, 1.577-1.579 s for these clips)
cell c16 ts mpegts h264 aac
cell c17 ts mpegts hevc mp3
# AVI, the fifth video extension the blob table ingests by default
cell c18 avi avi mpeg4 mp3

# An edit list trimmed inside a GOP. The source has a keyframe every 2 s (B-frames on); a stream copy from 2.5 s keeps the
# GOP of 2 s and starts the edit list 0.5 s into it, so the demuxer flags the keyframe of 2 s DISCARD and the first presented
# keyframe is the source's 4 s, at 1.5 s of the trimmed file.
"$FFMPEG" "${COMMON[@]}" -f lavfi -i "testsrc=size=160x120:rate=10:duration=10" \
  -f lavfi -i "aevalsrc=exprs=${TONE}:sample_rate=48000:duration=10" -map 0:v -map 1:a \
  -c:v libx264 -preset veryfast -crf 36 -pix_fmt yuv420p -g 20 -keyint_min 20 -sc_threshold 0 -c:a aac -b:a 24k -ac 1 \
  "${OUTPUT[@]}" -f mp4 "${WORK}/gop2s.mp4"
"$FFMPEG" "${COMMON[@]}" -ss 2.5 -i "${WORK}/gop2s.mp4" -map 0 -c copy "${OUTPUT[@]}" -f mp4 "${OUT}/t01-h264-aac-trimmed-in-gop.mp4"
expect "${OUT}/t01-h264-aac-trimmed-in-gop.mp4" t01 mp4 h264 aac >> "${WORK}/expected"
# A video track delayed 1.2 s by an empty edit while its audio starts at 0 (the container starts at 0)
"$FFMPEG" "${COMMON[@]}" -i "${OUT}/c01-h264-aac.mp4" -itsoffset 1.2 -i "${OUT}/c01-h264-aac.mp4" -map 1:v -map 0:a -c copy \
  "${OUTPUT[@]}" -f mp4 "${OUT}/t02-h264-aac-video-delayed.mp4"
expect "${OUT}/t02-h264-aac-video-delayed.mp4" t02 mp4 h264 aac >> "${WORK}/expected"

cp "${OUT}"/* "${FINAL}/"
OUT="$FINAL"
{
  echo "# Generated by generate-codec-matrix.sh with $("$FFMPEG" -hide_banner -version | head -1 | cut -d' ' -f1-3) (bytedeco 7.1-1.5.11 GPL)."
  echo "# file|cell|container|video codec:probed|audio codec:probed|container start_time ms|presented keyframes ms"
  cat "${WORK}/expected"
} > "${OUT}/codec-matrix-expected.txt"
cat "${OUT}/codec-matrix-expected.txt"
