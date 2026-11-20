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

# Fails if any jar given as an argument carries FFmpeg or its Java bindings: an org/bytedeco entry (JavaCV, JavaCPP, the FFmpeg
# preset and their native classifier jars all live there), Maven metadata of an org.bytedeco artifact, or an FFmpeg / JavaCPP
# native library anywhere in the jar (libavcodec, libavformat, libavutil, libavfilter, libavdevice, libswscale, libswresample,
# libpostproc, libjnijavacpp, libjniav*, libjniswscale, libjniswresample, libjnipostproc). FFmpeg is LGPL and Hudi does not ship
# it: a user who processes video adds the FFmpeg jars at launch (for example through the Spark setting spark.jars).
# Other native libraries a bundle may carry are not judged, so the check names FFmpeg's libraries rather than every .so.
# Prints each offending entry and exits 1; exits 0 when every jar is clean.
set -euo pipefail

if [ $# -eq 0 ]; then
  echo "usage: $0 <jar>..." >&2
  exit 2
fi

PATTERN='(^|/)org/bytedeco/|^META-INF/maven/org\.bytedeco/|(^|/)lib(avcodec|avformat|avutil|avfilter|avdevice|swscale|swresample|postproc|jnijavacpp|jniav[a-z]*|jniswscale|jniswresample|jnipostproc)[^/]*\.(so|dylib|dll)[^/]*$|(^|/)(avcodec|avformat|avutil|avfilter|avdevice|swscale|swresample|postproc|jnijavacpp|jniav[a-z]*|jniswscale|jniswresample|jnipostproc)[^/]*\.dll$'
failed=0
for jar in "$@"; do
  if [ ! -f "$jar" ]; then
    echo "MISSING $jar" >&2
    failed=1
    continue
  fi
  if ! listing=$(jar tf "$jar"); then
    echo "UNREADABLE $jar (jar tf failed)" >&2
    failed=1
    continue
  fi
  hits=$(printf '%s\n' "$listing" | grep -E "$PATTERN" || true)
  if [ -n "$hits" ]; then
    echo "FAIL $jar carries FFmpeg / org.bytedeco entries ($(printf '%s\n' "$hits" | wc -l)):"
    printf '%s\n' "$hits" | head -20 | sed 's/^/  /'
    failed=1
  else
    echo "OK   $jar ($(printf '%s\n' "$listing" | wc -l) entries, no org/bytedeco, no FFmpeg natives)"
  fi
done
exit "$failed"
