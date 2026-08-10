#!/bin/bash

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

# Run this from the root of a source release tree, i.e. the output of
# create_source_directory.sh (see the validate-source job in bot.yml) or an
# extracted source tarball (see validate_staged_release.sh). Paths that never
# reach the source release are excluded by create_source_directory.sh, not here.

# fail immediately; pipefail so that a broken `file` invocation can never again
# yield an empty pipeline that reads as "no binary files found"
set -o errexit
set -o nounset
set -o pipefail

echo "Checking for binary files in the source files"

# `--mime` is the long option for both GNU file's `-i` and BSD file's `-I`, so it
# is the only spelling that works on Linux CI and on a release manager's macOS.
# Its charset field answers the question directly: libmagic reports
# charset=binary for anything that is not text. Matching on the charset rather
# than on a list of permitted MIME types keeps this robust against libmagic's
# growing type vocabulary, which already labels the JSON test data
# application/x-ndjson and misdetects at least one .java source as
# application/javascript.
mimeTypes=$(find . -type f -print0 | xargs -0 file --mime)

# An empty file reports as inode/x-empty; charset=binary, and is neither binary nor
# a licensing concern. The top-level release/ holds the release guide's screenshot;
# src/test/ holds the binary fixtures (parquet, avro, hfile) that tests read. Every
# stage carries -a so a non-UTF-8 filename can never flip a grep into binary mode and
# drop a line. release/ is anchored to the top-level dir; src/test/ stays a substring
# because those fixtures live under every module's src/test/.
binaryFiles=$(echo "$mimeTypes" | grep -a 'charset=binary' | grep -av 'inode/x-empty' \
  | grep -av '^\./release/' | grep -av '/src/test/' || true)

if [ -n "$binaryFiles" ]; then
  echo -e "There were non-text files in source release. [ERROR]\n Please check below\n"
  echo "$binaryFiles"
  exit 1
fi
echo -e "\t\tNo Binary Files in the source files? - [OK]\n"
