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
set -euo pipefail

JACOCO_VERSION=0.8.12
JACOCO_ZIP="jacoco-${JACOCO_VERSION}.zip"
JACOCO_URL="https://repo1.maven.org/maven2/org/jacoco/jacoco/${JACOCO_VERSION}/${JACOCO_ZIP}"

# Maven Central answers 429 (Too Many Requests) when the shared CI egress IP is rate limited, and
# wget treats every 4xx as fatal. Without a retry a single 429 fails a job whose build and tests
# have already passed, so back off long enough to outlast the rate-limit window before giving up.
for backoff in 15 30 60 0; do
  if wget --tries=1 -O "$JACOCO_ZIP" "$JACOCO_URL"; then
    break
  fi
  if [ "$backoff" -eq 0 ]; then
    echo "Unable to download $JACOCO_URL" >&2
    exit 1
  fi
  echo "Download failed, retrying in ${backoff}s"
  sleep "$backoff"
done

unzip "$JACOCO_ZIP" -d jacoco-lib
ls -l jacoco-lib/lib/jacococli.jar
