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

# fail immediately
set -o errexit
set -o nounset

USE_AUTH=false
if [[ "${1:-}" == "--auth" ]]; then
  USE_AUTH=true
  shift
fi

if [[ $# -ne 2 ]]; then
  echo "Usage: $(basename "$0") [--auth] <repo-id> <version>"
  echo ""
  echo "  Default:  uses public URL (repo must be closed)"
  echo "  --auth:   uses private staging URL with ~/.m2/settings.xml credentials"
  echo ""
  echo "Examples:"
  echo "  $(basename "$0") orgapachehudi-1176 1.2.0-rc1"
  echo "  $(basename "$0") --auth orgapachehudi-1177 1.2.0-rc1"
  exit 1
fi

REPO=$1
VERSION=$2

if [[ "$USE_AUTH" == true ]]; then
  STAGING_REPO="https://repository.apache.org/service/local/repositories/${REPO}/content/org/apache/hudi"

  SETTINGS_XML="$HOME/.m2/settings.xml"
  if [[ ! -f "$SETTINGS_XML" ]]; then
    echo "ERROR: $SETTINGS_XML not found"
    exit 1
  fi

  if command -v xmllint &>/dev/null; then
    NEXUS_USER=$(xmllint --xpath \
      "string(//server[id='apache.releases.https']/username)" "$SETTINGS_XML")
    NEXUS_PASS=$(xmllint --xpath \
      "string(//server[id='apache.releases.https']/password)" "$SETTINGS_XML")
  else
    NEXUS_USER=$(sed -n '/<server>/,/<\/server>/{ /<id>apache.releases.https<\/id>/,/<\/server>/{ s/.*<username>\(.*\)<\/username>.*/\1/p; }; }' "$SETTINGS_XML" | head -1 | xargs)
    NEXUS_PASS=$(sed -n '/<server>/,/<\/server>/{ /<id>apache.releases.https<\/id>/,/<\/server>/{ s/.*<password>\(.*\)<\/password>.*/\1/p; }; }' "$SETTINGS_XML" | head -1 | xargs)
  fi

  if [[ -z "$NEXUS_USER" || -z "$NEXUS_PASS" ]]; then
    echo "ERROR: Could not extract credentials for 'apache.releases.https' from $SETTINGS_XML"
    exit 1
  fi

  export NEXUS_USER NEXUS_PASS
  CURL_AUTH="-u ${NEXUS_USER}:${NEXUS_PASS}"
  echo "==> Using private staging URL with authentication (user: $NEXUS_USER)"
else
  STAGING_REPO="https://repository.apache.org/content/repositories/${REPO}/org/apache/hudi"
  CURL_AUTH=""
  echo "==> Using public URL (no authentication)"
fi

export CURL_AUTH

declare -a extensions=("-javadoc.jar" "-javadoc.jar.asc" "-javadoc.jar.md5" "-javadoc.jar.sha1" "-sources.jar"
"-sources.jar.asc" "-sources.jar.md5" "-sources.jar.sha1" ".jar" ".jar.asc" ".jar.md5" ".jar.sha1" ".pom" ".pom.asc"
".pom.md5" ".pom.sha1")

declare -a bundles=("hudi-aws-bundle" "hudi-azure-bundle" "hudi-cli-bundle_2.12" "hudi-cli-bundle_2.13" "hudi-datahub-sync-bundle"
"hudi-flink1.17-bundle" "hudi-flink1.18-bundle" "hudi-flink1.19-bundle" "hudi-flink1.20-bundle"
"hudi-flink2.0-bundle" "hudi-flink2.1-bundle" "hudi-gcp-bundle" "hudi-hadoop-mr-bundle" "hudi-hive-sync-bundle" "hudi-integ-test-bundle"
"hudi-kafka-connect-bundle" "hudi-metaserver-server-bundle" "hudi-presto-bundle"
"hudi-spark3.3-bundle_2.12" "hudi-spark3.4-bundle_2.12" "hudi-spark3.5-bundle_2.12"
"hudi-spark3.5-bundle_2.13" "hudi-spark4.0-bundle_2.13" "hudi-spark4.1-bundle_2.13" "hudi-timeline-server-bundle" "hudi-trino-bundle"
"hudi-utilities-bundle_2.12" "hudi-utilities-bundle_2.13"
"hudi-utilities-slim-bundle_2.12" "hudi-utilities-slim-bundle_2.13")

MISSING_FILE=$(mktemp)
export MISSING_FILE

curl_with_url() {
    local url="$1"
    if curl -s -o /dev/null --head --fail $CURL_AUTH "$url"; then
      echo "  OK: $url"
    else
      echo "  MISSING: $url"
      echo "$url" >> "$MISSING_FILE"
    fi
}

export -f curl_with_url

ALL_URLS=""
TOTAL=0

for bundle in "${bundles[@]}"
do
   for extension in "${extensions[@]}"
   do
       url=${STAGING_REPO}/$bundle/${VERSION}/$bundle-${VERSION}$extension
       ALL_URLS+="$url\n"
       TOTAL=$((TOTAL + 1))
   done
done

echo "-- Checking $TOTAL artifacts ..."
echo ""
echo -e "$ALL_URLS" | xargs -n 1 -P 16 -I {} bash -c 'curl_with_url "{}"'

MISSING_COUNT=0
if [[ -s "$MISSING_FILE" ]]; then
  MISSING_COUNT=$(wc -l < "$MISSING_FILE" | xargs)
fi

echo ""
echo "==========================================="
echo "Total: $TOTAL | Present: $((TOTAL - MISSING_COUNT)) | Missing: $MISSING_COUNT"
echo "==========================================="

if [[ "$MISSING_COUNT" -gt 0 ]]; then
  echo ""
  echo "Missing artifacts:"
  sort "$MISSING_FILE" | while read -r url; do
    echo "  $url"
  done
  rm -f "$MISSING_FILE"
  exit 1
else
  rm -f "$MISSING_FILE"
  echo "All artifacts exist. Validation succeeds."
fi
