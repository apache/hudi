#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "Usage: $0 <BASE_URL> <MODULE_NAME> <TARGET_ROOT_DIR>"
  echo "  BASE_URL      = e.g. https://repository.apache.org/service/local/repositories/orgapachehudi-1168/content/org/apache/hudi/"
  echo "  MODULE_NAME   = e.g. hudi-flink2.0-bundle"
  echo "  TARGET_ROOT   = e.g. ./target_folder"
  echo "  RELEASE_VERSION   = e.g. 1.1.1-rc1"
  exit 1
fi

BASE_URL="${1%/}"        # strip trailing /
MODULE_NAME="$2"
TARGET_ROOT="${3%/}"     # strip trailing /
VERSION="$4"      # <-- change this if needed, or turn into another arg later

# --------------------------------------------------------------------
# Directory layout
# --------------------------------------------------------------------
MODULE_DIR="${TARGET_ROOT}/${MODULE_NAME}"
VERSION_DIR="${MODULE_DIR}/${VERSION}"

mkdir -p "${MODULE_DIR}"
mkdir -p "${VERSION_DIR}"

# --------------------------------------------------------------------
# 1) Download maven-metadata.* under:
#    <TARGET_ROOT>/<MODULE_NAME>/
# --------------------------------------------------------------------
META_BASE_URL="${BASE_URL}/${MODULE_NAME}"

metadata_files=(
  "maven-metadata.xml"
  "maven-metadata.xml.md5"
  "maven-metadata.xml.sha1"
)

for f in "${metadata_files[@]}"; do
  url="${META_BASE_URL}/${f}"
  echo "Downloading metadata: ${url}"
  wget -q -P "${MODULE_DIR}" "${url}"
done

# --------------------------------------------------------------------
# 2) Download all artifact variants for the given version under:
#    <TARGET_ROOT>/<MODULE_NAME>/<VERSION>/
# --------------------------------------------------------------------
VERSION_BASE_URL="${META_BASE_URL}/${VERSION}"
PREFIX="${MODULE_NAME}-${VERSION}"

suffixes=(
  "-javadoc.jar"
  "-javadoc.jar.asc"
  "-javadoc.jar.md5"
  "-javadoc.jar.sha1"
  "-sources.jar"
  "-sources.jar.asc"
  "-sources.jar.md5"
  "-sources.jar.sha1"
  ".jar"
  ".jar.asc"
  ".jar.md5"
  ".jar.sha1"
  ".pom"
  ".pom.asc"
  ".pom.md5"
  ".pom.sha1"
)

for suf in "${suffixes[@]}"; do
  url="${VERSION_BASE_URL}/${PREFIX}${suf}"
  echo "Downloading artifact: ${url}"
  wget -q -P "${VERSION_DIR}" "${url}"
done

echo "Done. Files downloaded under: ${MODULE_DIR}"

