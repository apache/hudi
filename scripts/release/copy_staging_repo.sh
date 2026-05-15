#!/usr/bin/env bash
#
# Copies all artifacts from one Nexus staging repository to another.
#
# Usage:
#   ./copy_staging_repo.sh [--dry-run] <source-repo-id> <target-repo-id>
#
# Example:
#   ./copy_staging_repo.sh --dry-run orgapachehudi-1177 orgapachehudi-1176
#   ./copy_staging_repo.sh orgapachehudi-1177 orgapachehudi-1176
#

set -euo pipefail

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
  shift
fi

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 [--dry-run] <source-repo-id> <target-repo-id>"
  echo "Example: $0 --dry-run orgapachehudi-1177 orgapachehudi-1176"
  exit 1
fi

SOURCE_REPO="$1"
TARGET_REPO="$2"
NEXUS_BASE="https://repository.apache.org"
SETTINGS_XML="$HOME/.m2/settings.xml"
WORK_DIR="./staging-copy-${SOURCE_REPO}-to-${TARGET_REPO}"
mkdir -p "$WORK_DIR"
CONTENT_BASE="${NEXUS_BASE}/service/local/repositories/${SOURCE_REPO}/content"

echo "==> Work directory: $WORK_DIR"

# ---------------------------------------------------------------------------
# Extract credentials from ~/.m2/settings.xml for apache.releases.https
# ---------------------------------------------------------------------------
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

echo "==> Credentials loaded for user: $NEXUS_USER"

# ---------------------------------------------------------------------------
# Crawl the Nexus content XML API to discover all artifact paths
# ---------------------------------------------------------------------------
# Nexus returns XML with <content-item> elements; <leaf>true</leaf> means file.
# We recursively crawl directories to collect every file's relativePath.
# ---------------------------------------------------------------------------
ARTIFACT_LIST_FILE="$WORK_DIR/.artifact_list"
: > "$ARTIFACT_LIST_FILE"

crawl_nexus_dir() {
  local dir_url="$1"
  local xml
  xml=$(curl --silent --fail "$dir_url") || {
    echo "  WARN: Failed to list $dir_url" >&2
    return
  }

  # Parse <relativePath> and <leaf> from each <content-item> block.
  # They appear in matching order, one per block.
  echo "$xml" | awk '
    /<relativePath>/ { gsub(/.*<relativePath>/, ""); gsub(/<\/relativePath>.*/, ""); path=$0 }
    /<leaf>/         { gsub(/.*<leaf>/, "");         gsub(/<\/leaf>.*/,         ""); print $0 "\t" path }
  ' | while IFS=$'\t' read -r is_leaf rel_path; do
        if [[ "$is_leaf" == "true" ]]; then
          echo "$rel_path" >> "$ARTIFACT_LIST_FILE"
        else
          crawl_nexus_dir "${CONTENT_BASE}${rel_path}/"
        fi
      done
}

echo "==> Crawling $SOURCE_REPO for artifacts ..."
crawl_nexus_dir "${CONTENT_BASE}/org/apache/hudi/"

# Filter out checksums and maven-metadata.xml (Nexus regenerates these)
ARTIFACT_LIST=$(sort "$ARTIFACT_LIST_FILE")

TOTAL=$(echo "$ARTIFACT_LIST" | grep -c . || true)

echo "==> Found $TOTAL artifacts."
echo ""

# ---------------------------------------------------------------------------
# Dry-run mode: list files and exit
# ---------------------------------------------------------------------------
if [[ "$DRY_RUN" == true ]]; then
  echo "$ARTIFACT_LIST" | while read -r path; do
    echo "  $path"
  done
  echo ""
  echo "==> [DRY RUN] No files were downloaded or uploaded."
  rm -rf "$WORK_DIR"
  exit 0
fi

# ---------------------------------------------------------------------------
# Download all artifacts
# ---------------------------------------------------------------------------
echo "==> Downloading $TOTAL artifacts from $SOURCE_REPO ..."

echo "$ARTIFACT_LIST" | while read -r rel_path; do
  local_path="${WORK_DIR}${rel_path}"
  mkdir -p "$(dirname "$local_path")"
  echo "  Downloading: $rel_path"
  curl --silent --fail --output "$local_path" "${CONTENT_BASE}${rel_path}"
done

echo "==> Download complete."

# ---------------------------------------------------------------------------
# Upload each artifact to the target staging repo
# ---------------------------------------------------------------------------
echo "==> Uploading $TOTAL artifacts to $TARGET_REPO ..."

UPLOAD_BASE="${NEXUS_BASE}/service/local/staging/deployByRepositoryId/${TARGET_REPO}"

SUCCESS=0
FAIL=0

echo "$ARTIFACT_LIST" | while read -r rel_path; do
  local_path="${WORK_DIR}${rel_path}"
  echo "  Uploading: $rel_path"

  HTTP_CODE=$(curl --silent --output /dev/null --write-out "%{http_code}" \
    -u "${NEXUS_USER}:${NEXUS_PASS}" \
    --upload-file "$local_path" \
    "${UPLOAD_BASE}${rel_path}" 2>&1) || true

  if [[ "$HTTP_CODE" =~ ^2 ]]; then
    SUCCESS=$((SUCCESS + 1))
  else
    FAIL=$((FAIL + 1))
    echo "  FAILED (HTTP $HTTP_CODE): $rel_path"
  fi
done

echo ""
echo "==> Done. Total: $TOTAL | Success: $SUCCESS | Failed: $FAIL"
echo "==> Artifacts are in: $WORK_DIR (delete when no longer needed)"
