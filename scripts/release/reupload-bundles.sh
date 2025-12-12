#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=false

# Check for optional --dry-run flag
if [[ "${1-}" == "--dry-run" ]]; then
  DRY_RUN=true
  shift
fi

if [[ $# -ne 5 ]]; then
  echo "Usage: $0 [--dry-run] <USERNAME> <PASSWORD> <TMP_FOLDER> <STAGING_REPO_NUM> <HUDI_VERSION>"
  echo
  echo "Example:"
  echo "  $0 --dry-run abc pswd1 /tmp/downloaded-target 1170 1.1.1-rc1"
  exit 1
fi

USERNAME="$1"
PASSWORD="$2"
TMP_FOLDER="${3%/}"
STAGING_REPO_NUM="$4"
HUDI_VERSION="$5"

BASE_DEPLOY_URL="https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-${STAGING_REPO_NUM}"

echo "=============================================================="
echo " Uploading artifacts under: $TMP_FOLDER"
echo " Staging Repo ID          : orgapachehudi-${STAGING_REPO_NUM}"
echo " HUDI Version             : $HUDI_VERSION"
echo " Dry-run mode             : $DRY_RUN"
echo "=============================================================="
echo

content_type_for() {
  local f="$1"
  case "$f" in
    *.jar|*.jar.asc|*.jar.md5|*.jar.sha1)
      echo "application/java-archive"
      ;;
    *.pom|*.pom.asc|*.pom.md5|*.pom.sha1)
      echo "text/xml"
      ;;
    *.xml|*.xml.md5|*.xml.sha1)
      echo "application/xml"
      ;;
    *)
      echo "application/octet-stream"
      ;;
  esac
}

find "$TMP_FOLDER" -type f | while read -r file; do
  rel_path="${file#$TMP_FOLDER/}"
  remote_path="org/apache/hudi/${rel_path}"
  url="${BASE_DEPLOY_URL}/${remote_path}"
  ctype=$(content_type_for "$file")

  echo "→ Preparing upload:"
  echo "     Local : $file"
  echo "     Remote: $url"
  echo "     Type  : $ctype"
  echo

  if [[ "$DRY_RUN" == true ]]; then
    echo "DRY RUN: curl --upload-file \"$file\" -u \"$USERNAME:*****\" -H \"Content-Type: $ctype\" \"$url\""
    echo
    continue
  fi

  curl -v --fail --upload-file "$file" \
    -u "$USERNAME:$PASSWORD" \
    -H "Content-Type: ${ctype}" \
    "$url"

  echo
done

echo "=============================================================="
echo " Completed processing TMP_FOLDER=${TMP_FOLDER}"
echo " Dry-run mode was: $DRY_RUN"
echo "=============================================================="


