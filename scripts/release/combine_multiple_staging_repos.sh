USERNAME=$1
PASSWORD=$2
TMP_FOLDER=$3
STAGING_REPO_NUM=$4
HUDI_VERSION=$5
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION-javadoc.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION-javadoc.jar
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION-javadoc.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION-javadoc.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION-javadoc.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION-javadoc.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION-javadoc.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION-javadoc.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION-sources.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION-sources.jar
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION-sources.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION-sources.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION-sources.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION-sources.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION-sources.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION-sources.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION.jar
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION.pom \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION.pom
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION.pom.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION.pom.asc
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION.pom.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION.pom.md5
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle-$HUDI_VERSION.pom.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/$HUDI_VERSION/hudi-flink2.0-bundle-$HUDI_VERSION.pom.sha1

curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle/maven-metadata.xml \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/maven-metadata.xml
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle/maven-metadata.xml.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/maven-metadata.xml.md5
curl -v --upload-file $TMP_FOLDER/hudi-flink2.0-bundle/maven-metadata.xml.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-flink2.0-bundle/maven-metadata.xml.sha1

curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-javadoc.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-javadoc.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-javadoc.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-javadoc.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-javadoc.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-javadoc.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-javadoc.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-javadoc.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-sources.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-sources.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-sources.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-sources.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-sources.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-sources.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-sources.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-sources.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-tests.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-tests.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-tests.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-tests.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-tests.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-tests.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION-tests.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION-tests.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION.pom \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION.pom
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION.pom.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION.pom.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION.pom.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION.pom.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common-$HUDI_VERSION.pom.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/$HUDI_VERSION/hudi-spark4-common-$HUDI_VERSION.pom.sha1

curl -v --upload-file $TMP_FOLDER/hudi-spark4-common/maven-metadata.xml \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/maven-metadata.xml
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common/maven-metadata.xml.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/maven-metadata.xml.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4-common/maven-metadata.xml.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4-common/maven-metadata.xml.sha1

curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-javadoc.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-javadoc.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-javadoc.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-javadoc.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-javadoc.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-javadoc.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-javadoc.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-javadoc.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-sources.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-sources.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-sources.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-sources.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-sources.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-sources.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-sources.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION-sources.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.pom \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.pom
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.pom.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.pom.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.pom.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.pom.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.pom.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/$HUDI_VERSION/hudi-spark4.0-bundle_2.13-$HUDI_VERSION.pom.sha1

curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13/maven-metadata.xml \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/maven-metadata.xml
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13/maven-metadata.xml.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/maven-metadata.xml.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0-bundle_2.13/maven-metadata.xml.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0-bundle_2.13/maven-metadata.xml.sha1

curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-javadoc.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-javadoc.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-javadoc.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-javadoc.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-javadoc.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-javadoc.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-javadoc.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-javadoc.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-sources.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-sources.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-sources.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-sources.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-sources.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-sources.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-sources.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-sources.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-tests.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-tests.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-tests.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-tests.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-tests.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-tests.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION-tests.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION-tests.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION.jar \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION.jar
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION.jar.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION.jar.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION.jar.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION.jar.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION.jar.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION.jar.sha1
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION.pom \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION.pom
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION.pom.asc \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION.pom.asc
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION.pom.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION.pom.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13-$HUDI_VERSION.pom.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/$HUDI_VERSION/hudi-spark4.0.x_2.13-$HUDI_VERSION.pom.sha1

curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13/maven-metadata.xml \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/maven-metadata.xml
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13/maven-metadata.xml.md5 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/maven-metadata.xml.md5
curl -v --upload-file $TMP_FOLDER/hudi-spark4.0.x_2.13/maven-metadata.xml.sha1 \
  -u $USERNAME:$PASSWORD \
  -H "Content-type: application/java-archive" \
  https://repository.apache.org/service/local/staging/deployByRepositoryId/orgapachehudi-$STAGING_REPO_NUM/org/apache/hudi/hudi-spark4.0.x_2.13/maven-metadata.xml.sha1