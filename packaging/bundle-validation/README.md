<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
* 
* http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
-->

# Bundle Validation for Hudi

This directory contains scripts for running bundle validation in GitHub Actions (`validate-bundles`
specified in `.github/workflows/bot.yml`) and build profile for Docker images used.

## Docker Image for Bundle Validation

The base image for bundle validation is pre-built and upload to the Docker Hub:
https://hub.docker.com/r/apachehudi/hudi-ci-bundle-validation-base. If you only need to run bundle validation then 
check [running bundle validation](#running-bundle-validation-on-a-release-candidate) section. This step is only 
necessary when the base image needs to be updated, or when a new bundle version is added. For example, let's say we want 
to add a new spark bundle for Spark 4.0.0, we need to build a new image with the new version and upload it to the Docker 
Hub. To do so, create a new shell script like [build_flink1180hive313spark350.sh](base/build_flink1180hive313spark350.sh) 
and change the Spark (or Flink/Hive/Hadoop) version in the script. Then run the script to build the image and upload it 
to the Docker Hub.

The `Dockerfile` for the image is under `base/`. To build the image with updated `Dockerfile`, you may use the script in
the folder. Here are the docker commands to build the image by specifying different versions:

```shell
docker build \
 --build-arg HIVE_VERSION=3.1.3 \
 --build-arg FLINK_VERSION=1.15.3 \
 --build-arg SPARK_VERSION=3.3.1 \
 --build-arg SPARK_HADOOP_VERSION=2.7 \
 -t hudi-ci-bundle-validation-base:flink1153hive313spark331 .
docker image tag hudi-ci-bundle-validation-base:flink1153hive313spark331 apachehudi/hudi-ci-bundle-validation-base:flink1153hive313spark331
```

To upload the image with the tag:

```shell
docker push apachehudi/hudi-ci-bundle-validation-base:flink1153hive313spark331
```

Note that for each library like Hive and Spark, the download and extraction happen under one `RUN` instruction so that
only one layer is generated to limit the size of the image. However, this makes repeated downloads when rebuilding the
image. If you need faster iteration for local build, you may use the `Dockerfile` under `base-dev/`, which uses `ADD`
instruction for downloads, which provides caching across builds. This increases the size of the generated image compared
to `base/` and the image should only be used for development only and not be pushed to remote.

## Building Image for Azure CI

The `Dockerfile` and build script for the Docker image used by Azure CI are under `azure/`. To build the image:

```shell
cd azure
./build.sh
```

This builds and tags the image as `apachehudi/hudi-ci-bundle-validation-base:azure_ci_test_base_v2`.

To push the image to Docker Hub (only from a few PMCs with permissions):

```shell
docker push apachehudi/hudi-ci-bundle-validation-base:azure_ci_test_base_v2
```

## Running Bundle Validation on a Release Candidate

The bundle validation on a release candidate is specified in the GitHub Action job `validate-release-candidate-bundles`
in `.github/workflows/release_candidate_validation.yml`. By default, this is disabled.

To enable the bundle validation on a particular release candidate, makes the following changes to the job by flipping the
flag and adding the release candidate version and staging repo number:

```shell
if: true
env:
  HUDI_VERSION: 0.13.1-rc1
  STAGING_REPO_NUM: 1123
```

## [Running Bundle Validation on Release Artifacts in Maven Central](#running-bundle-validation-on-release-artifacts-in-maven-central)

After the release candidate artifacts are finalized and released from the staging repository, the artifacts usually take
24 hours to be available in [Maven Central](https://repo1.maven.org/maven2/org/apache/hudi). The bundle validation can
be run on the release artifacts in Maven Central by specifying the version in the GitHub Action job
`validate-release-maven-artifacts` in `.github/workflows/maven_artifact_validation.yml`. By default, this is
disabled.

To enable the bundle validation on a particular release version, make the following changes to the job by flipping the
flag and adding the release version:

```yaml
  validate-release-maven-artifacts:
    runs-on: ubuntu-latest
    if: false             ## Change to true, or simply remove/comment this line
    env:
      HUDI_VERSION: 1.0.0 ## Change to the release version
      MAVEN_BASE_URL: 'https://repo1.maven.org/maven2'  ## Maven Central URL, no need to change unless necessary
```
