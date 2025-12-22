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

This directory contains scripts for running bundle validation in Github Actions (`validate-bundles`
specified in `.github/workflows/bot.yml`) and build profile for Docker images used.

## Docker Image for Bundle Validation

The base image for bundle validation is pre-built and upload to the Docker Hub:
https://hub.docker.com/r/apachehudi/hudi-ci-bundle-validation-base.

The `Dockerfile` for the image is under `base/`. To build the image with updated `Dockerfile`, you may use the script in
the folder. Here are the docker commands to build the image by specifying different versions:

```shell
docker build \
 --build-arg HIVE_VERSION=3.1.3 \
 --build-arg FLINK_VERSION=1.14.6 \
 --build-arg SPARK_VERSION=3.1.3 \
 --build-arg SPARK_HADOOP_VERSION=2.7 \
 -t hudi-ci-bundle-validation-base:flink1146hive313spark313 .
docker image tag hudi-ci-bundle-validation-base:flink1146hive313spark313 apachehudi/hudi-ci-bundle-validation-base:flink1146hive313spark313
```

To upload the image with the tag:

```shell
docker push apachehudi/hudi-ci-bundle-validation-base:flink1146hive313spark313
```

Note that for each library like Hive and Spark, the download and extraction happen under one `RUN` instruction so that
only one layer is generated to limit the size of the image. However, this makes repeated downloads when rebuilding the
image. If you need faster iteration for local build, you may use the `Dockerfile` under `base-dev/`, which uses `ADD`
instruction for downloads, which provides caching across builds. This increases the size of the generated image compared
to `base/` and the image should only be used for development only and not be pushed to remote.

## Running Bundle Validation on a Release Candidate

The bundle validation on a release candidate is specified in the Github Action job `validate-release-candidate-bundles`
in `.github/workflows/bot.yml`. By default, this is disabled.

To enable the bundle validation on a particular release candidate, makes the following changes to the job by flipping the
flag and adding the release candidate version and staging repo number:

```shell
if: true
env:
  HUDI_VERSION: 0.13.1-rc1
  STAGING_REPO_NUM: 1123
```

