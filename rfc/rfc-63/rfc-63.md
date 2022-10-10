<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-63: Hudi bundle standards

## Proposers

- @xushiyan

## Approvers

- @vinoth
- @prasanna

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-3529

## Abstract

Hudi bundle jars are the user-facing artifacts that deserve special attention and careful reviews when changes happen on
the bundle names, included jars, intended usage, etc. In this RFC, a group of standards about bundle building,
dependency governance, and release are presented to serve as references for developers and users.

## Building

Bundles are built under [packaging/](../../packaging/) directory.

| Bundle                      | Category  | Usage                                                                     |
|-----------------------------|-----------|---------------------------------------------------------------------------|
| hudi-spark-bundle           | Engine    | Integrates with Spark                                                     |
| hudi-flink-bundle           | Engine    | Integrates with Flink                                                     |
| hudi-hadoop-mr-bundle       | Engine    | Integrates with Hive                                                      |
| hudi-presto-bundle          | Engine    | Integrates with Presto                                                    |
| hudi-trino-bundle           | Engine    | Integrates with Trino                                                     |
| hudi-kafka-connect-bundle   | Engine    | Integrates with Kafka Connect                                             |
| hudi-aws-bundle             | Cloud     | Supports AWS-specific integration and features                            |
| hudi-gcp-bundle             | Cloud     | Supports GCP-specific integration and features                            |
| hudi-hive-sync-bundle       | Catalog   | Supports meta sync with Hive metastore                                    |
| hudi-datahub-sync-bundle    | Catalog   | Supports meta sync with DataHub                                           |
| hudi-timeline-server-bundle | Server    | Runs standalone timeline server                                           |
| hudi-utilities-slim-bundle  | Utilities | Provides Hudi utilities to run with different engines                     |
| hudi-utilities-bundle       | Utilities | Similar to "utilities-slim"; legacy bundle that contains Spark integration |
| hudi-integ-test-bundle      | Test      | Runs with docker demo for integration tests                               |

### Standard: engine integration

An engine bundle should clearly declare which version it supports. If it's scala-dependent, it should also declare scala
version in the jar name. The format is `hudi-{engine name}X.Y-bundle{_scala version}`. Examples:

- `hudi-spark3.2-bundle_2.12`
- `hudi-flink1.15-bundle`

### Standard: cloud integration

A cloud bundle should be the only artifact for the target cloud provider that includes all the required dependencies and
supports that cloud's specific features. Therefore, if a Hudi module requires some classes from cloud-specific
dependencies, it should include the needed Hudi cloud module as a dependency. For example, `hudi-utilities` module
includes `hudi-aws` and `hudi-gcp` to provide `S3EventSource` and `GcsEventSource` respectively.

### Standard: runnable bundle

Bundle that can run as a standalone application should contain a bash script under the bundle directory to guide users
how to run the application. It should show clearly what dependencies should be provided by user environment. An example
is hudi-timeline-server-bundle, which provides `run_server.sh`.

### Standard: bundle combinations

An engine-bundle alone should be able to run with the desired engine. Cloud, catalog, utilities bundles are add-on
bundles that are engine-independent and can be plugged in to the engine-bundle's classpath to provide additional
support.

- Example 1: users can run hudi-spark3.1-bundle_2.12 alone with Spark 3.1. Users can also add hudi-aws-bundle to work
  with DynamoDB lock provider and hudi-datahub-sync-bundle to sync with DataHub catalog.
- Example 2: users are running Spark DataSource writer using hudi-spark3.3-bundle_2.12 alone with Spark 3.3. Users add
  hudi-utilities-slim-bundle and migrate to running `HoodieDeltaStreamer` as the main ingestion application.

A server-bundle is supposed to run alone as a server application without other bundles' support.

### Standard: bundle name changes

Bundle names may evolve at rare cases with strong reasons. Bundles with the old names should still be published at each
release to keep usage non-breaking. In case there are obvious evidence showing minimum negligible usage, the obsolete
bundle names may be dropped and skipped during releases. The evidence should be presented in the PR for such change.

## Dependency Governance

### Standard: consistent shading

If a dependency is deemed as common and to be shaded (a.k.a., relocation), its shading should be done consistently
across all bundles. This is to avoid class conflicts when use different bundles together, e.g., hudi-utilities-slim
bundle and hudi-spark-bundle. A common dependency shading should be configured in `maven-shade-plugin` under `<build>`
section of the root [pom.xml](../../pom.xml). All bundles will inherit the common shaded dependencies. For
bundle-specific ones, configure it separately in the bundle pom.xml.

### Standard: change process

Any PR that affects bundle dependency tree should be flagged by CI to highlight what the changes look
like. [dependency.sh](../../scripts/dependency.sh) was introduced to run and generate the bundle dependency lists
under [dependencies/](../../dependencies/) to show the differences. Reviewers should consider the risks of such changes
and approve PR accordingly.

#### CI setup

Configure a GitHub Actions job to run [dependency.sh](../../scripts/dependency.sh) for each PR and comment on the PR to
show dependency trees' changes if any. If PR review decides that the changes are needed, apply the diff in the PR to
keep [dependencies/](../../dependencies/) up-to-date. If decides otherwise, PR should be fixed to avoid such changes.

## Release

### Standard: test coverage

Each bundle should have an end-to-end sanity test case in the CI to cover the intended usage. A GitHub Actions job
should be configured and triggered upon differences detected in the [dependencies/](../../dependencies/) list.

For example, after `mvn install`, the CI job can setup local Spark and Hive metastore, and add
hudi-utilities-slim-bundle, hudi-spark-bundle, and hudi-hive-sync-bundle to run a simple `HoodieDeltaStreamer` job that
tests ingestion and hive-sync features to prove the bundles are compatible with each other and with vanilla open-source
stacks.

### Standard: documentation

Each bundle directory should have a `README.md` to explain clearly about the bundle's usage, including necessary
environment setup for demonstration. Hudi website (under `asf-site` branch) should contain a section of bundle usage
that is generated from the `README.md`s and updated automatically for each release through versioned docs update.
