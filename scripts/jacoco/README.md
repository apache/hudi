<!--
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
-->

# JaCoCo Code Coverage Report for Hudi

This README describes how code coverage report across multiple modules works for Hudi
by leveraging JaCoCo.

## Problem

We used to report code coverage on each PR in early days (
see https://github.com/apache/hudi/pull/1667#issuecomment-633665810, screenshot below).
However, we have disabled it due to several problems:

- We now use Azure DevOps Pipeline as CI to run tests in multiple jobs. Due to permission issues, we mirror the repo and
  branch for running the tests. This creates hurdles in reporting aggregation.
- Hudi project contains multiple source modules. There has been multiple code refactoring iterations around module
  organization so that some functional tests cover production code logic in a different module, where JaCoCo simple
  reporting cannot handle it well, leading to under-reporting on code coverage.

<img width="928" alt="Screenshot 2025-02-14 at 13 54 08" src="https://github.com/user-attachments/assets/e0ac34f4-a099-4e76-92b6-a8eac9bd2ee6" />

## Tools

JaCoCo is a free, open-source code coverage library for Java. It helps developers understand how much of their codebase
is actually being exercised by their tests. It is still the defacto standard for Java code coverage reporting.

JaCoCo supports `report-aggregate` for multi-module project but there are certain limitations as of 0.8.12
release ([1](https://www.eclemma.org/jacoco/trunk/doc/report-aggregate-mojo.html), [2](https://stackoverflow.com/questions/50806424/reporting-and-merging-multi-module-jacoco-reports-with-report-aggregate), [3](https://stackoverflow.com/questions/33078745/jacoco-maven-multi-module-project-coverage), [4](https://github.com/jacoco/jacoco/issues/1731), [5](https://github.com/jacoco/jacoco/issues/842), [6](https://github.com/jacoco/jacoco/issues?q=is%3Aissue%20state%3Aopen%20aggregate)).
One hack includes creating a new source module for reporting aggregation, which is sth we want to avoid if possible.

However, JaCoCo also provides a powerful CLI tool (https://www.jacoco.org/jacoco/trunk/doc/cli.html) which can do report
manipulation at the file level, which we can use for custom report aggregation.

## Solution

At high level, here's how JaCoCo generates the code coverage report:

(1) While running tests, JaCoCo generates binary execution data for reporting later. The execution data can be stored in
a `jacoco.exec` file if enabled. It's not a human-readable text format. It's designed for consumption by JaCoCo's
reporting tools. The following key information is stored in `jacoco.exec`:

- Session Information:  Identifies the specific test run or program execution (session ID, start time, dump time).
- Class Identification:  Uniquely identifies each class that was instrumented and monitored, including its name and a
  checksum (ID) of the original class bytecode.
- Execution Data (Probes): For each instrumented class, it stores an array of boolean values (probes). Each boolean
  indicates whether a specific execution point (line of code or branch) within that class was executed (true) or not (
  false) during the session. This is the core data that drives code coverage analysis.

(2) Once tests finish, JaCoCo generates code coverage report in HTML and/or XML based on the binary execution
data (`jacoco.exec`).

To make cross-module code coverage report work in Azure DevOps Pipeline (or in other similar CI environments) for Hudi,
here's the workflow:

(1) When running tests from mvn command in each job, enable binary execution data to be written to the storage, i.e.,
through `prepare-agent` goal (see `pom.xml`). As we run multiple `mvn test` commands in the same job with different
args, to avoid collision, a unique `destFile` is configured for each command (see `azure-pipelines-20230430.yml`);

(2) Once each job finishes, multiple `*.exec` binary execution data files are merged into one `merged-jacoco.exec`
through JaCoCo CLI (see `Merge JaCoCo Execution Data Files` task in `azure-pipelines-20230430.yml`). The merged
execution data file is published as an artifact for later analysis (see `Publish Merged JaCoCo Execution Data File` task
in `azure-pipelines-20230430.yml`).

(3) Once all jobs finish running all tests, all the JaCoCo execution data files are processed (
see `MergeAndPublishCoverage` job in `azure-pipelines-20230430.yml`). The execution data files from multiple jobs are
downloaded and merged again into a single file `jacoco.exec` through JaCoCo CLI;

(4) To generate the final report, the source files (`*.java`, `*.scala`) and class files (`*.class`) must be under the
same directory, not in different modules, due to the limitation of JaCoCo CLI taking only a single directory path for
each. So a new maven plugin execution target is added to do that (see `copy-source-files` and `copy-class-files`
in `pom.xml`). Once that's done, the final reporting is done through JaCoCo CLI by using the aggregated source files,
class files, and `jacoco.exec` (see `MergeAndPublishCoverage` job in `azure-pipelines-20230430.yml`). Both
the `jacoco.exec` and final reports are published.

## Example Results

Azure Run
<img width="1543" alt="Screenshot 2025-02-14 at 13 28 16" src="https://github.com/user-attachments/assets/05e7052c-2842-4a0e-ab0a-014eeb8e7652" />

JaCoCo Coverage Report
<img width="1559" alt="Screenshot 2025-02-14 at 13 30 32" src="https://github.com/user-attachments/assets/b47a8e78-8f98-4dfb-b64d-d926bfea5198" />
<img width="1570" alt="Screenshot 2025-02-14 at 13 30 40" src="https://github.com/user-attachments/assets/369768a0-9e82-4a29-a14d-2550048ef07f" />

Published Artifacts
<img width="1586" alt="Screenshot 2025-02-14 at 13 31 05" src="https://github.com/user-attachments/assets/02cb75b1-3f7f-4f17-8392-8e0a452d31cf" />

## Scripts

- `download_jacoco.sh`: downloads JaCoCo binaries, especially the CLI jar, for usage.
- `merge_jacoco_exec_files.sh`: merges multiple JaCoCo execution data files in multiple modules.
- `merge_jacoco_job_files.sh`: merges multiple JaCoCo execution data files from multiple Azure pipeline jobs.
- `generate_jacoco_coverage_report.sh`: generates the JaCoCo code coverage report by taking the execution data file,
  source files and class files.