---
title: Developer Setup
keywords: hudi, ide, developer, setup
sidebar: mydoc_sidebar
toc: false
permalink: contributing.html
---
## Pre-requisites

To contribute code, you need

 - a GitHub account
 - a Linux (or) macOS development environment with Java JDK 8, Apache Maven (3.x+) installed
 - [Docker](https://www.docker.com/) installed for running demo, integ tests or building website
 - for large contributions, a signed [Individual Contributor License
   Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA) to the Apache
   Software Foundation (ASF).
 - (Recommended) Create an account on [JIRA](https://issues.apache.org/jira/projects/HUDI/summary) to open issues/find similar issues.
 - (Recommended) Join our dev mailing list & slack channel, listed on [community](community.html) page.


## IDE Setup

To contribute, you would need to fork the Hudi code on Github & then clone your own fork locally. Once cloned, we recommend building as per instructions on [quickstart](quickstart.html)

We have embraced the code style largely based on [google format](https://google.github.io/styleguide/javaguide.html). Please setup your IDE with style files from [here](../style/).
These instructions have been tested on IntelliJ. We also recommend setting up the [Save Action Plugin](https://plugins.jetbrains.com/plugin/7642-save-actions) to auto format & organize imports on save. The Maven Compilation life-cycle will fail if there are checkstyle violations.


## Lifecycle

Here's a typical lifecycle of events to contribute to Hudi.

 - [Recommended] Share your intent on the mailing list, so that community can provide early feedback, point out any similar JIRAs or HIPs.
 - [Optional] If you want to get involved, but don't have a project in mind, please check JIRA for small, quick-starters.
 - [Optional] Familiarize yourself with internals of Hudi using content on this page, as well as [wiki](https://cwiki.apache.org/confluence/display/HUDI)
 - Once you finalize on a project/task, please open a new JIRA or assign an existing one to yourself. (If you don't have perms to do this, please email the dev mailing list with your JIRA id and a small intro for yourself. We'd be happy to add you as a contributor)
 - Make your code change
   - Every source file needs to include the Apache license header. Every new dependency needs to
     have an open source license [compatible](https://www.apache.org/legal/resolved.html#criteria) with Apache.
   - Get existing tests to pass using `mvn clean install -DskipITs`
   - Add adequate tests for your new functionality
   - [Optional] For involved changes, its best to also run the entire integration test suite using `mvn clean install`
   - For website changes, please build the site locally & test navigation, formatting & links thoroughly
   - If your code change changes some aspect of documentation (e.g new config, default value change), 
     please ensure there is a another PR to [update the docs](https://github.com/apache/incubator-hudi/blob/asf-site/docs/README.md) as well.
 - Format commit messages and the pull request title like `[HUDI-XXX] Fixes bug in Spark Datasource`,
   where you replace HUDI-XXX with the appropriate JIRA issue.
 - Push your commit to your own fork/branch & create a pull request (PR) against the Hudi repo.
 - If you don't hear back within 3 days on the PR, please send an email to dev @ mailing list.
 - Address code review comments & keep pushing changes to your fork/branch, which automatically updates the PR
 - Before your change can be merged, it should be squashed into a single commit for cleaner commit history.


## Releases

 - Apache Hudi community plans to do minor version releases every 6 weeks or so.
 - If your contribution merged onto `master` branch after the last release, it will become part of next release.
 - Website changes are regenerated once a week (until automation in place to reflect immediately)


## Accounts and Permissions

 - [Hudi issue tracker (JIRA)](https://issues.apache.org/jira/projects/HUDI/issues):
   Anyone can access it and browse issues. Anyone can register an account and login
   to create issues or add comments. Only contributors can be assigned issues. If
   you want to be assigned issues, a PMC member can add you to the project contributor
   group.  Email the dev mailing list to ask to be added as a contributor, and include your ASF Jira username.

 - [Hudi Wiki Space](https://cwiki.apache.org/confluence/display/HUDI):
   Anyone has read access. If you wish to contribute changes, please create an account and
   request edit access on the dev@ mailing list (include your Wiki account user ID).

 - Pull requests can only be merged by a HUDI committer, listed [here](https://incubator.apache.org/projects/hudi.html)

 - [Voting on a release](https://www.apache.org/foundation/voting.html): Everyone can vote.
   Only Hudi PMC members should mark their votes as binding.

## Communication

All communication is expected to align with the [Code of Conduct](https://www.apache.org/foundation/policies/conduct).
Discussion about contributing code to Hudi happens on the [dev@ mailing list](community.html). Introduce yourself!


## Code & Project Structure

  * `docker` : Docker containers used by demo and integration tests. Brings up a mini data ecosystem locally
  * `hoodie-cli` : CLI to inspect, manage and administer datasets
  * `hoodie-client` : Spark client library to take a bunch of inserts + updates and apply them to a Hoodie table
  * `hoodie-common` : Common classes used across modules
  * `hoodie-hadoop-mr` : InputFormat implementations for ReadOptimized, Incremental, Realtime views
  * `hoodie-hive` : Manage hive tables off Hudi datasets and houses the HiveSyncTool
  * `hoodie-integ-test` : Longer running integration test processes
  * `hoodie-spark` : Spark datasource for writing and reading Hudi datasets. Streaming sink.
  * `hoodie-utilities` : Houses tools like DeltaStreamer, SnapshotCopier
  * `packaging` : Poms for building out bundles for easier drop in to Spark, Hive, Presto, Utilities
  * `style`  : Code formatting, checkstyle files


## Website

[Apache Hudi site](https://hudi.apache.org) is hosted on a special `asf-site` branch. Please follow the `README` file under `docs` on that branch for
instructions on making changes to the website.
