---
title: Developer Setup
keywords: hudi, ide, developer, setup
permalink: /cn/contributing
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
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

To contribute, you would need to do the following
 
 - Fork the Hudi code on Github & then clone your own fork locally. Once cloned, we recommend building as per instructions on [quickstart](/docs/quick-start-guide.html)
 - [Recommended] We have embraced the code style largely based on [google format](https://google.github.io/styleguide/javaguide.html). Please setup your IDE with style files from [here](https://github.com/apache/incubator-hudi/tree/master/style).
These instructions have been tested on IntelliJ. 
 - [Recommended] Set up the [Save Action Plugin](https://plugins.jetbrains.com/plugin/7642-save-actions) to auto format & organize imports on save. The Maven Compilation life-cycle will fail if there are checkstyle violations.
 - [Optional] If needed, add spark jars to the classpath of your module in Intellij by following the steps from [here](https://stackoverflow.com/questions/1051640/correct-way-to-add-external-jars-lib-jar-to-an-intellij-idea-project). 

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

## Life of a Contributor

This document details processes and procedures we follow to make contributions to the project and take it forward. 
If you are looking to ramp up into the project as a contributor, we highly encourage you to read this guide in full, familiarize yourself with the workflow 
and more importantly also try to improve the process along the way as well. 

### Filing JIRAs

 - Hudi uses JIRA to manage issues. First, familiarize yourself with the various [components](https://issues.apache.org/jira/projects/HUDI/components) against which issues are filed in Hudi.
 - Make an attempt to find an existing JIRA, that may solve the same issue you are reporting. When in doubt, you can always email the mailing list so that the community can provide early feedback, 
   point out any similar JIRAs or RFCs. 
 - Try to gauge whether this JIRA needs an [RFC](https://cwiki.apache.org/confluence/display/HUDI/RFC+Process). As always, email the mailing list if unsure. If you need an RFC since the change is
   large in scope, then please follow the wiki instructions to get the process rolling along.
 - While raising a new JIRA or updating an existing one, please make sure to do the following
      - The issue `type` and `components` (when resolving the ticket) are set correctly
      - If you intend to target the JIRA for a specific release, please fill in the `fix version(s)` field, with the [release number](https://issues.apache.org/jira/projects/HUDI/releases).
      - Summary should be descriptive enough to catch the essence of the problem/ feature
      - Where necessary, capture the version of Hudi/Spark/Hive/Hadoop/Cloud environments in the ticket
      - Whenever possible, provide steps to reproduce via sample code or on the [docker setup](https://hudi.apache.org/docker_demo.html)
 - All newly filed JIRAs are placed in the `NEW` state. If you are sure about this JIRA representing valid, scoped piece of work, please click `Accept Issue` to move it `OPEN` state
 - If you are not sure, please wait for a PMC/Committer to confirm/triage the issue and accept it. This process avoids contributors spending time on JIRAs with unclear scope.
 - Whenever possible, break down large JIRAs (e.g JIRAs resulting from an [RFC](https://cwiki.apache.org/confluence/display/HUDI/RFC+Process)) into `sub tasks` by clicking `More > create sub-task` from the parent JIRA , 
   so that the community can contribute at large and help implement it much quickly. We recommend prefixing such JIRA titles with `[UMBRELLA]`

### Claiming JIRAs

 - Finding a JIRA to work on 
      - If you are new to the project, you can ramp up by picking up any issues tagged with the [newbie](https://issues.apache.org/jira/issues/?jql=project+%3D+HUDI+AND+component+%3D+newbie) component.
      - If you want to work on some higher priority issue, then scout for Open issues against the next release on the JIRA, engage on unassigned/inactive JIRAs and offer help.
      - Issues tagged with `Usability` , `Code Cleanup`, `Testing` components often present excellent opportunities to make a great impact.
 - If you don't have perms to self-assign JIRAs, please email the dev mailing list with your JIRA id and a small intro for yourself. We'd be happy to add you as a contributor.
 - As courtesy, if you are unable to continue working on a JIRA, please move it back to "OPEN" state and un-assign yourself.
      - If a JIRA or its corresponding pull request has been inactive for a week, awaiting feedback from you, PMC/Committers could choose to re-assign them to another contributor.
      - Such re-assignment process would be communicated over JIRA/GitHub comments, checking with the original contributor on his/her intent to continue working on the issue.
      - You can also contribute by helping others contribute. So, if you don't have cycles to work on a JIRA and another contributor offers help, take it!

### Contributing Code

 - Once you finalize on a project/task, please open a new JIRA or assign an existing one to yourself. 
      - Almost all PRs should be linked to a JIRA. It's always good to have a JIRA upfront to avoid duplicating efforts.
      - If the changes are minor, then `[MINOR]` prefix can be added to Pull Request title without a JIRA. Below are some tips to judge **MINOR** Pull Request :
        - trivial fixes (for example, a typo, a broken link, intellisense or an obvious error)
        - the change does not alter functionality or performance in any way
        - changed lines less than 100
        - obviously judge that the PR would pass without waiting for CI / CD verification
      - But, you may be asked to file a JIRA, if reviewer deems it necessary
 - Before you begin work,
      - Claim the JIRA using the process above and assign the JIRA to yourself.
      - Click "Start Progress" on the JIRA, which tells everyone that you are working on the issue actively.
 - [Optional] Familiarize yourself with internals of Hudi using content on this page, as well as [wiki](https://cwiki.apache.org/confluence/display/HUDI)
 - Make your code change
   - Every source file needs to include the Apache license header. Every new dependency needs to have an 
     open source license [compatible](https://www.apache.org/legal/resolved.html#criteria) with Apache.
   - If you are re-using code from another apache/open-source project, licensing needs to be compatible and attribution added to `LICENSE` file
   - Please DO NOT copy paste any code from StackOverflow or other online sources, since their license attribution would be unclear. Author them yourself!
   - Get existing tests to pass using `mvn clean install -DskipITs`
   - Add adequate tests for your new functionality
   - For involved changes, it's best to also run the entire integration test suite using `mvn clean install`
   - For website changes, please build the site locally & test navigation, formatting & links thoroughly
   - If your code change changes some aspect of documentation (e.g new config, default value change), 
     please ensure there is another PR to [update the docs](https://github.com/apache/incubator-hudi/blob/asf-site/docs/README.md) as well.
 - Sending a Pull Request
   - Format commit and the pull request title like `[HUDI-XXX] Fixes bug in Spark Datasource`, 
     where you replace `HUDI-XXX` with the appropriate JIRA issue. 
   - Please ensure your commit message body is descriptive of the change. Bulleted summary would be appreciated.
   - Push your commit to your own fork/branch & create a pull request (PR) against the Hudi repo.
   - If you don't hear back within 3 days on the PR, please send an email to the dev @ mailing list.
   - Address code review comments & keep pushing changes to your fork/branch, which automatically updates the PR
   - Before your change can be merged, it should be squashed into a single commit for cleaner commit history.
 - Finally, once your pull request is merged, make sure to `Close` the JIRA.

### Reviewing Code/RFCs

 - All pull requests would be subject to code reviews, from one or more of the PMC/Committers. 
 - Typically, each PR will get an "Assignee" based on their area of expertise, who will work with you to land the PR.
 - Code reviews are vital, but also often time-consuming for everyone involved. Below are some principles which could help align us better.
   - Reviewers need to provide actionable, concrete feedback that states what needs to be done to get the PR closer to landing.
   - Reviewers need to make it explicit, which of the requested changes would block the PR vs good-to-dos.
   - Both contributors/reviewers need to keep an open mind and ground themselves to making the most technically sound argument.
   - If progress is hard, please involve another PMC member/Committer to share another perspective.
   - Staying humble and eager to learn, goes a long way in ensuring these reviews are smooth.

### Suggest Changes

We welcome new ideas and suggestions to improve the project, along any dimensions - management, processes, technical vision/direction. To kick start a discussion on the mailing thread
to effect change and source feedback, start a new email thread with the `[DISCUSS]` prefix and share your thoughts. If your proposal leads to a larger change, then it may be followed up
by a [vote](https://www.apache.org/foundation/voting.html) by a PMC member or others (depending on the specific scenario). 
For technical suggestions, you can also leverage [our RFC Process](https://cwiki.apache.org/confluence/display/HUDI/RFC+Process) to outline your ideas in greater detail.


## Releases

 - Apache Hudi community plans to do minor version releases every 6 weeks or so.
 - If your contribution merged onto the `master` branch after the last release, it will become part of the next release.
 - Website changes are regenerated on-demand basis (until automation in place to reflect immediately)

## Communication

All communication is expected to align with the [Code of Conduct](https://www.apache.org/foundation/policies/conduct).
Discussion about contributing code to Hudi happens on the [dev@ mailing list](community.html). Introduce yourself!

## Code & Project Structure

  * `docker` : Docker containers used by demo and integration tests. Brings up a mini data ecosystem locally
  * `hudi-cli` : CLI to inspect, manage and administer datasets
  * `hudi-client` : Spark client library to take a bunch of inserts + updates and apply them to a Hoodie table
  * `hudi-common` : Common classes used across modules
  * `hudi-hadoop-mr` : InputFormat implementations for ReadOptimized, Incremental, Realtime views
  * `hudi-hive` : Manage hive tables off Hudi datasets and houses the HiveSyncTool
  * `hudi-integ-test` : Longer running integration test processes
  * `hudi-spark` : Spark datasource for writing and reading Hudi datasets. Streaming sink.
  * `hudi-utilities` : Houses tools like DeltaStreamer, SnapshotCopier
  * `packaging` : Poms for building out bundles for easier drop in to Spark, Hive, Presto, Utilities
  * `style`  : Code formatting, checkstyle files


## Website

[Apache Hudi site](https://hudi.apache.org) is hosted on a special `asf-site` branch. Please follow the `README` file under `docs` on that branch for
instructions on making changes to the website.