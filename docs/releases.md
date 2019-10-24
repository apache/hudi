---
title: Releases
keywords: apache, hudi, release, data lake, upsert,
sidebar: home_sidebar
permalink: releases.html
toc: true
summary: "Apache Hudi (incubating) Releases Page"
---


## Latest Stable Release
  * Stable Release : `0.5.0-incubating`


## Release 0.5.0-incubating

### Download Information
  * Source Release : [Apache Hudi(incubating) 0.5.0-incubating Source Release](https://www.apache.org/dist/incubator/hudi/0.5.0-incubating/hudi-0.5.0-incubating.src.tgz) ([asc](https://www.apache.org/dist/incubator/hudi/0.5.0-incubating/hudi-0.5.0-incubating.src.tgz.asc), [sha512](https://www.apache.org/dist/incubator/hudi/0.5.0-incubating/hudi-0.5.0-incubating.src.tgz.sha512))
  * Apache Hudi (incubating) jars corresponding to this release is available [here](https://repository.apache.org/#nexus-search;quick~hudi)

### Release Highlights
  * Package and format renaming from com.uber.hoodie to org.apache.hudi (See migration guide section below)
  * Major redo of Hudi bundles to address class and jar version mismatches in different environments
  * Upgrade from Hive 1.x to Hive 2.x for compile time dependencies - Hive 1.x runtime integration still works with a patch : See [the discussion thread](https://lists.apache.org/thread.html/48b3f0553f47c576fd7072f56bb0d8a24fb47d4003880d179c7f88a3@%3Cdev.hudi.apache.org%3E)
  * DeltaStreamer now supports continuous running mode with managed concurrent compaction
  * Support for Composite Keys as record key
  * HoodieCombinedInputFormat to scale huge hive queries running on Hoodie tables

### Migration Guide for this release
  This is the first Apache release for Hudi (incubating). Prior to this release, Hudi Jars were published using "com.uber.hoodie" maven co-ordinates. We have a [migration guide](https://cwiki.apache.org/confluence/display/HUDI/Migration+Guide+From+com.uber.hoodie+to+org.apache.hudi)

### Raw Release Notes
  The raw release notes are available [here](https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346087)

