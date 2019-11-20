---
title: 发布
keywords: apache, hudi, release, data lake, upsert
sidebar: home_sidebar
permalink: releases.html
toc: true
---


## 最新稳定版本
  * 稳定版本 : `0.5.0-incubating`

## 0.5.0-incubating 版本

### 下载
  * 源码包 : [Apache Hudi(incubating) 0.5.0-incubating Source Release](https://www.apache.org/dist/incubator/hudi/0.5.0-incubating/hudi-0.5.0-incubating.src.tgz) ([asc](https://www.apache.org/dist/incubator/hudi/0.5.0-incubating/hudi-0.5.0-incubating.src.tgz.asc), [sha512](https://www.apache.org/dist/incubator/hudi/0.5.0-incubating/hudi-0.5.0-incubating.src.tgz.sha512))
  * 这里提供与此版本对应的 Apache Hudi (incubating) JAR 包，[here](https://repository.apache.org/#nexus-search;quick~hudi)

### 发布要点
  * Package and format renaming from com.uber.hoodie to org.apache.hudi (See migration guide section below)
  * Major redo of Hudi bundles to address class and jar version mismatches in different environments
  * Upgrade from Hive 1.x to Hive 2.x for compile time dependencies - Hive 1.x runtime integration still works with a patch : See [the discussion thread](https://lists.apache.org/thread.html/48b3f0553f47c576fd7072f56bb0d8a24fb47d4003880d179c7f88a3@%3Cdev.hudi.apache.org%3E)
  * DeltaStreamer now supports continuous running mode with managed concurrent compaction
  * Support for Composite Keys as record key
  * HoodieCombinedInputFormat to scale huge hive queries running on Hoodie tables

### 此版本的迁移指南
  这是 Apache Hudi (incubating) 的第一次发布。 在此版本之前，Hudi Jars 使用 "com.uber.hoodie" maven co-ordinates 来发布。这里有迁移指南 [migration guide](https://cwiki.apache.org/confluence/display/HUDI/Migration+Guide+From+com.uber.hoodie+to+org.apache.hudi)

### 原始发布说明书
  获取原始发布说明书 [here](https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12346087)

