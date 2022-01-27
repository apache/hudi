---
title: "Release 0.10.1"
sidebar_position: 2
layout: releases
toc: true
last_modified_at: 2022-01-26T22:07:00+08:00
---
# [Release 0.10.1](https://github.com/apache/hudi/releases/tag/release-0.10.1) ([docs](/docs/quick-start-guide))

## Migration Guide

* If migrating from an older release, please also check the upgrade instructions for each subsequent release below. Specifically check upgrade instructions for 0.6.0, 0.9.0 and 0.10.0.
* This release (0.10.1) does not introduce any new table versions.

## Release Highlights
0.10.1 comes 1 month after 0.10.0 and is purely stability and bug fix release. These bug fixes span all layers. 
We made some fixes in deltastreamer around checkpointing and some timeline fixes. Few fixes went into table services and 
metadata table had some stability fixes. Lot of fixes around sql-dml layer to stabilize our support.
Also, we fixed some inconsistencies in timestampbased key gen between row writer and non row writer path.
Bugs filed around hive sync have been fixed. Flink and Java engine had bug fixes that have been triaged as well. 
Some fixes were made around kafka connect which was added in 0.10.0 and documentation was fixed with updated instructions. 

With 0.10.1, we are starting to support spark bundles with explicit spark versions. So, this release comes with two 
spark3 bundles, namely hudi-spark3.0.3-bundle_2.12-0.10.1.jar and hudi-spark3.1.2-bundle_2.12-0.10.1.jar. 
Recommend users to use the respective bundles compatible with their run time spark versions.

## Raw Release Notes
The raw release notes are available [here](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12322822&version=12351135)
