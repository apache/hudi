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
# RFC-40: Hudi Snowflake Integration



## Proposers

- @vingov

## Approvers
 - @vinothchandar
 - @bhasudha

## Status

JIRA: [HUDI-2832](https://issues.apache.org/jira/browse/HUDI-2832)

> Please keep the status updated in `rfc/README.md`.

# Hudi Snowflake Integration

## Abstract

Snowflake is a fully managed service that&#39;s simple to use but can power a near-unlimited number of concurrent workloads. Snowflake is a solution for data warehousing, data lakes, data engineering, data science, data application development, and securely sharing and consuming shared data. Snowflake [doesn&#39;t support](https://docs.snowflake.com/en/sql-reference/sql/alter-file-format.html) Apache Hudi file format yet, but it has support for Parquet, ORC and Delta file format. This proposal is to implement a SnowflakeSync similar to HiveSync to sync the Hudi table as the Snowflake External Parquet table, so that users can query the Hudi tables using Snowflake. Many users have expressed interest in Hudi and other support channels asking to integrate Hudi with Snowflake, this will unlock new use cases for Hudi.

## Background

Hudi table types define how data is indexed &amp; laid out on the DFS and how the above primitives and timeline activities are implemented on top of such organization (i.e how data is written). In turn, query types define how the underlying data is exposed to the queries (i.e how data is read).

Hudi supports the following table types:

- [Copy On Write](https://hudi.apache.org/docs/overview/#copy-on-write-table): Stores data using exclusively columnar file formats (e.g parquet). Updates simply version &amp; rewrite the files by performing a synchronous merge during write.
- [Merge On Read](https://hudi.apache.org/docs/overview/#merge-on-read-table): Stores data using a combination of columnar (e.g parquet) + row based (e.g avro) file formats. Updates are logged to delta files &amp; later compacted to produce new versions of columnar files synchronously or asynchronously.

Hudi maintains multiple versions of the Parquet files and tracks the latest version using Hudi metadata (Cow), since Snowflake doesn&#39;t support Hudi yet, when you sync the Hudi&#39;s parquet files to Snowflake and query it without Hudi&#39;s metadata layer, it will query all the versions of the parquet files which might cause duplicate rows.

To avoid the above scenario, this proposal is to implement a Snowflake sync tool which will use the Hudi metadata to know which files are latest and sync only the latest version of parquet files to Snowflake external table so that users can query the Hudi tables without any duplicate records.

## Implementation

A Hudi table can be read by Snowflake using a manifest file, which is a text file containing the list of data files to read for querying a Hudi table. This proposal describes how to set up a Snowflake to Hudi integration using manifest files and query Hudi tables.

This new feature will implement the [AbstractSyncTool](https://github.com/apache/hudi/blob/master/hudi-sync/hudi-sync-common/src/main/java/org/apache/hudi/sync/common/AbstractSyncTool.java) similar to the [HiveSyncTool](https://github.com/apache/hudi/blob/master/hudi-sync/hudi-hive-sync/src/main/java/org/apache/hudi/hive/HiveSyncTool.java) named SnowflakeSyncTool with sync methods for CoW tables. The sync implementation will identify the latest parquet files for each .commit file and keep these manifests synced with the Snowflake external table. Spark datasource &amp; DeltaStreamer can already take a list of such classes to keep these manifests synced.

### Steps to Snowflake Sync

1. Generate manifests of a Hudi table using Spark Runtime
2. Configure Snowflake to read the generated manifests
3. Update manifests
   1. Update explicitly: After all the data updates, you can run the generate operation to update the manifests.
   2. Update automatically: You can configure a Hudi table so that all write operations on the table automatically update the manifests. To enable this automatic mode, set the corresponding hoodie property.

## Rollout/Adoption Plan

There are no impacts to existing users since this is entirely a new feature to support a new use case hence there are no migrations/behavior changes required.

After the Snowflake sync tool has been implemented, I will reach out to beta customers who have already expressed interest to roll out this feature for their Snowflake ingestion service.

## Test Plan

This RFC aims to implement a new SyncTool to sync the Hudi table to Snowflake, to test this feature, there will be some test tables created and updated on to the Snowflake along with unit tests for the code. Since this is an entirely new feature, I am confident that this will not cause any regressions during and after roll out.

## Future Plan

After this feature has been rolled out, the same model can be applied to sync the Hudi tables to other external data warehouses like BigQuery.

After CoW rollout and based on adoption, we can explore supporting MoR table format.
