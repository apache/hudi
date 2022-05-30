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

# RFC-54: New Table APIs and Streamline Hudi Configs

## Proposers

- @codope

## Approvers

- @xushiyan
- @vinothchandar

## Status

JIRA: [HUDI-4141](https://issues.apache.org/jira/browse/HUDI-4141)

## Abstract

Users configure jobs to write Hudi tables and control the behaviour of their
jobs at different levels such as table, write client, datasource, record
payload, etc. On one hand, this is the true strength of Hudi which makes it
suitable for many use cases and offers the users a solution to the tradeoffs
encountered in data systems. On the other, it has also resulted in the learning
curve for new users to be steeper. In this RFC, we propose to streamline some of
these configurations. Additionally, we propose a few table level APIs to create
or update Hudi table programmatically. Together, they would help in a smoother
onboarding experience and increase the usability of Hudi. It would also help
existing users through better configuration maintenance.

## Background

Currently, users can create and update Hudi Table using three different
ways: [Spark datasource](https://hudi.apache.org/docs/writing_data),
[SQL](https://hudi.apache.org/docs/table_management)
and [DeltaStreamer](https://hudi.apache.org/docs/hoodie_deltastreamer). Each one
of these ways is setup using a bunch
of [configurations](https://hudi.apache.org/docs/configurations), which has
grown over the years as new features have beed added. Imagine yourself as a data
engineer who has been using Spark to write parquet tables. You want to try out
Hudi and land on
the [quickstart](https://hudi.apache.org/docs/quick-start-guide) page. You see a
bunch of configurations (precombine field, record key, partition path) to be set
and wonder why can't I just do `spark.write.format("hudi").save()`. Apart from
configurations, there is no first-class support for table management APIs such
as to create or drop table. The implementation section below presents the
proposals to fill such gaps.

## Implementation

Implementation can be split into two independent changes: streamline
configuration and new table APIs.

### Streamline Configuration

#### Minimal set of quickstart configurations

* Users should be able to simply write Hudi table
  using `spark.write.format("hudi")`. If no record key and precombine field is
  provided, then assume append only and avoid index lookup and merging.
* Hudi should infer partition field if users provide
  as `spark.write.format("hudi").partitionBy(field)`.
* Users need not pass all the configurations in each write operation. Once the
  table has been created, most table configs do not change, e.g. table name
  needs to be passed in every write, even though its only needed first time.
  Hudi should fetch from table configs when options are not provided by the
  user.

#### Better defaults

* Default values for configurations should be optimized for simple bulk load
  scenario e.g. by default if we have NONE sort mode then it's as good as
  parquet writes with some additional work for meta columns.
* Make reasonable assumptions, such as do not rely on any external system (e.g.
  hbase) for default. As another example, enable schema reconciliation by
  default instead of failing writes.

#### Consistency across write paths

* Keep configs for Spark SQL, Spark DataSource and HoodieDeltaStreamer in sync as much
  as possible. Document exceptions, e.g. key generator for sql is
  ComplexKeyGenerator while for datasource it is SimpleKeyGenerator.
* Rename/reuse existing datasource keys that are meant for the same purpose.
* In all these changes, we should support backward compatibility.

#### Refactor Meta Sync ([RFC-55](/rfc/rfc-55/rfc-55.md))

* Reduce the number of configs needed for Hive sync, e.g. table name once
  provided at the time of first write can be reused for hive sync table name
  config as well.
* Refactor the class hierarchy and APIs.

#### Support `HoodieConfig` API

* Users should be able to use the config builders instead of specifying config
  keys,
  e.g. `spark.write.format("hudi").options(HoodieClusteringConfig.Builder().withXYZ().build())`

### Table APIs

These APIs are meant for programmatically interacting with Hudi tables. Users
should be able to create or update the tables using static methods.

| Method Name   | Description   |
| ------------- | ------------- |
| bootstrap     | Create a Hudi table from the given table in parquet and other supported formats  |
| create        | Create a Hudi table with the given configs if it does not exist. Returns an instance of `HudiTable` for the newly created or an existing Hudi table.   |
| update        | Update rows in a Hudi table that match the given condition with the given update expression   |
| drop          | Drop the given Hudi table completely  |
| truncate      | Delete data from the given Hudi table but does not drop it |
| restoreTo     | Restore Hudi table to the given older commit time or a logical time.  |

Let's look at some examples:

```java
// create Hudi table
HudiTable hudiTable = HudiTable.create(HoodieTableConfig.newBuilder()
    .withTableName("tableName")
    .withBasePath("basePath")
    .withTableType(TableType.MERGE_ON_READ)
    .build())

// update Hudi table, add 1 to colA for all records of the current year (2022)
hudiTable.update(
    functions.col("dateCol").gt("2021-12-31"), // filter condition
    functions.col("colA").plus(1) // update expression
)

// restore to previous commit
hudiTable.restoreTo("0000000" // previous commit time)

// drop
hudiTable.drop() // deletes the whole data and the base path as well
```

**Phase 1**

Spark will be the execution engine behind these APIs. We will use spark sql functions for update expressions.

**Phase 2**

Support other engines such as Flink.

## Rollout/Adoption Plan

- What impact (if any) will there be on existing users?

Minimal impact. New APIs are intended to be used for new tables. Most of the
configuration changes will be backward compatible.

- If we are changing behavior how will we phase out the older behavior?

Some behaviour changes will be handled during table upgrade. Any breaking
changes will be called out in the release notes.

- If we need special migration tools, describe them here.

For breaking changes that cannot be handled automatically, we will add commands
to hudi-cli to support migration of existing tables to the newer version.

- When will we remove the existing behavior?

Not required.

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the
implementation works as expected? How will we know nothing broke?.
