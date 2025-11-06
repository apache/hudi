---
title: Querying Tables
keywords: [hudi, writing, reading]
---
# Querying Tables

### Does deleted records appear in Hudi's incremental query results?

Soft Deletes (unlike hard deletes) do appear in the incremental pull query results. So, if you need a mechanism to propagate deletes to downstream tables, you can use Soft deletes.

### How do I pass hudi configurations to my beeline Hive queries?

If Hudi's input format is not picked the returned results may be incorrect. To ensure correct inputformat is picked, please use `org.apache.hadoop.hive.ql.io.HiveInputFormat` or `org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat` for `hive.input.format` config. This can be set like shown below:

```plain
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat
```

or

```plain
set hive.input.format=org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat
```

### Does Hudi guarantee consistent reads? How to think about read optimized queries?

Hudi does offer consistent reads. To read the latest snapshot of a MOR table, a user should use snapshot query. The [read-optimized queries](table_types#query-types) (targeted for the MOR table ONLY) are an add on benefit to provides users with a practical tradeoff of decoupling writer performance vs query performance, leveraging the fact that most queries query say the most recent data in the table.

Hudi’s read-optimized query is targeted for the MOR table only, with guidance around how compaction should be run to achieve predictable results. In the MOR table, the compaction, which runs every few commits (or “deltacommit” to be exact for the MOR table) by default, merges the base (parquet) file and corresponding change log files to a new base file within each file group, so that the snapshot query serving the latest data immediately after compaction reads the base files only.  Similarly, the read-optimized query always reads the base files only as of the latest compaction commit, usually a few commits before the latest commit, which is still a valid table state.

Users must use snapshot queries to read the latest snapshot of a MOR table.  Popular engines including Spark, Presto, and Hive already support snapshot queries on MOR table and the snapshot query support in Trino is in progress (the [PR](https://github.com/trinodb/trino/pull/14786) is under review).  Note that the read-optimized query does not apply to the COW table.
