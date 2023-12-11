---
title: Query Engine Setup
summary: "In this page, we describe how to setup various query engines for Hudi."
toc: true
last_modified_at:
---

## Spark
The Spark Datasource API is a popular way of authoring Spark ETL pipelines. Hudi tables can be queried via the Spark datasource with a simple `spark.read.parquet`.
See the [Spark Quick Start](/docs/quick-start-guide) for more examples of Spark datasource reading queries.

If your Spark environment does not have the Hudi jars installed, add `--jars <path to jar>/hudi-spark-bundle_2.11-<hudi version>.jar` to the classpath of drivers
and executors. Alternatively, hudi-spark-bundle can also fetched via the `--packages` options (e.g: `--packages org.apache.hudi:hudi-spark-bundle_2.11:0.5.3`).

## PrestoDB
PrestoDB is a popular query engine, providing interactive query performance.
One can use both Hive or Hudi connector (Presto version 0.275 onwards) for querying Hudi tables.
Both connectors currently support snapshot querying on COPY_ON_WRITE tables, and
snapshot and read optimized queries on MERGE_ON_READ Hudi tables.

Since PrestoDB-Hudi integration has evolved over time, the installation
instructions for PrestoDB would vary based on versions.
Please check the below table for query types supported and installation instructions
for different versions of PrestoDB.

| **PrestoDB Version** | **Installation description**                                                                                              | **Query types supported**                                                   |
|----------------------|---------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| < 0.233              | Requires the `hudi-presto-bundle` jar to be placed into `<presto_install>/plugin/hive-hadoop2/`, across the installation. | Snapshot querying on COW tables. Read optimized querying on MOR tables.     |
| >= 0.233             | No action needed. Hudi (0.5.1-incubating) is a compile time dependency.                                                   | Snapshot querying on COW tables. Read optimized querying on MOR tables.     |
| >= 0.240             | No action needed. Hudi 0.5.3 version is a compile time dependency.                                                        | Snapshot querying on both COW and MOR tables.                               |
| >= 0.268             | No action needed. Hudi 0.9.0 version is a compile time dependency.                                                        | Snapshot querying on bootstrap tables.                                      |
| >= 0.272             | No action needed. Hudi 0.10.1 version is a compile time dependency.                                                       | File listing optimizations. Improved query performance.                     |
| >= 0.275             | No action needed. Hudi 0.11.0 version is a compile time dependency.                                                       | All of the above. Native Hudi connector that is on par with Hive connector. |

To learn more about the usage of Hudi connector, please checkout [prestodb documentation](https://prestodb.io/docs/current/connector/hudi.html).

:::note
Incremental queries and point in time queries are not supported either through the Hive connector or Hudi connector.
However, it is in our roadmap and you can track the development under [HUDI-3210](https://issues.apache.org/jira/browse/HUDI-3210).

There is a known issue ([HUDI-4290](https://issues.apache.org/jira/browse/HUDI-4290)) for a clustered Hudi table. Presto query using version 0.272 or later
may contain duplicates in results if clustering is enabled. This issue has been fixed in Hudi version 0.12.0 and we need to upgrade `hudi-presto-bundle`
in presto to version 0.12.0. It is tracked in [HUDI-4605](https://issues.apache.org/jira/browse/HUDI-4605).
:::

### Presto Environment
1. Configure Presto according to the [Presto configuration document](https://prestodb.io/docs/current/installation/deployment.html).
2. Configure hive catalog in ` /presto-server-0.2xxx/etc/catalog/hive.properties` as follows:

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://xxx.xxx.xxx.xxx:9083
hive.config.resources=.../hadoop-2.x/etc/hadoop/core-site.xml,.../hadoop-2.x/etc/hadoop/hdfs-site.xml
```
3. Alternatively, configure hudi catalog in ` /presto-server-0.2xxx/etc/catalog/hudi.properties` as follows:

```properties
connector.name=hudi
hive.metastore.uri=thrift://xxx.xxx.xxx.xxx:9083
hive.config.resources=.../hadoop-2.x/etc/hadoop/core-site.xml,.../hadoop-2.x/etc/hadoop/hdfs-site.xml
```

### Query
Beginning query by connecting hive metastore with presto client. The presto client connection command is as follows:

```bash
# The presto client connection command where <catalog_name> is either hudi or hive,
# and <schema_name> is the database name used in hive sync.
./presto --server xxx.xxx.xxx.xxx:9999 --catalog <catalog_name> --schema <schema_name>
```

## Trino

Just like PrestoDB, there are two ways to query Hudi tables using Trino i.e. either using [Hive](https://trino.io/docs/current/connector/hive.html) connector or the native
[Hudi](https://trino.io/docs/current/connector/hudi.html) connector (available since version 398). However, since version 411, Hive connector redirects to Hudi catalog for reading Hudi tables.

### Hive Connector

| **Trino Version** | **Installation description**                                                         | **Query types supported**                                                                                                                                                                                |
|-------------------|--------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| < 406             | Requires the `hudi-trino-bundle` jar to be placed into `<trino_install>/plugin/hive` | Snapshot querying on COW tables. Read optimized querying on MOR tables.                                                                                                                                  |
| > = 406           | Requires the `hudi-trino-bundle` jar to be placed into `<trino_install>/plugin/hive` | Snapshot querying on COW tables. Read optimized querying on MOR tables. **Redirection to Hudi catalog also supported.**                                                                                  |
| > = 411           | NA                                                                                   | Snapshot querying on COW tables. Read optimized querying on MOR tables. Hudi tables can be **only** queried by [table redirection](https://trino.io/docs/current/connector/hive.html#table-redirection). |

If you are using Trino version 411 or greater, and also using Hive connector to query Hudi tables, please set the below config to support table redirection.
```
hive.hudi-catalog-name=hudi
```
It is recommended to use `hudi-trino-bundle` version 0.12.2 or later for optimal query performance with Hive connector.

### Hudi Connector

| **Trino Version** | **Installation description**                                                    | **Query types supported**                                               |
|-------------------|---------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| < 398             | NA - can only use Hive connector to query Hudi tables                           | Same as that of Hive connector version < 406.                           |
| > = 398           | NA - no need to place bundle jars manually, as they are compile-time dependency | Snapshot querying on COW tables. Read optimized querying on MOR tables. |

To learn more about the usage of Hudi connector, please check out
the [connector documentation](https://trino.io/docs/current/connector/hudi.html). Both the connectors are on par in
terms of query support, i.e. 'Snapshot' queries for COW tables and 'Read Optimized' queries for MOR
tables. We have an active [PR](https://github.com/trinodb/trino/pull/16034) under review to bring the performance of
Hudi connector on par with Hive connector. Furthermore, we
expect [MOR table snapshot query](https://github.com/trinodb/trino/pull/14786) support will soon be added to the Hudi
connector.

## Hive

In order for Hive to recognize Hudi tables and query correctly,
- the HiveServer2 needs to be provided with the `hudi-hadoop-mr-bundle-x.y.z-SNAPSHOT.jar` in its [aux jars path](https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cm_mc_hive_udf.html#concept_nc3_mms_lr). This will ensure the input format
  classes with its dependencies are available for query planning & execution.
- For MERGE_ON_READ tables, additionally the bundle needs to be put on the hadoop/hive installation across the cluster, so that queries can pick up the custom RecordReader as well.

In addition to setup above, for beeline cli access, the `hive.input.format` variable needs to be set to the fully qualified path name of the
inputformat `org.apache.hudi.hadoop.HoodieParquetInputFormat`. For Tez, additionally the `hive.tez.input.format` needs to be set
to `org.apache.hadoop.hive.ql.io.HiveInputFormat`. Then proceed to query the table like any other Hive table.



## Redshift Spectrum
Latest version of Redshift spectrum supports snapshot queries on Hudi Copy-on-Write tables and Read Optimized queries on Hudi Merge-on-Read tables.

:::note NOTE:
Hudi tables are supported only when AWS Glue Data Catalog is used. It's not supported when you use an Apache Hive metastore as the external catalog.
:::

Please refer to [Redshift Spectrum Integration with Apache Hudi](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html#c-spectrum-column-mapping-hudi)
for more details.

## StarRocks
As of StarRocks v3.1, there's complete support for both Hudi Copy-on-Write and Merge-on-Read tables.
Please refer [StarRocks docs](https://docs.starrocks.io/docs/data_source/catalog/hudi_catalog/) to get started.