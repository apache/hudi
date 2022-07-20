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
PrestoDB is a popular query engine, providing interactive query performance. PrestoDB currently supports snapshot querying on COPY_ON_WRITE tables.
Both snapshot and read optimized queries are supported on MERGE_ON_READ Hudi tables. Since PrestoDB-Hudi integration has evolved over time, the installation
instructions for PrestoDB would vary based on versions. Please check the below table for query types supported and installation instructions
for different versions of PrestoDB.

| **PrestoDB Version** | **Installation description** | **Query types supported** |
|----------------------|------------------------------|---------------------------|
| < 0.233              | Requires the `hudi-presto-bundle` jar to be placed into `<presto_install>/plugin/hive-hadoop2/`, across the installation. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| >= 0.233             | No action needed. Hudi (0.5.1-incubating) is a compile time dependency. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| >= 0.240             | No action needed. Hudi 0.5.3 version is a compile time dependency. | Snapshot querying on both COW and MOR tables |

:::note
We upgraded Hudi version from 0.5.3 to 0.9.0 in Presto 0.265 but that introduced a breaking dependency change in
another presto module. See [this issue](https://github.com/prestodb/presto/issues/17164) for more details. Since then,
we have [fixed the hudi-presto-bundle](https://github.com/apache/hudi/pull/4551) in version 0.10.1. Now, we need to
upgrade Hudi in Presto again. This is being tracked by [HUDI-3010](https://issues.apache.org/jira/browse/HUDI-3010).
Our suggestion is to avoid upgrading Presto until the issue is fixed. However, if this is not an option, then the
workaround is to download the hudi-presto-bundle jar from our [maven repo](https://mvnrepository.com/artifact/org.apache.hudi/hudi-presto-bundle)
and place it in `<presto_install>/plugin/hive-hadoop2/`.
:::

### Presto Environment
1. Configure Presto according to the [Presto configuration document](https://prestodb.io/docs/current/installation/deployment.html).
2. Configure hive catalog in ` /presto-server-0.2xxx/etc/catalog/hive.properties` as follows:

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://xxx.xxx.xxx.xxx:9083
hive.config.resources=.../hadoop-2.x/etc/hadoop/core-site.xml,.../hadoop-2.x/etc/hadoop/hdfs-site.xml
```

### Query
Beginning query by connecting hive metastore with presto client. The presto client connection command is as follows:

```bash
# The presto client connection command
./presto --server xxx.xxx.xxx.xxx:9999 --catalog hive --schema default
```

## Trino
:::note
[Trino](https://trino.io/) (formerly PrestoSQL) was forked off of PrestoDB a few years ago. Hudi supports 'Snapshot' queries for Copy-On-Write tables and 'Read Optimized' queries
for Merge-On-Read tables. This is through the initial input format based integration in PrestoDB (pre forking). This approach has
known performance limitations with very large tables, which has been since fixed on PrestoDB. We are working on replicating the same fixes on Trino as well.
:::

To query Hudi tables on Trino, please place the `hudi-trino-bundle` jar into the Hive connector installation `<trino_install>/plugin/hive-hadoop2`.

## Hive

In order for Hive to recognize Hudi tables and query correctly,
- the HiveServer2 needs to be provided with the `hudi-hadoop-mr-bundle-x.y.z-SNAPSHOT.jar` in its [aux jars path](https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cm_mc_hive_udf#concept_nc3_mms_lr). This will ensure the input format
  classes with its dependencies are available for query planning & execution.
- For MERGE_ON_READ tables, additionally the bundle needs to be put on the hadoop/hive installation across the cluster, so that queries can pick up the custom RecordReader as well.

In addition to setup above, for beeline cli access, the `hive.input.format` variable needs to be set to the fully qualified path name of the
inputformat `org.apache.hudi.hadoop.HoodieParquetInputFormat`. For Tez, additionally the `hive.tez.input.format` needs to be set
to `org.apache.hadoop.hive.ql.io.HiveInputFormat`. Then proceed to query the table like any other Hive table.

## Redshift Spectrum
Copy on Write Tables in Apache Hudi versions 0.5.2, 0.6.0, 0.7.0, 0.8.0, 0.9.0, and 0.10.0 can be queried via Amazon Redshift Spectrum external tables.
:::note
Hudi tables are supported only when AWS Glue Data Catalog is used. It's not supported when you use an Apache Hive metastore as the external catalog.
:::

Please refer to [Redshift Spectrum Integration with Apache Hudi](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html#c-spectrum-column-mapping-hudi)
for more details.

