---
title: Ecosystem
---

# Ecosystem Support

While Apache Hudi works seamlessly with various application frameworks, SQL query engines, and data warehouses, some systems might only offer read capabilities.
In such cases, you can leverage another tool like Apache Spark or Apache Flink to write data to Hudi tables and then use the read-compatible system for querying.

| Project / Product | Apache Hudi (as of June 2024)                                                                                            | Comments    |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------- |
| Onehouse.ai       | [Read + Write](https://www.onehouse.ai/universal-data-lakehouse)                                                         |             |
| Apache Spark      | [Read + Write](https://hudi.apache.org/docs/quick-start-guide)                                                           |             |
| Apache Flink      | [Read + Write](https://hudi.apache.org/docs/flink-quick-start-guide)                                                     |             |
| Presto            | [Read](https://prestodb.io/docs/current/connector/hudi.html)                                                             |             |
| Trino             | [Read](https://trino.io/docs/current/connector/hudi.html)                                                                |             |
| Hive              | [Read](https://hudi.apache.org/docs/next/query_engine_setup/#hive)                                                       |             |
| DBT               | [Read + Write](https://hudi.apache.org/blog/2022/07/11/build-open-lakehouse-using-apache-hudi-and-dbt)                   |             |
| Kafka Connect     | [Write](https://github.com/apache/hudi/tree/master/hudi-kafka-connect)                                                   |             |
| Kafka             | [Write](https://hudi.apache.org/docs/hoodie_streaming_ingestion/#kafka)                                                         |             |
| Pulsar            | [Write](https://hub.streamnative.io/connectors/lakehouse-sink/2.9.2/)                                                    |             |
| Debezium          | [Write](https://hudi.apache.org/cn/blog/2022/01/14/change-data-capture-with-debezium-and-apache-hudi/)                   |             |
| Kyuubi            | [Read + Write](https://kyuubi.readthedocs.io/en/v1.6.0-incubating-rc0/connector/flink/hudi.html)                         |             |
| ClickHouse        | [Read](https://clickhouse.com/docs/en/whats-new/changelog/#-clickhouse-release-2211-2022-11-17)                          |             |
| Apache Impala     | [Read + Write](https://hudi.apache.org/docs/querying_data/#impala-34-or-later)                                           |             |
| AWS Athena        | [Read](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html)                                                  | [Demo with S3](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html) |
| AWS EMR           | [Read + Write](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hudi-installation-and-configuration.html)         |             |
| AWS Redshift      | [Read](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html)                                   |             |
| AWS Glue          | [Read + Write](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-hudi.html)                     |             |
| Google BigQuery   | [Read](https://hudi.apache.org/docs/gcp_bigquery/)                                                                       |             |
| Google DataProc   | [Read + Write](https://cloud.google.com/blog/products/data-analytics/getting-started-with-new-table-formats-on-dataproc) |             |
| Azure Synapse     | [Read + Write](https://www.onehouse.ai/blog/apache-hudi-on-microsoft-azure)                                              |             |
| Azure HDInsight   | [Read + Write](https://www.onehouse.ai/blog/apache-hudi-on-microsoft-azure)                                              |             |
| Databricks        | [Read + Write](https://hudi.apache.org/docs/azure_hoodie/)                                                               |             |
| Snowflake         |                                                                                                                          |             |
| Vertica           | [Read](https://www.vertica.com/kb/Apache_Hudi_TE/Content/Partner/Apache_Hudi_TE.htm)                                     |             |
| Apache Doris      | [Read](https://doris.apache.org/docs/ecosystem/external-table/hudi-external-table/)                                      |             |
| Starrocks         | [Read](https://docs.starrocks.io/docs/data_source/catalog/hudi_catalog/)                                                 | [Demo with HMS + Min.IO](https://github.com/StarRocks/demo/tree/master/documentation-samples/hudi)            |
| Dremio            |                                                                                                                          |             |
| Daft              | [Read](https://docs.daft.ai/en/stable/io/hudi/)                                                |             |
| Ray Data          | [Read](https://docs.ray.io/en/master/data/api/input_output.html#hudi)                                                    |             |
