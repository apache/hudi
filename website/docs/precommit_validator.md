---
title: Data Quality
keywords: [ hudi, quality, expectations, pre-commit validator]
---

Data quality refers to the overall accuracy, completeness, consistency, and validity of data. Ensuring data quality is vital for accurate analysis and reporting, as well as for compliance with regulations and maintaining trust in your organization's data infrastructure.

Hudi offers **Pre-Commit Validators** that allow you to ensure that your data meets certain data quality expectations as you are writing with Hudi Streamer or Spark Datasource writers.

:::note
Pre-commit validators are skipped when using the [BULK_INSERT](write_operations.md#bulk_insert) write operation type.
:::

Multiple class names can be separated by `,` delimiter.

Syntax: `hoodie.precommit.validators=class_name1,class_name2`

Example:
```scala
spark.write.format("hudi")
    .option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator")
```

Today you can use any of these validators and even have the flexibility to extend your own:

## SQL Query Single Result
[org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQuerySingleResultPreCommitValidator.java)

The SQL Query Single Result validator can be used to validate that a query on the table results in a specific value. This validator allows you to run a SQL query and abort the commit if it does not match the expected output.

Multiple queries can be separated by `;` delimiter. Include the expected result as part of the query separated by `#`.

Syntax: `query1#result1;query2#result2`

Example:
```scala
// In this example, we set up a validator that expects there is no row with `col` column as `null`

import org.apache.hudi.config.HoodiePreCommitValidatorConfig._

df.write.format("hudi").mode(Overwrite).
  option("hoodie.table.name", tableName).
  option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator").
  option("hoodie.precommit.validators.single.value.sql.queries", "select count(*) from <TABLE_NAME> where col is null#0").
  save(basePath)
```

## SQL Query Equality
[org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQueryEqualityPreCommitValidator.java)

The SQL Query Equality validator runs a query before ingesting the data, then runs the same query after ingesting the data and confirms that both outputs match. This allows you to validate for equality of rows before and after the commit.

This validator is useful when you want to verify that your query does not change a specific subset of the data. Some examples:
- Validate that the number of null fields is the same before and after your query
- Validate that there are no duplicate records after your query runs
- Validate that you are only updating the data, and no inserts slip through

Multiple queries can be separated by `;` delimiter.

Syntax: `query1;query2`

Example:
```scala
// In this example, we set up a validator that expects no change of null rows with the new commit

import org.apache.hudi.config.HoodiePreCommitValidatorConfig._

df.write.format("hudi").mode(Overwrite).
  option("hoodie.table.name", tableName).
  option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator").
  option("hoodie.precommit.validators.equality.sql.queries", "select count(*) from <TABLE_NAME> where col is null").
  save(basePath)
```

## SQL Query Inequality
[org.apache.hudi.client.validator.SqlQueryInequalityPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQueryInequalityPreCommitValidator.java)

The SQL Query Inquality validator runs a query before ingesting the data, then runs the same query after ingesting the data and confirms that both outputs DO NOT match. This allows you to confirm changes in the rows before and after the commit.

Multiple queries can be separated by `;` delimiter.

Syntax: `query1;query2`

Example:
```scala
// In this example, we set up a validator that expects a change of null rows with the new commit

import org.apache.hudi.config.HoodiePreCommitValidatorConfig._

df.write.format("hudi").mode(Overwrite).
  option("hoodie.table.name", tableName).
  option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryInequalityPreCommitValidator").
  option("hoodie.precommit.validators.inequality.sql.queries", "select count(*) from <TABLE_NAME> where col is null").
  save(basePath)
```

## Extend Custom Validator 
Users can also provide their own implementations by extending the abstract class [SparkPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SparkPreCommitValidator.java)
and overriding this method

```java
void validateRecordsBeforeAndAfter(Dataset<Row> before, 
                                   Dataset<Row> after, 
                                   Set<String> partitionsAffected)
```

## Additional Monitoring with Notifications
Hudi offers a [commit notification service](platform_services_post_commit_callback.md) that can be configured to trigger notifications about write commits.

The commit notification service can be combined with pre-commit validators to send a notification when a commit fails a validation. This is possible by passing details about the validation as a custom value to the HTTP endpoint.

## Notes on Validator Behavior (1.2.0)

**Metadata fields in SQL queries**: Validator SQL can now reference Hudi metadata fields (`_hoodie_record_key`, `_hoodie_partition_path`, `_hoodie_file_name`, `_hoodie_commit_time`, `_hoodie_commit_seqno`) directly in query expressions.

**Empty writes**: Empty write commits no longer cause pre-commit validators to error. Validators are skipped gracefully when no records are present in the write.

## Failure Policy

Hudi 1.2.0 introduces a configurable failure policy for pre-commit validators:

| Config Key | Default | Description |
|---|---|---|
| `hoodie.precommit.validators.failure.policy` | `FAIL` | How to handle validator failures. `FAIL`: block the commit with an exception. `WARN_LOG`: emit a warning log but allow the commit to proceed (useful for soft monitoring). |

## Flink and Streaming-Offset Validators (1.2.0)

Flink writers now honor `hoodie.precommit.validators` using the same configuration key as Spark. Validators intended for use with Flink must extend the engine-agnostic `org.apache.hudi.client.validator.BasePreCommitValidator` (in `hudi-common`), which provides access to commit metadata and timeline information independently of Spark.

Two built-in streaming-offset validators are now available for Kafka-sourced pipelines:

| Validator Class | Engine | Description |
|---|---|---|
| `org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator` | Flink | Validates that the number of records written matches the Kafka offset difference for the batch |
| `org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator` | Spark / HoodieStreamer | Same semantics for Spark-based Kafka ingestion pipelines |

Both validators use the following configuration:

| Config Key | Default | Description |
|---|---|---|
| `hoodie.precommit.validators.streaming.offset.tolerance.percentage` | `0.0` | Tolerance percentage for offset-based record-count validation. A value of `0.0` requires an exact match between expected records (from Kafka offset delta) and actual records written. For upsert workloads with deduplication, set a higher tolerance (e.g., `10.0` for 10%). |
| `hoodie.precommit.validators.failure.policy` | `FAIL` | See [Failure Policy](#failure-policy) above. |

Example (Flink):
```properties
hoodie.precommit.validators=org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator
hoodie.precommit.validators.streaming.offset.tolerance.percentage=5.0
hoodie.precommit.validators.failure.policy=WARN_LOG
```

## Pre-Write Validators (1.2.0)

Pre-write validators run **before** data is written to storage, in contrast to pre-commit validators which run **after** data is written but before the commit is published to the timeline. This enables earlier rejection of invalid operations, avoiding unnecessary I/O.

Configuration:

| Config Key | Default | Description |
|---|---|---|
| `hoodie.prewrite.validators` | `""` | Comma-separated list of fully-qualified class names implementing `org.apache.hudi.client.validator.PreWriteValidator`. |

To implement a custom pre-write validator, implement the `org.apache.hudi.client.validator.PreWriteValidator` interface:

```java
public interface PreWriteValidator {
  <T> void validate(
      String instantTime,
      WriteOperationType writeOperationType,
      HoodieTableMetaClient metaClient,
      HoodieWriteConfig writeConfig,
      HoodieEngineContext engineContext,
      Option<HoodieData<HoodieRecord<T>>> recordsOpt) throws HoodieValidationException;
}
```

No built-in pre-write validator implementations are provided yet; this framework is designed for custom user extensions. Unlike pre-commit validators, pre-write validators have access to the incoming records before any write I/O occurs.

## Related Resources

<h3>Blogs</h3>
* [Apply Pre-Commit Validation for Data Quality in Apache Hudi](https://www.onehouse.ai/blog/apply-pre-commit-validation-for-data-quality-in-apache-hudi)

<h3>Videos</h3>
* [Learn About Apache Hudi Pre Commit Validator with Hands on Lab](https://www.youtube.com/watch?v=KNzs9dj_Btc)
