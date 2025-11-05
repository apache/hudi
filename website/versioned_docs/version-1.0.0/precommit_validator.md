---
title: Data Quality
keywords: [ hudi, quality, expectations, pre-commit validator]
---

Data quality refers to the overall accuracy, completeness, consistency, and validity of data. Ensuring data quality is vital for accurate analysis and reporting, as well as for compliance with regulations and maintaining trust in your organization's data infrastructure.

Hudi offers **Pre-Commit Validators** that allow you to ensure that your data meets certain data quality expectations as you are writing with Hudi Streamer or Spark Datasource writers.

:::note
Pre-commit validators are skipped when using the [BULK_INSERT](write_operations#bulk_insert) write operation type.
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
Hudi offers a [commit notification service](platform_services_post_commit_callback) that can be configured to trigger notifications about write commits.

The commit notification service can be combined with pre-commit validators to send a notification when a commit fails a validation. This is possible by passing details about the validation as a custom value to the HTTP endpoint.

## Related Resources
<h3>Videos</h3>

* [Learn About Apache Hudi Pre Commit Validator with Hands on Lab](https://www.youtube.com/watch?v=KNzs9dj_Btc)
