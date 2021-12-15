---
title: Data Quality
keywords: [ hudi, quality, expectations, pre-commit validator]
---

Apache Hudi has what are called **Pre-Commit Validators** that allow you to validate that your data meets certain data quality
expectations as you are writing with DeltaStreamer or Spark Datasource writers.

To configure pre-commit validators, use this setting `hoodie.precommit.validators=<comma separated list of validator class names>`.

Example:
```scala
spark.write.format("hudi")
    .option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator")
```

Today you can use any of these validators and even have the flexibility to extend your own:

## SQL Query Single Result
Can be used to validate that a query on the table results in a specific value.
- [org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQuerySingleResultPreCommitValidator.java)

Multiple queries separated by ';' delimiter are supported.Expected result is included as part of query separated by '#'. Example query: `query1#result1;query2#result2`

Example, "expect exactly 0 null rows":
```scala
import org.apache.hudi.config.HoodiePreCommitValidatorConfig._

df.write.format("hudi").mode(Overwrite).
  option(TABLE_NAME, tableName).
  option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator").
  option("hoodie.precommit.validators.single.value.sql.queries", "select count(*) from <TABLE_NAME> where col=null#0").
  save(basePath)
```

## SQL Query Equality
Can be used to validate for equality of rows before and after the commit.
- [org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQueryEqualityPreCommitValidator.java)

Example, "expect no change of null rows with this commit":
```scala
import org.apache.hudi.config.HoodiePreCommitValidatorConfig._

df.write.format("hudi").mode(Overwrite).
  option(TABLE_NAME, tableName).
  option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator").
  option("hoodie.precommit.validators.equality.sql.queries", "select count(*) from <TABLE_NAME> where col=null").
  save(basePath)
```

## SQL Query Inequality
Can be used to validate for inequality of rows before and after the commit.
- [org.apache.hudi.client.validator.SqlQueryInequalityPreCommitValidator](https://github.com/apache/hudi/blob/bf5a52e51bbeaa089995335a0a4c55884792e505/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/validator/SqlQueryInequalityPreCommitValidator.java)

Example, "expect there must be a change of null rows with this commit":
```scala
import org.apache.hudi.config.HoodiePreCommitValidatorConfig._

df.write.format("hudi").mode(Overwrite).
  option(TABLE_NAME, tableName).
  option("hoodie.precommit.validators", "org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator").
  option("hoodie.precommit.validators.inequality.sql.queries", "select count(*) from <TABLE_NAME> where col=null").
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
