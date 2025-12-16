/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.hudi.client.validator.{SqlQueryEqualityPreCommitValidator, SqlQueryInequalityPreCommitValidator}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig.{TIMESTAMP_INPUT_DATE_FORMAT, TIMESTAMP_OUTPUT_DATE_FORMAT, TIMESTAMP_TYPE_FIELD}
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.{HoodiePreCommitValidatorConfig, HoodieWriteConfig}
import org.apache.hudi.exception.{HoodieUpsertException, HoodieValidationException}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.{DataSourceTestUtils, SparkClientFunctionalTestHarness}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, CsvSource, MethodSource, ValueSource}
import org.junit.jupiter.params.provider.Arguments.arguments

import scala.collection.JavaConverters._


@Tag("functional")
class TestCOWDataSourceStorage extends SparkClientFunctionalTestHarness {

  var commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "false"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  override def conf: SparkConf = conf(getSparkSqlConf)

  @ParameterizedTest
  @CsvSource(value = Array(
    "true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key",
    "true|org.apache.hudi.keygen.ComplexKeyGenerator|_row_key,fare.currency",
    "true|org.apache.hudi.keygen.TimestampBasedKeyGenerator|_row_key",
    "false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key",
    "false|org.apache.hudi.keygen.ComplexKeyGenerator|_row_key,fare.currency",
    "false|org.apache.hudi.keygen.TimestampBasedKeyGenerator|_row_key"
  ), delimiter = '|')
  def testCopyOnWriteStorage(isMetadataEnabled: Boolean, keyGenClass: String, recordKeys: String): Unit = {
    var options: Map[String, String] = commonOpts ++ Map(
      HoodieMetadataConfig.ENABLE.key -> String.valueOf(isMetadataEnabled),
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> keyGenClass,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeys,
      HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key -> "true")

    val readOptions = Map(HoodieMetadataConfig.ENABLE.key() -> String.valueOf(isMetadataEnabled))

    val isTimestampBasedKeyGen: Boolean = classOf[TimestampBasedKeyGenerator].getName.equals(keyGenClass)
    if (isTimestampBasedKeyGen) {
      options += DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "_row_key"
      options += TIMESTAMP_TYPE_FIELD.key -> "DATE_STRING"
      options += TIMESTAMP_INPUT_DATE_FORMAT.key -> "yyyy/MM/dd"
      options += TIMESTAMP_OUTPUT_DATE_FORMAT.key -> "yyyyMMdd"
    }
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
    // Insert Operation
    val records0 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF0 = spark.read.json(spark.sparkContext.parallelize(records0, 2))
    inputDF0.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val completionTime1 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))

    // Snapshot query
    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOptions)
      .load(basePath)
    assertEquals(100, snapshotDF1.count())

    val records1 = recordsToStrings(dataGen.generateUpdates("001", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    val verificationRowKey = inputDF1.limit(1).select("_row_key").first.getString(0)
    var updateDf: DataFrame = null
    if (isTimestampBasedKeyGen) {
      // update current_ts to be same as original record so that partition path does not change with timestamp based key gen
      val originalRow = snapshotDF1.filter(col("_row_key") === verificationRowKey).collectAsList().get(0)
      updateDf = inputDF1.filter(col("_row_key") === verificationRowKey)
        .withColumn(verificationCol, lit(updatedVerificationVal))
        .withColumn("current_ts", lit(originalRow.getAs[Long]("current_ts")))
        .limit(1)
      val updatedRow = updateDf.collectAsList().get(0)
      assertEquals(originalRow.getAs[Long]("current_ts"), updatedRow.getAs[Long]("current_ts"));
    } else {
      updateDf = snapshotDF1.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))
    }

    updateDf.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    val completionTime2 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)
    val snapshotDF2 = spark.read.format("hudi")
      .options(readOptions)
      .load(basePath)
    assertEquals(100, snapshotDF2.count())
    assertEquals(updatedVerificationVal, snapshotDF2.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))

    // Upsert Operation without Hudi metadata columns
    val records2 = recordsToStrings(dataGen.generateUpdates("002", 100)).asScala.toList
    var inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))

    if (isTimestampBasedKeyGen) {
      // in case of Timestamp based key gen, current_ts should not be updated. but dataGen.generateUpdates() would have updated
      // the value of current_ts. So, we need to revert it back to original value.
      // here is what we are going to do. Copy values to temp columns, join with original df and update the current_ts
      // and drop the temp columns.

      val inputDF2WithTempCols = inputDF2.withColumn("current_ts_temp", col("current_ts"))
        .withColumn("_row_key_temp", col("_row_key"))
      val originalRowCurrentTsDf = inputDF0.select("_row_key", "current_ts")
      // join with original df
      val joinedDf = inputDF2WithTempCols.drop("_row_key", "current_ts").join(originalRowCurrentTsDf, (inputDF2WithTempCols("_row_key_temp") === originalRowCurrentTsDf("_row_key")))
      // copy values from temp back to original cols and drop temp cols
      inputDF2 = joinedDf.withColumn("current_ts_temp", col("current_ts"))
        .drop("current_ts", "_row_key_temp").withColumn("current_ts", col("current_ts_temp"))
        .drop("current_ts_temp")
    }

    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime3 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Snapshot Query
    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .options(readOptions)
      .load(basePath)
    assertEquals(100, snapshotDF3.count()) // still 100, since we only updated

    // Read Incremental Query
    // we have 2 commits, try pulling the first commit (which is not the latest)
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0)
    // Setting HoodieROTablePathFilter here to test whether pathFilter can filter out correctly for IncrementalRelation
    spark.sparkContext.hadoopConfiguration.set("mapreduce.input.pathFilter.class", "org.apache.hudi.hadoop.HoodieROTablePathFilter")
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .options(readOptions)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "000")
      .option(DataSourceReadOptions.END_COMMIT.key, completionTime1)
      .load(basePath)
    assertEquals(100, hoodieIncViewDF1.count()) // 100 initial inserts must be pulled
    spark.sparkContext.hadoopConfiguration.unset("mapreduce.input.pathFilter.class")
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0))

    // Test incremental query has no instant in range
    val emptyIncDF = spark.read.format("org.apache.hudi")
      .options(readOptions)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "000")
      .option(DataSourceReadOptions.END_COMMIT.key, "002")
      .load(basePath)
    assertEquals(0, emptyIncDF.count())

    // Upsert an empty dataFrame
    val emptyRecords = recordsToStrings(dataGen.generateUpdates("003", 0)).asScala.toList
    val emptyDF = spark.read.json(spark.sparkContext.parallelize(emptyRecords, 1))
    emptyDF.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    // pull the latest commit
    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .options(readOptions)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, completionTime2)
      .load(basePath)

    assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime3, countsPerCommit(0).get(0))

    // pull the latest commit within certain partitions
    val hoodieIncViewDF3 = spark.read.format("org.apache.hudi")
      .options(readOptions)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, completionTime2)
      .option(DataSourceReadOptions.INCR_PATH_GLOB.key, if (isTimestampBasedKeyGen) "/2016*/*" else "/2016/*/*/*")
      .load(basePath)
    assertEquals(hoodieIncViewDF2
      .filter(col("_hoodie_partition_path").startsWith("2016")).count(), hoodieIncViewDF3.count())

    val timeTravelDF = spark.read.format("org.apache.hudi")
      .options(readOptions)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "000")
      .option(DataSourceReadOptions.END_COMMIT.key, completionTime1)
      .load(basePath)
    assertEquals(100, timeTravelDF.count()) // 100 initial inserts must be pulled
  }

  @ParameterizedTest
  @ValueSource(strings = Array("insert_overwrite", "delete_partition"))
  def testArchivalWithReplaceCommitActions(writeOperation: String): Unit = {

    val dataGen = new HoodieTestDataGenerator()
    // use this to generate records only for certain partitions.
    val dataGenPartition1 = new HoodieTestDataGenerator(Array[String](HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH))
    val dataGenPartition2 = new HoodieTestDataGenerator(Array[String](HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH))

    // do one bulk insert to all partitions
    val records = recordsToStrings(dataGen.generateInserts("%05d".format(1), 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    val partition1RecordCount = inputDF.filter(row => row.getAs("partition_path")
      .equals(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).count()
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option("hoodie.keep.min.commits", "2")
      .option("hoodie.keep.max.commits", "3")
      .option("hoodie.clean.commits.retained", "1")
      .option("hoodie.metadata.enable", "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertRecordCount(basePath, 100)

    // issue delete partition to partition1
    writeRecords(2, dataGenPartition1, writeOperation, basePath)

    val expectedRecCount = if (writeOperation.equals(DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)) {
      200 - partition1RecordCount
    } else {
      100 - partition1RecordCount
    }
    assertRecordCount(basePath, expectedRecCount)

    // add more data to partition2.
    for (i <- 3 to 7) {
      writeRecords(i, dataGenPartition2, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL, basePath)
    }

    assertRecordCount(basePath, expectedRecCount + 500)
    val metaClient = createMetaClient(spark, basePath)
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    // assert replace commit is archived and not part of active timeline.
    assertFalse(commits.contains(HoodieTimeline.REPLACE_COMMIT_ACTION))
    // assert that archival timeline has replace commit actions.
    val archivedTimeline = metaClient.getArchivedTimeline();
    assertTrue(archivedTimeline.getInstants.toArray.map(instant => instant.asInstanceOf[HoodieInstant].getAction)
      .filter(action => action.equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).size > 0)
  }

  @ParameterizedTest
  @MethodSource(Array("testSqlValidatorParams"))
  def testPreCommitValidationWithSQLQueryEqualityInequality(preCommitValidatorClassName: String,
                                                            sqlQuery: String,
                                                            isTablePartitioned: java.lang.Boolean,
                                                            lastWriteInSamePartition: java.lang.Boolean,
                                                            shouldSucceed: java.lang.Boolean): Unit = {
    var options: Map[String, String] = commonOpts ++ Map(
      DataSourceWriteOptions.OPERATION.key -> WriteOperationType.INSERT.value,
      HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key -> preCommitValidatorClassName)

    if (!isTablePartitioned) {
      options ++= Map(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key
        -> classOf[NonpartitionedKeyGenerator].getCanonicalName,
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "")
    }

    if (classOf[SqlQueryEqualityPreCommitValidator[_, _, _, _]]
      .getCanonicalName.equals(preCommitValidatorClassName)) {
      options += (HoodiePreCommitValidatorConfig.EQUALITY_SQL_QUERIES.key -> sqlQuery)
    } else if (classOf[SqlQueryInequalityPreCommitValidator[_, _, _, _]]
      .getCanonicalName.equals(preCommitValidatorClassName)) {
      options += (HoodiePreCommitValidatorConfig.INEQUALITY_SQL_QUERIES.key -> sqlQuery)
    }

    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val fs = HadoopFSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    val records = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList

    // First commit, new partition, no existing table schema
    // Validation should succeed
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    val inputDF1 = inputDF.filter(
      col("partition") === HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
    inputDF1.write.format("hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Second commit, new partition, has existing table schema
    // Validation should succeed
    val inputDF2All = inputDF.filter(
      col("partition") === HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)
    val count = inputDF2All.count.toInt
    val input2Rows = inputDF2All.take(count)
    val firstHalfCount = count / 2
    val inputDF2 = spark.createDataFrame(
      spark.sparkContext.parallelize(input2Rows.slice(0, firstHalfCount)), inputDF2All.schema)
    inputDF2.write.format("hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Third commit, new or existing partition, overwrite "driver" column to null for validation
    // Validation should succeed or fail, based on the query
    val inputDF3Original = if (lastWriteInSamePartition) {
      spark.createDataFrame(
        spark.sparkContext.parallelize(input2Rows.slice(firstHalfCount, count)), inputDF2All.schema)
    } else {
      inputDF.filter(
        col("partition") === HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    }
    val inputDF3 = inputDF3Original.withColumn("driver", lit(null).cast(StringType))

    if (shouldSucceed) {
      inputDF3.write.format("hudi")
        .options(options)
        .mode(SaveMode.Append)
        .save(basePath)
      assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())
    } else {
      assertThrowsWithPreCommitValidator(new Executable() {
        override def execute(): Unit = {
          inputDF3.write.format("hudi")
            .options(options)
            .mode(SaveMode.Append)
            .save(basePath)
        }
      })
    }
  }

  def assertThrowsWithPreCommitValidator(executable: Executable): Unit = {
    val thrown = assertThrows(
      classOf[HoodieUpsertException],
      executable,
      "Commit should fail due to HoodieUpsertException with pre-commit validator.")
    assertTrue(thrown.getCause.isInstanceOf[HoodieValidationException])
    assertTrue(thrown.getCause.getMessage.contains("At least one pre-commit validation failed"))
  }

  def writeRecords(commitTime: Int, dataGen: HoodieTestDataGenerator, writeOperation: String, basePath: String): Unit = {
    val records = recordsToStrings(dataGen.generateInserts("%05d".format(commitTime), 100)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option("hoodie.keep.min.commits", "2")
      .option("hoodie.keep.max.commits", "3")
      .option("hoodie.clean.commits.retained", "1")
      .option("hoodie.metadata.enable", "false")
      .option(DataSourceWriteOptions.OPERATION.key, writeOperation)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  def assertRecordCount(basePath: String, expectedRecordCount: Long): Unit = {
    val snapshotDF = spark.read.format("org.apache.hudi").load(basePath)
    assertEquals(expectedRecordCount, snapshotDF.count())
  }
}

object TestCOWDataSourceStorage {
  private final val SQL_QUERY_EQUALITY_VALIDATOR_CLASS_NAME =
    classOf[SqlQueryEqualityPreCommitValidator[_, _, _, _]].getCanonicalName
  private final val SQL_QUERY_INEQUALITY_VALIDATOR_CLASS_NAME =
    classOf[SqlQueryInequalityPreCommitValidator[_, _, _, _]].getCanonicalName
  private final val SQL_DRIVER_IS_NULL = "select count(*) from <TABLE_NAME> where driver is null"
  private final val SQL_RIDER_IS_NULL = "select count(*) from <TABLE_NAME> where rider is null"
  private final val SQL_DRIVER_IS_NOT_NULL = "select count(*) from <TABLE_NAME> where driver is not null"
  private final val SQL_RIDER_IS_NOT_NULL = "select count(*) from <TABLE_NAME> where rider is not null"

  def testSqlValidatorParams(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments(SQL_QUERY_EQUALITY_VALIDATOR_CLASS_NAME, SQL_DRIVER_IS_NULL,
        new java.lang.Boolean(true), new java.lang.Boolean(false), new java.lang.Boolean(false)),
      arguments(SQL_QUERY_EQUALITY_VALIDATOR_CLASS_NAME, SQL_DRIVER_IS_NULL,
        new java.lang.Boolean(true), new java.lang.Boolean(true), new java.lang.Boolean(false)),
      arguments(SQL_QUERY_EQUALITY_VALIDATOR_CLASS_NAME, SQL_RIDER_IS_NULL,
        new java.lang.Boolean(true), new java.lang.Boolean(true), new java.lang.Boolean(true)),
      arguments(SQL_QUERY_EQUALITY_VALIDATOR_CLASS_NAME, SQL_DRIVER_IS_NULL,
        new java.lang.Boolean(false), new java.lang.Boolean(true), new java.lang.Boolean(false)),
      arguments(SQL_QUERY_EQUALITY_VALIDATOR_CLASS_NAME, SQL_RIDER_IS_NULL,
        new java.lang.Boolean(false), new java.lang.Boolean(true), new java.lang.Boolean(true)),
      arguments(SQL_QUERY_INEQUALITY_VALIDATOR_CLASS_NAME, SQL_DRIVER_IS_NOT_NULL,
        new java.lang.Boolean(true), new java.lang.Boolean(false), new java.lang.Boolean(false)),
      arguments(SQL_QUERY_INEQUALITY_VALIDATOR_CLASS_NAME, SQL_DRIVER_IS_NOT_NULL,
        new java.lang.Boolean(true), new java.lang.Boolean(true), new java.lang.Boolean(false)),
      arguments(SQL_QUERY_INEQUALITY_VALIDATOR_CLASS_NAME, SQL_RIDER_IS_NOT_NULL,
        new java.lang.Boolean(true), new java.lang.Boolean(true), new java.lang.Boolean(true)),
      arguments(SQL_QUERY_INEQUALITY_VALIDATOR_CLASS_NAME, SQL_DRIVER_IS_NOT_NULL,
        new java.lang.Boolean(false), new java.lang.Boolean(true), new java.lang.Boolean(false)),
      arguments(SQL_QUERY_INEQUALITY_VALIDATOR_CLASS_NAME, SQL_RIDER_IS_NOT_NULL,
        new java.lang.Boolean(false), new java.lang.Boolean(true), new java.lang.Boolean(true))
    )
  }
}
