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

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.HoodieDataSourceHelpers.{hasNewCommits, latestCompletedCommit, listCommitsSince, streamCompletedInstantSince}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.model.WriteOperationType.{BULK_INSERT, INSERT, UPSERT}
import org.apache.hudi.common.table.timeline.TimelineUtils
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.scalatest.Inspectors.forAll

import java.io.File

import scala.collection.JavaConverters._

@SparkSQLCoreFlow
class TestSparkSqlCoreFlow extends HoodieSparkSqlTestBase {
  val colsToCompare = "timestamp, _row_key, partition_path, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare.amount, fare.currency, _hoodie_is_deleted"

  //params for core flow tests
  val params: List[String] = List(
    "COPY_ON_WRITE|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "COPY_ON_WRITE|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "COPY_ON_WRITE|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "COPY_ON_WRITE|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "COPY_ON_WRITE|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "MERGE_ON_READ|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "MERGE_ON_READ|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "MERGE_ON_READ|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "MERGE_ON_READ|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "MERGE_ON_READ|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "MERGE_ON_READ|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE"
  )

  //extracts the params and runs each core flow test
  forAll (params) { (paramStr: String) =>
    test(s"Core flow with params: $paramStr") {
      val splits = paramStr.split('|')
      withTempDir { basePath =>
        testCoreFlows(basePath,
          tableType = splits(0),
          isMetadataEnabled = splits(1).toBoolean,
          keyGenClass = splits(2),
          indexType = splits(3))
      }
    }
  }

  def testCoreFlows(basePath: File, tableType: String, isMetadataEnabled: Boolean,
                    keyGenClass: String, indexType: String): Unit = {
    //Create table and set up for testing
    val tableName = generateTableName
    val tableBasePath = basePath.getCanonicalPath + "/" + tableName
    val writeOptions = getWriteOptions(tableName, tableType, keyGenClass, indexType)
    createTable(tableName, keyGenClass, writeOptions, tableBasePath)
    val fs = HadoopFSUtils.getFs(tableBasePath, spark.sparkContext.hadoopConfiguration)
    val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)

    //Bulk insert first set of records
    val inputDf0 = generateInserts(dataGen, "000", 100).cache()
    insertInto(tableName, tableBasePath, inputDf0, BULK_INSERT, isMetadataEnabled, 1)
    val inputDf0Rows = canonicalizeDF(inputDf0).collect()
    inputDf0.unpersist(true)
    assertTrue(hasNewCommits(fs, tableBasePath, "000"))
    //Verify bulk insert works correctly
    val snapshotDf1 = doSnapshotRead(tableName, isMetadataEnabled)
    val snapshotDf1Rows = canonicalizeDF(dropMetaColumns(snapshotDf1)).collect()
    assertEquals(100, snapshotDf1.count())
    compareEntireInputRowsWithHudiRows(inputDf0Rows, snapshotDf1Rows)

    //Test updated records
    val updateDf = generateUniqueUpdates(dataGen, "001", 50).cache()
    insertInto(tableName, tableBasePath, updateDf, UPSERT, isMetadataEnabled, 2)
    val commitInstant2 = latestCompletedCommit(fs, tableBasePath)
    val commitCompletionTime2 = commitInstant2.getCompletionTime
    val snapshotDf2 = doSnapshotRead(tableName, isMetadataEnabled)
    val snapshotDf2Rows = canonicalizeDF(dropMetaColumns(snapshotDf2)).collect()
    assertEquals(100, snapshotDf2Rows.length)
    compareUpdateRowsWithHudiRows(
      canonicalizeDF(updateDf).collect(),
      snapshotDf2Rows,
      snapshotDf1Rows)
    updateDf.unpersist(true)

    val inputDf2 = generateUniqueUpdates(dataGen, "002", 60).cache()
    val uniqueKeyCnt2 = inputDf2.select("_row_key").distinct().count()
    insertInto(tableName, tableBasePath, inputDf2, UPSERT, isMetadataEnabled, 3)
    val commitInstant3 = latestCompletedCommit(fs, tableBasePath)
    val commitCompletionTime3 = commitInstant3.getCompletionTime
    assertEquals(3, listCommitsSince(fs, tableBasePath, "000").size())

    val snapshotDf3Rows = canonicalizeDF(doSnapshotRead(tableName, isMetadataEnabled)).collect()
    assertEquals(100, snapshotDf3Rows.length)
    compareUpdateRowsWithHudiRows(canonicalizeDF(inputDf2).collect(),
      snapshotDf3Rows, snapshotDf3Rows)
    inputDf2.unpersist(true)

    // Read Incremental Query, uses hudi_table_changes() table valued function for spark sql
    // we have 2 commits, try pulling the first commit (which is not the latest)
    //HUDI-5266
    val firstCommitInstant = streamCompletedInstantSince(fs, tableBasePath, "000").findFirst().get()
    val firstCommitCompletionTime = firstCommitInstant.getCompletionTime
    val hoodieIncViewDf1 = spark.sql(s"select * from hudi_table_changes('$tableName', 'latest_state', 'earliest', '$firstCommitCompletionTime')")

    assertEquals(100, hoodieIncViewDf1.count()) // 100 initial inserts must be pulled
    var countsPerCommit = hoodieIncViewDf1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommitInstant.requestedTime(), countsPerCommit(0).get(0).toString)

    val inputDf3 = generateUniqueUpdates(dataGen, "003", 80)
    insertInto(tableName, tableBasePath, inputDf3, UPSERT, isMetadataEnabled, 4)

    //another incremental query with commit2 and commit3
    //HUDI-5266
    val commitCompletionTime2_1 = addMinimumTimeUnit(commitCompletionTime2)
    val hoodieIncViewDf2 = spark.sql(s"select * from hudi_table_changes('$tableName', 'latest_state', '$commitCompletionTime2_1', '$commitCompletionTime3')")

    assertEquals(uniqueKeyCnt2, hoodieIncViewDf2.count()) // 60 records must be pulled
    countsPerCommit = hoodieIncViewDf2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstant3.requestedTime(), countsPerCommit(0).get(0).toString)

    val commit2RequestTime = commitInstant2.requestedTime()
    val timeTravelDf = spark.sql(s"select * from $tableName timestamp as of '$commit2RequestTime'")
    val timeTravelDfRows = dropMetaColumns(canonicalizeDF(timeTravelDf)).collect()
    assertEquals(100, timeTravelDfRows.length)
    compareEntireInputRowsWithHudiRows(snapshotDf2Rows, timeTravelDfRows)
    timeTravelDf.unpersist(true)

    if (tableType.equals("MERGE_ON_READ")) {
      val readOptRows = canonicalizeDF(doMORReadOptimizedQuery(isMetadataEnabled, tableBasePath)).collect()
      compareEntireInputRowsWithHudiRows(inputDf0Rows, readOptRows)

      val snapshotDf4Rows = canonicalizeDF(doSnapshotRead(tableName, isMetadataEnabled)).collect()

      // trigger compaction and try out Read optimized query.
      val inputDf4 = generateUniqueUpdates(dataGen, "004", 40).cache()
      //count is increased by 2 because inline compaction will add extra commit to the timeline
      doInlineCompact(tableName, tableBasePath, inputDf4, UPSERT, isMetadataEnabled, "3", 6)
      val snapshotDf5Rows = canonicalizeDF(doSnapshotRead(tableName, isMetadataEnabled)).collect()
      compareUpdateRowsWithHudiRows(canonicalizeDF(inputDf4).collect(), snapshotDf5Rows, snapshotDf4Rows)
      inputDf4.unpersist(true)

      // compaction is expected to have completed. both RO and RT are expected to return same results.
      compareROAndRT(isMetadataEnabled, tableName, tableBasePath)
    }
  }

  private def canonicalizeDF(inputDf0: DataFrame) = {
    inputDf0.selectExpr(colsToCompare.split(","): _*)
  }

  private def addMinimumTimeUnit(commitCompletionTime2: String) : String = {
    String.valueOf(commitCompletionTime2.toLong + 1)
  }

  def doSnapshotRead(tableName: String, isMetadataEnabledOnRead: Boolean): sql.DataFrame = {
    try {
      spark.sql(s"set hoodie.datasource.query.type=$QUERY_TYPE_SNAPSHOT_OPT_VAL")
      spark.sql(s"set hoodie.metadata.enable=$isMetadataEnabledOnRead")
      spark.sql(s"select * from $tableName")
    } finally {
      spark.conf.unset("hoodie.datasource.query.type")
      spark.conf.unset("hoodie.metadata.enable")
    }
  }

  def doInlineCompact(tableName: String, tableBasePath: String, recDf: sql.DataFrame, writeOp: WriteOperationType,
                      isMetadataEnabledOnWrite: Boolean, numDeltaCommits: String, count: Int): Unit = {
    try {
      spark.sql("set hoodie.compact.inline=true")
      spark.sql(s"set hoodie.compact.inline.max.delta.commits=$numDeltaCommits")
      insertInto(tableName, tableBasePath, recDf, writeOp, isMetadataEnabledOnWrite, count)
    } finally {
      spark.conf.unset("hoodie.compact.inline")
      spark.conf.unset("hoodie.compact.inline.max.delta.commits")
    }
  }

  def getWriteOptions(tableName: String, tableType: String, keyGenClass: String, indexType: String): String = {
    val typeString = if (tableType.equals("COPY_ON_WRITE")) {
      "cow"
    } else if (tableType.equals("MERGE_ON_READ")) {
      "mor"
    } else {
      tableType
    }

    s"""
       |tblproperties (
       |  type = '$typeString',
       |  primaryKey = '_row_key',
       |  orderingFields = 'timestamp',
       |  hoodie.bulkinsert.shuffle.parallelism = 4,
       |  hoodie.database.name = "databaseName",
       |  hoodie.table.keygenerator.class = '$keyGenClass',
       |  hoodie.delete.shuffle.parallelism = 2,
       |  hoodie.index.type = "$indexType",
       |  hoodie.insert.shuffle.parallelism = 4,
       |  hoodie.table.name = "$tableName",
       |  hoodie.upsert.shuffle.parallelism = 4
       | )""".stripMargin
  }

  def assertOperation(basePath: String, count: Int, operationType: WriteOperationType): Boolean = {
    val metaClient = createMetaClient(spark, basePath)
    val timeline = metaClient.getActiveTimeline.getAllCommitsTimeline
    assert(timeline.countInstants() == count)
    val latestCommit = timeline.lastInstant()
    assert(latestCommit.isPresent)
    assert(latestCommit.get().isCompleted)
    val metadata = TimelineUtils.getCommitMetadata(latestCommit.get(), timeline)
    metadata.getOperationType.equals(operationType)
  }

  def insertInto(tableName: String, tableBasePath: String, inputDf: sql.DataFrame, writeOp: WriteOperationType,
                 isMetadataEnabledOnWrite: Boolean, count: Int): Unit = {
    inputDf.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon", "fare",
      "_hoodie_is_deleted", "partition_path").createOrReplaceTempView("insert_temp_table")
    try {
      spark.sql(s"set hoodie.metadata.enable=${String.valueOf(isMetadataEnabledOnWrite)}")
      if (writeOp.equals(UPSERT)) {
        spark.sql("set hoodie.sql.bulk.insert.enable=false")
        spark.sql("set hoodie.sql.insert.mode=upsert")
        spark.sql(
          s"""
             | merge into $tableName as target
             | using insert_temp_table as source
             | on target._row_key = source._row_key and
             | target.partition_path = source.partition_path
             | when matched then update set *
             | when not matched then insert *
             | """.stripMargin)
      } else if (writeOp.equals(BULK_INSERT)) {
        spark.sql("set hoodie.sql.bulk.insert.enable=true")
        spark.sql("set hoodie.sql.insert.mode=non-strict")
        spark.sql(s"insert into $tableName select * from insert_temp_table")
      } else if (writeOp.equals(INSERT)) {
        spark.sql("set hoodie.sql.bulk.insert.enable=false")
        spark.sql("set hoodie.sql.insert.mode=non-strict")
        spark.sql(s"insert into $tableName select * from insert_temp_table")
      }
      assertOperation(tableBasePath, count, writeOp)
    } finally {
      spark.conf.unset("hoodie.metadata.enable")
      spark.conf.unset("hoodie.datasource.write.keygenerator.class")
      spark.conf.unset("hoodie.sql.bulk.insert.enable")
      spark.conf.unset("hoodie.sql.insert.mode")
    }
  }

  def createTable(tableName: String, keyGenClass: String, writeOptions: String, tableBasePath: String): Unit = {
    //If you have partitioned by (partition_path) with nonpartitioned keygen, the partition_path will be empty in the table
    val partitionedBy = if (!keyGenClass.equals(classOf[NonpartitionedKeyGenerator].getName)) {
      "partitioned by (partition_path)"
    } else {
      ""
    }

    spark.sql(
      s"""
         | create table $tableName (
         |  timestamp long,
         |  _row_key string,
         |  rider string,
         |  driver string,
         |  begin_lat double,
         |  begin_lon double,
         |  end_lat double,
         |  end_lon double,
         |  fare STRUCT<
         |    amount: double,
         |    currency: string >,
         |  _hoodie_is_deleted boolean,
         |  partition_path string
         |) using hudi
         | $partitionedBy
         | $writeOptions
         | location '$tableBasePath'
         |
    """.stripMargin)
  }

  def generateInserts(dataGen: HoodieTestDataGenerator, instantTime: String, n: Int): sql.DataFrame = {
    val recs = dataGen.generateInsertsNestedExample(instantTime, n)
    spark.read.json(spark.sparkContext.parallelize(recordsToStrings(recs).asScala.toSeq, 2))
  }

  def generateUniqueUpdates(dataGen: HoodieTestDataGenerator, instantTime: String, n: Int): sql.DataFrame = {
    val recs = dataGen.generateUniqueUpdatesNestedExample(instantTime, n)
    spark.read.json(spark.sparkContext.parallelize(recordsToStrings(recs).asScala.toSeq, 2))
  }

  // Helper function to check if two rows are equal (comparing only the columns we care about)
  def rowsEqual(row1: Row, row2: Row): Boolean = {
    // Get schemas from rows
    val schema1 = row1.asInstanceOf[GenericRowWithSchema].schema
    val schema2 = row2.asInstanceOf[GenericRowWithSchema].schema

    // Verify schemas are identical
    if (schema1 != schema2) {
      throw new AssertionError(
        s"""Schemas are different:
            |Schema 1: ${schema1.treeString}
            |Schema 2: ${schema2.treeString}""".stripMargin)
    }

    // Compare all fields using schema
    schema1.fields.forall { field =>
      val idx1 = row1.fieldIndex(field.name)
      val idx2 = row2.fieldIndex(field.name)
      row1.get(idx1) == row2.get(idx2)
    }
  }

  // Verify beforeRows + deltaRows = afterRows
  // Make sure rows in [[afterRows]] are presented in either [[deltaRows]] or [[beforeRows]]
  def compareUpdateRowsWithHudiRows(deltaRows: Array[Row], afterRows: Array[Row], beforeRows: Array[Row]): Unit = {
    // Helper function to get _row_key from a Row
    def getRowKey(row: Row): String = row.getAs[String]("_row_key")

    // Create hashmaps for O(1) lookups
    val deltaRowsMap = deltaRows.map(row => getRowKey(row) -> row).toMap
    val beforeRowsMap = beforeRows.map(row => getRowKey(row) -> row).toMap
    // Ensure no duplicated record keys.
    assertEquals(deltaRowsMap.size, deltaRows.length)
    assertEquals(beforeRowsMap.size, beforeRows.length)

    // Check that all input rows exist in afterRows
    deltaRows.foreach { inputRow =>
      val key = getRowKey(inputRow)
      val hudiRow = afterRows.find(row => getRowKey(row) == key)
      assertTrue(hudiRow.isDefined && rowsEqual(inputRow, hudiRow.get),
        s"Input row with _row_key: $key not found in Hudi rows or content mismatch")
    }

    // Check that each hudi row either exists in input or before
    afterRows.foreach { hudiRow =>
      val key = getRowKey(hudiRow)
      val foundInInput = deltaRowsMap.get(key).exists(row => rowsEqual(hudiRow, row))
      val foundInBefore = !foundInInput && beforeRowsMap.get(key).exists(row => rowsEqual(hudiRow, row))

      assertTrue(foundInInput || foundInBefore,
        s"Hudi row with _row_key: $key not found in either input or before rows")
    }
  }

  def compareEntireInputRowsWithHudiRows(expectedRows: Array[Row], actualRows: Array[Row]): Unit = {
    compareUpdateRowsWithHudiRows(Array.empty, expectedRows, actualRows)
  }

  def doMORReadOptimizedQuery(isMetadataEnabledOnRead: Boolean, basePath: String): sql.DataFrame = {
    spark.read.format("org.apache.hudi")
      .option(QUERY_TYPE.key, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key(), isMetadataEnabledOnRead)
      .load(basePath)
  }

  def compareROAndRT(isMetadataEnabledOnRead: Boolean, tableName: String, basePath: String): Unit = {
    val roDf = doMORReadOptimizedQuery(isMetadataEnabledOnRead, basePath)
    val rtDf = doSnapshotRead(tableName, isMetadataEnabledOnRead)
    dropMetaColumns(roDf).createOrReplaceTempView("hudiTbl1")
    dropMetaColumns(rtDf).createOrReplaceTempView("hudiTbl2")

    val hudiDf1ToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl1")
    val hudiDf2ToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl2")

    assertEquals(hudiDf1ToCompare.intersect(hudiDf2ToCompare).count, hudiDf1ToCompare.count)
    assertEquals(hudiDf1ToCompare.except(hudiDf2ToCompare).count, 0)
  }

  def dropMetaColumns(inputDf: sql.DataFrame): sql.DataFrame = {
    inputDf.drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD,
      HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD,
      HoodieRecord.FILENAME_METADATA_FIELD)
  }

  //params for immutable user flow
  val paramsForImmutable: List[String] = List(
    "COPY_ON_WRITE|insert|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|insert|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|insert|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "COPY_ON_WRITE|insert|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "COPY_ON_WRITE|insert|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "COPY_ON_WRITE|insert|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "COPY_ON_WRITE|insert|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|insert|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "MERGE_ON_READ|insert|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|insert|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|insert|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "MERGE_ON_READ|insert|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "MERGE_ON_READ|insert|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "MERGE_ON_READ|insert|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "MERGE_ON_READ|insert|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "MERGE_ON_READ|insert|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|bulk_insert|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|bulk_insert|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "COPY_ON_WRITE|bulk_insert|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "COPY_ON_WRITE|bulk_insert|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "COPY_ON_WRITE|bulk_insert|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|bulk_insert|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "MERGE_ON_READ|bulk_insert|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|bulk_insert|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|bulk_insert|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "MERGE_ON_READ|bulk_insert|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
    "MERGE_ON_READ|bulk_insert|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "MERGE_ON_READ|bulk_insert|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
    "MERGE_ON_READ|bulk_insert|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
    "MERGE_ON_READ|bulk_insert|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE"
  )

  //extracts the params and runs each immutable user flow test
  forAll(paramsForImmutable) { (paramStr: String) =>
    test(s"Immutable user flow with params: $paramStr") {
      val splits = paramStr.split('|')
      withTempDir { basePath =>
        val writeOp = if (splits(1).equals("insert")) {
          INSERT
        } else if (splits(1).equals("bulk_insert")) {
          BULK_INSERT
        } else  {
          throw new UnsupportedOperationException("This test is only meant for immutable operations.")
        }
        testImmutableUserFlow(basePath,
          tableType = splits(0),
          writeOp = writeOp,
          isMetadataEnabled = splits(2).toBoolean,
          keyGenClass = splits(3),
          indexType = splits(4))
      }
    }
  }

  def testImmutableUserFlow(basePath: File, tableType: String, writeOp: WriteOperationType,
                            isMetadataEnabled: Boolean, keyGenClass: String,
                            indexType: String): Unit = {
    val tableName = generateTableName
    val tableBasePath = basePath.getCanonicalPath + "/" + tableName
    val writeOptions = getWriteOptions(tableName, tableType, keyGenClass, indexType)
    createTable(tableName, keyGenClass, writeOptions, tableBasePath)
    val fs = HadoopFSUtils.getFs(tableBasePath, spark.sparkContext.hadoopConfiguration)

    //Insert Operation
    val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
    val inputDf0 = generateInserts(dataGen, "000", 100).cache
    insertInto(tableName, tableBasePath, inputDf0, BULK_INSERT, isMetadataEnabled, 1)
    val inputDf0Rows = canonicalizeDF(inputDf0).collect()
    inputDf0.unpersist(true)

    assertTrue(hasNewCommits(fs, tableBasePath, "000"))

    //Snapshot query
    val snapshotDf1Rows = canonicalizeDF(doSnapshotRead(tableName, isMetadataEnabled)).collect()
    assertEquals(100, snapshotDf1Rows.length)
    compareEntireInputRowsWithHudiRows(inputDf0Rows, snapshotDf1Rows)

    val inputDf1 = generateInserts(dataGen, "001", 50).cache
    insertInto(tableName, tableBasePath, inputDf1, writeOp, isMetadataEnabled, 2)
    val inputDf1rows = canonicalizeDF(inputDf0).collect()
    inputDf1.unpersist(true)

    val snapshotDf2 = canonicalizeDF(doSnapshotRead(tableName, isMetadataEnabled)).collect()
    assertEquals(150, snapshotDf2.length)
    compareEntireInputRowsWithHudiRows(inputDf1rows ++ inputDf0Rows, snapshotDf2)

    val inputDf2 = generateInserts(dataGen, "002", 60).cache()
    insertInto(tableName, tableBasePath, inputDf2, writeOp, isMetadataEnabled, 3)
    val inputDf2rows = canonicalizeDF(inputDf0).collect()
    inputDf2.unpersist(true)

    assertEquals(3, listCommitsSince(fs, tableBasePath, "000").size())

    // Snapshot Query
    val snapshotDf3 = canonicalizeDF(doSnapshotRead(tableName, isMetadataEnabled)).collect()
    assertEquals(210, snapshotDf3.length)
    compareEntireInputRowsWithHudiRows(inputDf1rows ++ inputDf0Rows ++ inputDf2rows, snapshotDf3)
  }
}
