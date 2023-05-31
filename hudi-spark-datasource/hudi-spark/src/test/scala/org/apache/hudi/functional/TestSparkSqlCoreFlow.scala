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

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.{DataSourceReadOptions, HoodieDataSourceHelpers, HoodieSparkUtils}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.scalatest.Inspectors.forAll

import java.io.File
import scala.collection.JavaConversions._

@SparkSQLCoreFlow
class TestSparkSqlCoreFlow extends HoodieSparkSqlTestBase {
  val colsToCompare = "timestamp, _row_key, partition_path, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare.amount, fare.currency, _hoodie_is_deleted"

  //params for core flow tests
  val params: List[String] = List(
        "COPY_ON_WRITE|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
        "COPY_ON_WRITE|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
        "COPY_ON_WRITE|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
        "COPY_ON_WRITE|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
        "COPY_ON_WRITE|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
        "COPY_ON_WRITE|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
        "COPY_ON_WRITE|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
        "COPY_ON_WRITE|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
        "COPY_ON_WRITE|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
        "MERGE_ON_READ|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
        "MERGE_ON_READ|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
        "MERGE_ON_READ|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
        "MERGE_ON_READ|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
        "MERGE_ON_READ|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
        "MERGE_ON_READ|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
        "MERGE_ON_READ|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
        "MERGE_ON_READ|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
        "MERGE_ON_READ|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM"
      )

  //extracts the params and runs each core flow test
  forAll (params) { (paramStr: String) =>
    test(s"Core flow with params: $paramStr") {
      val splits = paramStr.split('|')
      withTempDir { basePath =>
        testCoreFlows(basePath,
          tableType = splits(0),
          isMetadataEnabledOnWrite = splits(1).toBoolean,
          isMetadataEnabledOnRead = splits(2).toBoolean,
          keyGenClass = splits(3),
          indexType = splits(4))
      }
    }
  }

  def testCoreFlows(basePath: File, tableType: String, isMetadataEnabledOnWrite: Boolean, isMetadataEnabledOnRead: Boolean, keyGenClass: String, indexType: String): Unit = {
    //Create table and set up for testing
    val tableName = generateTableName
    val tableBasePath = basePath.getCanonicalPath + "/" + tableName
    val writeOptions = getWriteOptions(tableName, tableType, isMetadataEnabledOnWrite, keyGenClass, indexType)
    createTable(tableName, keyGenClass, writeOptions, tableBasePath)
    val fs = FSUtils.getFs(tableBasePath, spark.sparkContext.hadoopConfiguration)
    val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)

    //Bulk insert first set of records
    val inputDf0 = generateInserts(dataGen, "000", 100)
    inputDf0.cache()
    insertInto(tableName, inputDf0, "bulk_insert", isMetadataEnabledOnWrite, keyGenClass)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, tableBasePath, "000"))
    //Verify bulk insert works correctly
    val snapshotDf1 = doSnapshotRead(tableName, isMetadataEnabledOnRead)
    snapshotDf1.cache()
    assertEquals(100, snapshotDf1.count())
    compareEntireInputDfWithHudiDf(inputDf0, snapshotDf1)

    //Test updated records
    val updateDf = generateUniqueUpdates(dataGen, "001", 50)
    insertInto(tableName, updateDf, "upsert", isMetadataEnabledOnWrite, keyGenClass)
    val commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, tableBasePath)
    val snapshotDf2 = doSnapshotRead(tableName, isMetadataEnabledOnRead)
    snapshotDf2.cache()
    assertEquals(100, snapshotDf2.count())
    compareUpdateDfWithHudiDf(updateDf, snapshotDf2, snapshotDf1)

    val inputDf2 = generateUniqueUpdates(dataGen, "002", 60)
    val uniqueKeyCnt2 = inputDf2.select("_row_key").distinct().count()
    insertInto(tableName, inputDf2, "upsert", isMetadataEnabledOnWrite, keyGenClass)
    val commitInstantTime3 = HoodieDataSourceHelpers.latestCommit(fs, tableBasePath)
    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, tableBasePath, "000").size())

    val snapshotDf3 = doSnapshotRead(tableName, isMetadataEnabledOnRead)
    snapshotDf3.cache()
    assertEquals(100, snapshotDf3.count())
    compareUpdateDfWithHudiDf(inputDf2, snapshotDf3, snapshotDf3)

    // Read Incremental Query, need to use spark-ds because functionality does not exist for spark sql
    // we have 2 commits, try pulling the first commit (which is not the latest)
    //HUDI-5266
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, tableBasePath, "000").get(0)
    val hoodieIncViewDf1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, firstCommit)
      .load(tableBasePath)
    //val hoodieIncViewDf1 = doIncRead(tableName, isMetadataEnabledOnRead, "000", firstCommit)
    assertEquals(100, hoodieIncViewDf1.count()) // 100 initial inserts must be pulled
    var countsPerCommit = hoodieIncViewDf1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0).toString)

    val inputDf3 = generateUniqueUpdates(dataGen, "003", 80)
    insertInto(tableName, inputDf3, "upsert", isMetadataEnabledOnWrite, keyGenClass)

    //another incremental query with commit2 and commit3
    //HUDI-5266
    val hoodieIncViewDf2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime2)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), commitInstantTime3)
      .load(tableBasePath)

    assertEquals(uniqueKeyCnt2, hoodieIncViewDf2.count()) // 60 records must be pulled
    countsPerCommit = hoodieIncViewDf2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime3, countsPerCommit(0).get(0).toString)


    val timeTravelDf = if (HoodieSparkUtils.gteqSpark3_2_1) {
      spark.sql(s"select * from $tableName timestamp as of '$commitInstantTime2'")
    } else {
      //HUDI-5265
      spark.read.format("org.apache.hudi")
        .option("as.of.instant", commitInstantTime2)
        .load(tableBasePath)
    }
    assertEquals(100, timeTravelDf.count())
    compareEntireInputDfWithHudiDf(snapshotDf2, timeTravelDf)

    if (tableType.equals("MERGE_ON_READ")) {
      val readOptDf = doMORReadOptimizedQuery(isMetadataEnabledOnRead, tableBasePath)
      compareEntireInputDfWithHudiDf(inputDf0, readOptDf)

      val snapshotDf4 = doSnapshotRead(tableName, isMetadataEnabledOnRead)
      snapshotDf4.cache()

      // trigger compaction and try out Read optimized query.
      val inputDf4 = generateUniqueUpdates(dataGen, "004", 40)
      doInlineCompact(tableName, inputDf4, "upsert", isMetadataEnabledOnWrite, "3", keyGenClass)
      val snapshotDf5 = doSnapshotRead(tableName, isMetadataEnabledOnRead)
      snapshotDf5.cache()
      compareUpdateDfWithHudiDf(inputDf4, snapshotDf5, snapshotDf4)
      inputDf4.unpersist(true)
      snapshotDf5.unpersist(true)

      // compaction is expected to have completed. both RO and RT are expected to return same results.
      compareROAndRT(isMetadataEnabledOnRead, tableName, tableBasePath)
    }

    inputDf0.unpersist(true)
    updateDf.unpersist(true)
    inputDf2.unpersist(true)
    inputDf3.unpersist(true)
  }

  def doSnapshotRead(tableName: String, isMetadataEnabledOnRead: Boolean): sql.DataFrame = {
    spark.sql("set hoodie.datasource.query.type=\"snapshot\"")
    spark.sql(s"set hoodie.metadata.enable=${String.valueOf(isMetadataEnabledOnRead)}")
    spark.sql(s"select * from $tableName")
  }

  def doInlineCompact(tableName: String, recDf: sql.DataFrame, writeOp: String, isMetadataEnabledOnWrite: Boolean, numDeltaCommits: String, keyGenClass: String): Unit = {
    spark.sql("set hoodie.compact.inline=true")
    spark.sql(s"set hoodie.compact.inline.max.delta.commits=$numDeltaCommits")
    insertInto(tableName, recDf, writeOp, isMetadataEnabledOnWrite, keyGenClass)
    spark.sql("set hoodie.compact.inline=false")
  }

  def getWriteOptions(tableName: String, tableType: String, isMetadataEnabledOnWrite: Boolean,
                      keyGenClass: String, indexType: String): String = {
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
       |  preCombineField = 'timestamp',
       |  hoodie.bulkinsert.shuffle.parallelism = 4,
       |  hoodie.database.name = "databaseName",
       |  hoodie.table.keygenerator.class = '$keyGenClass',
       |  hoodie.delete.shuffle.parallelism = 2,
       |  hoodie.index.type = "$indexType",
       |  hoodie.insert.shuffle.parallelism = 4,
       |  hoodie.metadata.enable = ${String.valueOf(isMetadataEnabledOnWrite)},
       |  hoodie.table.name = "$tableName",
       |  hoodie.upsert.shuffle.parallelism = 4
       | )""".stripMargin
  }

  def insertInto(tableName: String, inputDf: sql.DataFrame, writeOp: String, isMetadataEnabledOnWrite: Boolean, keyGenClass: String): Unit = {
    inputDf.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon", "fare",
      "_hoodie_is_deleted", "partition_path").createOrReplaceTempView("insert_temp_table")
    spark.sql(s"set hoodie.metadata.enable=${String.valueOf(isMetadataEnabledOnWrite)}")
    spark.sql(s"set hoodie.datasource.write.keygenerator.class=$keyGenClass")
    if (writeOp.equals("upsert")) {
      spark.sql(s"set hoodie.datasource.write.operation=$writeOp")
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
    } else if (writeOp.equals("bulk_insert")) {
      //If HUDI-5257 is resolved, write operation should be bulk_insert, and this function can be more compact due to
      //less repeated code
      spark.sql("set hoodie.datasource.write.operation=insert")
      spark.sql("set hoodie.sql.bulk.insert.enable=true")
      spark.sql("set hoodie.sql.insert.mode=non-strict")
      spark.sql(s"insert into $tableName select * from insert_temp_table")
    } else if (writeOp.equals("insert")) {
      spark.sql(s"set hoodie.datasource.write.operation=$writeOp")
      spark.sql("set hoodie.sql.bulk.insert.enable=false")
      spark.sql("set hoodie.sql.insert.mode=non-strict")
      spark.sql(s"insert into $tableName select * from insert_temp_table")
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
    spark.read.json(spark.sparkContext.parallelize(recordsToStrings(recs), 2))
  }

  def generateUniqueUpdates(dataGen: HoodieTestDataGenerator, instantTime: String, n: Int): sql.DataFrame = {
    val recs = dataGen.generateUniqueUpdatesNestedExample(instantTime, n)
    spark.read.json(spark.sparkContext.parallelize(recordsToStrings(recs), 2))
  }

  def compareUpdateDfWithHudiDf(inputDf: Dataset[Row], hudiDf: Dataset[Row], beforeDf: Dataset[Row]): Unit = {
    dropMetaColumns(hudiDf).createOrReplaceTempView("hudiTbl")
    inputDf.createOrReplaceTempView("inputTbl")
    beforeDf.createOrReplaceTempView("beforeTbl")
    val hudiDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl")
    val inputDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from inputTbl")
    val beforeDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from beforeTbl")

    assertEquals(hudiDfToCompare.intersect(inputDfToCompare).count, inputDfToCompare.count)
    assertEquals(hudiDfToCompare.except(inputDfToCompare).except(beforeDfToCompare).count, 0)
  }

  def compareEntireInputDfWithHudiDf(inputDf: Dataset[Row], hudiDf: Dataset[Row]): Unit = {
    dropMetaColumns(hudiDf).createOrReplaceTempView("hudiTbl")
    inputDf.createOrReplaceTempView("inputTbl")
    val hudiDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl")
    val inputDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from inputTbl")

    assertEquals(hudiDfToCompare.intersect(inputDfToCompare).count, inputDfToCompare.count)
    assertEquals(hudiDfToCompare.except(inputDfToCompare).count, 0)
  }

  def doMORReadOptimizedQuery(isMetadataEnabledOnRead: Boolean, basePath: String): sql.DataFrame = {
    spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
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
    "COPY_ON_WRITE|insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "COPY_ON_WRITE|insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "COPY_ON_WRITE|insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "COPY_ON_WRITE|insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|insert|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|insert|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|insert|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "MERGE_ON_READ|insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "MERGE_ON_READ|insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "MERGE_ON_READ|insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "MERGE_ON_READ|insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "MERGE_ON_READ|insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "MERGE_ON_READ|insert|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|insert|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|insert|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|bulk_insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "COPY_ON_WRITE|bulk_insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|bulk_insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|bulk_insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "COPY_ON_WRITE|bulk_insert|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|bulk_insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "MERGE_ON_READ|bulk_insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "MERGE_ON_READ|bulk_insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|BLOOM",
    "MERGE_ON_READ|bulk_insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "MERGE_ON_READ|bulk_insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "MERGE_ON_READ|bulk_insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|SIMPLE",
    "MERGE_ON_READ|bulk_insert|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|bulk_insert|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM",
    "MERGE_ON_READ|bulk_insert|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|GLOBAL_BLOOM"
  )

  //extracts the params and runs each immutable user flow test
  forAll(paramsForImmutable) { (paramStr: String) =>
    test(s"Immutable user flow with params: $paramStr") {
      val splits = paramStr.split('|')
      withTempDir { basePath =>
        testImmutableUserFlow(basePath,
          tableType = splits(0),
          operation = splits(1),
          isMetadataEnabledOnWrite = splits(2).toBoolean,
          isMetadataEnabledOnRead = splits(3).toBoolean,
          keyGenClass = splits(4),
          indexType = splits(5))
      }
    }
  }

  def testImmutableUserFlow(basePath: File, tableType: String, operation: String, isMetadataEnabledOnWrite: Boolean,
                                isMetadataEnabledOnRead: Boolean, keyGenClass: String, indexType: String): Unit = {
    val tableName = generateTableName
    val tableBasePath = basePath.getCanonicalPath + "/" + tableName
    val writeOptions = getWriteOptions(tableName, tableType, isMetadataEnabledOnWrite, keyGenClass, indexType)
    createTable(tableName, keyGenClass, writeOptions, tableBasePath)
    val fs = FSUtils.getFs(tableBasePath, spark.sparkContext.hadoopConfiguration)

    //Insert Operation
    val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
    val inputDf0 = generateInserts(dataGen, "000", 100)
    insertInto(tableName, inputDf0, "bulk_insert", isMetadataEnabledOnWrite, keyGenClass)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, tableBasePath, "000"))

    //Snapshot query
    val snapshotDf1 = doSnapshotRead(tableName, isMetadataEnabledOnRead)
    snapshotDf1.cache()
    assertEquals(100, snapshotDf1.count())
    compareEntireInputDfWithHudiDf(inputDf0, snapshotDf1)

    val inputDf1 = generateInserts(dataGen, "001", 50)
    insertInto(tableName, inputDf1, operation, isMetadataEnabledOnWrite, keyGenClass)

    val snapshotDf2 = doSnapshotRead(tableName, isMetadataEnabledOnRead)
    snapshotDf2.cache()
    assertEquals(150, snapshotDf2.count())

    compareEntireInputDfWithHudiDf(inputDf1.union(inputDf0), snapshotDf2)

    val inputDf2 = generateInserts(dataGen, "002", 60)
    inputDf2.cache()
    insertInto(tableName, inputDf2, operation, isMetadataEnabledOnWrite, keyGenClass)

    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, tableBasePath, "000").size())

    // Snapshot Query
    val snapshotDf3 = doSnapshotRead(tableName, isMetadataEnabledOnRead)
    snapshotDf3.cache()
    assertEquals(210, snapshotDf3.count())
    compareEntireInputDfWithHudiDf(inputDf1.union(inputDf0).union(inputDf2), snapshotDf3)
    inputDf0.unpersist(true)
    inputDf1.unpersist(true)
    inputDf2.unpersist(true)
  }

}
