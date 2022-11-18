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
import org.scalatest.Inspectors.forAll

import java.io.File
import scala.collection.JavaConversions._


class TestSparkSql extends HoodieSparkSqlTestBase {
  //params for core flow tests
  val params: List[String] = List(
        "COPY_ON_WRITE|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
        "COPY_ON_WRITE|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
        "COPY_ON_WRITE|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
        "COPY_ON_WRITE|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
        "COPY_ON_WRITE|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
        "COPY_ON_WRITE|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
        "COPY_ON_WRITE|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
        "COPY_ON_WRITE|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
        "COPY_ON_WRITE|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
        "MERGE_ON_READ|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
        "MERGE_ON_READ|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
        "MERGE_ON_READ|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
        "MERGE_ON_READ|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
        "MERGE_ON_READ|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
        "MERGE_ON_READ|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
        "MERGE_ON_READ|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
        "MERGE_ON_READ|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
        "MERGE_ON_READ|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM"
      )

  //extracts the params and runs each core flow test
  forAll (params) { (paramStr: String) =>
    test(s"Core flow with params: $paramStr") {
      val splits = paramStr.split('|')
      withTempDir { tmp =>
        testCoreFlows(tmp,
          tableType = splits(0),
          isMetadataEnabledOnWrite = splits(1).toBoolean,
          isMetadataEnabledOnRead = splits(2).toBoolean,
          keyGenClass = splits(3),
          recordKeys = splits(4),
          indexType = splits(5))
      }
    }
  }

  def testCoreFlows(tmp: File, tableType: String, isMetadataEnabledOnWrite: Boolean, isMetadataEnabledOnRead: Boolean, keyGenClass: String, recordKeys: String, indexType: String): Unit = {
    val tableName = generateTableName
    val basePath = tmp.getCanonicalPath + "/" + tableName
    val writeOptions = getWriteOptions(tableName, tableType, isMetadataEnabledOnWrite, keyGenClass,
      recordKeys, indexType)
    val partitionedBy = if (!keyGenClass.equals("org.apache.hudi.keygen.NonpartitionedKeyGenerator")) {
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
         | location '$basePath'
         |
  """.stripMargin)
    val cols = Seq("timestamp", "_row_key", "partition_path", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon", "fare.amount", "fare.currency", "_hoodie_is_deleted")
    val colsToSelect = getColsToSelect(cols)
    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)

    //Insert Operation
    val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
    val (inputDf0, _, values0) = generateInserts(dataGen, "000", 100)
    insertInto(tableName, values0, "bulk_insert", isMetadataEnabledOnWrite, keyGenClass)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    //Snapshot query
    val snapshotDf1 = doRead(tableName, isMetadataEnabledOnRead)
    snapshotDf1.cache()
    assertEquals(100, snapshotDf1.count())

    val (updateDf, _, values1) = generateUniqueUpdates(dataGen, "001", 50)
    insertInto(tableName, values1, "upsert", isMetadataEnabledOnWrite, keyGenClass)

    val commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    val snapshotDf2 = doRead(tableName, isMetadataEnabledOnRead)
    snapshotDf2.cache()
    assertEquals(100, snapshotDf2.count())

    compareUpdateDfWithHudiDf(updateDf, snapshotDf2, snapshotDf1, colsToSelect)

    val (inputDf2, _, values2) = generateUniqueUpdates(dataGen, "002", 60)
    val uniqueKeyCnt2 = inputDf2.select("_row_key").distinct().count()
    insertInto(tableName, values2, "upsert", isMetadataEnabledOnWrite, keyGenClass)

    val commitInstantTime3 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Snapshot Query
    val snapshotDf3 = doRead(tableName, isMetadataEnabledOnRead)
    snapshotDf3.cache()
    assertEquals(100, snapshotDf3.count())

    compareUpdateDfWithHudiDf(inputDf2, snapshotDf3, snapshotDf3, colsToSelect)

    // Read Incremental Query, need to use spark-ds because functionality does not exist for spark sql
    // we have 2 commits, try pulling the first commit (which is not the latest)
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0)
    val hoodieIncViewDf1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME.key, firstCommit)
      .load(basePath)
    //val hoodieIncViewDf1 = doIncRead(tableName, isMetadataEnabledOnRead, "000", firstCommit)
    assertEquals(100, hoodieIncViewDf1.count()) // 100 initial inserts must be pulled
    var countsPerCommit = hoodieIncViewDf1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0).toString)

    val (inputDf3, _, values3) = generateUniqueUpdates(dataGen, "003", 80)
    val uniqueKeyCnt3 = inputDf3.select("_row_key").distinct().count()
    insertInto(tableName, values3, "upsert", isMetadataEnabledOnWrite, keyGenClass)

    //another incremental query with commit2 and commit3
    val hoodieIncViewDf2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime2)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), commitInstantTime3)
      .load(basePath)
    //val hoodieIncViewDf2 = doIncRead(tableName, isMetadataEnabledOnRead, commitInstantTime2, commitInstantTime3)


    assertEquals(uniqueKeyCnt2, hoodieIncViewDf2.count()) // 60 records must be pulled
    countsPerCommit = hoodieIncViewDf2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime3, countsPerCommit(0).get(0).toString)

    // time travel query.
    val timeTravelDf = if (HoodieSparkUtils.gteqSpark3_2_1) {
      spark.sql(s"select * from $tableName timestamp as of '$commitInstantTime2'")
    } else {
      spark.read.format("org.apache.hudi")
        .option("as.of.instant", commitInstantTime2)
        .load(basePath)
    }
    //val timeTravelDf = doTimeTravel(tableName, isMetadataEnabledOnRead, commitInstantTime2)
    assertEquals(100, timeTravelDf.count())
    compareEntireInputDfWithHudiDf(snapshotDf2, timeTravelDf, colsToSelect)

    if (tableType.equals("MERGE_ON_READ")) {
      doMORReadOptimizedQuery(inputDf0, colsToSelect, isMetadataEnabledOnRead, tableName, basePath)

      val snapshotDf4 = doRead(tableName, isMetadataEnabledOnRead)
      snapshotDf4.cache()

      // trigger compaction and try out Read optimized query.
      val (inputDf4, _, values4) = generateUniqueUpdates(dataGen, "004", 40)
      val uniqueKeyCnt4 = inputDf4.select("_row_key").distinct().count()
      doInlineCompact(tableName, values4, "upsert", isMetadataEnabledOnWrite, "3", keyGenClass)

      val snapshotDf5 = doRead(tableName, isMetadataEnabledOnRead)
      snapshotDf5.cache()

      compareUpdateDfWithHudiDf(inputDf4, snapshotDf5, snapshotDf4, colsToSelect)
      // compaction is expected to have completed. both RO and RT are expected to return same results.
      compareROAndRT(colsToSelect, isMetadataEnabledOnRead, tableName)
    }
  }

  def doRead(tableName: String, isMetadataEnabledOnRead: Boolean): sql.DataFrame = {
    spark.sql("set hoodie.datasource.query.type=\"snapshot\"")
    spark.sql(s"set hoodie.metadata.enable=${String.valueOf(isMetadataEnabledOnRead)}")
    spark.sql(s"select * from $tableName")
  }

  def doIncRead(tableName: String, isMetadataEnabledOnRead: Boolean, beginInstantTime: String, endInstantTime: String): sql.DataFrame = {
    spark.sql("set hoodie.datasource.query.type=\"incremental\"")
    spark.sql(s"set hoodie.datasource.read.begin.instanttime='$beginInstantTime'")
    spark.sql(s"set hoodie.datasource.read.end.instanttime='$endInstantTime''")
    spark.sql(s"set hoodie.metadata.enable=${String.valueOf(isMetadataEnabledOnRead)}")
    spark.sql(s"select * from $tableName")
  }

  def doTimeTravel(tableName: String, isMetadataEnabledOnRead: Boolean, asOf: String): sql.DataFrame = {
    spark.sql("set hoodie.datasource.query.type=\"snapshot\"")
    spark.sql(s"set hoodie.metadata.enable=${String.valueOf(isMetadataEnabledOnRead)}")
    if (HoodieSparkUtils.gteqSpark3_2_1) {
      spark.sql(s"select * from $tableName timestamp as of '$asOf'")
    } else {
      spark.sql(s"set as.of.instant='$asOf'")
      val asOfDf = spark.sql(s"select * from $tableName")
      spark.sql(s"set as.of.instant=''")
      asOfDf
    }
  }

  def doInlineCompact(tableName: String, values: String, writeOp: String, isMetadataEnabledOnWrite: Boolean, numDeltaCommits: String, keyGenClass: String): Unit = {
    spark.sql("set hoodie.compact.inline=true")
    spark.sql(s"set hoodie.compact.inline.max.delta.commits=$numDeltaCommits")
    insertInto(tableName, values, writeOp, isMetadataEnabledOnWrite, keyGenClass)
    spark.sql("set hoodie.compact.inline=false")
  }

  def getWriteOptions(tableName: String, tableType: String, isMetadataEnabledOnWrite: Boolean,
                      keyGenClass: String, recordKeys: String, indexType: String): String = {
    val typeString = if (tableType.equals("COPY_ON_WRITE")) {
      "cow"
    } else if (tableType.equals("MERGE_ON_READ")) {
      "mor"
    } else {
      tableType
    }

    s"""
       |options (
       |  type = '$typeString',
       |  primaryKey = '$recordKeys',
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


  def getColsToSelect(cols: Seq[String]): String = {
    var colsToSelect = cols.get(0)
    for (i <- 1 until cols.size) {
      colsToSelect += ", " + cols.get(i)
    }
    colsToSelect
  }

  def insertInto(tableName: String, values: String, writeOp: String, isMetadataEnabledOnWrite: Boolean, keyGenClass: String): Unit = {
    if (writeOp.equals("upsert")||writeOp.equals("insert")) {
      spark.sql("set hoodie.sql.insert.mode=upsert")
    } else if (writeOp.equals("bulk_insert")) {
      spark.sql("set hoodie.sql.insert.mode=non-strict")
    }
    spark.sql(s"set hoodie.datasource.write.operation=$writeOp")
    spark.sql(s"set hoodie.metadata.enable=${String.valueOf(isMetadataEnabledOnWrite)}")
    spark.sql(s"set hoodie.datasource.write.keygenerator.class=$keyGenClass")
    spark.sql(
      s"""
         | insert into $tableName values
         | $values
         |""".stripMargin
    )
  }

  def getDataSeq(cols: Seq[String], m: Map[String, Any]): Seq[Any] = {
    cols.map(col => m.getOrElse(col, ""))
  }

  def rowsToSeq(cols: Seq[String], rows: List[Map[String, Any]]): Seq[Seq[Any]] = {
    rows.map(data => getDataSeq(cols, data))
  }

  def fixJavaListMap(m: java.util.List[java.util.Map[String, AnyRef]]): List[Map[String, Any]] = {
    m.map(i => i.toMap).toList
  }

  def recConvert(dataGen: HoodieTestDataGenerator, recs: java.util.List[HoodieRecord[_]]): (sql.DataFrame, List[Map[String, Any]], String) = {
    val recDf = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(recs), 2))
    val inserts = dataGen.convertInsertsToMapNestedExample(recs)
    var tableValues = dataGen.getNestedExampleSQLString(inserts.get(0))
    for (i <- 1 until inserts.size()) {
      tableValues += "," + dataGen.getNestedExampleSQLString(inserts.get(i))
    }
    (recDf, fixJavaListMap(inserts), tableValues)
  }

  def generateInserts(dataGen: HoodieTestDataGenerator, instantTime: String, n: Int): (sql.DataFrame, List[Map[String, Any]], String) = {
    recConvert(dataGen, dataGen.generateInsertsNestedExample(instantTime, n))
  }

  def generateUniqueUpdates(dataGen: HoodieTestDataGenerator, instantTime: String, n: Int): (sql.DataFrame, List[Map[String, Any]], String) = {
    recConvert(dataGen, dataGen.generateUniqueUpdatesNestedExample(instantTime, n))
  }

  def compareUpdateDfWithHudiDf(inputDf: Dataset[Row], hudiDf: Dataset[Row], beforeDf: Dataset[Row], colsToCompare: String): Unit = {
    val hudiWithoutMeta = hudiDf.drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    hudiWithoutMeta.registerTempTable("hudiTbl")
    inputDf.registerTempTable("inputTbl")
    beforeDf.registerTempTable("beforeTbl")
    val hudiDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl")
    val inputDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from inputTbl")
    val beforeDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from beforeTbl")

    assertEquals(hudiDfToCompare.intersect(inputDfToCompare).count, inputDfToCompare.count)
    assertEquals(hudiDfToCompare.except(inputDfToCompare).except(beforeDfToCompare).count, 0)
  }

  def compareEntireInputDfWithHudiDf(inputDf: Dataset[Row], hudiDf: Dataset[Row], colsToCompare: String): Unit = {
    val hudiWithoutMeta = hudiDf.drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    hudiWithoutMeta.registerTempTable("hudiTbl")
    inputDf.registerTempTable("inputTbl")
    val hudiDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl")
    val inputDfToCompare = spark.sqlContext.sql("select " + colsToCompare + " from inputTbl")

    assertEquals(hudiDfToCompare.intersect(inputDfToCompare).count, inputDfToCompare.count)
    assertEquals(hudiDfToCompare.except(inputDfToCompare).count, 0)
  }

  def doMORReadOptimizedQuery(inputDf: Dataset[Row], colsToSelect: String, isMetadataEnabledOnRead: Boolean, tableName: String, basePath: String): Unit = {
    // read optimized query.
    // spark.sql("set hoodie.datasource.query.type=\"read_optimized\"")
    // spark.sql(s"set hoodie.metadata.enable=${String.valueOf(isMetadataEnabledOnRead)}")
    // val readOptDf = spark.sql(s"select * from $tableName")

    val readOptDf = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key(), isMetadataEnabledOnRead)
      .load(basePath)

    compareEntireInputDfWithHudiDf(inputDf, readOptDf, colsToSelect)
  }

  def compareROAndRT(colsToCompare: String, isMetadataEnabledOnRead: Boolean, tableName: String): Unit = {
    spark.sql("set hoodie.datasource.query.type=\"read_optimized\"")
    spark.sql(s"set hoodie.metadata.enable=${String.valueOf(isMetadataEnabledOnRead)}")
    val roDf = spark.sql(s"select * from $tableName")
    val rtDf = doRead(tableName, isMetadataEnabledOnRead)


    val hudiWithoutMeta1 = roDf
      .drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    val hudiWithoutMeta2 = rtDf
      .drop(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD)
    hudiWithoutMeta1.registerTempTable("hudiTbl1")
    hudiWithoutMeta2.registerTempTable("hudiTbl2")

    val hudiDf1ToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl1")
    val hudiDf2ToCompare = spark.sqlContext.sql("select " + colsToCompare + " from hudiTbl2")

    assertEquals(hudiDf1ToCompare.intersect(hudiDf2ToCompare).count, hudiDf1ToCompare.count)
    assertEquals(hudiDf1ToCompare.except(hudiDf2ToCompare).count, 0)
  }

  //params for immutable user flow
  val paramsForImmutable: List[String] = List(
    "COPY_ON_WRITE|insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "COPY_ON_WRITE|insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "COPY_ON_WRITE|insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "COPY_ON_WRITE|insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "COPY_ON_WRITE|insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "COPY_ON_WRITE|insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "COPY_ON_WRITE|insert|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "COPY_ON_WRITE|insert|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "COPY_ON_WRITE|insert|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "MERGE_ON_READ|insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "MERGE_ON_READ|insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "MERGE_ON_READ|insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "MERGE_ON_READ|insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "MERGE_ON_READ|insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "MERGE_ON_READ|insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "MERGE_ON_READ|insert|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "MERGE_ON_READ|insert|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "MERGE_ON_READ|insert|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "COPY_ON_WRITE|bulk_insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "COPY_ON_WRITE|bulk_insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "COPY_ON_WRITE|bulk_insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "COPY_ON_WRITE|bulk_insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "COPY_ON_WRITE|bulk_insert|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "COPY_ON_WRITE|bulk_insert|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "MERGE_ON_READ|bulk_insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "MERGE_ON_READ|bulk_insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "MERGE_ON_READ|bulk_insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|BLOOM",
    "MERGE_ON_READ|bulk_insert|false|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "MERGE_ON_READ|bulk_insert|true|false|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "MERGE_ON_READ|bulk_insert|true|true|org.apache.hudi.keygen.SimpleKeyGenerator|_row_key|SIMPLE",
    "MERGE_ON_READ|bulk_insert|false|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "MERGE_ON_READ|bulk_insert|true|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM",
    "MERGE_ON_READ|bulk_insert|true|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|_row_key|GLOBAL_BLOOM"
  )

  //extracts the params and runs each immutable user flow test
  forAll(paramsForImmutable) { (paramStr: String) =>
    test(s"Immutable user flow with params: $paramStr") {
      val splits = paramStr.split('|')
      withTempDir { tmp =>
        testImmutableUserFlowFlow(tmp,
          tableType = splits(0),
          operation = splits(1),
          isMetadataEnabledOnWrite = splits(2).toBoolean,
          isMetadataEnabledOnRead = splits(3).toBoolean,
          keyGenClass = splits(4),
          recordKeys = splits(5),
          indexType = splits(6))
      }
    }
  }

  def testImmutableUserFlowFlow(tmp: File, tableType: String, operation: String, isMetadataEnabledOnWrite: Boolean,
                                isMetadataEnabledOnRead: Boolean, keyGenClass: String, recordKeys: String, indexType: String): Unit = {
    val tableName = generateTableName
    val basePath = tmp.getCanonicalPath + "/" + tableName
    val writeOptions = getWriteOptions(tableName, tableType, isMetadataEnabledOnWrite, keyGenClass,
      recordKeys, indexType)
    val partitionedBy = if (!keyGenClass.equals("org.apache.hudi.keygen.NonpartitionedKeyGenerator")) {
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
         | location '$basePath'
         |
    """.stripMargin)
    val cols = Seq("timestamp", "_row_key", "partition_path", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon", "fare.amount", "fare.currency", "_hoodie_is_deleted")
    val colsToSelect = getColsToSelect(cols)
    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)

    //Insert Operation
    val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
    val (inputDf0, _, values0) = generateInserts(dataGen, "000", 100)
    spark.sql("set hoodie.sql.insert.mode=non-strict")
    insertInto(tableName, values0, "bulk_insert", isMetadataEnabledOnWrite, keyGenClass)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    //Snapshot query
    val snapshotDf1 = doRead(tableName, isMetadataEnabledOnRead)
    snapshotDf1.cache()
    assertEquals(100, snapshotDf1.count())

    val (inputDf1, _, values1) = generateInserts(dataGen, "001", 50)
    insertInto(tableName, values1, operation, isMetadataEnabledOnWrite, keyGenClass)

    val snapshotDf2 = doRead(tableName, isMetadataEnabledOnRead)
    snapshotDf2.cache()
    assertEquals(150, snapshotDf2.count())

    compareEntireInputDfWithHudiDf(inputDf1.union(inputDf0), snapshotDf2, colsToSelect)

    val (inputDf2, _, values2) = generateInserts(dataGen, "002", 60)
    inputDf2.cache()
    insertInto(tableName, values2, operation, isMetadataEnabledOnWrite, keyGenClass)

    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Snapshot Query
    val snapshotDf3 = doRead(tableName, isMetadataEnabledOnRead)
    snapshotDf3.cache()
    assertEquals(210, snapshotDf3.count())
    compareEntireInputDfWithHudiDf(inputDf1.union(inputDf0).union(inputDf2), snapshotDf3, colsToSelect)
  }

}
