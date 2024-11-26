/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.model.WriteOperationType.{BULK_INSERT, INSERT, UPSERT}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.TimelineUtils
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.scalatest.Inspectors.forAll

import java.io.File
import scala.util.Random;
import scala.collection.JavaConversions._


class TestExtensibleBucketDataCorrectness extends HoodieSparkSqlTestBase {

  val colsToCompare = "timestamp, _row_key, partition_path, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare.amount, fare.currency, _hoodie_is_deleted"

  //params for core flow tests
  val params: List[String] = List(
        "false|org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "true|org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "false|org.apache.hudi.keygen.SimpleKeyGenerator",
        "true|org.apache.hudi.keygen.SimpleKeyGenerator"
  )

  //extracts the params and runs each core flow test
  forAll (params) { (paramStr: String) =>
    test(s"Core flow with params: $paramStr") {
      val splits = paramStr.split('|')
      withTempDir { basePath =>
        testCoreFlows(basePath,
            isMetadataEnabled = splits(0).toBoolean,
            keyGenClass = splits(1))
      }
    }
  }

  def testCoreFlows(basePath: File, isMetadataEnabled: Boolean, keyGenClass: String): Unit = {
    val simpleTableName = "simple_table"
    val extensibleTableName = "extensible_table"
    val simpleTablePath = new File(basePath, simpleTableName).getAbsolutePath
    val extensibleTablePath = new File(basePath, extensibleTableName).getAbsolutePath
    val simpleTableOptions = getWriteOptions(simpleTableName, keyGenClass, "SIMPLE_BUCKET")
    val extensibleTableOptions = getWriteOptions(extensibleTableName, keyGenClass, "EXTENSIBLE_BUCKET")
    createTable("simple_table", keyGenClass, simpleTableOptions, simpleTablePath)
    createTable("extensible_table", keyGenClass, extensibleTableOptions, extensibleTablePath)
    val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)

    // loop 100 times to randomly do operations
    for (_ <- 0 until 100) {
      val instantTime = String.valueOf(System.currentTimeMillis())
      val n = 100

    }

    def randomlyDoOperation(instantTime: String): Unit = {
      val random = new Random()
      val i = random.nextInt(10)
      if (i < 3) {
        // insert
        val inserts = generateInserts(dataGen, instantTime, 100)
        insertInto(simpleTableName, simpleTablePath, inserts, INSERT, isMetadataEnabled, 1)
      } else if (i < 5) {
        // upsert
        val updates = generateUniqueUpdates(dataGen, instantTime, 60)
        insertInto(simpleTableName, simpleTablePath, updates, UPSERT, isMetadataEnabled, 2)
      } else if (i < 7) {
        // schedule cluster
        val inserts = generateInserts(dataGen, instantTime, 100)
        insertInto(simpleTableName, simpleTablePath, inserts, BULK_INSERT, isMetadataEnabled, 1)
      }
    }


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

  def assertOperation(basePath: String, count: Int, operationType: WriteOperationType): Boolean = {
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf())
      .build()
    val timeline = metaClient.getActiveTimeline.getAllCommitsTimeline
    assert(timeline.countInstants() == count)
    val latestCommit = timeline.lastInstant()
    assert(latestCommit.isPresent)
    assert(latestCommit.get().isCompleted)
    val metadata = TimelineUtils.getCommitMetadata(latestCommit.get(), timeline)
    metadata.getOperationType.equals(operationType)
  }

  def getWriteOptions(tableName: String, keyGenClass: String, indexType: String): String = {

    val (realIndexType, bucketOptions) = if (indexType.equals("SIMPLE_BUCKET")) {
      ("BUCKET", ", hoodie.index.bucket.engine='SIMPLE', hoodie.bucket.index.num.buckets=16")
    } else if (indexType.equals("EXTENSIBLE_BUCKET")) {
      ("BUCKET", ", hoodie.index.bucket.engine='EXTENSIBLE_BUCKET', hoodie.table.initial.bucket.number=16")
    }

    else {
      (indexType, "")
    }

    s"""
       |tblproperties (
       |  type = 'mor',
       |  primaryKey = '_row_key',
       |  preCombineField = 'timestamp',
       |  hoodie.bulkinsert.shuffle.parallelism = 4,
       |  hoodie.database.name = "databaseName",
       |  hoodie.table.keygenerator.class = '$keyGenClass',
       |  hoodie.delete.shuffle.parallelism = 2,
       |  hoodie.index.type = "$realIndexType",
       |  hoodie.insert.shuffle.parallelism = 4,
       |  hoodie.table.name = "$tableName",
       |  hoodie.upsert.shuffle.parallelism = 4$bucketOptions
       | )""".stripMargin
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

  private def readRecords(tableName: String, isMetadataEnable: Boolean): List[Row] = {
    try {
      spark.sql(s"set hoodie.datasource.query.type=$QUERY_TYPE_SNAPSHOT_OPT_VAL")
      spark.sql(s"set hoodie.metadata.enable=$isMetadataEnable")
      spark.sql(s"select " + colsToCompare + " from $tableName order by partition_path, _row_key").collect().toList
    } finally {
      spark.conf.unset("hoodie.datasource.query.type")
      spark.conf.unset("hoodie.metadata.enable")
    }
  }
}
