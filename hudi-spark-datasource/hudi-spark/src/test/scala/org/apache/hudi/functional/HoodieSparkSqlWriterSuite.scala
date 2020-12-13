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

import java.time.Instant
import java.util
import java.util.{Collections, Date, UUID}

import org.apache.commons.io.FileUtils
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.{SparkRDDWriteClient, TestBootstrap}
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.{AvroConversionUtils, DataSourceUtils, DataSourceWriteOptions, HoodieSparkSqlWriter, HoodieWriterUtils}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConversions._

class HoodieSparkSqlWriterSuite extends FunSuite with Matchers {

  var spark: SparkSession = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  test("Parameters With Write Defaults") {
    val originals = HoodieWriterUtils.parametersWithWriteDefaults(Map.empty)
    val rhsKey = "hoodie.right.hand.side.key"
    val rhsVal = "hoodie.right.hand.side.val"
    val modifier = Map(OPERATION_OPT_KEY -> INSERT_OPERATION_OPT_VAL, TABLE_TYPE_OPT_KEY -> MOR_TABLE_TYPE_OPT_VAL, rhsKey -> rhsVal)
    val modified = HoodieWriterUtils.parametersWithWriteDefaults(modifier)
    val matcher = (k: String, v: String) => modified(k) should be(v)

    originals foreach {
      case (OPERATION_OPT_KEY, _) => matcher(OPERATION_OPT_KEY, INSERT_OPERATION_OPT_VAL)
      case (TABLE_TYPE_OPT_KEY, _) => matcher(TABLE_TYPE_OPT_KEY, MOR_TABLE_TYPE_OPT_VAL)
      case (`rhsKey`, _) => matcher(rhsKey, rhsVal)
      case (k, v) => matcher(k, v)
    }
  }

  test("throw hoodie exception when invalid serializer") {
    val session = SparkSession.builder().appName("hoodie_test").master("local").getOrCreate()
    try {
      val sqlContext = session.sqlContext
      val options = Map("path" -> "hoodie/test/path", HoodieWriteConfig.TABLE_NAME -> "hoodie_test_tbl")
      val e = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.ErrorIfExists, options,
        session.emptyDataFrame))
      assert(e.getMessage.contains("spark.serializer"))
    } finally {
      session.stop()
    }
  }


  test("throw hoodie exception when there already exist a table with different name with Append Save mode") {

    initSparkContext("test_append_mode")
    val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    try {

      val hoodieFooTableName = "hoodie_foo_tbl"

      //create a new table
      val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME -> hoodieFooTableName,
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
      val dataFrame = spark.createDataFrame(Seq(Test(UUID.randomUUID().toString, new Date().getTime)))
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, dataFrame)

      //on same path try append with different("hoodie_bar_tbl") table name which should throw an exception
      val barTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME -> "hoodie_bar_tbl",
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4")
      val barTableParams = HoodieWriterUtils.parametersWithWriteDefaults(barTableModifier)
      val dataFrame2 = spark.createDataFrame(Seq(Test(UUID.randomUUID().toString, new Date().getTime)))
      val tableAlreadyExistException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, barTableParams, dataFrame2))
      assert(tableAlreadyExistException.getMessage.contains("hoodie table with name " + hoodieFooTableName + " already exist"))

      //on same path try append with delete operation and different("hoodie_bar_tbl") table name which should throw an exception
      val deleteTableParams = barTableParams ++ Map(OPERATION_OPT_KEY -> "delete")
      val deleteCmdException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, deleteTableParams, dataFrame2))
      assert(deleteCmdException.getMessage.contains("hoodie table with name " + hoodieFooTableName + " already exist"))
    } finally {
      spark.stop()
      FileUtils.deleteDirectory(path.toFile)
    }
  }

  test("test bulk insert dataset with datasource impl") {
    initSparkContext("test_bulk_insert_datasource")
    val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    try {

      val hoodieFooTableName = "hoodie_foo_tbl"

      //create a new table
      val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME -> hoodieFooTableName,
        "hoodie.bulkinsert.shuffle.parallelism" -> "4",
        DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
        DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
        DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> "org.apache.hudi.keygen.SimpleKeyGenerator")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      val records = DataSourceTestUtils.generateRandomRows(100)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df)

      // collect all parition paths to issue read of parquet files
      val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
      // Check the entire dataset has all records still
      val fullPartitionPaths = new Array[String](3)
      for (i <- 0 until fullPartitionPaths.length) {
        fullPartitionPaths(i) = String.format("%s/%s/*", path.toAbsolutePath.toString, partitions(i))
      }

      // fetch all records from parquet files generated from write to hudi
      val actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))

      // remove metadata columns so that expected and actual DFs can be compared as is
      val trimmedDf = actualDf.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
        .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
        .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))

      assert(df.except(trimmedDf).count() == 0)
    } finally {
      spark.stop()
      FileUtils.deleteDirectory(path.toFile)
    }
  }

  test("test insert dataset without precombine field") {
    val session = SparkSession.builder()
      .appName("test_insert_without_precombine")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    try {

      val sqlContext = session.sqlContext
      val sc = session.sparkContext
      val hoodieFooTableName = "hoodie_foo_tbl"

      //create a new table
      val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME -> hoodieFooTableName,
        "hoodie.bulkinsert.shuffle.parallelism" -> "1",
        DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
        DataSourceWriteOptions.INSERT_DROP_DUPS_OPT_KEY -> "false",
        DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
        DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> "org.apache.hudi.keygen.SimpleKeyGenerator")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      val records = DataSourceTestUtils.generateRandomRows(100)
      val recordsSeq = convertRowListToSeq(records)
      val df = session.createDataFrame(sc.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams - DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, df)

      // collect all parition paths to issue read of parquet files
      val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
      // Check the entire dataset has all records still
      val fullPartitionPaths = new Array[String](3)
      for (i <- 0 until fullPartitionPaths.length) {
        fullPartitionPaths(i) = String.format("%s/%s/*", path.toAbsolutePath.toString, partitions(i))
      }

      // fetch all records from parquet files generated from write to hudi
      val actualDf = session.sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))

      // remove metadata columns so that expected and actual DFs can be compared as is
      val trimmedDf = actualDf.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
        .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
        .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))

      assert(df.except(trimmedDf).count() == 0)
    } finally {
      session.stop()
      FileUtils.deleteDirectory(path.toFile)
    }
  }

  test("test bulk insert dataset with datasource impl multiple rounds") {
    initSparkContext("test_bulk_insert_datasource")
    val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    try {

      val hoodieFooTableName = "hoodie_foo_tbl"

      //create a new table
      val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME -> hoodieFooTableName,
        "hoodie.bulkinsert.shuffle.parallelism" -> "4",
        DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
        DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
        DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> "org.apache.hudi.keygen.SimpleKeyGenerator")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

      val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
      val fullPartitionPaths = new Array[String](3)
      for (i <- 0 to 2) {
        fullPartitionPaths(i) = String.format("%s/%s/*", path.toAbsolutePath.toString, partitions(i))
      }

      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      var totalExpectedDf = spark.createDataFrame(sc.emptyRDD[Row], structType)

      for (_ <- 0 to 2) {
        // generate the inserts
        val records = DataSourceTestUtils.generateRandomRows(200)
        val recordsSeq = convertRowListToSeq(records)
        val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
        // write to Hudi
        HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df)

        // Fetch records from entire dataset
        val actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))

        // remove metadata columns so that expected and actual DFs can be compared as is
        val trimmedDf = actualDf.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
          .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
          .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))

        // find total df (union from multiple rounds)
        totalExpectedDf = totalExpectedDf.union(df)
        // find mismatch between actual and expected df
        assert(totalExpectedDf.except(trimmedDf).count() == 0)
      }
    } finally {
      spark.stop()
      FileUtils.deleteDirectory(path.toFile)
    }
  }

  List(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
    .foreach(tableType => {
      test("test basic HoodieSparkSqlWriter functionality with datasource insert for " + tableType) {
        initSparkContext("test_insert_datasource")
        val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
        try {

          val hoodieFooTableName = "hoodie_foo_tbl"

          //create a new table
          val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
            HoodieWriteConfig.TABLE_NAME -> hoodieFooTableName,
            DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> tableType,
            HoodieWriteConfig.INSERT_PARALLELISM -> "4",
            DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
            DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
            DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
            DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> classOf[SimpleKeyGenerator].getCanonicalName)
          val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

          // generate the inserts
          val schema = DataSourceTestUtils.getStructTypeExampleSchema
          val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
          val records = DataSourceTestUtils.generateRandomRows(100)
          val recordsSeq = convertRowListToSeq(records)
          val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)

          val client = spy(DataSourceUtils.createHoodieClient(
            new JavaSparkContext(sc),
            schema.toString,
            path.toAbsolutePath.toString,
            hoodieFooTableName,
            mapAsJavaMap(fooTableParams)).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]])

          // write to Hudi
          HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df, Option.empty,
            Option(client))
          // Verify that asynchronous compaction is not scheduled
          verify(client, times(0)).scheduleCompaction(any())
          // Verify that HoodieWriteClient is closed correctly
          verify(client, times(1)).close()

          // collect all partition paths to issue read of parquet files
          val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
            HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
          // Check the entire dataset has all records still
          val fullPartitionPaths = new Array[String](3)
          for (i <- fullPartitionPaths.indices) {
            fullPartitionPaths(i) = String.format("%s/%s/*", path.toAbsolutePath.toString, partitions(i))
          }

          // fetch all records from parquet files generated from write to hudi
          val actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))

          // remove metadata columns so that expected and actual DFs can be compared as is
          val trimmedDf = actualDf.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
            .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
            .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))

          assert(df.except(trimmedDf).count() == 0)
        } finally {
          spark.stop()
          FileUtils.deleteDirectory(path.toFile)
        }
      }
    })

  List(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
    .foreach(tableType => {
      test("test HoodieSparkSqlWriter functionality with datasource bootstrap for " + tableType) {
        initSparkContext("test_bootstrap_datasource")
        val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
        val srcPath = java.nio.file.Files.createTempDirectory("hoodie_bootstrap_source_path")

        try {

          val hoodieFooTableName = "hoodie_foo_tbl"

          val sourceDF = TestBootstrap.generateTestRawTripDataset(Instant.now.toEpochMilli, 0, 100, Collections.emptyList(), sc,
            spark.sqlContext)

          // Write source data non-partitioned
          sourceDF.write
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save(srcPath.toAbsolutePath.toString)

          val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
            HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP -> srcPath.toAbsolutePath.toString,
            HoodieWriteConfig.TABLE_NAME -> hoodieFooTableName,
            DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> tableType,
            HoodieBootstrapConfig.BOOTSTRAP_PARALLELISM -> "4",
            DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL,
            DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
            DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
            HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS -> classOf[NonpartitionedKeyGenerator].getCanonicalName)
          val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

          val client = spy(DataSourceUtils.createHoodieClient(
            new JavaSparkContext(sc),
            null,
            path.toAbsolutePath.toString,
            hoodieFooTableName,
            mapAsJavaMap(fooTableParams)).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]])

          HoodieSparkSqlWriter.bootstrap(sqlContext, SaveMode.Append, fooTableParams, spark.emptyDataFrame, Option.empty,
            Option(client))

          // Verify that HoodieWriteClient is closed correctly
          verify(client, times(1)).close()

          // fetch all records from parquet files generated from write to hudi
          val actualDf = sqlContext.read.parquet(path.toAbsolutePath.toString)
          assert(actualDf.count == 100)
        } finally {
          spark.stop()
          FileUtils.deleteDirectory(path.toFile)
          FileUtils.deleteDirectory(srcPath.toFile)
        }
      }
    })

  case class Test(uuid: String, ts: Long)

  import scala.collection.JavaConverters

  def convertRowListToSeq(inputList: util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq

  def initSparkContext(appName: String): Unit = {
    spark = SparkSession.builder()
      .appName(appName)
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlContext = spark.sqlContext
  }
}
