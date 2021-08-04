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
import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.{SparkRDDWriteClient, TestBootstrap}
import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.hive.HiveSyncConfig
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceUtils, DataSourceWriteOptions, HoodieSparkSqlWriter, HoodieWriterUtils}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConversions._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Tag

@Tag("functional")
class HoodieSparkSqlWriterSuite extends FunSuite with Matchers {

  var spark: SparkSession = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  test("Parameters With Write Defaults") {
    val originals = HoodieWriterUtils.parametersWithWriteDefaults(Map.empty)
    val rhsKey = "hoodie.right.hand.side.key"
    val rhsVal = "hoodie.right.hand.side.val"
    val modifier = Map(OPERATION.key -> INSERT_OPERATION_OPT_VAL, TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL, rhsKey -> rhsVal)
    val modified = HoodieWriterUtils.parametersWithWriteDefaults(modifier)
    val matcher = (k: String, v: String) => modified(k) should be(v)

    originals foreach {
      case ("hoodie.datasource.write.operation", _) => matcher("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      case ("hoodie.datasource.write.table.type", _) => matcher("hoodie.datasource.write.table.type", MOR_TABLE_TYPE_OPT_VAL)
      case (`rhsKey`, _) => matcher(rhsKey, rhsVal)
      case (k, v) => matcher(k, v)
    }
  }

  test("throw hoodie exception when invalid serializer") {
    val session = SparkSession.builder().appName("hoodie_test").master("local").getOrCreate()
    try {
      val sqlContext = session.sqlContext
      val options = Map("path" -> "hoodie/test/path", HoodieWriteConfig.TABLE_NAME.key -> "hoodie_test_tbl")
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
        HoodieWriteConfig.TABLE_NAME.key -> hoodieFooTableName,
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
      val dataFrame = spark.createDataFrame(Seq(Test(UUID.randomUUID().toString, new Date().getTime)))
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, dataFrame)

      //on same path try append with different("hoodie_bar_tbl") table name which should throw an exception
      val barTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME.key -> "hoodie_bar_tbl",
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4")
      val barTableParams = HoodieWriterUtils.parametersWithWriteDefaults(barTableModifier)
      val dataFrame2 = spark.createDataFrame(Seq(Test(UUID.randomUUID().toString, new Date().getTime)))
      val tableAlreadyExistException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, barTableParams, dataFrame2))
      assert(tableAlreadyExistException.getMessage.contains("hoodie table with name " + hoodieFooTableName + " already exist"))

      //on same path try append with delete operation and different("hoodie_bar_tbl") table name which should throw an exception
      val deleteTableParams = barTableParams ++ Map(OPERATION.key -> "delete")
      val deleteCmdException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, deleteTableParams, dataFrame2))
      assert(deleteCmdException.getMessage.contains("hoodie table with name " + hoodieFooTableName + " already exist"))
    } finally {
      spark.stop()
      FileUtils.deleteDirectory(path.toFile)
    }
  }

  List(BulkInsertSortMode.GLOBAL_SORT, BulkInsertSortMode.NONE, BulkInsertSortMode.PARTITION_SORT)
    .foreach(sortMode => {
      test("test_bulk_insert_for_" + sortMode) {
        initSparkContext("test_bulk_insert_datasource")
        val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
        try {
          testBulkInsertWithSortMode(sortMode, path)
        } finally {
          spark.stop()
          FileUtils.deleteDirectory(path.toFile)
        }
      }
    })

  List(true, false)
    .foreach(populateMetaFields => {
      test("test_bulk_insert_for_populate_meta_fields_" + populateMetaFields) {
        initSparkContext("test_bulk_insert_datasource_populate_meta_fields")
        val path = java.nio.file.Files.createTempDirectory("hoodie_test_path_populate_meta_fields")
        try {
          testBulkInsertWithSortMode(BulkInsertSortMode.NONE, path, populateMetaFields)
        } finally {
          spark.stop()
          FileUtils.deleteDirectory(path.toFile)
        }
      }
    })

  def testBulkInsertWithSortMode(sortMode: BulkInsertSortMode, path: java.nio.file.Path, populateMetaFields : Boolean = true) : Unit = {

    val hoodieFooTableName = "hoodie_foo_tbl"
    //create a new table
    val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
    HoodieWriteConfig.TABLE_NAME.key -> hoodieFooTableName,
    DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    "hoodie.bulkinsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
    DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "true",
    HoodieTableConfig.HOODIE_POPULATE_META_FIELDS.key() -> String.valueOf(populateMetaFields),
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.BULKINSERT_SORT_MODE.key() -> sortMode.name(),
    DataSourceWriteOptions.KEYGENERATOR_CLASS.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val inserts = DataSourceTestUtils.generateRandomRows(1000)

    // add some updates so that preCombine kicks in
    val toUpdateDataset = sqlContext.createDataFrame(DataSourceTestUtils.getUniqueRows(inserts, 40), structType)
    val updates = DataSourceTestUtils.updateRowsWithHigherTs(toUpdateDataset)
    val records = inserts.union(updates)

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

    if (!populateMetaFields) {
      assertEquals(0, actualDf.select(HoodieRecord.HOODIE_META_COLUMNS.get(0)).filter(entry => !(entry.mkString(",").equals(""))).count())
      assertEquals(0, actualDf.select(HoodieRecord.HOODIE_META_COLUMNS.get(1)).filter(entry => !(entry.mkString(",").equals(""))).count())
      assertEquals(0, actualDf.select(HoodieRecord.HOODIE_META_COLUMNS.get(2)).filter(entry => !(entry.mkString(",").equals(""))).count())
      assertEquals(0, actualDf.select(HoodieRecord.HOODIE_META_COLUMNS.get(3)).filter(entry => !(entry.mkString(",").equals(""))).count())
      assertEquals(0, actualDf.select(HoodieRecord.HOODIE_META_COLUMNS.get(4)).filter(entry => !(entry.mkString(",").equals(""))).count())
    }
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf = actualDf.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
    .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
    .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))

    assert(df.except(trimmedDf).count() == 0)
  }

  test("test disable and enable meta fields") {
    initSparkContext("test_disable_enable_meta_fields")
    val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    try {
      testBulkInsertWithSortMode(BulkInsertSortMode.NONE, path, false)

      // enabling meta fields back should throw exception
      val hoodieFooTableName = "hoodie_foo_tbl"
      //create a new table
      val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME.key -> hoodieFooTableName,
        DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
        "hoodie.bulkinsert.shuffle.parallelism" -> "4",
        DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
        DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "true",
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
        HoodieWriteConfig.BULKINSERT_SORT_MODE.key() -> BulkInsertSortMode.NONE.name(),
        DataSourceWriteOptions.KEYGENERATOR_CLASS.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      val inserts = DataSourceTestUtils.generateRandomRows(1000)
      val df = spark.createDataFrame(sc.parallelize(inserts), structType)
      try {
        // write to Hudi
        HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df)
        fail("Should have thrown exception")
      } catch {
        case e: HoodieException => assertTrue(e.getMessage.contains("hoodie.populate.meta.fields already disabled for the table. Can't be re-enabled back"))
      }
    } finally {
      spark.stop()
      FileUtils.deleteDirectory(path.toFile)
    }
  }

  test("test drop duplicates row writing for bulk_insert") {
    initSparkContext("test_append_mode")
    val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    try {

      val hoodieFooTableName = "hoodie_foo_tbl"

      //create a new table
      val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME.key -> hoodieFooTableName,
        DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
        "hoodie.bulkinsert.shuffle.parallelism" -> "4",
        DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
        DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "true",
        INSERT_DROP_DUPS.key -> "true",
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
        DataSourceWriteOptions.KEYGENERATOR_CLASS.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      val records = DataSourceTestUtils.generateRandomRows(100)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(spark.sparkContext.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df)
      fail("Drop duplicates with bulk insert in row writing should have thrown exception")
    } catch {
      case e: HoodieException => assertTrue(e.getMessage.contains("Dropping duplicates with bulk_insert in row writer path is not supported yet"))
    } finally {
      spark.stop()
      FileUtils.deleteDirectory(path.toFile)
    }
  }

  test("test insert dataset without precombine field") {
    initSparkContext("test_bulk_insert_datasource")
    val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    try {

      val sqlContext = spark.sqlContext
      val sc = spark.sparkContext
      val hoodieFooTableName = "hoodie_foo_tbl"

      //create a new table
      val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME.key -> hoodieFooTableName,
        "hoodie.bulkinsert.shuffle.parallelism" -> "1",
        DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
        DataSourceWriteOptions.INSERT_DROP_DUPS.key -> "false",
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
        DataSourceWriteOptions.KEYGENERATOR_CLASS.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      val records = DataSourceTestUtils.generateRandomRows(100)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams - DataSourceWriteOptions.PRECOMBINE_FIELD.key, df)

      // collect all parition paths to issue read of parquet files
      val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
      // Check the entire dataset has all records still
      val fullPartitionPaths = new Array[String](3)
      for (i <- 0 until fullPartitionPaths.length) {
        fullPartitionPaths(i) = String.format("%s/%s/*", path.toAbsolutePath.toString, partitions(i))
      }

      // fetch all records from parquet files generated from write to hudi
      val actualDf = spark.sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))

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

  test("test bulk insert dataset with datasource impl multiple rounds") {
    initSparkContext("test_bulk_insert_datasource")
    val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    try {

      val hoodieFooTableName = "hoodie_foo_tbl"

      //create a new table
      val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
        HoodieWriteConfig.TABLE_NAME.key -> hoodieFooTableName,
        "hoodie.bulkinsert.shuffle.parallelism" -> "4",
        DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
        DataSourceWriteOptions.ENABLE_ROW_WRITER.key -> "true",
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
        DataSourceWriteOptions.KEYGENERATOR_CLASS.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
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

  List((DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, HoodieFileFormat.PARQUET.name(), true), (DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, HoodieFileFormat.ORC.name(), true),
    (DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL, HoodieFileFormat.PARQUET.name(), true), (DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL, HoodieFileFormat.ORC.name(), true),
    (DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, HoodieFileFormat.PARQUET.name(), false), (DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL, HoodieFileFormat.PARQUET.name(), false))
    .foreach(t => {
      val tableType = t._1
      val baseFileFormat = t._2
      val populateMetaFields = t._3
      test("test basic HoodieSparkSqlWriter functionality with datasource insert for " + tableType + " with " + baseFileFormat + " as the base file format "
      + " with populate meta fields " + populateMetaFields) {
        initSparkContext("test_insert_base_file_format_datasource")
        val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
        try {
          val hoodieFooTableName = "hoodie_foo_tbl"
          //create a new table
          val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
            HoodieWriteConfig.TABLE_NAME.key -> hoodieFooTableName,
            HoodieWriteConfig.BASE_FILE_FORMAT.key -> baseFileFormat,
            DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
            HoodieWriteConfig.INSERT_PARALLELISM.key -> "4",
            DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
            DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
            DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
            HoodieTableConfig.HOODIE_POPULATE_META_FIELDS.key() -> String.valueOf(populateMetaFields),
            DataSourceWriteOptions.KEYGENERATOR_CLASS.key -> classOf[SimpleKeyGenerator].getCanonicalName)
          val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

          // generate the inserts
          val schema = DataSourceTestUtils.getStructTypeExampleSchema
          val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
          val modifiedSchema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, "trip", "example.schema")
          val records = DataSourceTestUtils.generateRandomRows(100)
          val recordsSeq = convertRowListToSeq(records)
          val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)

          val client = spy(DataSourceUtils.createHoodieClient(
            new JavaSparkContext(sc),
            modifiedSchema.toString,
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
          var actualDf : DataFrame = null
          if (baseFileFormat.equalsIgnoreCase(HoodieFileFormat.PARQUET.name())) {
            actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
          } else if (baseFileFormat.equalsIgnoreCase(HoodieFileFormat.ORC.name())) {
            actualDf = sqlContext.read.orc(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
          }

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
            HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP.key -> srcPath.toAbsolutePath.toString,
            HoodieWriteConfig.TABLE_NAME.key -> hoodieFooTableName,
            DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
            HoodieBootstrapConfig.BOOTSTRAP_PARALLELISM.key -> "4",
            DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL,
            DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
            DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
            HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS.key -> classOf[NonpartitionedKeyGenerator].getCanonicalName)
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

  List(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
    .foreach(tableType => {
      test("test schema evolution for " + tableType) {
        initSparkContext("test_schema_evolution")
        val path = java.nio.file.Files.createTempDirectory("hoodie_test_path_schema_evol")
        try {
          val hoodieFooTableName = "hoodie_foo_tbl_schema_evolution_" + tableType
          //create a new table
          val fooTableModifier = Map("path" -> path.toAbsolutePath.toString,
            HoodieWriteConfig.TABLE_NAME.key -> hoodieFooTableName,
            "hoodie.insert.shuffle.parallelism" -> "1",
            "hoodie.upsert.shuffle.parallelism" -> "1",
            DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
            DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
            DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
            DataSourceWriteOptions.KEYGENERATOR_CLASS.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
          val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

          // generate the inserts
          var schema = DataSourceTestUtils.getStructTypeExampleSchema
          var structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
          var records = DataSourceTestUtils.generateRandomRows(10)
          var recordsSeq = convertRowListToSeq(records)
          var df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
          // write to Hudi
          HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableParams, df1)

          val snapshotDF1 = spark.read.format("org.apache.hudi")
            .load(path.toAbsolutePath.toString + "/*/*/*/*")
          assertEquals(10, snapshotDF1.count())

          // remove metadata columns so that expected and actual DFs can be compared as is
          val trimmedDf1 = snapshotDF1.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
            .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
            .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))

          assert(df1.except(trimmedDf1).count() == 0)

          // issue updates so that log files are created for MOR table
          var updates = DataSourceTestUtils.generateUpdates(records, 5);
          var updatesSeq = convertRowListToSeq(updates)
          var updatesDf = spark.createDataFrame(sc.parallelize(updatesSeq), structType)
          // write updates to Hudi
          HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, updatesDf)

          val snapshotDF2 = spark.read.format("org.apache.hudi")
            .load(path.toAbsolutePath.toString + "/*/*/*/*")
          assertEquals(10, snapshotDF2.count())

          // remove metadata columns so that expected and actual DFs can be compared as is
          val trimmedDf2 = snapshotDF1.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
            .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
            .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))

          // ensure 2nd batch of updates matches.
          assert(updatesDf.intersect(trimmedDf2).except(updatesDf).count() == 0)

          // getting new schema with new column
          schema = DataSourceTestUtils.getStructTypeExampleEvolvedSchema
          structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
          records = DataSourceTestUtils.generateRandomRowsEvolvedSchema(5)
          recordsSeq = convertRowListToSeq(records)
          val df3 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
          // write to Hudi with new column
          HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams, df3)

          val snapshotDF3 = spark.read.format("org.apache.hudi")
            .load(path.toAbsolutePath.toString + "/*/*/*/*")
          assertEquals(15, snapshotDF3.count())

          // remove metadata columns so that expected and actual DFs can be compared as is
          val trimmedDf3 = snapshotDF3.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
            .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
            .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))

          // ensure 2nd batch of updates matches.
          assert(df3.intersect(trimmedDf3).except(df3).count() == 0)

        } finally {
          spark.stop()
          FileUtils.deleteDirectory(path.toFile)
        }
      }
    })

  test("Test build sync config for spark sql") {
    initSparkContext("test build sync config")
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val basePath = "/tmp/hoodie_test"
    val params = Map(
      "path" -> basePath,
      DataSourceWriteOptions.TABLE_NAME.key -> "test_hoodie",
      DataSourceWriteOptions.HIVE_PARTITION_FIELDS.key -> "partition",
      DataSourceWriteOptions.HIVE_SKIP_RO_SUFFIX.key -> "true",
      DataSourceWriteOptions.HIVE_CREATE_MANAGED_TABLE.key -> "true"
    )
    val parameters = HoodieWriterUtils.parametersWithWriteDefaults(params)
    val hoodieConfig = HoodieWriterUtils.convertMapToHoodieConfig(parameters)

    val buildSyncConfigMethod =
      HoodieSparkSqlWriter.getClass.getDeclaredMethod("buildSyncConfig", classOf[Path],
        classOf[HoodieConfig], classOf[SQLConf])
    buildSyncConfigMethod.setAccessible(true)

    val hiveSyncConfig = buildSyncConfigMethod.invoke(HoodieSparkSqlWriter,
      new Path(basePath), hoodieConfig, spark.sessionState.conf).asInstanceOf[HiveSyncConfig]
    assertTrue(hiveSyncConfig.skipROSuffix)
    assertTrue(hiveSyncConfig.createManagedTable)
    assertTrue(hiveSyncConfig.syncAsSparkDataSourceTable)
    assertResult(spark.sessionState.conf.getConf(StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD))(hiveSyncConfig.sparkSchemaLengthThreshold)
  }

  test("Test build sync config for skip Ro Suffix vals") {
    initSparkContext("test build sync config for skip Ro suffix vals")
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val basePath = "/tmp/hoodie_test"
    val params = Map(
      "path" -> basePath,
      DataSourceWriteOptions.TABLE_NAME.key -> "test_hoodie",
      DataSourceWriteOptions.HIVE_PARTITION_FIELDS.key -> "partition"
    )
    val parameters = HoodieWriterUtils.parametersWithWriteDefaults(params)
    val hoodieConfig = HoodieWriterUtils.convertMapToHoodieConfig(parameters)

    val buildSyncConfigMethod =
      HoodieSparkSqlWriter.getClass.getDeclaredMethod("buildSyncConfig", classOf[Path],
        classOf[HoodieConfig], classOf[SQLConf])
    buildSyncConfigMethod.setAccessible(true)

    val hiveSyncConfig = buildSyncConfigMethod.invoke(HoodieSparkSqlWriter,
      new Path(basePath), hoodieConfig, spark.sessionState.conf).asInstanceOf[HiveSyncConfig]
    assertFalse(hiveSyncConfig.skipROSuffix)
  }

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

  test("test Incremental View WithReplacement") {
    List(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL).foreach { tableType =>
      initSparkContext("testNonPartitionTableWithMetaTable")
      initSparkContext("test_schema_evolution")
      val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
      val bootStrapPath = java.nio.file.Files.createTempDirectory("hoodie_test_bootstrap")
      val basePath = path.toAbsolutePath.toString
      val baseBootStrapPath = bootStrapPath.toAbsolutePath.toString
      val options = Map(DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "col3",
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "keyid",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
        DataSourceWriteOptions.KEYGENERATOR_CLASS.key -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        HoodieWriteConfig.TABLE_NAME.key -> "hoodie_test")
      try {
        val df = spark.range(0, 1000).toDF("keyid")
          .withColumn("col3", expr("keyid"))
          .withColumn("age", lit(1))
          .withColumn("p", lit(2))

        df.write.format("hudi")
          .options(options)
          .option(DataSourceWriteOptions.OPERATION.key, "insert")
          .option("hoodie.insert.shuffle.parallelism", "4")
          .mode(SaveMode.Overwrite).save(basePath)

        df.write.format("hudi")
          .options(options)
          .option(DataSourceWriteOptions.OPERATION.key, "insert_overwrite_table")
          .option("hoodie.insert.shuffle.parallelism", "4")
          .mode(SaveMode.Append).save(basePath)

        val currentCommits = spark.read.format("hudi").load(basePath).select("_hoodie_commit_time").take(1).map(_.getString(0))
        val incrementalKeyIdNum = spark.read.format("hudi").option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "0000")
          .option(DataSourceReadOptions.END_INSTANTTIME.key, currentCommits(0))
          .load(basePath).select("keyid").orderBy("keyid").count
        assert(incrementalKeyIdNum == 1000)

        // add bootstap test
        df.write.mode(SaveMode.Overwrite).save(baseBootStrapPath)
        // boostrap table
        spark.emptyDataFrame.write.format("hudi")
          .options(options)
          .option(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP.key, baseBootStrapPath)
          .option(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS.key, classOf[NonpartitionedKeyGenerator].getCanonicalName)
          .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
          .option(HoodieBootstrapConfig.BOOTSTRAP_PARALLELISM.key, "4")
          .mode(SaveMode.Overwrite).save(basePath)

        df.write.format("hudi")
          .options(options)
          .option(DataSourceWriteOptions.OPERATION.key, "insert_overwrite_table")
          .option("hoodie.insert.shuffle.parallelism", "4")
          .mode(SaveMode.Append).save(basePath)

        val currentCommitsBootstrap = spark.read.format("hudi").load(basePath).select("_hoodie_commit_time").take(1).map(_.getString(0))
        val incrementalKeyIdNumBootstrap = spark.read.format("hudi").option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "0000")
          .option(DataSourceReadOptions.END_INSTANTTIME.key, currentCommitsBootstrap(0))
          .load(basePath).select("keyid").orderBy("keyid").count
        assert(incrementalKeyIdNumBootstrap == 1000)
      } finally {
        spark.stop()
        FileUtils.deleteDirectory(path.toFile)
        FileUtils.deleteDirectory(bootStrapPath.toFile)
      }
    }
  }

  test("test Non partition table with metatable support") {
    List(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL).foreach { tableType =>
      initSparkContext("testNonPartitionTableWithMetaTable")
      initSparkContext("test_schema_evolution")
      val path = java.nio.file.Files.createTempDirectory("hoodie_test_path")
      val basePath = path.toAbsolutePath.toString
      try {
        val df = spark.range(0, 10).toDF("keyid")
          .withColumn("col3", expr("keyid"))
          .withColumn("age", expr("keyid + 1000"))

        df.write.format("hudi")
          .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "col3")
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "keyid")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "")
          .option(DataSourceWriteOptions.KEYGENERATOR_CLASS.key, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
          .option(DataSourceWriteOptions.OPERATION.key, "insert")
          .option("hoodie.insert.shuffle.parallelism", "1")
          .option("hoodie.metadata.enable", "true")
          .option(HoodieWriteConfig.TABLE_NAME.key, "hoodie_test")
          .mode(SaveMode.Overwrite).save(basePath)
        // upsert same record again
        val df_update = spark.range(0, 10).toDF("keyid")
          .withColumn("col3", expr("keyid"))
          .withColumn("age", expr("keyid + 2000"))
        df_update.write.format("hudi")
          .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "col3")
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "keyid")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "")
          .option(DataSourceWriteOptions.KEYGENERATOR_CLASS.key, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
          .option(DataSourceWriteOptions.OPERATION.key, "upsert")
          .option("hoodie.upsert.shuffle.parallelism", "1")
          .option("hoodie.metadata.enable", "true")
          .option(HoodieWriteConfig.TABLE_NAME.key, "hoodie_test")
          .mode(SaveMode.Append).save(basePath)
        assert(spark.read.format("hudi").load(basePath).count() == 10)
        assert(spark.read.format("hudi").load(basePath).where("age >= 2000").count() == 10)
      } finally {
        spark.stop()
        FileUtils.deleteDirectory(path.toFile)
      }
    }
  }
}
