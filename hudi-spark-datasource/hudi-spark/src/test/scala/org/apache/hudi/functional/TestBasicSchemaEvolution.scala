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

import org.apache.hudi.{DataSourceWriteOptions, HoodieSchemaConversionUtils, ScalaAssertionSupport, SparkAdapterSupport}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, TableSchemaResolver}
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.SchemaCompatibilityException
import org.apache.hudi.functional.TestBasicSchemaEvolution.{dropColumn, injectColumnAt}
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{functions, Row, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.function.Consumer

import scala.collection.JavaConverters._

class TestBasicSchemaEvolution extends HoodieSparkClientTestBase with ScalaAssertionSupport with SparkAdapterSupport {

  var spark: SparkSession = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key() -> "true",
    HoodieWriteConfig.RECORD_MERGE_MODE.key() -> RecordMergeMode.COMMIT_TIME_ORDERING.name(),
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  override def getSparkSessionExtensionsInjector: Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
    FileSystem.closeAll()
    System.gc()
  }

  // TODO add test-case for upcasting

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,bulk_insert,true",
    "COPY_ON_WRITE,bulk_insert,false",
    "COPY_ON_WRITE,insert,true",
    "COPY_ON_WRITE,insert,false",
    "COPY_ON_WRITE,upsert,true",
    "COPY_ON_WRITE,upsert,false",
    "MERGE_ON_READ,bulk_insert,true",
    "MERGE_ON_READ,bulk_insert,false",
    "MERGE_ON_READ,insert,true",
    "MERGE_ON_READ,insert,false",
    "MERGE_ON_READ,upsert,true",
    "MERGE_ON_READ,upsert,false"
  ))
  def testBasicSchemaEvolution(tableType: HoodieTableType, opType: String, shouldReconcileSchema: Boolean): Unit = {
    // open the schema validate
    val opts = commonOpts ++
      Map(
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
        HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key -> "true",
        DataSourceWriteOptions.RECONCILE_SCHEMA.key -> shouldReconcileSchema.toString,
        DataSourceWriteOptions.OPERATION.key -> opType
      )

    def appendData(schema: StructType, batch: Seq[Row], shouldAllowDroppedColumns: Boolean = false): Unit = {
      sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, batch, schema)
        .write
        .format("org.apache.hudi")
        .options(opts ++ Map(HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key -> shouldAllowDroppedColumns.toString))
        .mode(SaveMode.Append)
        .save(basePath)
    }

    def loadTable(): (StructType, Seq[Row]) = {
      val tableMetaClient = createMetaClient(spark, basePath)

      tableMetaClient.reloadActiveTimeline()

      val resolver = new TableSchemaResolver(tableMetaClient)
      val latestTableSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(resolver.getTableSchema(false))

      val df =
        spark.read.format("org.apache.hudi")
          .load(basePath)
          .drop(HoodieRecord.HOODIE_META_COLUMNS.asScala.toSeq: _*)
          .orderBy(functions.col("_row_key").cast(IntegerType))

      (latestTableSchema, df.collectAsList.asScala.toSeq)
    }

    //
    // 1. Write 1st batch with schema A
    //

    val firstSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("last_name", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val firstBatch = Seq(
      Row("1", "Andy", "Cooper", 1, 1),
      Row("2", "Lisi", "Wallace", 1, 1),
      Row("3", "Zhangsan", "Shu", 1, 1))

    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, firstBatch, firstSchema)
      .write
      .format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    //
    // 2. Write 2d batch with another schema (added column `age`)
    //

    val secondSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("last_name", StringType, nullable = true) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val secondSchemaWithOrdering = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("last_name", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) ::
        StructField("age", StringType, nullable = true) :: Nil)

    val secondBatch = Seq(
      Row("4", "John", "Green", "10", 1, 1),
      Row("5", "Jack", "Sparrow", "13", 1, 1),
      Row("6", "Jill", "Fiorella", "12", 1, 1))

    appendData(secondSchema, secondBatch)
    val (tableSchemaAfterSecondBatch, rowsAfterSecondBatch) = loadTable()

    // NOTE: In case schema reconciliation is ENABLED, Hudi would prefer the new batch's schema (since it's adding a
    //       new column, compared w/ the table's one), therefore this case would be identical to reconciliation
    //       being DISABLED
    //
    //       In case schema reconciliation is DISABLED, table will be overwritten in the batch's schema,
    //       entailing that the data in the added columns for table's existing records will be added w/ nulls,
    //       in case new column is nullable, and would fail otherwise
    if (true) {
      if (shouldReconcileSchema) {
        assertEquals(secondSchema, tableSchemaAfterSecondBatch)
        val ageColOrd = secondSchema.indexWhere(_.name == "age")
        val rowsToAdd = secondBatch

        val expectedRows = injectColumnAt(firstBatch, ageColOrd, null) ++ rowsToAdd
        assertEquals(expectedRows, rowsAfterSecondBatch)
      } else {
        // Second schema for the table is expected to reconcile ordering if enabled

        // Reorder batch based on the expected schema
        val secondBatchWithProperOrder = Seq(
          Row("4", "John", "Green", 1, 1, "10"),
          Row("5", "Jack", "Sparrow", 1, 1, "13"),
          Row("6", "Jill", "Fiorella", 1, 1, "12"))

        assertEquals(secondSchemaWithOrdering, tableSchemaAfterSecondBatch)
        val ageColOrd = secondSchemaWithOrdering.indexWhere(_.name == "age")
        val rowsToAdd = secondBatchWithProperOrder
        val expectedRows = injectColumnAt(firstBatch, ageColOrd, null) ++ rowsToAdd
        assertEquals(expectedRows, rowsAfterSecondBatch)
      }
    }

    //
    // 3. Write 3d batch with another schema (w/ omitted a _nullable_ column `second_name`, expected to succeed if
    // col drop is enabled)
    //

    val thirdSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val thirdSchemaWithOrdering = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("first_name", StringType, nullable = false) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) ::
        StructField("age", StringType, nullable = true) :: Nil)

    val thirdBatch = Seq(
      Row("7", "Harry", "15", 1, 1),
      Row("8", "Ron", "14", 1, 1),
      Row("9", "Germiona", "16", 1, 1))

    if (shouldReconcileSchema) {
      appendData(thirdSchema, thirdBatch)
    } else {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(thirdSchema, thirdBatch)
      }
      appendData(thirdSchema, thirdBatch, shouldAllowDroppedColumns = true)
    }
    val (tableSchemaAfterThirdBatch, rowsAfterThirdBatch) = loadTable()

    // NOTE: In case schema reconciliation is ENABLED, Hudi would prefer the table's schema over the new batch
    //       schema (since we drop the column in the new batch), therefore table's schema after commit will actually
    //       stay the same, adding back (dropped) columns to the records in the batch (setting them as null).
    //
    //       In case schema reconciliation is DISABLED, table will be overwritten in the batch's schema,
    //       entailing that the data in the dropped columns for table's existing records will be dropped.
    if (shouldReconcileSchema) {
      assertEquals(secondSchema, tableSchemaAfterThirdBatch)

      val lastNameColOrd = firstSchema.indexWhere(_.name == "last_name")
      val expectedRows = rowsAfterSecondBatch ++ injectColumnAt(thirdBatch, lastNameColOrd, null)

      assertEquals(expectedRows, rowsAfterThirdBatch)
    } else {
      assertEquals(thirdSchemaWithOrdering, tableSchemaAfterThirdBatch)

      val lastNameColOrd = secondSchemaWithOrdering.indexWhere(_.name == "last_name")
      // properly maintain order of columns
      val rowsToAdd = Seq(
        Row("7", "Harry", 1, 1, "15"),
        Row("8", "Ron", 1, 1, "14"),
        Row("9", "Germiona", 1, 1, "16"))
      val expectedRows = dropColumn(rowsAfterSecondBatch, lastNameColOrd) ++ rowsToAdd

      assertEquals(expectedRows, rowsAfterThirdBatch)
    }

    //
    // 4. Write 4th batch with another schema (w/ omitted a _non-nullable_ column `first_name`, expected to fail
    //    in case when schema reconciliation is enabled, expected to succeed otherwise)
    //

    val fourthSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", IntegerType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val fourthBatch = Seq(
      Row("10", "15", 1, 1),
      Row("11", "14", 1, 1),
      Row("12", "16", 1, 1))

    // NOTE: In case schema reconciliation is ENABLED, Hudi would prefer the table's schema over the new batch
    //       schema, therefore table's schema after commit will actually stay the same, adding back (dropped) columns
    //       to the records in the batch. Since batch omits column that is designated as non-null, write is expected
    //       to fail (being unable to set the missing column values to null).
    //
    //       In case schema reconciliation is DISABLED, table will be overwritten in the batch's schema,
    //       entailing that the data in the dropped columns for table's existing records will be dropped.
    if (shouldReconcileSchema) {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(fourthSchema, fourthBatch)
      }
    } else {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(fourthSchema, fourthBatch)
      }
      appendData(fourthSchema, fourthBatch, shouldAllowDroppedColumns = true)
      val (latestTableSchema, rows) = loadTable()

      val fourthSchemaWithOrdering = StructType(
        StructField("_row_key", StringType, nullable = true) ::
          StructField("timestamp", IntegerType, nullable = true) ::
          StructField("partition", IntegerType, nullable = true) ::
          StructField("age", StringType, nullable = true) :: Nil)
      assertEquals(fourthSchemaWithOrdering, latestTableSchema)

      val firstNameColOrd = thirdSchemaWithOrdering.indexWhere(_.name == "first_name")

      // Order the columns
      val rowsToAdd = Seq(
        Row("10", 1, 1, "15"),
        Row("11", 1, 1, "14"),
        Row("12", 1, 1, "16"))
      val expectedRecords =
        dropColumn(rowsAfterThirdBatch, firstNameColOrd) ++ rowsToAdd

      assertEquals(expectedRecords, rows)
    }

    //
    // 5. Write 5th batch with another schema w/ data-type changing for a column `timestamp`;
    //      - Expected to succeed when reconciliation is off, and
    //      - Expected to fail when reconciliation is on (b/c we can't down-cast Long to Int)
    //

    val fifthSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", LongType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val fifthBatch = Seq(
      Row("10", "15", 9876543210L, 1),
      Row("11", "14", 9876543211L, 1),
      Row("12", "16", 9876543212L, 1))

    if (shouldReconcileSchema) {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(fifthSchema, fifthBatch)
      }
    } else {
      appendData(fifthSchema, fifthBatch)

      // TODO(SPARK-40876) this is disabled, until primitive-type promotions are properly supported
      //                   w/in Spark's vectorized reader
      //val (latestTableSchema, rows) = loadTable()
    }

    //
    // 6. Write 6th batch with another schema (w/ data-type changed for a column `timestamp`, expected to fail)
    //

    val sixthSchema = StructType(
      StructField("_row_key", StringType, nullable = true) ::
        StructField("age", StringType, nullable = true) ::
        StructField("timestamp", StringType, nullable = true) ::
        StructField("partition", IntegerType, nullable = true) :: Nil)

    val sixthBatch = Seq(
      Row("10", "15", "1", 1),
      Row("11", "14", "1", 1),
      Row("12", "16", "1", 1))

    // Now, only fails for reconcile
    if (shouldReconcileSchema) {
      assertThrows(classOf[SchemaCompatibilityException]) {
        appendData(sixthSchema, sixthBatch)
      }
    } else {
      appendData(sixthSchema, sixthBatch)
    }


    // TODO add test w/ overlapping updates
  }
}

object TestBasicSchemaEvolution {

  def dropColumn(rows: Seq[Row], idx: Int): Seq[Row] =
    rows.map { r =>
      val values = r.toSeq.zipWithIndex
        .filterNot { case (_, cidx) => cidx == idx }
        .map { case (c, _) => c }
      Row(values: _*)
    }

  def injectColumnAt(rows: Seq[Row], idx: Int, value: Any): Seq[Row] =
    rows.map { r =>
      val (left, right) = r.toSeq.splitAt(idx)
      val values = (left :+ value) ++ right
      Row(values: _*)
    }

}
