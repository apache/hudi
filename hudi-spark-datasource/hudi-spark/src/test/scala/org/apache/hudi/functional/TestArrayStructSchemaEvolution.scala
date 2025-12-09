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

import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkRecordMerger, ScalaAssertionSupport}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

/**
 * Test to verify schema evolution for array of structs with record mergers.
 * This test reproduces a bug where schema evolution of array<struct<...>> fields
 * can cause data corruption when using certain record mergers.
 */
class TestArrayStructSchemaEvolution extends HoodieSparkClientTestBase with ScalaAssertionSupport {
  var spark: SparkSession = _

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupFileSystem()
  }

  @Test
  def testArrayStructSchemaEvolutionWithRecordMerger(): Unit = {
    val tablePath = basePath + "/array_struct_bug_100"
    val tableName = "array_struct_bug_100"

    // ==========================================================
    // STEP 1: Initial schema (no evolution yet)
    // ==========================================================
    val schemaV1 = new StructType()
      .add("id", StringType, nullable = true)
      .add("items", ArrayType(new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", IntegerType, nullable = true)
        .add("c", IntegerType, nullable = true)
        .add("d", IntegerType, nullable = true)
      ), nullable = true)
    val row1Items = Seq(
      Row(1, 11, 111, 1111),
      Row(2, 22, 222, 2222),
      Row(3, 33, 333, 3333)
    )
    val row2Items = Seq(
      Row(10, 77, 777, 7777),
      Row(20, 88, 888, 8888),
      Row(30, 99, 999, 9999)
    )
    val initialData = Seq(
      Row("1", row1Items),
      Row("2", row2Items)
    )
    val dfInit = spark.createDataFrame(spark.sparkContext.parallelize(initialData), schemaV1)

    // ==========================================================
    // STEP 2: Write initial data using INSERT (not bulk-insert)
    // ==========================================================
    val hudiOpts = Map(
      HoodieTableConfig.NAME.key -> tableName,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "id",
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      HoodieWriteConfig.RECORD_MERGER_IMPLS.key -> classOf[HoodieSparkRecordMerger].getName
    )
    dfInit.write.format("hudi").options(hudiOpts).mode(SaveMode.Overwrite).save(tablePath)
    // Verify initial data
    val dfAfterInsert = spark.read.format("hudi").load(tablePath)
    assertEquals(2, dfAfterInsert.count(), "Should have 2 records after initial insert")

    // ==========================================================
    // STEP 3: Schema evolution - Add a new field to the struct inside the array
    // ==========================================================
    val schemaV2 = new StructType()
      .add("id", StringType, nullable = true)
      .add("items", ArrayType(new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", IntegerType, nullable = true)
        .add("c", IntegerType, nullable = true)
        .add("d", IntegerType, nullable = true)
        .add("e", IntegerType, nullable = true) // <-- NEW FIELD
      ), nullable = true)
    val row1ItemsEvolved = Seq(
      Row(1, 11, 111, 1111, 11111),
      Row(2, 22, 222, 2222, 22222),
      Row(3, 33, 333, 3333, 33333)
    )
    val dfEvolved = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("1", row1ItemsEvolved))),
      schemaV2
    )

    // ==========================================================
    // STEP 4: Upsert with HoodieSparkRecordMerger
    // ==========================================================
    val hudiOptsUpsert = Map(
      HoodieTableConfig.NAME.key -> tableName,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "id",
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      HoodieWriteConfig.RECORD_MERGER_IMPLS.key -> "org.apache.hudi.HoodieSparkRecordMerger"
    )
    dfEvolved.write.format("hudi").options(hudiOptsUpsert).mode(SaveMode.Append).save(tablePath)

    // ==========================================================
    // STEP 5: Load after upsert and verify data integrity
    // ==========================================================
    val dfFinal = spark.read.format("hudi").load(tablePath)
    // Verify we still have 2 records
    assertEquals(2, dfFinal.count(), "Should still have 2 records after upsert")
    // Verify the schema includes the new field 'e'
    val finalSchema = dfFinal.schema
    val itemsField = finalSchema.fields.find(_.name == "items").get
    assertTrue(itemsField.dataType.isInstanceOf[ArrayType], "items should be an ArrayType")
    val arrayType = itemsField.dataType.asInstanceOf[ArrayType]
    assertTrue(arrayType.elementType.isInstanceOf[StructType], "items array element should be StructType")
    val structType = arrayType.elementType.asInstanceOf[StructType]
    val fieldNames = structType.fields.map(_.name)
    assertTrue(fieldNames.contains("e"), "Schema should include the evolved field 'e'")
    assertEquals(5, fieldNames.length, "Struct should have 5 fields (a, b, c, d, e)")
    // Verify we can read all data without errors (this would fail if data is corrupted)
    dfFinal.foreach(_ => {})

    // Verify data for id="1" (updated record)
    val record1 = dfFinal.filter("id = '1'").collect()
    assertEquals(1, record1.length, "Should have exactly one record with id='1'")
    val items1 = record1(0).getAs[scala.collection.Seq[Row]]("items").toSeq
    assertNotNull(items1, "items should not be null for id='1'")
    assertEquals(3, items1.length, "id='1' should have 3 items")
    // Verify first item of id="1" has all fields including 'e'
    val firstItem1 = items1.head
    assertEquals(1, firstItem1.getInt(0), "First item 'a' should be 1")
    assertEquals(11, firstItem1.getInt(1), "First item 'b' should be 11")
    assertEquals(111, firstItem1.getInt(2), "First item 'c' should be 111")
    assertEquals(1111, firstItem1.getInt(3), "First item 'd' should be 1111")
    assertEquals(11111, firstItem1.getInt(4), "First item 'e' should be 11111")

    // Verify second item of id="1"
    val secondItem1 = items1(1)
    assertEquals(2, secondItem1.getInt(0), "Second item 'a' should be 2")
    assertEquals(22, secondItem1.getInt(1), "Second item 'b' should be 22")
    assertEquals(222, secondItem1.getInt(2), "Second item 'c' should be 222")
    assertEquals(2222, secondItem1.getInt(3), "Second item 'd' should be 2222")
    assertEquals(22222, secondItem1.getInt(4), "Second item 'e' should be 22222")
    // Verify data for id="2" (unchanged record - should have null for 'e')
    val record2 = dfFinal.filter("id = '2'").collect()
    assertEquals(1, record2.length, "Should have exactly one record with id='2'")
    val items2 = record2(0).getAs[scala.collection.Seq[Row]]("items").toSeq
    assertNotNull(items2, "items should not be null for id='2'")
    assertEquals(3, items2.length, "id='2' should have 3 items")
    // Verify first item of id="2" - should have original values and null for 'e'
    val firstItem2 = items2(0)
    assertEquals(10, firstItem2.getInt(0), "First item 'a' should be 10")
    assertEquals(77, firstItem2.getInt(1), "First item 'b' should be 77")
    assertEquals(777, firstItem2.getInt(2), "First item 'c' should be 777")
    assertEquals(7777, firstItem2.getInt(3), "First item 'd' should be 7777")
    assertTrue(firstItem2.isNullAt(4), "First item 'e' should be null for unchanged record")
    // Verify second item of id="2"
    val secondItem2 = items2(1)
    assertEquals(20, secondItem2.getInt(0), "Second item 'a' should be 20")
    assertEquals(88, secondItem2.getInt(1), "Second item 'b' should be 88")
    assertEquals(888, secondItem2.getInt(2), "Second item 'c' should be 888")
    assertEquals(8888, secondItem2.getInt(3), "Second item 'd' should be 8888")
    assertTrue(secondItem2.isNullAt(4), "Second item 'e' should be null for unchanged record")
  }
}

