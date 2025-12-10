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
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

/**
 * Test to verify schema evolution for complex data types (Array[Struct], Nested Structs, Array[Map[Struct]])
 * with record mergers. These tests verify that schema evolution of complex nested fields
 * works correctly and doesn't cause data corruption when using record mergers.
 */
class TestComplexTypeSchemaEvolution extends HoodieSparkClientTestBase with ScalaAssertionSupport {
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
    val tablePath = basePath + "/array_struct_evolution"
    val tableName = "array_struct_evolution"

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
        .add("e", IntegerType, nullable = true)
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

  @Test
  def testNestedStructSchemaEvolutionWithRecordMerger(): Unit = {
    val tablePath = basePath + "/nested_struct_evolution"
    val tableName = "nested_struct_evolution"

    // ==========================================================
    // STEP 1: Initial schema with nested struct (no evolution yet)
    // ==========================================================
    val nestedStructV1 = new StructType()
      .add("x", IntegerType, nullable = true)
      .add("y", IntegerType, nullable = true)
      .add("z", IntegerType, nullable = true)
    val schemaV1 = new StructType()
      .add("id", StringType, nullable = true)
      .add("location", nestedStructV1, nullable = true)
      .add("name", StringType, nullable = true)
    val initialData = Seq(
      Row("1", Row(10, 20, 30), "Location1"),
      Row("2", Row(40, 50, 60), "Location2")
    )
    val dfInit = spark.createDataFrame(spark.sparkContext.parallelize(initialData), schemaV1)

    // ==========================================================
    // STEP 2: Write initial data using INSERT
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
    val dfAfterInsert = spark.read.format("hudi").load(tablePath)
    assertEquals(2, dfAfterInsert.count(), "Should have 2 records after initial insert")

    // ==========================================================
    // STEP 3: Schema evolution - Add a new field to the nested struct
    // ==========================================================
    val nestedStructV2 = new StructType()
      .add("x", IntegerType, nullable = true)
      .add("y", IntegerType, nullable = true)
      .add("z", IntegerType, nullable = true)
      .add("w", IntegerType, nullable = true)
    val schemaV2 = new StructType()
      .add("id", StringType, nullable = true)
      .add("location", nestedStructV2, nullable = true)
      .add("name", StringType, nullable = true)
    val evolvedData = Seq(
      Row("1", Row(10, 20, 30, 40), "Location1")
    )
    val dfEvolved = spark.createDataFrame(
      spark.sparkContext.parallelize(evolvedData),
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
    assertEquals(2, dfFinal.count(), "Should still have 2 records after upsert")

    // Verify the schema includes the new field 'w' in nested struct
    val finalSchema = dfFinal.schema
    val locationField = finalSchema.fields.find(_.name == "location").get
    assertTrue(locationField.dataType.isInstanceOf[StructType], "location should be a StructType")
    val nestedStructType = locationField.dataType.asInstanceOf[StructType]
    val fieldNames = nestedStructType.fields.map(_.name)
    assertTrue(fieldNames.contains("w"), "Schema should include the evolved field 'w'")
    assertEquals(4, fieldNames.length, "Nested struct should have 4 fields (x, y, z, w)")
    dfFinal.foreach(_ => {})

    // Verify data for id="1" (updated record)
    val record1 = dfFinal.filter("id = '1'").collect()
    assertEquals(1, record1.length, "Should have exactly one record with id='1'")
    val location1 = record1(0).getAs[Row]("location")
    assertNotNull(location1, "location should not be null for id='1'")
    assertEquals(10, location1.getInt(0), "Location 'x' should be 10")
    assertEquals(20, location1.getInt(1), "Location 'y' should be 20")
    assertEquals(30, location1.getInt(2), "Location 'z' should be 30")
    assertEquals(40, location1.getInt(3), "Location 'w' should be 40")

    // Verify data for id="2" (unchanged record - should have null for 'w')
    val record2 = dfFinal.filter("id = '2'").collect()
    assertEquals(1, record2.length, "Should have exactly one record with id='2'")
    val location2 = record2(0).getAs[Row]("location")
    assertNotNull(location2, "location should not be null for id='2'")
    assertEquals(40, location2.getInt(0), "Location 'x' should be 40")
    assertEquals(50, location2.getInt(1), "Location 'y' should be 50")
    assertEquals(60, location2.getInt(2), "Location 'z' should be 60")
    assertTrue(location2.isNullAt(3), "Location 'w' should be null for unchanged record")
  }

  @Test
  def testArrayMapStructSchemaEvolutionWithRecordMerger(): Unit = {
    val tablePath = basePath + "/array_map_struct_evolution"
    val tableName = "array_map_struct_evolution"

    // ==========================================================
    // STEP 1: Initial schema with Array[Map[String, Struct]] (no evolution yet)
    // ==========================================================
    val innerStructV1 = new StructType()
      .add("col1", StringType, nullable = true)
      .add("col2", StringType, nullable = true)
      .add("col3", IntegerType, nullable = true)
    val schemaV1 = new StructType()
      .add("id", StringType, nullable = true)
      .add("events", ArrayType(
        new MapType(StringType, innerStructV1, true)
      ), nullable = true)
    val initialData = Seq(
      Row("1", Seq(
        Map("2022-12-01" -> Row("a1", "b1", 100)),
        Map("2022-12-02" -> Row("a2", "b2", 200))
      )),
      Row("2", Seq(
        Map("2022-12-03" -> Row("a3", "b3", 300)),
        Map("2022-12-04" -> Row("a4", "b4", 400))
      ))
    )
    val dfInit = spark.createDataFrame(spark.sparkContext.parallelize(initialData), schemaV1)

    // ==========================================================
    // STEP 2: Write initial data using INSERT
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
    val dfAfterInsert = spark.read.format("hudi").load(tablePath)
    assertEquals(2, dfAfterInsert.count(), "Should have 2 records after initial insert")

    // ==========================================================
    // STEP 3: Schema evolution - Add a new field to the struct inside the map
    // ==========================================================
    val innerStructV2 = new StructType()
      .add("col1", StringType, nullable = true)
      .add("col2", StringType, nullable = true)
      .add("col3", IntegerType, nullable = true)
      .add("col4", IntegerType, nullable = true)
    val schemaV2 = new StructType()
      .add("id", StringType, nullable = true)
      .add("events", ArrayType(
        new MapType(StringType, innerStructV2, true)
      ), nullable = true)
    val evolvedData = Seq(
      Row("1", Seq(
        Map("2022-12-01" -> Row("a1", "b1", 100, 1000)),
        Map("2022-12-02" -> Row("a2", "b2", 200, 2000))
      ))
    )
    val dfEvolved = spark.createDataFrame(
      spark.sparkContext.parallelize(evolvedData),
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
    assertEquals(2, dfFinal.count(), "Should still have 2 records after upsert")

    // Verify the schema includes the new field 'col4' in nested struct
    val finalSchema = dfFinal.schema
    val eventsField = finalSchema.fields.find(_.name == "events").get
    assertTrue(eventsField.dataType.isInstanceOf[ArrayType], "events should be an ArrayType")
    val arrayType = eventsField.dataType.asInstanceOf[ArrayType]
    assertTrue(arrayType.elementType.isInstanceOf[MapType], "events array element should be MapType")
    val mapType = arrayType.elementType.asInstanceOf[MapType]
    assertTrue(mapType.valueType.isInstanceOf[StructType], "map value should be StructType")
    val structType = mapType.valueType.asInstanceOf[StructType]
    val fieldNames = structType.fields.map(_.name)
    assertTrue(fieldNames.contains("col4"), "Schema should include the evolved field 'col4'")
    assertEquals(4, fieldNames.length, "Struct should have 4 fields (col1, col2, col3, col4)")
    dfFinal.foreach(_ => {})

    // Verify data for id="1" (updated record)
    val record1 = dfFinal.filter("id = '1'").collect()
    assertEquals(1, record1.length, "Should have exactly one record with id='1'")
    val events1 = record1(0).getAs[scala.collection.Seq[scala.collection.Map[String, Row]]]("events").toSeq
    assertNotNull(events1, "events should not be null for id='1'")
    assertEquals(2, events1.length, "id='1' should have 2 events")

    val firstEvent1 = events1.head.get("2022-12-01").get
    assertEquals("a1", firstEvent1.getString(0), "First event col1 should be 'a1'")
    assertEquals("b1", firstEvent1.getString(1), "First event col2 should be 'b1'")
    assertEquals(100, firstEvent1.getInt(2), "First event col3 should be 100")
    assertEquals(1000, firstEvent1.getInt(3), "First event col4 should be 1000")

    // Verify data for id="2" (unchanged record - should have null for 'col4')
    val record2 = dfFinal.filter("id = '2'").collect()
    assertEquals(1, record2.length, "Should have exactly one record with id='2'")
    val events2 = record2(0).getAs[scala.collection.Seq[scala.collection.Map[String, Row]]]("events").toSeq
    assertNotNull(events2, "events should not be null for id='2'")
    assertEquals(2, events2.length, "id='2' should have 2 events")

    val firstEvent2 = events2.head.get("2022-12-03").get
    assertEquals("a3", firstEvent2.getString(0), "First event col1 should be 'a3'")
    assertEquals("b3", firstEvent2.getString(1), "First event col2 should be 'b3'")
    assertEquals(300, firstEvent2.getInt(2), "First event col3 should be 300")
    assertTrue(firstEvent2.isNullAt(3), "First event col4 should be null for unchanged record")
  }
}
