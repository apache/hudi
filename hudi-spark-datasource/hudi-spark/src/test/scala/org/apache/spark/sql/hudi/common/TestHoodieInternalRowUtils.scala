/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.common

import org.apache.hudi.{AvroConversionUtils, SparkRowSerDe}
import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.common.schema.evolution.HoodieSchemaChangeApplier
import org.apache.hudi.testutils.HoodieClientTestUtils

import org.apache.avro.generic.GenericData
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{HoodieInternalRowUtils, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.assertEquals
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import java.nio.ByteBuffer
import java.util.{ArrayList, Collections => JCollections, HashMap, Objects}

class TestHoodieInternalRowUtils extends FunSuite with Matchers with BeforeAndAfterAll {
  private var sparkSession: SparkSession = _

  override protected def beforeAll(): Unit = {
    // Initialize a local spark env
    val jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(classOf[TestHoodieInternalRowUtils].getName))
    jsc.setLogLevel("ERROR")
    sparkSession = SparkSession.builder.config(jsc.getConf).getOrCreate
  }

  override protected def afterAll(): Unit = {
    sparkSession.close()
  }

  private val schema1 = StructType(Seq(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("address",
      StructType(Seq(
        StructField("city", StringType),
        StructField("street", StringType)
      ))
    )
  ))

  private val schema2 = StructType(Seq(
    StructField("name1", StringType),
    StructField("age1", IntegerType)
  ))

  private val mergedSchema = StructType(schema1.fields ++ schema2.fields)

  test("Test simple row rewriting") {
    val rows = Seq(
      Row("Andrew", 18, Row("Mission st", "SF"), "John", 19)
    )
    val data = sparkSession.sparkContext.parallelize(rows)
    val oldRow = sparkSession.createDataFrame(data, mergedSchema).queryExecution.toRdd.first()

    val rowWriter1 = HoodieInternalRowUtils.genUnsafeRowWriter(mergedSchema, schema1, JCollections.emptyMap(), JCollections.emptyMap())
    val newRow1 = rowWriter1(oldRow)

    val serDe1 = createSparkRowSerDe(schema1)
    assertEquals(serDe1.deserializeRow(newRow1), Row("Andrew", 18, Row("Mission st", "SF")));

    val rowWriter2 = HoodieInternalRowUtils.genUnsafeRowWriter(mergedSchema, schema2, JCollections.emptyMap(), JCollections.emptyMap())
    val newRow2 = rowWriter2(oldRow)

    val serDe2 = createSparkRowSerDe(schema2)
    assertEquals(serDe2.deserializeRow(newRow2), Row("John", 19));
  }

  test("Test simple rewriting (with nullable value)") {
    val data = sparkSession.sparkContext.parallelize(Seq(Row("Rob", 18, null.asInstanceOf[StructType])))
    val oldRow = sparkSession.createDataFrame(data, schema1).queryExecution.toRdd.first()
    val rowWriter = HoodieInternalRowUtils.genUnsafeRowWriter(schema1, mergedSchema, JCollections.emptyMap(), JCollections.emptyMap())
    val newRow = rowWriter(oldRow)

    val serDe = createSparkRowSerDe(mergedSchema)
    assertEquals(serDe.deserializeRow(newRow), Row("Rob", 18, null.asInstanceOf[StructType], null.asInstanceOf[StringType], null.asInstanceOf[IntegerType]))
  }

  test("Test rewriting with field value injections") {
    val rowWithNull = Seq(
      Row("Andrew", null, Row("Mission st", "SF"), "John", 19)
    )
    val oldRow = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rowWithNull), mergedSchema).queryExecution.toRdd.first()

    val updatedValuesMap: java.util.Map[Integer, Object] = JCollections.singletonMap(1, 18).asInstanceOf[java.util.Map[Integer, Object]]
    val rowWriter = HoodieInternalRowUtils.genUnsafeRowWriter(mergedSchema, schema1, JCollections.emptyMap(), updatedValuesMap)
    val newRow1 = rowWriter(oldRow)

    val serDe = createSparkRowSerDe(schema1)
    assertEquals(serDe.deserializeRow(newRow1), Row("Andrew", 18, Row("Mission st", "SF")));

    // non-nul value should not be rewritten
    val rowWithoutNull = Seq(
      Row("Andrew", 25, Row("Mission st", "SF"), "John", 19)
    )
    val oldRow2 = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rowWithoutNull), mergedSchema).queryExecution.toRdd.first()
    val newRow2 = rowWriter(oldRow2)
    assertEquals(serDe.deserializeRow(newRow2), Row("Andrew", 25, Row("Mission st", "SF")));
  }

  test("Test rewrite row with renamed columns") {
    // Original schema
    val oldSchema = StructType(Seq(
      StructField("first_name", StringType),
      StructField("years_old", IntegerType)
    ))

    // Renamed schema
    val newSchema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    // Rename mapping: new -> old
    val renameMap: java.util.Map[String, String] = new java.util.HashMap()
    renameMap.put("name", "first_name")
    renameMap.put("age", "years_old")

    // Sample row
    val oldRowData = sparkSession.sparkContext.parallelize(Seq(Row("Alice", 30)))
    val oldRow = sparkSession.createDataFrame(oldRowData, oldSchema).queryExecution.toRdd.first()

    // Generate writer with rename map
    val rowWriter = HoodieInternalRowUtils.genUnsafeRowWriter(oldSchema, newSchema, renameMap, JCollections.emptyMap())
    val newRow = rowWriter(oldRow)

    val serDe = createSparkRowSerDe(newSchema)
    assertEquals(Row("Alice", 30), serDe.deserializeRow(newRow))
  }

  test("Test rewrite row with columns swap") {
    // Original schema
    val oldSchema = StructType(Seq(
      StructField("first_name", StringType),
      StructField("years_old", IntegerType)
    ))

    // Renamed schema
    val newSchema = StructType(Seq(
      StructField("years_old", StringType),
      StructField("first_name", IntegerType)
    ))

    // Rename mapping: new -> old
    val renameMap: java.util.Map[String, String] = new java.util.HashMap()
    renameMap.put("years_old", "first_name")
    renameMap.put("first_name", "years_old")

    // Sample row
    val oldRowData = sparkSession.sparkContext.parallelize(Seq(Row("Alice", 30)))
    val oldRow = sparkSession.createDataFrame(oldRowData, oldSchema).queryExecution.toRdd.first()

    // Generate writer with rename map
    val rowWriter = HoodieInternalRowUtils.genUnsafeRowWriter(oldSchema, newSchema, renameMap, JCollections.emptyMap())
    val newRow = rowWriter(oldRow)

    val serDe = createSparkRowSerDe(newSchema)
    assertEquals(Row("Alice", 30), serDe.deserializeRow(newRow))
  }

  test("Test rewrite row with columns swap nested") {
    // Original schema
    val oldSchema = StructType(Seq(
      StructField("first_name", StringType),
      StructField("years_old", IntegerType),
      StructField("address",
        StructType(Seq(
          StructField("city", StringType),
          StructField("street", StringType)
        )
    ))))

    // Renamed schema
    val newSchema = StructType(Seq(
      StructField("years_old", StringType),
      StructField("first_name", IntegerType),
      StructField("address",
        StructType(Seq(
          StructField("street", StringType),
          StructField("city", StringType)
        )
        ))))

    // Rename mapping: new -> old
    val renameMap: java.util.Map[String, String] = new java.util.HashMap()
    renameMap.put("years_old", "first_name")
    renameMap.put("first_name", "years_old")
    renameMap.put("address.city", "street")
    renameMap.put("address.street", "city")

    // Sample row
    val oldRowData = sparkSession.sparkContext.parallelize(Seq(Row("Alice", 30, Row("SF", "Mission st"))))
    val oldRow = sparkSession.createDataFrame(oldRowData, oldSchema).queryExecution.toRdd.first()

    // Generate writer with rename map
    val rowWriter = HoodieInternalRowUtils.genUnsafeRowWriter(oldSchema, newSchema, renameMap, JCollections.emptyMap())
    val newRow = rowWriter(oldRow)

    val serDe = createSparkRowSerDe(newSchema)
    assertEquals(Row("Alice", 30, Row("SF", "Mission st")), serDe.deserializeRow(newRow))
  }

  /**
   * test record data type changes.
   * int => long/float/double/string
   * long => float/double/string
   * float => double/String
   * double => String/Decimal
   * Decimal => Decimal/String
   * String => date/decimal
   * date => String
   */
  test("Test rewrite record with type changed") {
    val schema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"h0_record\",\"namespace\":\"hoodie.h0\",\"fields\""
      + ":[{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"comb\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"com1\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"col0\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"col1\",\"type\":[\"null\",\"long\"],\"default\":null},"
      + "{\"name\":\"col11\",\"type\":[\"null\",\"long\"],\"default\":null},"
      + "{\"name\":\"col12\",\"type\":[\"null\",\"long\"],\"default\":null},"
      + "{\"name\":\"col2\",\"type\":[\"null\",\"float\"],\"default\":null},"
      + "{\"name\":\"col21\",\"type\":[\"null\",\"float\"],\"default\":null},"
      + "{\"name\":\"col3\",\"type\":[\"null\",\"double\"],\"default\":null},"
      + "{\"name\":\"col31\",\"type\":[\"null\",\"double\"],\"default\":null},"
      + "{\"name\":\"col4\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"hoodie.h0.h0_record.col4\","
      + "\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":4}],\"default\":null},"
      + "{\"name\":\"col41\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"hoodie.h0.h0_record.col41\","
      + "\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":4}],\"default\":null},"
      + "{\"name\":\"col5\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"col51\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"col6\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
      + "{\"name\":\"col7\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}],\"default\":null},"
      + "{\"name\":\"col8\",\"type\":[\"null\",\"boolean\"],\"default\":null},"
      + "{\"name\":\"col9\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"par\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null}]}")
    // create a test record with avroSchema
    val avroRecord = new GenericData.Record(schema.toAvroSchema)
    avroRecord.put("id", 1)
    avroRecord.put("comb", 100)
    avroRecord.put("com1", -100)
    avroRecord.put("col0", 256)
    avroRecord.put("col1", 1000L)
    avroRecord.put("col11", -100L)
    avroRecord.put("col12", 2000L)
    avroRecord.put("col2", -5.001f)
    avroRecord.put("col21", 5.001f)
    avroRecord.put("col3", 12.999d)
    avroRecord.put("col31", 9999.999d)
    val currentDecimalType = schema.getField("col4").get().schema().getNonNullType.asInstanceOf[HoodieSchema.Decimal]
    val bd = new java.math.BigDecimal("123.456").setScale(currentDecimalType.getScale)
    avroRecord.put("col4", HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(bd, currentDecimalType.toAvroSchema, currentDecimalType.toAvroSchema.getLogicalType))
    val currentDecimalType1 = schema.getField("col41").get().schema().getNonNullType.asInstanceOf[HoodieSchema.Decimal]
    val bd1 = new java.math.BigDecimal("7890.456").setScale(currentDecimalType1.getScale)
    avroRecord.put("col41", HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(bd1, currentDecimalType1.toAvroSchema, currentDecimalType1.toAvroSchema.getLogicalType))
    avroRecord.put("col5", "2011-01-01")
    avroRecord.put("col51", "199.342")
    avroRecord.put("col6", 18987)
    avroRecord.put("col7", 1640491505000000L)
    avroRecord.put("col8", false)
    val bb = ByteBuffer.wrap(Array[Byte](97, 48, 53))
    avroRecord.put("col9", bb)
    assert(GenericData.get.validate(schema.toAvroSchema, avroRecord))
    // Apply 16 type promotions through the HoodieSchema-direct applier
    // (replaces the legacy TableChanges.ColumnUpdateChange chain).
    val typeChanges = Seq(
      ("id", HoodieSchema.create(HoodieSchemaType.LONG)),
      ("comb", HoodieSchema.create(HoodieSchemaType.FLOAT)),
      ("com1", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
      ("col0", HoodieSchema.create(HoodieSchemaType.STRING)),
      ("col1", HoodieSchema.create(HoodieSchemaType.FLOAT)),
      ("col11", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
      ("col12", HoodieSchema.create(HoodieSchemaType.STRING)),
      ("col2", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
      ("col21", HoodieSchema.create(HoodieSchemaType.STRING)),
      ("col3", HoodieSchema.create(HoodieSchemaType.STRING)),
      ("col31", HoodieSchema.createDecimalFixed(18, 9)),
      ("col4", HoodieSchema.createDecimalFixed(18, 9)),
      ("col41", HoodieSchema.create(HoodieSchemaType.STRING)),
      ("col5", HoodieSchema.createDate()),
      ("col51", HoodieSchema.createDecimalFixed(18, 9)),
      ("col6", HoodieSchema.create(HoodieSchemaType.STRING)))
    val newHoodieSchema = typeChanges.foldLeft(schema) { case (acc, (col, newType)) =>
      new HoodieSchemaChangeApplier(acc).applyColumnTypeChange(col, newType)
    }
    val newRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, newHoodieSchema.toAvroSchema, new HashMap[String, String])
    assert(GenericData.get.validate(newHoodieSchema.toAvroSchema, newRecord))
    // Convert avro to internalRow
    val structTypeSchema = HoodieInternalRowUtils.getCachedSchema(schema)
    val newStructTypeSchema = HoodieInternalRowUtils.getCachedSchema(newHoodieSchema)
    val row = AvroConversionUtils.createAvroToInternalRowConverter(schema, structTypeSchema).apply(avroRecord).get
    val newRowExpected = AvroConversionUtils.createAvroToInternalRowConverter(newHoodieSchema, newStructTypeSchema)
      .apply(newRecord).get

    val rowWriter = HoodieInternalRowUtils.genUnsafeRowWriter(structTypeSchema, newStructTypeSchema, JCollections.emptyMap(), JCollections.emptyMap())
    val newRow = rowWriter(row)

    internalRowCompare(newRowExpected, newRow, newStructTypeSchema)
  }

  test("Test rewrite nest record") {
    val schema = HoodieSchema.parse(
      "{\"type\":\"record\",\"name\":\"test1\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"int\"}," +
        "{\"name\":\"data\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"preferences\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"preferences\",\"namespace\":\"test1\",\"fields\":[" +
          "{\"name\":\"feature1\",\"type\":\"boolean\"}," +
          "{\"name\":\"feature2\",\"type\":[\"null\",\"boolean\"],\"default\":null}]}],\"default\":null}," +
        "{\"name\":\"doubles\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}," +
        "{\"name\":\"locations\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"locations\",\"namespace\":\"test1\",\"fields\":[" +
          "{\"name\":\"lat\",\"type\":\"float\"}," +
          "{\"name\":\"long\",\"type\":\"float\"}]}}}]}")
    val avroRecord = new GenericData.Record(schema.toAvroSchema)
    GenericData.get.validate(schema.toAvroSchema, avroRecord)
    avroRecord.put("id", 2)
    avroRecord.put("data", "xs")
    // fill record type
    val preferencesRecord = new GenericData.Record(schema.getField("preferences").get.schema().getNonNullType.toAvroSchema)
    preferencesRecord.put("feature1", false)
    preferencesRecord.put("feature2", true)
    assert(GenericData.get.validate(schema.getField("preferences").get.schema().getNonNullType.toAvroSchema, preferencesRecord))
    avroRecord.put("preferences", preferencesRecord)
    // fill mapType
    val locations = new HashMap[String, GenericData.Record]
    val mapSchema = schema.getField("locations").get.schema().getValueType.toAvroSchema
    val locationsValue: GenericData.Record = new GenericData.Record(mapSchema)
    locationsValue.put("lat", 1.2f)
    locationsValue.put("long", 1.4f)
    val locationsValue1: GenericData.Record = new GenericData.Record(mapSchema)
    locationsValue1.put("lat", 2.2f)
    locationsValue1.put("long", 2.4f)
    locations.put("key1", locationsValue)
    locations.put("key2", locationsValue1)
    avroRecord.put("locations", locations)
    val doubles = new ArrayList[Double]
    doubles.add(2.0d)
    doubles.add(3.0d)
    avroRecord.put("doubles", doubles)
    // create newSchema: preferences gains "featurex"; locations.value.lat is renamed to "laty" (nullable).
    val newSchema = HoodieSchema.parse(
      "{\"type\":\"record\",\"name\":\"test1\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"int\"}," +
        "{\"name\":\"data\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"preferences\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"preferences\",\"namespace\":\"test1\",\"fields\":[" +
          "{\"name\":\"feature1\",\"type\":\"boolean\"}," +
          "{\"name\":\"featurex\",\"type\":[\"null\",\"boolean\"],\"default\":null}," +
          "{\"name\":\"feature2\",\"type\":[\"null\",\"boolean\"],\"default\":null}]}],\"default\":null}," +
        "{\"name\":\"doubles\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}," +
        "{\"name\":\"locations\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"locations\",\"namespace\":\"test1\",\"fields\":[" +
          "{\"name\":\"laty\",\"type\":[\"null\",\"float\"],\"default\":null}," +
          "{\"name\":\"long\",\"type\":\"float\"}]}}}]}")
    val newAvroRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, newSchema.toAvroSchema, new HashMap[String, String])
    // test the correctness of rewrite
    assert(GenericData.get.validate(newSchema.toAvroSchema, newAvroRecord))
    // Convert avro to internalRow
    val structTypeSchema = HoodieInternalRowUtils.getCachedSchema(schema)
    val newStructTypeSchema = HoodieInternalRowUtils.getCachedSchema(newSchema)
    val row = AvroConversionUtils.createAvroToInternalRowConverter(schema, structTypeSchema).apply(avroRecord).get
    val newRowExpected = AvroConversionUtils.createAvroToInternalRowConverter(newSchema, newStructTypeSchema).apply(newAvroRecord).get

    val rowWriter = HoodieInternalRowUtils.genUnsafeRowWriter(structTypeSchema, newStructTypeSchema, JCollections.emptyMap(), JCollections.emptyMap())
    val newRow = rowWriter(row)

    internalRowCompare(newRowExpected, newRow, newStructTypeSchema)
  }

  private def internalRowCompare(expected: Any, actual: Any, schema: DataType): Unit = {
    schema match {
      case StructType(fields) =>
        val expectedRow = expected.asInstanceOf[InternalRow]
        val actualRow = actual.asInstanceOf[InternalRow]
        fields.zipWithIndex.foreach { case (field, i) => internalRowCompare(expectedRow.get(i, field.dataType), actualRow.get(i, field.dataType), field.dataType) }
      case ArrayType(elementType, _) =>
        val expectedArray = expected.asInstanceOf[ArrayData].toSeq[Any](elementType)
        val actualArray = actual.asInstanceOf[ArrayData].toSeq[Any](elementType)
        if (expectedArray.size != actualArray.size) {
          throw new AssertionError()
        } else {
          expectedArray.zip(actualArray).foreach { case (e1, e2) => internalRowCompare(e1, e2, elementType) }
        }
      case MapType(keyType, valueType, _) =>
        val expectedKeyArray = expected.asInstanceOf[MapData].keyArray()
        val expectedValueArray = expected.asInstanceOf[MapData].valueArray()
        val actualKeyArray = actual.asInstanceOf[MapData].keyArray()
        val actualValueArray = actual.asInstanceOf[MapData].valueArray()
        internalRowCompare(expectedKeyArray, actualKeyArray, ArrayType(keyType))
        internalRowCompare(expectedValueArray, actualValueArray, ArrayType(valueType))
      case StringType => if (checkNull(expected, actual) || !expected.toString.equals(actual.toString)) {
        throw new AssertionError(String.format("%s is not equals %s", expected.toString, actual.toString))
      }
      // TODO Verify after 'https://github.com/apache/hudi/pull/5907' merge
      case BinaryType => if (checkNull(expected, actual) || !expected.asInstanceOf[Array[Byte]].sameElements(actual.asInstanceOf[Array[Byte]])) {
      // throw new AssertionError(String.format("%s is not equals %s", expected.toString, actual.toString))
      }
      case _ => if (!Objects.equals(expected, actual)) {
      // throw new AssertionError(String.format("%s is not equals %s", expected.toString, actual.toString))
      }
    }
  }

  private def checkNull(left: Any, right: Any): Boolean = {
    (left == null && right != null) || (left == null && right != null)
  }

  private def createSparkRowSerDe(schema: StructType): SparkRowSerDe = {
    new SparkRowSerDe(sparkAdapter.getCatalystExpressionUtils.getEncoder(schema))
  }
}
