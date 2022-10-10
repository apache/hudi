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

package org.apache.hudi

import org.apache.avro.generic.GenericRecord
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.JavaConverters

class TestHoodieSparkUtils {

  @ParameterizedTest
  @ValueSource(strings = Array("2.4.4", "3.1.0", "3.2.0", "3.3.0"))
  def testSparkVersionCheckers(sparkVersion: String): Unit = {
    val vsMock = new SparkVersionsSupport {
      override def getSparkVersion: String = sparkVersion
    }

    sparkVersion match {
      case "2.4.4" =>
        assertTrue(vsMock.isSpark2)

        assertFalse(vsMock.isSpark3)
        assertFalse(vsMock.isSpark3_1)
        assertFalse(vsMock.isSpark3_0)
        assertFalse(vsMock.isSpark3_2)
        assertFalse(vsMock.gteqSpark3_1)
        assertFalse(vsMock.gteqSpark3_1_3)
        assertFalse(vsMock.gteqSpark3_2)

      case "3.1.0" =>
        assertTrue(vsMock.isSpark3)
        assertTrue(vsMock.isSpark3_1)
        assertTrue(vsMock.gteqSpark3_1)

        assertFalse(vsMock.isSpark2)
        assertFalse(vsMock.isSpark3_0)
        assertFalse(vsMock.isSpark3_2)
        assertFalse(vsMock.gteqSpark3_1_3)
        assertFalse(vsMock.gteqSpark3_2)

      case "3.2.0" =>
        assertTrue(vsMock.isSpark3)
        assertTrue(vsMock.isSpark3_2)
        assertTrue(vsMock.gteqSpark3_1)
        assertTrue(vsMock.gteqSpark3_1_3)
        assertTrue(vsMock.gteqSpark3_2)

        assertFalse(vsMock.isSpark2)
        assertFalse(vsMock.isSpark3_0)
        assertFalse(vsMock.isSpark3_1)

      case "3.3.0" =>
        assertTrue(vsMock.isSpark3)
        assertTrue(vsMock.gteqSpark3_1)
        assertTrue(vsMock.gteqSpark3_1_3)
        assertTrue(vsMock.gteqSpark3_2)

        assertFalse(vsMock.isSpark3_2)
        assertFalse(vsMock.isSpark2)
        assertFalse(vsMock.isSpark3_0)
        assertFalse(vsMock.isSpark3_1)
    }
  }

  @Test
  def testCreateRddSchemaEvol(): Unit = {
    val spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate

    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    var records = DataSourceTestUtils.generateRandomRows(5)
    var recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(recordsSeq), structType)

    var genRecRDD = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema))
    genRecRDD.collect()

    val evolSchema = DataSourceTestUtils.getStructTypeExampleEvolvedSchema
    records = DataSourceTestUtils.generateRandomRowsEvolvedSchema(5)
    recordsSeq = convertRowListToSeq(records)

    genRecRDD = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(evolSchema))
    genRecRDD.collect()

    // pass in evolved schema but with records serialized with old schema. should be able to convert with out any exception.
    // Before https://github.com/apache/hudi/pull/2927, this will throw exception.
    genRecRDD = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(evolSchema))
    val genRecs = genRecRDD.collect()
    // if this succeeds w/o throwing any exception, test succeeded.
    assertEquals(genRecs.size, 5)
    spark.stop()
  }

  @Test
  def testCreateRddWithNestedSchemas(): Unit = {
    val spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate

    val innerStruct1 = new StructType().add("innerKey","string",false).add("innerValue", "long", true)
    val structType1 = new StructType().add("key", "string", false)
      .add("nonNullableInnerStruct",innerStruct1,false).add("nullableInnerStruct",innerStruct1,true)
    val schema1 = AvroConversionUtils.convertStructTypeToAvroSchema(structType1, "test_struct_name", "test_namespace")
    val records1 = Seq(Row("key1", Row("innerKey1_1", 1L), Row("innerKey1_2", 2L)))

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(records1), structType1)
    val genRecRDD1 = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema1))
    assert(schema1.equals(genRecRDD1.collect()(0).getSchema))

    // create schema2 which has one addition column at the root level compared to schema1
    val structType2 = new StructType().add("key", "string", false)
      .add("nonNullableInnerStruct",innerStruct1,false).add("nullableInnerStruct",innerStruct1,true)
      .add("nullableInnerStruct2",innerStruct1,true)
    val schema2 = AvroConversionUtils.convertStructTypeToAvroSchema(structType2, "test_struct_name", "test_namespace")
    val records2 = Seq(Row("key2", Row("innerKey2_1", 2L), Row("innerKey2_2", 2L), Row("innerKey2_3", 2L)))
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(records2), structType2)
    val genRecRDD2 = HoodieSparkUtils.createRdd(df2, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema2))
    assert(schema2.equals(genRecRDD2.collect()(0).getSchema))

    // send records1 with schema2. should succeed since the new column is nullable.
    val genRecRDD3 = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema2))
    assert(genRecRDD3.collect()(0).getSchema.equals(schema2))
    genRecRDD3.foreach(entry => assertNull(entry.get("nullableInnerStruct2")))

    val innerStruct3 = new StructType().add("innerKey","string",false).add("innerValue", "long", true)
      .add("new_nested_col","string",true)

    // create a schema which has one additional nested column compared to schema1, which is nullable
    val structType4 = new StructType().add("key", "string", false)
      .add("nonNullableInnerStruct",innerStruct1,false).add("nullableInnerStruct",innerStruct3,true)

    val schema4 = AvroConversionUtils.convertStructTypeToAvroSchema(structType4, "test_struct_name", "test_namespace")
    val records4 = Seq(Row("key2", Row("innerKey2_1", 2L), Row("innerKey2_2", 2L, "new_nested_col_val1")))
    val df4 = spark.createDataFrame(spark.sparkContext.parallelize(records4), structType4)
    val genRecRDD4 = HoodieSparkUtils.createRdd(df4, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema4))
    assert(schema4.equals(genRecRDD4.collect()(0).getSchema))

    // convert batch 1 with schema4. should succeed.
    val genRecRDD5 = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema4))
    assert(schema4.equals(genRecRDD4.collect()(0).getSchema))
    val genRec = genRecRDD5.collect()(0)
    val nestedRec : GenericRecord = genRec.get("nullableInnerStruct").asInstanceOf[GenericRecord]
    assertNull(nestedRec.get("new_nested_col"))
    assertNotNull(nestedRec.get("innerKey"))
    assertNotNull(nestedRec.get("innerValue"))

    val innerStruct4 = new StructType().add("innerKey","string",false).add("innerValue", "long", true)
      .add("new_nested_col","string",false)
    // create a schema which has one additional nested column compared to schema1, which is non nullable
    val structType6 = new StructType().add("key", "string", false)
      .add("nonNullableInnerStruct",innerStruct1,false).add("nullableInnerStruct",innerStruct4,true)

    val schema6 = AvroConversionUtils.convertStructTypeToAvroSchema(structType6, "test_struct_name", "test_namespace")
    // convert batch 1 with schema5. should fail since the missed out column is not nullable.
    try {
      val genRecRDD6 = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
        org.apache.hudi.common.util.Option.of(schema6))
      genRecRDD6.collect()
      fail("createRdd should fail, because records don't have a column which is not nullable in the passed in schema")
    } catch {
      case e: Exception =>
        if (HoodieSparkUtils.gteqSpark3_3) {
          assertTrue(e.getMessage.contains("null value for (non-nullable) string at test_struct_name.nullableInnerStruct[nullableInnerStruct].new_nested_col"))
        } else {
          assertTrue(e.getMessage.contains("null of string in field new_nested_col of test_namespace.test_struct_name.nullableInnerStruct of union"))
        }
    }
    spark.stop()
  }

  def convertRowListToSeq(inputList: java.util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq
}
