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

package org.apache.hudi

import org.apache.hudi.HoodieSparkUtils.sparkAdapter
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model._
import org.apache.hudi.common.testutils.{OrderingFieldsTestUtils, SchemaTestUtil}
import org.apache.hudi.common.util.Option
import org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH
import org.apache.hudi.config.{HoodiePayloadConfig, HoodieWriteConfig}
import org.apache.hudi.exception.{HoodieException, HoodieKeyException}
import org.apache.hudi.keygen._
import org.apache.hudi.testutils.SparkDatasetTestUtils

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

/**
 * Tests on the default key generator, payload classes.
 */
class TestDataSourceDefaults extends ScalaAssertionSupport {

  val schema = SchemaTestUtil.getComplexEvolvedSchema
  val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
  var baseRecord: GenericRecord = _
  var baseRow: Row = _
  var internalRow: InternalRow = _
  val testStructName = "testStructName"
  val testNamespace = "testNamespace"
  val encoder = sparkAdapter.getCatalystExpressionUtils.getEncoder(structType)

  @BeforeEach def initialize(): Unit = {
    baseRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 1, "001", "f1")
    baseRow = getRow(baseRecord, schema, structType)
    internalRow = getInternalRow(baseRow)
  }

  private def getKeyConfig(recordKeyFieldName: String, partitionPathField: String, hiveStylePartitioning: String): TypedProperties = {
    val props = new TypedProperties()
    props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, recordKeyFieldName)
    props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, partitionPathField)
    props.setProperty(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, hiveStylePartitioning)
    props
  }

  @Test def testSimpleKeyGenerator(): Unit = {

    {
      // Top level, valid fields
      val keyGen = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false"))

      val expectedKey = new HoodieKey("field1", "name1")
      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // Partition path field not specified
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")

      assertThrows(classOf[IllegalArgumentException]) {
        new SimpleKeyGenerator(props)
      }
    }

    {
      // Record's key field not specified
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "partitionField")

      assertThrows(classOf[IndexOutOfBoundsException]) {
        new SimpleKeyGenerator(props).getRecordKey(baseRecord)
      }
    }

    {
      // nested field as record key and partition path
      val keyGen = new SimpleKeyGenerator(getKeyConfig("testNestedRecord.userId", "testNestedRecord.isAdmin", "false"))

      assertEquals(new HoodieKey("UserId1@001", "false"), keyGen.getKey(baseRecord))
    }

    {
      // Nested record key not found
      val keyGen = new SimpleKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin", "false"))

      assertThrows(classOf[HoodieException]) {
        keyGen.getKey(baseRecord)
      }
    }

    {
      // Fail in case partition path can't be found in schema
      val keyGen = new SimpleKeyGenerator(getKeyConfig("testNestedRecord.userId", "testNestedRecord.notThere", "false"))

      // TODO this should throw
      //assertThrows(classOf[HoodieException]) {
      //  keyGen.getKey(baseRecord)
      //}
      assertThrows(classOf[HoodieException]) {
        keyGen.getPartitionPath(baseRow)
      }
      assertThrows(classOf[HoodieException]) {
        keyGen.getPartitionPath(internalRow, structType)
      }
    }

    {
      val keyGen = new SimpleKeyGenerator(getKeyConfig("field1", "name", "true"))

      assertEquals("name=name1", keyGen.getKey(baseRecord).getPartitionPath)
      assertEquals("name=name1", keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString("name=name1"), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // If partition is null/empty, return default partition path
      val keyGen = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false"))

      baseRecord.put("name", "")
      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      assertEquals(DEFAULT_PARTITION_PATH, keyGen.getKey(baseRecord).getPartitionPath)
      assertEquals(DEFAULT_PARTITION_PATH, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(DEFAULT_PARTITION_PATH), keyGen.getPartitionPath(internalRow, structType))

      baseRecord.put("name", null)
      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      assertEquals(DEFAULT_PARTITION_PATH, keyGen.getKey(baseRecord).getPartitionPath)
      assertEquals(DEFAULT_PARTITION_PATH, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(DEFAULT_PARTITION_PATH), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // If record key is null/empty, throw error
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "name")
      val keyGen = new SimpleKeyGenerator(props)

      baseRecord.put("field1", "")
      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getKey(baseRecord)
      }
      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getRecordKey(baseRow)
      }
      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getRecordKey(internalRow, structType)
      }

      baseRecord.put("field1", null)
      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getKey(baseRecord)
      }
      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getRecordKey(baseRow)
      }
      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getRecordKey(internalRow, structType)
      }
    }
  }

  @Test def testUserDefinedKeyGeneratorWorksWithRows(): Unit = {
    val keyGen = new UserDefinedKeyGenerator(getKeyConfig("field1", "name", "false"))
    assertEquals("field1", keyGen.getRecordKey(baseRow))
    assertEquals("name1", keyGen.getPartitionPath(baseRow))
  }

  class UserDefinedKeyGenerator(props: TypedProperties) extends KeyGenerator(props) with SparkKeyGeneratorInterface {
    val recordKeyProp: String = props.getString(DataSourceWriteOptions.RECORDKEY_FIELD.key)
    val partitionPathProp: String = props.getString(DataSourceWriteOptions.PARTITIONPATH_FIELD.key)
    val STRUCT_NAME: String = "hoodieRowTopLevelField"
    val NAMESPACE: String = "hoodieRow"
    var converterFn: Function1[Row, GenericRecord] = _
    var internalConverterFn: Function1[InternalRow, GenericRecord] = _

    override def getKey(record: GenericRecord): HoodieKey = {
      new HoodieKey(HoodieAvroUtils.getNestedFieldValAsString(record, recordKeyProp, true, false),
        HoodieAvroUtils.getNestedFieldValAsString(record, partitionPathProp, true, false))
    }

    override def getRecordKey(row: Row): String = {
      if (null == converterFn) converterFn = AvroConversionUtils.createConverterToAvro(row.schema, STRUCT_NAME, NAMESPACE)
      val genericRecord = converterFn.apply(row).asInstanceOf[GenericRecord]
      getKey(genericRecord).getRecordKey
    }

    override def getRecordKey(row: InternalRow, schema: StructType): UTF8String = null

    override def getPartitionPath(row: Row): String = {
      if (null == converterFn) converterFn = AvroConversionUtils.createConverterToAvro(row.schema, STRUCT_NAME, NAMESPACE)
      val genericRecord = converterFn.apply(row).asInstanceOf[GenericRecord]
      getKey(genericRecord).getPartitionPath
    }

    override def getPartitionPath(internalRow: InternalRow, structType: StructType): UTF8String = null
  }

  @Test def testComplexKeyGenerator(): Unit = {

    {
      // Top level, valid fields
      val keyGen = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))
      val expectedKey = new HoodieKey("field1:field1,name:name1", "field1/name1")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    // Partition path field not specified
    assertThrows(classOf[IllegalArgumentException]) {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      val keyGen = new ComplexKeyGenerator(props)

      keyGen.getKey(baseRecord)
      keyGen.getRecordKey(baseRow)
      keyGen.getRecordKey(internalRow, structType)
    }

    // Record's key field not specified
    assertThrows(classOf[HoodieKeyException]) {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionField")
      val keyGen = new ComplexKeyGenerator(props)

      keyGen.getKey(baseRecord)
      keyGen.getPartitionPath(baseRow)
      keyGen.getPartitionPath(internalRow, structType)
    }

    {
      // Nested field as record key and partition path
      val keyGen = new ComplexKeyGenerator(getKeyConfig("testNestedRecord.userId,testNestedRecord.isAdmin", "testNestedRecord.userId,testNestedRecord.isAdmin", "false"))

      val expectedKey = new HoodieKey("testNestedRecord.userId:UserId1@001,testNestedRecord.isAdmin:false", "UserId1@001/false")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // Nested record key not found
      val keyGen = new ComplexKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin", "false"))

      assertThrows(classOf[HoodieException]) {
        keyGen.getKey(baseRecord)
      }
      assertThrows(classOf[HoodieException]) {
        keyGen.getRecordKey(baseRow)
      }
      assertThrows(classOf[HoodieException]) {
        keyGen.getRecordKey(internalRow, structType)
      }
    }

    {
      // If partition path can't be found, return default partition path
      val keyGen = new ComplexKeyGenerator(getKeyConfig("testNestedRecord.userId", "testNestedRecord.notThere", "false"))

      // TODO this should throw
      //assertThrows(classOf[HoodieException]) {
      //  keyGen.getKey(baseRecord)
      //}
      assertThrows(classOf[HoodieException]) {
        keyGen.getPartitionPath(baseRow)
      }
      assertThrows(classOf[HoodieException]) {
        keyGen.getPartitionPath(internalRow, structType)
      }
    }

    {
      // If enable hive style partitioning
      val keyGen = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "true"))

      val expectedKey = new HoodieKey("field1:field1,name:name1", "field1=field1/name=name1")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // If one part of the record key is empty, replace with "__empty__"
      val keyGen = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))

      baseRecord.put("name", "")
      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      val expectedKey = new HoodieKey("field1:field1,name:__empty__", "field1/" + DEFAULT_PARTITION_PATH)

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // If one part of the record key is null, replace with "__null__"
      val keyGen = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))

      baseRecord.put("name", null)
      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      val expectedKey = new HoodieKey("field1:field1,name:__null__", "field1/" + DEFAULT_PARTITION_PATH)

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // If all parts of the composite record key are null/empty, throw error
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1,name")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "field1,name")
      val keyGen = new ComplexKeyGenerator(props)

      baseRecord.put("name", "")
      baseRecord.put("field1", null)

      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getKey(baseRecord)
      }
      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getRecordKey(baseRow)
      }
      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getRecordKey(internalRow, structType)
      }
    }

    {
      // Reset name and field1 values.
      val keyGen = new ComplexKeyGenerator(getKeyConfig("field1, name", "field1, name", "false"))

      baseRecord.put("name", "name1")
      baseRecord.put("field1", "field1")

      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      val expectedKey = new HoodieKey("field1:field1,name:name1", "field1/name1")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      val config = getKeyConfig("field1, name", "field1, name", "false")
      // This config should not affect record key encoding for multiple record key fields
      config.put(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key, "true")
      val keyGen = new ComplexKeyGenerator(config)

      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      val expectedKey = new HoodieKey("field1:field1,name:name1", "field1/name1")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      baseRecord.put("name", "value1")
      baseRecord.put("field1", "value2")
      // Default: encode record key field name if there is a single record key field
      val keyGen = new ComplexKeyGenerator(getKeyConfig("name,", "field1,", "false"))

      val expectedKey = new HoodieKey("name:value1", "value2")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // Testing that config set to true but expecting old format (field name included)
      val config = getKeyConfig("name,", "field1,", "false")
      config.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key, "8")
      config.put(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key, "false")
      val keyGen = new ComplexKeyGenerator(config)

      val expectedKey = new HoodieKey("name:value1", "value2")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // Turning off the encoding of record key field name if there is a single record key field
      val config = getKeyConfig("name,", "field1,", "false")
      config.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key, "8")
      // This config should affect record key encoding for single record key fields
      config.put(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key, "true")
      val keyGen = new ComplexKeyGenerator(config)

      val expectedKey = new HoodieKey("value1", "value2")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }
  }

  @Test def testGlobalDeleteKeyGenerator(): Unit = {
    {
      // Top level, partition value included but not actually used
      val keyGen = new GlobalDeleteKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))

      val expectedKey = new HoodieKey("field1:field1,name:name1", "")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // top level, partition value not included
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1,name")

      val keyGen = new GlobalDeleteKeyGenerator(props)

      val expectedKey = new HoodieKey("field1:field1,name:name1", "")

      assertEquals(expectedKey, keyGen.getKey(baseRecord))

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // If one part of the record key is empty, replace with "__empty__"
      baseRecord.put("name", "")
      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      val keyGen = new GlobalDeleteKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))

      val expectedKey = new HoodieKey("field1:field1,name:__empty__", "")

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // If one part of the record key is null, replace with "__null__"
      baseRecord.put("name", null)
      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      val keyGen = new GlobalDeleteKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))

      val expectedKey = new HoodieKey("field1:field1,name:__null__", "")

      assertEquals(expectedKey.getRecordKey, keyGen.getRecordKey(baseRow))
      assertEquals(expectedKey.getPartitionPath, keyGen.getPartitionPath(baseRow))
      assertEquals(UTF8String.fromString(expectedKey.getRecordKey), keyGen.getRecordKey(internalRow, structType))
      assertEquals(UTF8String.fromString(expectedKey.getPartitionPath), keyGen.getPartitionPath(internalRow, structType))
    }

    {
      // Record's key field not specified
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionField")

      assertThrows(classOf[HoodieKeyException]) {
        new GlobalDeleteKeyGenerator(props).getRecordKey(baseRecord)
      }
    }

    {
      // Nested record key not found
      val keyGen = new GlobalDeleteKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin", "false"))

      assertThrows(classOf[HoodieException]) {
        keyGen.getKey(baseRecord)
      }
      assertThrows(classOf[HoodieException]) {
        keyGen.getRecordKey(baseRow)
      }
      assertThrows(classOf[HoodieException]) {
        keyGen.getRecordKey(internalRow, structType)
      }
    }

    {
      // If all parts of the composite record key are null/empty, throw error
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1,name")
      val keyGen = new GlobalDeleteKeyGenerator(props)

      baseRecord.put("name", "")
      baseRecord.put("field1", null)
      baseRow = getRow(baseRecord, schema, structType)
      internalRow = getInternalRow(baseRow)

      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getKey(baseRecord)
      }
      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getRecordKey(baseRow)
      }
      assertThrows(classOf[HoodieKeyException]) {
        keyGen.getRecordKey(internalRow, structType)
      }
    }
  }

  @Test def testOverwriteWithLatestAvroPayload(): Unit = {
    val overWritePayload1 = new OverwriteWithLatestAvroPayload(baseRecord, 1)
    val laterRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 2, "001", "f1")
    val overWritePayload2 = new OverwriteWithLatestAvroPayload(laterRecord, 2)

    // it will provide the record with greatest combine value
    val combinedPayload12 = overWritePayload1.preCombine(overWritePayload2)
    val combinedGR12 = combinedPayload12.getInsertValue(schema).get().asInstanceOf[GenericRecord]
    assertEquals("field1", combinedGR12.get("field1").toString)

    // and it will be deterministic, to order of processing.
    val combinedPayload21 = overWritePayload2.preCombine(overWritePayload1)
    val combinedGR21 = combinedPayload21.getInsertValue(schema).get().asInstanceOf[GenericRecord]
    assertEquals("field2", combinedGR21.get("field1").toString)
  }

  @ParameterizedTest
  @MethodSource(Array("org.apache.hudi.common.testutils.OrderingFieldsTestUtils#configureOrderingFields"))
  def testOverwriteWithLatestAvroPayloadCombineAndGetUpdateValue(key: String): Unit = {
    val props = new TypedProperties()
    OrderingFieldsTestUtils.setOrderingFieldsConfig(props, key, "favoriteIntNumber")
    val baseOrderingVal: Object = baseRecord.get("favoriteIntNumber")
    val fieldSchema: Schema = baseRecord.getSchema().getField("favoriteIntNumber").schema()

    val basePayload = new OverwriteWithLatestAvroPayload(baseRecord, HoodieAvroUtils.convertValueForSpecificDataTypes(fieldSchema, baseOrderingVal, false).asInstanceOf[Comparable[_]])

    val laterRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 2, "001", "f1")
    val laterOrderingVal: Object = laterRecord.get("favoriteIntNumber")
    val newerPayload = new OverwriteWithLatestAvroPayload(laterRecord, HoodieAvroUtils.convertValueForSpecificDataTypes(fieldSchema, laterOrderingVal, false).asInstanceOf[Comparable[_]])

    // it always returns the latest payload.
    val preCombinedPayload = basePayload.preCombine(newerPayload)
    val precombinedGR = preCombinedPayload.getInsertValue(schema).get().asInstanceOf[GenericRecord]
    assertEquals("field1", precombinedGR.get("field1").toString)
  }

  @Test def testDefaultHoodieRecordPayloadCombineAndGetUpdateValue(): Unit = {
    val fieldSchema: Schema = baseRecord.getSchema().getField("favoriteIntNumber").schema()
    val props = HoodiePayloadConfig.newBuilder()
      .withPayloadOrderingFields("favoriteIntNumber").build().getProps;

    val laterRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 2, "001", "f1")
    val laterOrderingVal: Object = laterRecord.get("favoriteIntNumber")

    val earlierRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 1, "000", "f1")
    val earlierOrderingVal: Object = earlierRecord.get("favoriteIntNumber")

    val laterPayload = new DefaultHoodieRecordPayload(laterRecord,
      HoodieAvroUtils.convertValueForSpecificDataTypes(fieldSchema, laterOrderingVal, false).asInstanceOf[Comparable[_]])

    val earlierPayload = new DefaultHoodieRecordPayload(earlierRecord,
      HoodieAvroUtils.convertValueForSpecificDataTypes(fieldSchema, earlierOrderingVal, false).asInstanceOf[Comparable[_]])

    // it will provide the record with greatest combine value
    val preCombinedPayload = laterPayload.preCombine(earlierPayload)
    val precombinedGR = preCombinedPayload.getInsertValue(schema).get().asInstanceOf[GenericRecord]
    assertEquals("field2", precombinedGR.get("field1").toString)
    assertEquals(laterOrderingVal, precombinedGR.get("favoriteIntNumber"))

    val earlierWithLater = earlierPayload.combineAndGetUpdateValue(laterRecord, schema, props)
    val earlierwithLaterGR = earlierWithLater.get().asInstanceOf[GenericRecord]
    assertEquals("field2", earlierwithLaterGR.get("field1").toString)
    assertEquals(laterOrderingVal, earlierwithLaterGR.get("favoriteIntNumber"))

    val laterWithEarlier = laterPayload.combineAndGetUpdateValue(earlierRecord, schema, props)
    val laterWithEarlierGR = laterWithEarlier.get().asInstanceOf[GenericRecord]
    assertEquals("field2", laterWithEarlierGR.get("field1").toString)
    assertEquals(laterOrderingVal, laterWithEarlierGR.get("favoriteIntNumber"))
  }

  @Test def testEmptyHoodieRecordPayload(): Unit = {
    val emptyPayload1 = new EmptyHoodieRecordPayload(baseRecord, 1)
    val laterRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 2, "001", "f1")
    val emptyPayload2 = new EmptyHoodieRecordPayload(laterRecord, 2)

    // it will provide an empty record
    val combinedPayload12 = emptyPayload1.preCombine(emptyPayload2)
    val combined12 = combinedPayload12.getInsertValue(schema)
    assertEquals(Option.empty(), combined12)
  }

  def getRow(record: GenericRecord, schema: Schema, structType: StructType): Row = {
    val converterFn = AvroConversionUtils.createConverterToRow(schema, structType)
    val row = converterFn.apply(record)
    new GenericRowWithSchema(structType.fieldNames.indices.map(i => row.get(i)).toArray, structType)
  }

  def getInternalRow(row: Row): InternalRow = SparkDatasetTestUtils.serializeRow(encoder, row)
}
