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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model._
import org.apache.hudi.common.testutils.SchemaTestUtil
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodiePayloadConfig
import org.apache.hudi.exception.{HoodieException, HoodieKeyException}
import org.apache.hudi.keygen._
import org.apache.hudi.testutils.KeyGeneratorTestUtilities
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}
import org.scalatest.Assertions.fail

/**
 * Tests on the default key generator, payload classes.
 */
class TestDataSourceDefaults {

  val schema = SchemaTestUtil.getComplexEvolvedSchema
  val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
  var baseRecord: GenericRecord = _
  var baseRow: Row = _
  val testStructName = "testStructName"
  val testNamespace = "testNamespace"

  @BeforeEach def initialize(): Unit = {
    baseRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 1, "001", "f1")
    baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
  }

  private def getKeyConfig(recordKeyFieldName: String, partitionPathField: String, hiveStylePartitioning: String): TypedProperties = {
    val props = new TypedProperties()
    props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, recordKeyFieldName)
    props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, partitionPathField)
    props.setProperty(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, hiveStylePartitioning)
    props
  }

  @Test def testSimpleKeyGenerator() = {

    // top level, valid fields
    var keyGen = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false"))
    val hk1 = keyGen.getKey(baseRecord)
    assertEquals("field1", hk1.getRecordKey)
    assertEquals("name1", hk1.getPartitionPath)

    assertEquals("field1", keyGen.getRecordKey(baseRow))
    assertEquals("name1", keyGen.getPartitionPath(baseRow))

    // partition path field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      new SimpleKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // partition path field not specified using Row
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      val keyGen = new SimpleKeyGenerator(props)
      keyGen.getRecordKey(baseRow)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // recordkey field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "partitionField")
      new SimpleKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // recordkey field not specified using Row
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionField")
      val keyGen = new SimpleKeyGenerator(props)
      keyGen.getPartitionPath(baseRow)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // nested field as record key and partition path
    val hk2 = new SimpleKeyGenerator(getKeyConfig("testNestedRecord.userId", "testNestedRecord.isAdmin", "false"))
      .getKey(baseRecord)
    assertEquals("UserId1@001", hk2.getRecordKey)
    assertEquals("false", hk2.getPartitionPath)

    // Nested record key not found
    try {
      new SimpleKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin", "false"))
        .getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieException =>
      // do nothing
    }

    // if partition path can't be found, return default partition path
    val hk3 = new SimpleKeyGenerator(getKeyConfig("testNestedRecord.userId", "testNestedRecord.notThere", "false"))
      .getKey(baseRecord)
    assertEquals("default", hk3.getPartitionPath)

    // if partition path can't be found, return default partition path using row
    keyGen = new SimpleKeyGenerator(getKeyConfig("testNestedRecord.userId", "testNestedRecord.notThere", "false"))
    val hk3_row = keyGen.getPartitionPath(baseRow)
    assertEquals("default", hk3_row)

    // if enable hive style partitioning
    val hk4 = new SimpleKeyGenerator(getKeyConfig("field1", "name", "true")).getKey(baseRecord)
    assertEquals("name=name1", hk4.getPartitionPath)

    // if enable hive style partitioning using row
    keyGen = new SimpleKeyGenerator(getKeyConfig("field1", "name", "true"))
    val hk4_row = keyGen.getPartitionPath(baseRow)
    assertEquals("name=name1", hk4_row)

    // if partition is null, return default partition path
    baseRecord.put("name", "")
    val hk5 = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false"))
      .getKey(baseRecord)
    assertEquals("default", hk5.getPartitionPath)

    // if partition is null, return default partition path using Row
    keyGen = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false"))
    baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
    val hk5_row = keyGen.getPartitionPath(baseRow)
    assertEquals("default", hk5_row)

    // if partition is empty, return default partition path
    baseRecord.put("name", null)
    val hk6 = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false"))
      .getKey(baseRecord)
    assertEquals("default", hk6.getPartitionPath)

    // if partition is empty, return default partition path using Row
    keyGen = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false"))
    baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
    val hk6_row = keyGen.getPartitionPath(baseRow)
    assertEquals("default", hk6_row)

    // if record key is empty, throw error
    try {
      baseRecord.put("field1", "")
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "name")
      new SimpleKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
      // do nothing
    }

    // if record key is empty, throw error. Using Row
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "name")
      keyGen = new SimpleKeyGenerator(props)
      baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
      keyGen.getRecordKey(baseRow)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
      // do nothing
    }

    // if record key is null, throw error
    try {
      baseRecord.put("field1", null)
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "name")
      new SimpleKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
      // do nothing
    }

    // if record key is null, throw error. Using Row
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "name")
      keyGen = new SimpleKeyGenerator(props)
      baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
      keyGen.getRecordKey(baseRow)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
      // do nothing
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

    override def getRecordKey(internalRow: InternalRow, structType: StructType): String = {
      if (null == internalConverterFn) {
        val schema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, STRUCT_NAME, NAMESPACE)
        internalConverterFn = AvroConversionUtils.createInternalRowToAvroConverter(structType, schema, true)
      }
      getKey(internalConverterFn.apply(internalRow)).getRecordKey
    }

    override def getPartitionPath(row: Row): String = {
      if (null == converterFn) converterFn = AvroConversionUtils.createConverterToAvro(row.schema, STRUCT_NAME, NAMESPACE)
      val genericRecord = converterFn.apply(row).asInstanceOf[GenericRecord]
      getKey(genericRecord).getPartitionPath
    }

    override def getPartitionPath(internalRow: InternalRow, structType: StructType): String = null
  }

  @Test def testComplexKeyGenerator() = {
    // top level, valid fields
    var keyGen = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))
    val hk1 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk1.getRecordKey)
    assertEquals("field1/name1", hk1.getPartitionPath)

    // top level, valid fields with Row
    assertEquals("field1:field1,name:name1", keyGen.getRecordKey(baseRow))
    assertEquals("field1/name1", keyGen.getPartitionPath(baseRow))

    // partition path field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      new ComplexKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // partition path field not specified using Row
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1")
      val keyGen = new ComplexKeyGenerator(props)
      keyGen.getRecordKey(baseRow)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // recordkey field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionField")
      new ComplexKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // recordkey field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionField")
      val keyGen = new ComplexKeyGenerator(props)
      keyGen.getPartitionPath(baseRow)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // nested field as record key and partition path
    keyGen = new ComplexKeyGenerator(getKeyConfig("testNestedRecord.userId,testNestedRecord.isAdmin", "testNestedRecord.userId,testNestedRecord.isAdmin", "false"))
    val hk2 = keyGen.getKey(baseRecord)
    assertEquals("testNestedRecord.userId:UserId1@001,testNestedRecord.isAdmin:false", hk2.getRecordKey)
    assertEquals("UserId1@001/false", hk2.getPartitionPath)

    // nested field as record key and partition path
    assertEquals("testNestedRecord.userId:UserId1@001,testNestedRecord.isAdmin:false", keyGen.getRecordKey(baseRow))
    assertEquals("UserId1@001/false", keyGen.getPartitionPath(baseRow))

    // Nested record key not found
    try {
      new ComplexKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin", "false"))
        .getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieException =>
      // do nothing
    }

    // Nested record key not found
    try {
      val keyGen = new ComplexKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin", "false"))
      keyGen.getRecordKey(baseRow)
      fail("Should have errored out")
    } catch {
      case e: HoodieException =>
      // do nothing
    }

    // if partition path can't be found, return default partition path
    keyGen = new ComplexKeyGenerator(getKeyConfig("testNestedRecord.userId", "testNestedRecord.notThere", "false"))
    val hk3 = keyGen.getKey(baseRecord)
    assertEquals("default", hk3.getPartitionPath)

    assertEquals("default", keyGen.getPartitionPath(baseRow))

    // if enable hive style partitioning
    keyGen = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "true"))
    val hk4 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk4.getRecordKey)
    assertEquals("field1=field1/name=name1", hk4.getPartitionPath)

    assertEquals("field1:field1,name:name1", keyGen.getRecordKey(baseRow))
    assertEquals("field1=field1/name=name1", keyGen.getPartitionPath(baseRow))

    // if one part of the record key is empty, replace with "__empty__"
    baseRecord.put("name", "")
    keyGen = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))
    val hk5 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1,name:__empty__", hk5.getRecordKey)
    assertEquals("field1/default", hk5.getPartitionPath)

    baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
    assertEquals("field1:field1,name:__empty__", keyGen.getRecordKey(baseRow))
    assertEquals("field1/default", keyGen.getPartitionPath(baseRow))

    // if one part of the record key is null, replace with "__null__"
    baseRecord.put("name", null)
    keyGen = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))
    val hk6 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1,name:__null__", hk6.getRecordKey)
    assertEquals("field1/default", hk6.getPartitionPath)

    baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
    assertEquals("field1:field1,name:__null__", keyGen.getRecordKey(baseRow))
    assertEquals("field1/default", keyGen.getPartitionPath(baseRow))

    // if all parts of the composite record key are null/empty, throw error
    try {
      baseRecord.put("name", "")
      baseRecord.put("field1", null)
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1,name")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "field1,name")
      new ComplexKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
      // do nothing
    }

    // if all parts of the composite record key are null/empty, throw error
    try {
      baseRecord.put("name", "")
      baseRecord.put("field1", null)
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1,name")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "field1,name")
      keyGen = new ComplexKeyGenerator(props)
      baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
      keyGen.getRecordKey(baseRow)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
      // do nothing
    }

    // reset name and field1 values.
    baseRecord.put("name", "name1")
    baseRecord.put("field1", "field1")
    keyGen = new ComplexKeyGenerator(getKeyConfig("field1, name", "field1, name", "false"))
    val hk7 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk7.getRecordKey)
    assertEquals("field1/name1", hk7.getPartitionPath)

    baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
    assertEquals("field1:field1,name:name1", keyGen.getRecordKey(baseRow))
    assertEquals("field1/name1", keyGen.getPartitionPath(baseRow))

    keyGen = new ComplexKeyGenerator(getKeyConfig("field1,", "field1,", "false"))
    val hk8 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1", hk8.getRecordKey)
    assertEquals("field1", hk8.getPartitionPath)

    assertEquals("field1:field1", keyGen.getRecordKey(baseRow))
    assertEquals("field1", keyGen.getPartitionPath(baseRow))
  }

  @Test def testGlobalDeleteKeyGenerator() = {
    // top level, partition value included but not actually used
    var keyGen = new GlobalDeleteKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))
    val hk1 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk1.getRecordKey)
    assertEquals("", hk1.getPartitionPath)

    assertEquals("field1:field1,name:name1", keyGen.getRecordKey(baseRow))
    assertEquals("", keyGen.getPartitionPath(baseRow))

    // top level, partition value not included
    val props = new TypedProperties()
    props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1,name")
    keyGen = new GlobalDeleteKeyGenerator(props)
    val hk2 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk2.getRecordKey)
    assertEquals("", hk2.getPartitionPath)

    assertEquals("field1:field1,name:name1", keyGen.getRecordKey(baseRow))
    assertEquals("", keyGen.getPartitionPath(baseRow))

    // if one part of the record key is empty, replace with "__empty__"
    baseRecord.put("name", "")
    keyGen = new GlobalDeleteKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))
    val hk3 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1,name:__empty__", hk3.getRecordKey)
    assertEquals("", hk3.getPartitionPath)

    baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
    assertEquals("field1:field1,name:__empty__", keyGen.getRecordKey(baseRow))
    assertEquals("", keyGen.getPartitionPath(baseRow))

    // if one part of the record key is null, replace with "__null__"
    baseRecord.put("name", null)
    keyGen = new GlobalDeleteKeyGenerator(getKeyConfig("field1,name", "field1,name", "false"))
    val hk4 = keyGen.getKey(baseRecord)
    assertEquals("field1:field1,name:__null__", hk4.getRecordKey)
    assertEquals("", hk4.getPartitionPath)

    baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
    assertEquals("field1:field1,name:__null__", keyGen.getRecordKey(baseRow))
    assertEquals("", keyGen.getPartitionPath(baseRow))

    // recordkey field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionField")
      new GlobalDeleteKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // recordkey field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionField")
      val keyGen = new GlobalDeleteKeyGenerator(props)
      keyGen.getRecordKey(baseRow)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
      // do nothing
    }

    // Nested record key not found
    try {
      new GlobalDeleteKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin", "false"))
        .getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieException =>
      // do nothing
    }

    // Nested record key not found
    try {
      val keyGen = new GlobalDeleteKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin", "false"))
      keyGen.getRecordKey(baseRow)
      fail("Should have errored out")
    } catch {
      case e: HoodieException =>
      // do nothing
    }

    // if all parts of the composite record key are null/empty, throw error
    try {
      baseRecord.put("name", "")
      baseRecord.put("field1", null)
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1,name")
      new GlobalDeleteKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
      // do nothing
    }

    // if all parts of the composite record key are null/empty, throw error
    try {
      baseRecord.put("name", "")
      baseRecord.put("field1", null)
      baseRow = KeyGeneratorTestUtilities.getRow(baseRecord, schema, structType)
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD.key, "field1,name")
      val keyGen = new GlobalDeleteKeyGenerator(props)
      keyGen.getRecordKey(baseRow)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
      // do nothing
    }
  }

  @Test def testOverwriteWithLatestAvroPayload() = {
    val overWritePayload1 = new OverwriteWithLatestAvroPayload(baseRecord, 1)
    val laterRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 2, "001", "f1")
    val overWritePayload2 = new OverwriteWithLatestAvroPayload(laterRecord, 2)

    // it will provide the record with greatest combine value
    val combinedPayload12 = overWritePayload1.preCombine(overWritePayload2)
    val combinedGR12 = combinedPayload12.getInsertValue(schema).get().asInstanceOf[GenericRecord]
    assertEquals("field2", combinedGR12.get("field1").toString)

    // and it will be deterministic, to order of processing.
    val combinedPayload21 = overWritePayload2.preCombine(overWritePayload1)
    val combinedGR21 = combinedPayload21.getInsertValue(schema).get().asInstanceOf[GenericRecord]
    assertEquals("field2", combinedGR21.get("field1").toString)
  }

  @Test def testOverwriteWithLatestAvroPayloadCombineAndGetUpdateValue() = {
    val baseOrderingVal: Object = baseRecord.get("favoriteIntNumber")
    val fieldSchema: Schema = baseRecord.getSchema().getField("favoriteIntNumber").schema()
    val props = new TypedProperties()
    props.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "favoriteIntNumber");

    val basePayload = new OverwriteWithLatestAvroPayload(baseRecord, HoodieAvroUtils.convertValueForSpecificDataTypes(fieldSchema, baseOrderingVal, false).asInstanceOf[Comparable[_]])

    val laterRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 2, "001", "f1")
    val laterOrderingVal: Object = laterRecord.get("favoriteIntNumber")
    val newerPayload = new OverwriteWithLatestAvroPayload(laterRecord, HoodieAvroUtils.convertValueForSpecificDataTypes(fieldSchema, laterOrderingVal, false).asInstanceOf[Comparable[_]])

    // it will provide the record with greatest combine value
    val preCombinedPayload = basePayload.preCombine(newerPayload)
    val precombinedGR = preCombinedPayload.getInsertValue(schema).get().asInstanceOf[GenericRecord]
    assertEquals("field2", precombinedGR.get("field1").toString)
  }

  @Test def testDefaultHoodieRecordPayloadCombineAndGetUpdateValue() = {
    val fieldSchema: Schema = baseRecord.getSchema().getField("favoriteIntNumber").schema()
    val props = HoodiePayloadConfig.newBuilder()
      .withPayloadOrderingField("favoriteIntNumber").build().getProps;

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

  @Test def testEmptyHoodieRecordPayload() = {
    val emptyPayload1 = new EmptyHoodieRecordPayload(baseRecord, 1)
    val laterRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 2, "001", "f1")
    val emptyPayload2 = new EmptyHoodieRecordPayload(laterRecord, 2)

    // it will provide an empty record
    val combinedPayload12 = emptyPayload1.preCombine(emptyPayload2)
    val combined12 = combinedPayload12.getInsertValue(schema)
    assertEquals(Option.empty(), combined12)
  }
}
