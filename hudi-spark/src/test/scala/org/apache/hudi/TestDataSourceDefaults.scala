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

import org.apache.avro.generic.GenericRecord
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.{EmptyHoodieRecordPayload, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.testutils.SchemaTestUtil
import org.apache.hudi.common.util.Option
import org.apache.hudi.exception.{HoodieException, HoodieKeyException}
import org.apache.hudi.keygen.{ComplexKeyGenerator, GlobalDeleteKeyGenerator, SimpleKeyGenerator}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}
import org.scalatest.Assertions.fail

/**
 * Tests on the default key generator, payload classes.
 */
class TestDataSourceDefaults {

  val schema = SchemaTestUtil.getComplexEvolvedSchema
  var baseRecord: GenericRecord = _

  @BeforeEach def initialize(): Unit = {
    baseRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 1, "001", "f1")
  }


  private def getKeyConfig(recordKeyFieldName: String, partitionPathField: String, hiveStylePartitioning: String): TypedProperties = {
    val props = new TypedProperties()
    props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, recordKeyFieldName)
    props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, partitionPathField)
    props.setProperty(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, hiveStylePartitioning)
    props
  }

  @Test def testSimpleKeyGenerator() = {
    // top level, valid fields
    val hk1 = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false")).getKey(baseRecord)
    assertEquals("field1", hk1.getRecordKey)
    assertEquals("name1", hk1.getPartitionPath)

    // partition path field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "field1")
      new SimpleKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
        // do nothing
    }

    // recordkey field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partitionField")
      new SimpleKeyGenerator(props).getKey(baseRecord)
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

    // if enable hive style partitioning
    val hk4 = new SimpleKeyGenerator(getKeyConfig("field1", "name", "true")).getKey(baseRecord)
    assertEquals("name=name1", hk4.getPartitionPath)

    // if partition is null, return default partition path
    baseRecord.put("name", "")
    val hk5 = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false"))
      .getKey(baseRecord)
    assertEquals("default", hk5.getPartitionPath)

    // if partition is empty, return default partition path
    baseRecord.put("name", null)
    val hk6 = new SimpleKeyGenerator(getKeyConfig("field1", "name", "false"))
      .getKey(baseRecord)
    assertEquals("default", hk6.getPartitionPath)

    // if record key is empty, throw error
    try {
      baseRecord.put("field1", "")
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "field1")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "name")
      new SimpleKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
        // do nothing
    }

    // if record key is null, throw error
    try {
      baseRecord.put("field1", null)
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "field1")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "name")
      new SimpleKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
        // do nothing
    }
  }

  @Test def testComplexKeyGenerator() = {
    // top level, valid fields
    val hk1 = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false")).getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk1.getRecordKey)
    assertEquals("field1/name1", hk1.getPartitionPath)

    // partition path field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "field1")
      new ComplexKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
        // do nothing
    }

    // recordkey field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partitionField")
      new ComplexKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: IllegalArgumentException =>
        // do nothing
    }

    // nested field as record key and partition path
    val hk2 = new ComplexKeyGenerator(getKeyConfig("testNestedRecord.userId,testNestedRecord.isAdmin", "testNestedRecord.userId,testNestedRecord.isAdmin", "false"))
      .getKey(baseRecord)
    assertEquals("testNestedRecord.userId:UserId1@001,testNestedRecord.isAdmin:false", hk2.getRecordKey)
    assertEquals("UserId1@001/false", hk2.getPartitionPath)

    // Nested record key not found
    try {
      new ComplexKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin", "false"))
        .getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieException =>
        // do nothing
    }

    // if partition path can't be found, return default partition path
    val hk3 = new ComplexKeyGenerator(getKeyConfig("testNestedRecord.userId", "testNestedRecord.notThere", "false"))
      .getKey(baseRecord)
    assertEquals("default", hk3.getPartitionPath)

    // if enable hive style partitioning
    val hk4 = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "true")).getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk4.getRecordKey)
    assertEquals("field1=field1/name=name1", hk4.getPartitionPath)

    // if one part of the record key is empty, replace with "__empty__"
    baseRecord.put("name", "")
    val hk5 = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false")).getKey(baseRecord)
    assertEquals("field1:field1,name:__empty__", hk5.getRecordKey)
    assertEquals("field1/default", hk5.getPartitionPath)

    // if one part of the record key is null, replace with "__null__"
    baseRecord.put("name", null)
    val hk6 = new ComplexKeyGenerator(getKeyConfig("field1,name", "field1,name", "false")).getKey(baseRecord)
    assertEquals("field1:field1,name:__null__", hk6.getRecordKey)
    assertEquals("field1/default", hk6.getPartitionPath)

    // if all parts of the composite record key are null/empty, throw error
    try {
      baseRecord.put("name", "")
      baseRecord.put("field1", null)
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "field1,name")
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "field1,name")
      new ComplexKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieKeyException =>
        // do nothing
    }

    // reset name and field1 values.
    baseRecord.put("name", "name1")
    baseRecord.put("field1", "field1")
    val hk7 = new ComplexKeyGenerator(getKeyConfig("field1, name", "field1, name", "false")).getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk7.getRecordKey)
    assertEquals("field1/name1", hk7.getPartitionPath)

    val hk8 = new ComplexKeyGenerator(getKeyConfig("field1,", "field1,", "false")).getKey(baseRecord)
    assertEquals("field1:field1", hk8.getRecordKey)
    assertEquals("field1", hk8.getPartitionPath)
  }

  @Test def testGlobalDeleteKeyGenerator() = {
    // top level, partition value included but not actually used
    val hk1 = new GlobalDeleteKeyGenerator(getKeyConfig("field1,name", "field1,name", "false")).getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk1.getRecordKey)
    assertEquals("", hk1.getPartitionPath)

    // top level, partition value not included
    val props = new TypedProperties()
    props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "field1,name")
    val hk2 = new GlobalDeleteKeyGenerator(props).getKey(baseRecord)
    assertEquals("field1:field1,name:name1", hk2.getRecordKey)
    assertEquals("", hk2.getPartitionPath)

    // if one part of the record key is empty, replace with "__empty__"
    baseRecord.put("name", "")
    val hk3 = new GlobalDeleteKeyGenerator(getKeyConfig("field1,name", "field1,name", "false")).getKey(baseRecord)
    assertEquals("field1:field1,name:__empty__", hk3.getRecordKey)
    assertEquals("", hk3.getPartitionPath)

    // if one part of the record key is null, replace with "__null__"
    baseRecord.put("name", null)
    val hk4 = new GlobalDeleteKeyGenerator(getKeyConfig("field1,name", "field1,name", "false")).getKey(baseRecord)
    assertEquals("field1:field1,name:__null__", hk4.getRecordKey)
    assertEquals("", hk4.getPartitionPath)

    // recordkey field not specified
    try {
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partitionField")
      new GlobalDeleteKeyGenerator(props).getKey(baseRecord)
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

    // if all parts of the composite record key are null/empty, throw error
    try {
      baseRecord.put("name", "")
      baseRecord.put("field1", null)
      val props = new TypedProperties()
      props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "field1,name")
      new GlobalDeleteKeyGenerator(props).getKey(baseRecord)
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
