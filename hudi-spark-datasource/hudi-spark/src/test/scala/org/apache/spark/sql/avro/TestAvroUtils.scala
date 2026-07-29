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

package org.apache.spark.sql.avro

import org.apache.avro.Schema
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Direct coverage for [[AvroUtils]] and its [[AvroUtils.AvroSchemaHelper]], which are otherwise only
 * exercised indirectly through the Avro serializers. Focuses on the type-support predicate and the
 * schema-matching/validation error paths (extra Catalyst fields, extra required Avro fields,
 * positional vs by-name matching, ambiguous by-name lookup), pinning the raised exception messages.
 */
class TestAvroUtils {

  private def parse(json: String): Schema = new Schema.Parser().parse(json)

  @Test
  def testSupportsDataType(): Unit = {
    assertTrue(AvroUtils.supportsDataType(IntegerType))
    assertTrue(AvroUtils.supportsDataType(StringType))
    assertTrue(AvroUtils.supportsDataType(NullType))
    assertTrue(AvroUtils.supportsDataType(ArrayType(LongType)))
    assertTrue(AvroUtils.supportsDataType(MapType(StringType, IntegerType)))
    assertTrue(AvroUtils.supportsDataType(
      new StructType().add("a", IntegerType).add("b", ArrayType(StringType))))
    // CalendarInterval is not representable in Avro, so every wrapper around it is unsupported too.
    assertFalse(AvroUtils.supportsDataType(CalendarIntervalType))
    assertFalse(AvroUtils.supportsDataType(ArrayType(CalendarIntervalType)))
    assertFalse(AvroUtils.supportsDataType(MapType(StringType, CalendarIntervalType)))
    assertFalse(AvroUtils.supportsDataType(new StructType().add("a", CalendarIntervalType)))
  }

  @Test
  def testToFieldStr(): Unit = {
    assertEquals("top-level record", AvroUtils.toFieldStr(Seq.empty))
    assertEquals("field 'foo'", AvroUtils.toFieldStr(Seq("foo")))
    assertEquals("field 'foo.bar'", AvroUtils.toFieldStr(Seq("foo", "bar")))
  }

  @Test
  def testIsNullable(): Unit = {
    val record = parse(
      """{"type":"record","name":"r","fields":[
        |  {"name":"req","type":"int"},
        |  {"name":"opt","type":["null","int"]},
        |  {"name":"uni","type":["int","long"]}
        |]}""".stripMargin)
    assertFalse(AvroUtils.isNullable(record.getField("req")))
    assertTrue(AvroUtils.isNullable(record.getField("opt")))
    // A union without a NULL branch is not nullable.
    assertFalse(AvroUtils.isNullable(record.getField("uni")))
  }

  @Test
  def testSchemaHelperRejectsNonRecordSchema(): Unit = {
    val ex = assertThrows(classOf[IncompatibleSchemaException],
      () => new AvroUtils.AvroSchemaHelper(
        Schema.create(Schema.Type.INT), new StructType(), Seq.empty, Seq.empty, false))
    assertTrue(ex.getMessage.contains("as a RECORD"))
  }

  @Test
  def testMatchedFieldsAndGetAvroFieldByName(): Unit = {
    val avro = parse(
      """{"type":"record","name":"r","fields":[
        |  {"name":"id","type":"int"},
        |  {"name":"name","type":["null","string"]}
        |]}""".stripMargin)
    val catalyst = new StructType().add("id", IntegerType).add("name", StringType)
    val helper = new AvroUtils.AvroSchemaHelper(avro, catalyst, Seq.empty, Seq.empty, false)

    assertEquals(2, helper.matchedFields.size)
    assertEquals("id", helper.getAvroField("id", 0).get.name())
    assertTrue(helper.getAvroField("missing", 5).isEmpty)
  }

  @Test
  def testGetAvroFieldPositional(): Unit = {
    val avro = parse(
      """{"type":"record","name":"r","fields":[{"name":"only","type":"int"}]}""")
    val helper = new AvroUtils.AvroSchemaHelper(avro, new StructType(), Seq.empty, Seq.empty, true)
    // Positional matching ignores the name and selects by index.
    assertEquals("only", helper.getAvroField("anything", 0).get.name())
    assertTrue(helper.getAvroField("anything", 1).isEmpty)
  }

  @Test
  def testValidateNoExtraCatalystFieldsByName(): Unit = {
    val avro = parse(
      """{"type":"record","name":"r","fields":[{"name":"id","type":"int"}]}""")
    // A nullable Catalyst field with no Avro counterpart.
    val catalyst = new StructType().add("id", IntegerType).add("extra", StringType, nullable = true)
    val helper = new AvroUtils.AvroSchemaHelper(avro, catalyst, Seq.empty, Seq.empty, false)

    val ex = assertThrows(classOf[IncompatibleSchemaException],
      () => helper.validateNoExtraCatalystFields(ignoreNullable = false))
    assertTrue(ex.getMessage.contains("Cannot find field 'extra' in Avro schema"))

    // When nullable Catalyst fields are ignored, the same extra field is tolerated.
    helper.validateNoExtraCatalystFields(ignoreNullable = true)
  }

  @Test
  def testValidateNoExtraCatalystFieldsPositional(): Unit = {
    val avro = parse(
      """{"type":"record","name":"r","fields":[{"name":"id","type":"int"}]}""")
    val catalyst = new StructType().add("id", IntegerType).add("second", IntegerType)
    val helper = new AvroUtils.AvroSchemaHelper(avro, catalyst, Seq.empty, Seq.empty, true)

    val ex = assertThrows(classOf[IncompatibleSchemaException],
      () => helper.validateNoExtraCatalystFields(ignoreNullable = false))
    assertTrue(ex.getMessage.contains("Cannot find field at position 1"))
  }

  @Test
  def testValidateNoExtraRequiredAvroFields(): Unit = {
    val avroWithRequiredGhost = parse(
      """{"type":"record","name":"r","fields":[
        |  {"name":"id","type":"int"},
        |  {"name":"ghost","type":"int"}
        |]}""".stripMargin)
    val catalyst = new StructType().add("id", IntegerType)
    val helper = new AvroUtils.AvroSchemaHelper(
      avroWithRequiredGhost, catalyst, Seq.empty, Seq.empty, false)
    val ex = assertThrows(classOf[IncompatibleSchemaException],
      () => helper.validateNoExtraRequiredAvroFields())
    assertTrue(ex.getMessage.contains("Found field 'ghost'"))

    // A nullable extra Avro field is not required, so it is tolerated.
    val avroWithOptionalGhost = parse(
      """{"type":"record","name":"r","fields":[
        |  {"name":"id","type":"int"},
        |  {"name":"ghost","type":["null","int"]}
        |]}""".stripMargin)
    val helper2 = new AvroUtils.AvroSchemaHelper(
      avroWithOptionalGhost, catalyst, Seq.empty, Seq.empty, false)
    helper2.validateNoExtraRequiredAvroFields()
  }

  @Test
  def testGetFieldByNameAmbiguousMatch(): Unit = {
    // Two fields differing only by case collide under the default case-insensitive resolver.
    val avro = parse(
      """{"type":"record","name":"r","fields":[
        |  {"name":"ID","type":"int"},
        |  {"name":"id","type":"int"}
        |]}""".stripMargin)
    val helper = new AvroUtils.AvroSchemaHelper(avro, new StructType(), Seq.empty, Seq.empty, false)
    val ex = assertThrows(classOf[IncompatibleSchemaException],
      () => helper.getFieldByName("id"))
    assertTrue(ex.getMessage.contains("gave 2 matches"))
  }
}
