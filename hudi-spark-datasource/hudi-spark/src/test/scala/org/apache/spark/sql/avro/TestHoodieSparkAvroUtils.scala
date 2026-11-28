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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

/**
 * Direct coverage for [[HoodieSparkAvroUtils]] and its
 * [[HoodieSparkAvroUtils.AvroSchemaHelper]], which are otherwise only
 * exercised indirectly through the Avro serializers. Focuses on the type-support predicate and the
 * schema-matching/validation error paths (extra Catalyst fields, extra required Avro fields,
 * positional vs by-name matching, ambiguous by-name lookup), pinning the raised exception messages.
 */
class TestHoodieSparkAvroUtils {

  private def parse(json: String): Schema = new Schema.Parser().parse(json)

  @Test
  def testSupportsDataType(): Unit = {
    // supportsDataType has no live Hudi caller; this guards the vendored copy against edit drift.
    assertTrue(HoodieSparkAvroUtils.supportsDataType(IntegerType))
    assertTrue(HoodieSparkAvroUtils.supportsDataType(StringType))
    assertTrue(HoodieSparkAvroUtils.supportsDataType(NullType))
    assertTrue(HoodieSparkAvroUtils.supportsDataType(ArrayType(LongType)))
    assertTrue(HoodieSparkAvroUtils.supportsDataType(MapType(StringType, IntegerType)))
    assertTrue(HoodieSparkAvroUtils.supportsDataType(
      new StructType().add("a", IntegerType).add("b", ArrayType(StringType))))
    // CalendarInterval is not representable in Avro, so every wrapper around it is unsupported too.
    assertFalse(HoodieSparkAvroUtils.supportsDataType(CalendarIntervalType))
    assertFalse(HoodieSparkAvroUtils.supportsDataType(ArrayType(CalendarIntervalType)))
    assertFalse(HoodieSparkAvroUtils.supportsDataType(MapType(StringType, CalendarIntervalType)))
    assertFalse(HoodieSparkAvroUtils.supportsDataType(new StructType().add("a", CalendarIntervalType)))
  }

  @Test
  def testToFieldStr(): Unit = {
    assertEquals("top-level record", HoodieSparkAvroUtils.toFieldStr(Seq.empty))
    assertEquals("field 'foo'", HoodieSparkAvroUtils.toFieldStr(Seq("foo")))
    assertEquals("field 'foo.bar'", HoodieSparkAvroUtils.toFieldStr(Seq("foo", "bar")))
  }

  @Test
  def testIsNullable(): Unit = {
    val record = parse(
      """{"type":"record","name":"r","fields":[
        |  {"name":"req","type":"int"},
        |  {"name":"opt","type":["null","int"]},
        |  {"name":"uni","type":["int","long"]}
        |]}""".stripMargin)
    assertFalse(HoodieSparkAvroUtils.isNullable(record.getField("req")))
    assertTrue(HoodieSparkAvroUtils.isNullable(record.getField("opt")))
    // A union without a NULL branch is not nullable.
    assertFalse(HoodieSparkAvroUtils.isNullable(record.getField("uni")))
  }

  @Test
  def testSchemaHelperRejectsNonRecordSchema(): Unit = {
    val ex = assertThrows(classOf[IncompatibleSchemaException],
      () => new HoodieSparkAvroUtils.AvroSchemaHelper(
        Schema.create(Schema.Type.INT), new StructType(), Seq.empty, Seq.empty, false))
    assertTrue(ex.getMessage.contains("as a RECORD"))
  }

  @Test
  def testMatchedFieldsAndGetAvroFieldByName(): Unit = {
    // Avro order differs from Catalyst so by-name lookup is discriminated from positional.
    val avro = parse(
      """{"type":"record","name":"r","fields":[
        |  {"name":"name","type":["null","string"]},
        |  {"name":"id","type":"int"}
        |]}""".stripMargin)
    val catalyst = new StructType().add("id", IntegerType).add("name", StringType)
    val helper = new HoodieSparkAvroUtils.AvroSchemaHelper(avro, catalyst, Seq.empty, Seq.empty, false)

    assertEquals(2, helper.matchedFields.size)
    assertEquals("id", helper.getAvroField("id", 0).get.name())
    assertTrue(helper.getAvroField("missing", 5).isEmpty)
  }

  @Test
  def testGetAvroFieldPositional(): Unit = {
    val avro = parse(
      """{"type":"record","name":"r","fields":[{"name":"only","type":"int"}]}""")
    val helper = new HoodieSparkAvroUtils.AvroSchemaHelper(avro, new StructType(), Seq.empty, Seq.empty, true)
    // Hudi never enables positionalFieldMatch; vendored-path drift guard.
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
    val helper = new HoodieSparkAvroUtils.AvroSchemaHelper(avro, catalyst, Seq.empty, Seq.empty, false)

    val ex = assertThrows(classOf[IncompatibleSchemaException],
      () => helper.validateNoExtraCatalystFields(ignoreNullable = false))
    assertTrue(ex.getMessage.contains("Cannot find field 'extra' in Avro schema"))

    // When nullable Catalyst fields are ignored, the same extra field is tolerated.
    assertDoesNotThrow(new Executable {
      def execute(): Unit = helper.validateNoExtraCatalystFields(ignoreNullable = true)
    })

    // A non-nullable extra Catalyst field must still throw even when nullables are ignored;
    // this is the combination the deserializer relies on (ignoreNullable = true).
    val strictCatalyst = new StructType().add("id", IntegerType).add("extra", StringType, nullable = false)
    val strictHelper = new HoodieSparkAvroUtils.AvroSchemaHelper(avro, strictCatalyst, Seq.empty, Seq.empty, false)
    val strictEx = assertThrows(classOf[IncompatibleSchemaException],
      () => strictHelper.validateNoExtraCatalystFields(ignoreNullable = true))
    assertTrue(strictEx.getMessage.contains("Cannot find field 'extra' in Avro schema"))
  }

  @Test
  def testValidateNoExtraCatalystFieldsPositional(): Unit = {
    // Hudi never enables positionalFieldMatch; vendored-path drift guard.
    val avro = parse(
      """{"type":"record","name":"r","fields":[{"name":"id","type":"int"}]}""")
    val catalyst = new StructType().add("id", IntegerType).add("second", IntegerType)
    val helper = new HoodieSparkAvroUtils.AvroSchemaHelper(avro, catalyst, Seq.empty, Seq.empty, true)

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
    val helper = new HoodieSparkAvroUtils.AvroSchemaHelper(
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
    val helperWithOptionalGhost = new HoodieSparkAvroUtils.AvroSchemaHelper(
      avroWithOptionalGhost, catalyst, Seq.empty, Seq.empty, false)
    assertDoesNotThrow(new Executable {
      def execute(): Unit = helperWithOptionalGhost.validateNoExtraRequiredAvroFields()
    })
  }

  @Test
  def testGetFieldByNameAmbiguousMatch(): Unit = {
    // Two fields differing only by case collide under a case-insensitive resolver.
    val avro = parse(
      """{"type":"record","name":"r","fields":[
        |  {"name":"ID","type":"int"},
        |  {"name":"id","type":"int"}
        |]}""".stripMargin)
    val helper = new HoodieSparkAvroUtils.AvroSchemaHelper(avro, new StructType(), Seq.empty, Seq.empty, false)
    // The resolver comes from ambient SQLConf, so pin it instead of relying on the JVM default.
    val conf = SQLConf.get
    conf.setConfString("spark.sql.caseSensitive", "false")
    try {
      val ex = assertThrows(classOf[IncompatibleSchemaException],
        () => helper.getFieldByName("id"))
      assertTrue(ex.getMessage.contains("gave 2 matches"))
      // Under a case-sensitive resolver the same lookup is unambiguous.
      conf.setConfString("spark.sql.caseSensitive", "true")
      assertEquals("id", helper.getFieldByName("id").get.name())
    } finally {
      conf.unsetConf("spark.sql.caseSensitive")
    }
  }

  @Test
  def testSparkAvroUtilsIsNotShadowed(): Unit = {
    // Spark ships org.apache.spark.sql.avro.AvroUtils in spark-sql. The vendored fork used to be
    // shipped under the same name, so whichever copy came first on the classpath was loaded and
    // Spark's callers could bind to the Spark 3.3 fork: they lost methods the fork does not have
    // (Spark 4.0's SchemaConverters needs nonNullUnionBranches, Hudi #20601), and SPARK-59108's
    // sixth AvroSchemaHelper constructor parameter broke Hudi's AvroSerializer on Spark 4.0.5+
    // (Hudi #20916). The fork must therefore not be loadable under Spark's name.
    val sparkAvroUtils = Class.forName("org.apache.spark.sql.avro.AvroUtils$")
    val location = Option(sparkAvroUtils.getProtectionDomain)
      .flatMap(pd => Option(pd.getCodeSource))
      .flatMap(cs => Option(cs.getLocation))
      .map(_.toString)
    assertFalse(location.exists(_.contains("hudi-spark-common")),
      s"Spark's AvroUtils was shadowed by the Hudi fork, loaded from $location")
  }
}
