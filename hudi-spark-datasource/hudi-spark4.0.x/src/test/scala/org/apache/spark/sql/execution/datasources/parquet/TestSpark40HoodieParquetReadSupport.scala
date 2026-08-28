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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.exception.HoodieException

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.execution.datasources.parquet.VariantParquetTestFixtures.{shreddedVariant, stringKeyMap, threeLevelList, twoLevelList, unshreddedVariant}
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, MapType, MetadataBuilder, StringType, StructField, StructType, VariantType}
import org.junit.jupiter.api.{Assertions, Test}

class TestSpark40HoodieParquetReadSupport {

  /**
   * Validate that reorderVariantFields does not treat groups as variant when the value/metadata
   * fields fail the type checks in isVariantGroup. Each sub-group exercises a different false
   * branch of the short-circuit && chain.
   */
  @Test
  def testReorderVariantFieldsNonVariantGroupsUnchanged(): Unit = {
    val schema = Types.buildMessage()
      // value is non-primitive
      .addField(Types.requiredGroup()
        .addField(Types.requiredGroup().addField(Types.required(PrimitiveTypeName.INT32).named("x")).named("value"))
        .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
        .named("g1"))
      // value is primitive, metadata is non-primitive
      .addField(Types.requiredGroup()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("value"))
        .addField(Types.requiredGroup().addField(Types.required(PrimitiveTypeName.INT32).named("x")).named("metadata"))
        .named("g2"))
      // both primitive but non-BINARY
      .addField(Types.requiredGroup()
        .addField(Types.required(PrimitiveTypeName.INT32).named("value"))
        .addField(Types.required(PrimitiveTypeName.INT32).named("metadata"))
        .named("g3"))
      // value is BINARY, metadata is non-BINARY primitive
      .addField(Types.requiredGroup()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("value"))
        .addField(Types.required(PrimitiveTypeName.INT32).named("metadata"))
        .named("g4"))
      .named("test")

    val result = Spark40HoodieParquetReadSupport.reorderVariantFields(schema)
    Assertions.assertEquals(schema, result)
  }

  /**
   * A shredded variant group (typed_value present) must fail fast: Spark 4.0's unshredded
   * converter reads only [value, metadata], so reading the group (which used to drop
   * typed_value from the requested schema) silently lost the typed rows' payload. The
   * shape-only fallback, the catalyst-anchored walk and the PushVariantIntoScan rewrite of the
   * same column all reject it. The rewrite arm cannot recurse by name - its fields are the
   * ordinals "0" and "1", which exist nowhere in the parquet group - so it reads the shape.
   */
  @Test
  def testRejectShreddedVariantsFailsFastOnShreddedGroup(): Unit = {
    val schema = Types.buildMessage()
      .addField(shreddedVariant("v"))
      .named("test")

    val catalystArms = Seq(
      None,
      Some(new StructType().add("v", VariantType)),
      Some(new StructType().add("v", variantRewriteStruct)))
    catalystArms.foreach { sparkSchema =>
      val failure = Assertions.assertThrows(classOf[HoodieException],
        () => Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, sparkSchema))
      Assertions.assertTrue(
        failure.getMessage.contains("shredded variant") && failure.getMessage.contains("'v'"),
        s"The error must name the shredded variant column, got: ${failure.getMessage}")
    }

    // Only typed_value is fatal: the same rewrite over an unshredded file still reads.
    Spark40HoodieParquetReadSupport.rejectShreddedVariants(
      Types.buildMessage().addField(unshreddedVariant("v")).named("test"),
      Some(new StructType().add("v", variantRewriteStruct)))

    // The reorder itself no longer throws, and must leave typed_value in place: rebuilding the
    // group as [value, metadata] is what dropped the typed rows' payload.
    val reordered = Spark40HoodieParquetReadSupport.reorderVariantFields(schema)
    Assertions.assertTrue(
      reordered.getType(reordered.getFieldIndex("v")).asGroupType().containsField("typed_value"),
      "The reorder must not drop typed_value")
  }

  /**
   * A variant nested inside a struct is rejected too, reported by its dotted path, by both the
   * catalyst-anchored walk and the shape-only fallback. The catalyst-anchored walk is what keeps
   * the same parquet shape typed as a plain struct out of it.
   */
  @Test
  def testRejectShreddedVariantsFailsFastOnNestedVariant(): Unit = {
    val schema = Types.buildMessage()
      .addField(Types.requiredGroup()
        .addField(shreddedVariant("inner"))
        .named("s"))
      .named("test")

    val variantSchema = new StructType().add("s", new StructType().add("inner", VariantType))
    val rewriteSchema = new StructType().add("s", new StructType().add("inner", variantRewriteStruct))
    Seq(None, Some(variantSchema), Some(rewriteSchema)).foreach { sparkSchema =>
      val failure = Assertions.assertThrows(classOf[HoodieException],
        () => Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, sparkSchema))
      Assertions.assertTrue(failure.getMessage.contains("'s.inner'"),
        s"The error must name the nested variant path, got: ${failure.getMessage}")
    }

    val structSchema = new StructType().add("s", new StructType().add("inner", plainStructTwin))
    Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, Some(structSchema))
  }

  /**
   * A shredded variant inside an array is unreadable too, in both list layouts a Hudi base file
   * can carry: the 3-level list the row writer emits, reported as v.element, and the 2-level
   * "array" list the Avro write path emits (parquet-avro's default
   * parquet.avro.write-old-list-structure=true), where the repeated group is itself the element
   * record. The plain-struct catalyst twin over the same file is left alone.
   */
  @Test
  def testRejectShreddedVariantsFailsFastOnVariantInArray(): Unit = {
    val schema = Types.buildMessage()
      .addField(threeLevelList("v", shreddedVariant("element")))
      .named("test")

    val variantSchema = new StructType().add("v", ArrayType(VariantType))
    val failure = Assertions.assertThrows(classOf[HoodieException],
      () => Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, Some(variantSchema)))
    Assertions.assertTrue(failure.getMessage.contains("'v.element'"),
      s"The error must name the array element path, got: ${failure.getMessage}")

    val structSchema = new StructType().add("v", ArrayType(plainStructTwin))
    Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, Some(structSchema))

    // The Avro writer's own layout for the array position it does shred, array<struct<variant>>:
    // the repeated group named "array" IS the element record. A walk that only knew the 3-level
    // layout would unwrap that single-field group, pair the variant group itself against
    // catalyst's struct<inner: variant>, match no field name and let the file through.
    val avroSchema = Types.buildMessage()
      .addField(twoLevelList("v", "array", shreddedVariant("inner")))
      .named("test")
    val avroVariantSchema =
      new StructType().add("v", ArrayType(new StructType().add("inner", VariantType)))
    val avroFailure = Assertions.assertThrows(classOf[HoodieException],
      () => Spark40HoodieParquetReadSupport.rejectShreddedVariants(avroSchema, Some(avroVariantSchema)))
    Assertions.assertTrue(
      avroFailure.getMessage.contains("shredded variant") && avroFailure.getMessage.contains("'v.element.inner'"),
      s"The 2-level list error must name the shredded variant column, got: ${avroFailure.getMessage}")
  }

  /**
   * Same for a shredded variant as a map value, reported as v.value; only the value side of the
   * key_value group is walked.
   */
  @Test
  def testRejectShreddedVariantsFailsFastOnVariantInMap(): Unit = {
    val schema = Types.buildMessage()
      .addField(stringKeyMap("v", shreddedVariant("value")))
      .named("test")

    val variantSchema = new StructType().add("v", MapType(StringType, VariantType))
    val failure = Assertions.assertThrows(classOf[HoodieException],
      () => Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, Some(variantSchema)))
    Assertions.assertTrue(failure.getMessage.contains("'v.value'"),
      s"The error must name the map value path, got: ${failure.getMessage}")

    val structSchema = new StructType().add("v", MapType(StringType, plainStructTwin))
    Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, Some(structSchema))
  }

  /** The unshredded twin still reorders to [value, metadata] as before. */
  @Test
  def testReorderVariantFieldsReordersUnshreddedGroup(): Unit = {
    val schema = Types.buildMessage()
      .addField(unshreddedVariant("v"))
      .named("test")

    val result = Spark40HoodieParquetReadSupport.reorderVariantFields(schema)
    val group = result.getType(result.getFieldIndex("v")).asGroupType()
    Assertions.assertEquals("value", group.getFields.get(0).getName)
    Assertions.assertEquals("metadata", group.getFields.get(1).getName)
  }

  /**
   * Spark's PushVariantIntoScan rewrite of one variant column: extraction fields named by
   * ordinal, each carrying the marker metadata the rewrite arm keys on.
   */
  private def variantRewriteStruct: StructType = {
    val marker = new MetadataBuilder().putString(VariantMetadata.METADATA_KEY, "$").build()
    new StructType()
      .add(StructField("0", BinaryType, nullable = true, marker))
      .add(StructField("1", BinaryType, nullable = true, marker))
  }

  /** The same parquet shape typed as a plain struct in catalyst, which must be left alone. */
  private def plainStructTwin: StructType =
    new StructType()
      .add("metadata", BinaryType)
      .add("value", BinaryType)
      .add("typed_value", new StructType().add("a", IntegerType))
}
