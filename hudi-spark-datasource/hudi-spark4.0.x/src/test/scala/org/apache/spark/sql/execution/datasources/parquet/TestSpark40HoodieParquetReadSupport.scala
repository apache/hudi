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
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructType, VariantType}
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
   * typed_value from the requested schema) silently lost the typed rows' payload. Both the
   * catalyst-anchored walk and the shape-only fallback reject it.
   */
  @Test
  def testRejectShreddedVariantsFailsFastOnShreddedGroup(): Unit = {
    val schema = Types.buildMessage()
      .addField(Types.requiredGroup()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
        .addField(Types.optional(PrimitiveTypeName.BINARY).named("value"))
        .addField(Types.optionalGroup()
          .addField(Types.optional(PrimitiveTypeName.INT32).named("a")).named("typed_value"))
        .named("v"))
      .named("test")

    Seq(None, Some(new StructType().add("v", VariantType))).foreach { sparkSchema =>
      val failure = Assertions.assertThrows(classOf[HoodieException],
        () => Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, sparkSchema))
      Assertions.assertTrue(
        failure.getMessage.contains("shredded variant") && failure.getMessage.contains("'v'"),
        s"The error must name the shredded variant column, got: ${failure.getMessage}")
    }

    // The reorder itself no longer throws, and must leave typed_value in place: rebuilding the
    // group as [value, metadata] is what dropped the typed rows' payload.
    val reordered = Spark40HoodieParquetReadSupport.reorderVariantFields(schema)
    Assertions.assertTrue(
      reordered.getType(reordered.getFieldIndex("v")).asGroupType().containsField("typed_value"),
      "The reorder must not drop typed_value")
  }

  /**
   * A variant nested inside a struct is rejected too, reported by its dotted path - the walk is
   * anchored on catalyst, so the same parquet shape typed as a plain struct is left alone.
   */
  @Test
  def testRejectShreddedVariantsFailsFastOnNestedVariant(): Unit = {
    val schema = Types.buildMessage()
      .addField(Types.requiredGroup()
        .addField(Types.optionalGroup()
          .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
          .addField(Types.optional(PrimitiveTypeName.BINARY).named("value"))
          .addField(Types.optionalGroup()
            .addField(Types.optional(PrimitiveTypeName.INT32).named("a")).named("typed_value"))
          .named("inner"))
        .named("s"))
      .named("test")

    val variantSchema = new StructType().add("s", new StructType().add("inner", VariantType))
    val failure = Assertions.assertThrows(classOf[HoodieException],
      () => Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, Some(variantSchema)))
    Assertions.assertTrue(failure.getMessage.contains("'s.inner'"),
      s"The error must name the nested variant path, got: ${failure.getMessage}")

    val structSchema = new StructType().add("s", new StructType().add("inner", new StructType()
      .add("metadata", BinaryType)
      .add("value", BinaryType)
      .add("typed_value", new StructType().add("a", IntegerType))))
    Spark40HoodieParquetReadSupport.rejectShreddedVariants(schema, Some(structSchema))
  }

  /** The unshredded twin still reorders to [value, metadata] as before. */
  @Test
  def testReorderVariantFieldsReordersUnshreddedGroup(): Unit = {
    val schema = Types.buildMessage()
      .addField(Types.requiredGroup()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
        .addField(Types.required(PrimitiveTypeName.BINARY).named("value"))
        .named("v"))
      .named("test")

    val result = Spark40HoodieParquetReadSupport.reorderVariantFields(schema)
    val group = result.getType(result.getFieldIndex("v")).asGroupType()
    Assertions.assertEquals("value", group.getFields.get(0).getName)
    Assertions.assertEquals("metadata", group.getFields.get(1).getName)
  }
}
