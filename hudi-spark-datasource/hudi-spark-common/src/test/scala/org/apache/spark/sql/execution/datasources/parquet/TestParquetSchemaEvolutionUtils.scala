/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaField, HoodieSchemaType}
import org.apache.hudi.common.schema.internal.InternalSchema
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter
import org.apache.hudi.exception.HoodieException

import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.types.{BinaryType, MetadataBuilder, StructField, StructType}
import org.junit.jupiter.api.{Assertions, Test}

import java.util.{Arrays, Collections, HashMap}

/**
 * Unit tests for [[ParquetSchemaEvolutionUtils.validateNoShreddedVariants]], the schema-on-read
 * guard that fails a read the merged internal-schema request would otherwise serve with the
 * typed_value clipped away. No SparkSession: the guard is a pure schema walk.
 */
class TestParquetSchemaEvolutionUtils {

  /** Mirrors SparkInternalSchemaConverter.SPARK_VARIANT_METADATA_KEY, which is private there. */
  private val variantRewriteMarker = "__VARIANT_METADATA_KEY"

  // requiredSchema is read only by the PushVariantIntoScan rewrite arm, which keys on the marker
  // metadata above; the walk itself runs off querySchema. A plain marker-free schema therefore
  // keeps that arm silent, and VariantType is a Spark-4-only symbol this module cannot name.
  private val noVariantRewrite = new StructType().add("v", BinaryType)

  @Test
  def testValidateNoShreddedVariantsRejectsTopLevelShreddedVariant(): Unit = {
    val querySchema = querySchemaOf("v", HoodieSchema.createVariant())
    val failure = Assertions.assertThrows(classOf[HoodieException], () =>
      ParquetSchemaEvolutionUtils.validateNoShreddedVariants(
        noVariantRewrite, querySchema, footerOf(shreddedVariant("v"))))
    Assertions.assertTrue(
      failure.getMessage.contains("shredded variant") && failure.getMessage.contains("'v'"),
      s"The error must name the shredded variant column, got: ${failure.getMessage}")

    // The unshredded twin is exactly what the internal schema models, so it must read.
    ParquetSchemaEvolutionUtils.validateNoShreddedVariants(
      noVariantRewrite, querySchema, footerOf(unshreddedVariant("v")))
  }

  /**
   * An array of variants: the element is resolved through all three list layouts Hudi files can
   * carry - Spark's 3-level list, parquet-avro's 2-level list and parquet-thrift's tuple list.
   * A single-field repeated group is unwrapped everywhere except under the two reserved names,
   * so the 2-level shapes wrap the variant one struct deeper: only the name arms of
   * [[ParquetSchemaEvolutionUtils.parquetListElement]] then land on the right element.
   */
  @Test
  def testValidateNoShreddedVariantsRejectsShreddedVariantInList(): Unit = {
    // Pins the unwrapping arm: the repeated "list" group is not the element, its only field is.
    val threeLevel = Types.optionalGroup().as(LogicalTypeAnnotation.listType())
      .addField(Types.repeatedGroup().addField(shreddedVariant("element")).named("list"))
      .named("v")
    // Pins the "array" name arm: parquet-avro's repeated group is itself the element record.
    val twoLevel = Types.optionalGroup().as(LogicalTypeAnnotation.listType())
      .addField(nestedVariantElement("array"))
      .named("v")
    // Pins the "<field>_tuple" name arm: parquet-thrift's spelling of the same 2-level layout.
    val thriftTuple = Types.optionalGroup().as(LogicalTypeAnnotation.listType())
      .addField(nestedVariantElement("v_tuple"))
      .named("v")

    val variantElement = querySchemaOf("v", HoodieSchema.createArray(HoodieSchema.createVariant()))
    // array<struct<e: struct<inner: variant>>>, the query side of the two 2-level shapes: without
    // the name arms the walk would take "e" for the element and never pair "inner" up.
    val innerStruct = HoodieSchema.createRecord("e_record", "org.apache.hudi.test", null,
      Collections.singletonList(HoodieSchemaField.of("inner", HoodieSchema.createVariant())))
    val elementStruct = HoodieSchema.createRecord("element_record", "org.apache.hudi.test", null,
      Collections.singletonList(HoodieSchemaField.of("e", innerStruct)))
    val nestedElement = querySchemaOf("v", HoodieSchema.createArray(elementStruct))

    Seq(
      ("3-level", threeLevel, variantElement, "'v.element'"),
      ("2-level", twoLevel, nestedElement, "'v.element.e.inner'"),
      ("thrift", thriftTuple, nestedElement, "'v.element.e.inner'")
    ).foreach { case (shape, list, querySchema, path) =>
      val failure = Assertions.assertThrows(classOf[HoodieException], () =>
        ParquetSchemaEvolutionUtils.validateNoShreddedVariants(noVariantRewrite, querySchema, footerOf(list)))
      Assertions.assertTrue(failure.getMessage.contains(path),
        s"The $shape list error must name $path, got: ${failure.getMessage}")
    }
  }

  @Test
  def testValidateNoShreddedVariantsRejectsShreddedVariantInMap(): Unit = {
    val querySchema = querySchemaOf("v", HoodieSchema.createMap(HoodieSchema.createVariant()))
    val map = Types.optionalGroup().as(LogicalTypeAnnotation.mapType())
      .addField(Types.repeatedGroup()
        .addField(Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key"))
        .addField(shreddedVariant("value"))
        .named("key_value"))
      .named("v")

    val failure = Assertions.assertThrows(classOf[HoodieException], () =>
      ParquetSchemaEvolutionUtils.validateNoShreddedVariants(noVariantRewrite, querySchema, footerOf(map)))
    Assertions.assertTrue(failure.getMessage.contains("'v.value'"),
      s"The error must name the map value path, got: ${failure.getMessage}")
  }

  /**
   * The variant arm anchors on the sentinel negative field ids, so a user struct of the very same
   * shape over the very same file is left alone - both the two-field twin of the internal
   * variant record and the three-field twin of the shredded parquet group.
   */
  @Test
  def testValidateNoShreddedVariantsLeavesPlainUserStructsAlone(): Unit = {
    val bytes = HoodieSchema.create(HoodieSchemaType.BYTES)
    val typedValue = HoodieSchema.createRecord("plain_typed_value", "org.apache.hudi.test", null,
      Collections.singletonList(HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.INT))))
    val threeFieldStruct = HoodieSchema.createRecord("plain_three", "org.apache.hudi.test", null, Arrays.asList(
      HoodieSchemaField.of("metadata", bytes),
      HoodieSchemaField.of("value", bytes),
      HoodieSchemaField.of("typed_value", typedValue)))
    val twoFieldStruct = HoodieSchema.createRecord("plain_two", "org.apache.hudi.test", null, Arrays.asList(
      HoodieSchemaField.of("metadata", bytes),
      HoodieSchemaField.of("value", bytes)))

    Seq(threeFieldStruct, twoFieldStruct).foreach { struct =>
      ParquetSchemaEvolutionUtils.validateNoShreddedVariants(
        noVariantRewrite, querySchemaOf("v", struct), footerOf(shreddedVariant("v")))
    }
  }

  /** A column added after the file was written has no footer field to walk. */
  @Test
  def testValidateNoShreddedVariantsSkipsColumnsAbsentFromTheFooter(): Unit = {
    ParquetSchemaEvolutionUtils.validateNoShreddedVariants(
      noVariantRewrite, querySchemaOf("added", HoodieSchema.createVariant()), footerOf(shreddedVariant("v")))
  }

  @Test
  def testValidateNoShreddedVariantsRejectsNestedShreddedVariant(): Unit = {
    val struct = HoodieSchema.createRecord("nested", "org.apache.hudi.test", null,
      Collections.singletonList(HoodieSchemaField.of("inner", HoodieSchema.createVariant())))
    val footer = footerOf(Types.optionalGroup().addField(shreddedVariant("inner")).named("s"))

    val failure = Assertions.assertThrows(classOf[HoodieException], () =>
      ParquetSchemaEvolutionUtils.validateNoShreddedVariants(noVariantRewrite, querySchemaOf("s", struct), footer))
    Assertions.assertTrue(failure.getMessage.contains("'s.inner'"),
      s"The error must name the nested variant path, got: ${failure.getMessage}")
  }

  /**
   * A scan rewritten by PushVariantIntoScan fails regardless of the file's layout: the merged
   * request materializes {metadata, value} while codegen expects the extraction struct.
   */
  @Test
  def testValidateNoShreddedVariantsRejectsVariantRewriteOverUnshreddedFile(): Unit = {
    val marker = new MetadataBuilder().putString(variantRewriteMarker, "v").build()
    val rewritten = new StructType().add("v", new StructType()
      .add(StructField("0", BinaryType, nullable = true, marker))
      .add(StructField("1", BinaryType, nullable = true, marker)))

    val failure = Assertions.assertThrows(classOf[HoodieException], () =>
      ParquetSchemaEvolutionUtils.validateNoShreddedVariants(rewritten,
        querySchemaOf("v", HoodieSchema.createVariant()), footerOf(unshreddedVariant("v"))))
    Assertions.assertTrue(failure.getMessage.contains("pushVariantIntoScan") && failure.getMessage.contains("'v'"),
      s"The error must name the rewrite and the column, got: ${failure.getMessage}")
  }

  /** The query-side internal schema for a table of one top-level column. */
  private def querySchemaOf(name: String, schema: HoodieSchema): InternalSchema =
    InternalSchemaConverter.convert(HoodieSchema.createRecord("query", "org.apache.hudi.test", null,
      Collections.singletonList(HoodieSchemaField.of(name, schema))))

  /** A footer over a file of one top-level column. */
  private def footerOf(column: Type): FileMetaData =
    new FileMetaData(Types.buildMessage().addField(column).named("test"), new HashMap[String, String](), "test")

  /**
   * A 2-level list element as parquet-avro and parquet-thrift write it: the repeated group is
   * itself the element record - named "array" or "<field>_tuple" - and holds a single struct
   * field "e" wrapping the shredded variant "inner".
   */
  private def nestedVariantElement(name: String): Type =
    Types.repeatedGroup()
      .addField(Types.optionalGroup().addField(shreddedVariant("inner")).named("e"))
      .named(name)

  /** The shredded parquet layout: {metadata, value, typed_value}. */
  private def shreddedVariant(name: String): Type =
    Types.optionalGroup()
      .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
      .addField(Types.optional(PrimitiveTypeName.BINARY).named("value"))
      .addField(Types.optionalGroup()
        .addField(Types.optional(PrimitiveTypeName.INT32).named("a")).named("typed_value"))
      .named(name)

  /** The unshredded twin: {metadata, value}. */
  private def unshreddedVariant(name: String): Type =
    Types.optionalGroup()
      .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
      .addField(Types.required(PrimitiveTypeName.BINARY).named("value"))
      .named(name)
}
