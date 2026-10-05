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

import org.apache.hudi.HoodieSparkUtils

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, MessageTypeParser, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.execution.datasources.SparkSchemaTransformUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, MetadataBuilder, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotSame, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
 * The reconciliation of a parquet file schema with the requested schema is computed once per file schema, requested
 * schema and converter conf, and returns what the uncached computation returns.
 */
class TestHoodieParquetFileFormatHelper {

  private val fileSchemaText =
    """message spark_schema {
      |  optional int32 id;
      |  optional binary name (STRING);
      |  optional binary raw;
      |  optional group s {
      |    optional int32 a;
      |    optional binary b (STRING);
      |  }
      |  optional group arr (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |  optional group m (MAP) {
      |    repeated group key_value {
      |      required binary key (STRING);
      |      optional int32 value;
      |    }
      |  }
      |}""".stripMargin

  /** A new footer object per call, as every file's footer is. */
  private def footer(): FileMetaData =
    new FileMetaData(MessageTypeParser.parseMessageType(fileSchemaText), new JHashMap[String, String](), "test")

  @Test
  def testFilesWithTheSameSchemaShareOneReconciliation(): Unit = {
    val conf = converterConf()
    val requiredSchema = StructType.fromDDL("id long, name string, s struct<a: long, b: string>")
    val first = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(conf, footer(), requiredSchema)
    val second = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(conf, footer(),
      StructType.fromDDL("id long, name string, s struct<a: long, b: string>"))

    assertSame(first._1, second._1, "The second file reuses the first file's type changes")
    assertSame(first._2, second._2, "The second file reuses the first file's read schema")
    assertEquals(Set(0, 2), first._1.keySet().asScala.map(_.intValue()).toSet)
    assertThrows(classOf[UnsupportedOperationException], () => first._1.clear())
  }

  @Test
  def testCachedReconciliationMatchesTheUncachedOne(): Unit = {
    val requiredSchemas = Seq(
      "id int, name string",
      "id long, name string",
      "s struct<a: int, b: string>",
      "s struct<a: long, b: string, c: int>",
      "arr array<int>",
      "arr array<long>",
      "m map<string, int>",
      "m map<string, long>",
      "raw binary",
      "raw string",
      "missing int, id int",
      "ID int, Name string")
      .map(StructType.fromDDL) ++ Seq(
      // Nullability and metadata differences are not type changes.
      new StructType(Array(StructField("id", IntegerType, nullable = false),
        StructField("s", new StructType().add("a", IntegerType, nullable = false).add("b", StringType)))),
      new StructType().add("name", StringType, nullable = true,
        new MetadataBuilder().putString("comment", "the name").build()),
      new StructType().add("arr", ArrayType(IntegerType, containsNull = false))
        .add("m", MapType(StringType, IntegerType, valueContainsNull = false)))

    for (binaryAsString <- Seq(false, true); requiredSchema <- requiredSchemas) {
      val conf = converterConf(binaryAsString)
      val expected = SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(
        HoodieParquetFileFormatHelper.convertParquetSchemaToSparkSchema(conf, footer().getSchema), requiredSchema)
      // Twice: the first call fills the cache, the second reads it.
      for (_ <- 0 until 2) {
        val actual = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(conf, footer(), requiredSchema)
        assertEquals(expected._1, actual._1, s"Type changes for $requiredSchema, binaryAsString=$binaryAsString")
        assertEquals(expected._2, actual._2, s"Read schema for $requiredSchema, binaryAsString=$binaryAsString")
      }
    }
  }

  @Test
  def testConverterConfIsPartOfTheCacheKey(): Unit = {
    val requiredSchema = StructType.fromDDL("raw string")
    val asBinary = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(
      converterConf(binaryAsString = false), footer(), requiredSchema)
    val asString = HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(
      converterConf(binaryAsString = true), footer(), requiredSchema)
    assertTrue(asBinary._1.containsKey(0), "An unannotated binary read as a string is a type change")
    assertTrue(asString._1.isEmpty, "With binaryAsString the file column is already a string")
    assertNotSame(asBinary._2, asString._2)
  }

  /**
   * Every Hadoop conf key the running Spark version's schema converter reads is part of the cache key, so a conf change
   * can never be served a reconciliation computed under other values. Only Hadoop conf reads are recorded here; the
   * session conf the converter reads through `SQLConf.get` is covered by
   * [[testSessionConfOfTheConverterIsPartOfTheCacheKey]].
   */
  @Test
  def testCacheKeyCoversEveryConfKeyTheConverterReads(): Unit = {
    val readKeys = mutable.LinkedHashSet[String]()
    var recording = false
    val conf = new Configuration(false) {
      override def get(name: String): String = {
        if (recording) {
          readKeys.synchronized(readKeys += name)
        }
        super.get(name)
      }
    }
    converterConf().iterator().asScala.foreach(entry => conf.set(entry.getKey, entry.getValue))
    recording = true
    new ParquetToSparkSchemaConverter(conf).convert(footer().getSchema)
    recording = false

    assertTrue(readKeys.nonEmpty, "The converter reads its settings from the conf")
    val uncovered = readKeys.toSet -- HoodieParquetFileFormatHelper.SCHEMA_CONVERTER_CONF_KEYS
    assertTrue(uncovered.isEmpty, s"Conf keys the converter reads but the cache key misses: $uncovered")
  }

  /**
   * Spark 4.1+ converts a variant group depending on the session conf `spark.sql.variant.allowReadingShredded`. The
   * cached answer under each value equals the uncached one, whichever value was cached first.
   */
  @Test
  def testSessionConfOfTheConverterIsPartOfTheCacheKey(): Unit = {
    assumeTrue(HoodieSparkUtils.gteqSpark4_1, "Variant groups are converted by Spark 4.1+ only")
    // Built by reflection: the variant annotation is not in the parquet version of Spark 3.x builds.
    val variantAnnotation = classOf[LogicalTypeAnnotation].getMethod("variantType", java.lang.Byte.TYPE)
      .invoke(null, java.lang.Byte.valueOf(1.toByte)).asInstanceOf[LogicalTypeAnnotation]
    val fileSchema = Types.buildMessage()
      .addField(Types.optional(PrimitiveTypeName.INT32).named("id"))
      .addField(Types.optionalGroup().as(variantAnnotation)
        .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
        .addField(Types.optional(PrimitiveTypeName.BINARY).named("value"))
        .addField(Types.optional(PrimitiveTypeName.INT32).named("typed_value"))
        .named("v"))
      .named("spark_schema")
    def variantFooter(): FileMetaData = new FileMetaData(fileSchema, new JHashMap[String, String](), "test")
    val requiredSchema = StructType.fromDDL("id long")
    val conf = converterConf()
    val sessionKey = "spark.sql.variant.allowReadingShredded"

    def outcome(read: => HoodieParquetFileFormatHelper.ImplicitSchemaChange): Either[Class[_], (Map[Int, String], StructType)] =
      Try(read) match {
        case Success((changes, readSchema)) =>
          Right((changes.asScala.map { case (index, pair) => index.intValue() -> pair.toString }.toMap, readSchema))
        case Failure(error) => Left(error.getClass)
      }

    val sessionConf = SQLConf.get
    try {
      for (allowShredded <- Seq("true", "false", "true")) {
        sessionConf.setConfString(sessionKey, allowShredded)
        val uncached = outcome(SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(
          HoodieParquetFileFormatHelper.convertParquetSchemaToSparkSchema(conf, fileSchema), requiredSchema))
        val cached = outcome(HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo(conf, variantFooter(), requiredSchema))
        assertEquals(uncached, cached, s"$sessionKey=$allowShredded")
      }
    } finally {
      sessionConf.unsetConf(sessionKey)
    }
  }

  /** An entry weighs its file columns plus its requested leaf fields, and at least the minimum weight. */
  @Test
  def testEntryWeightCountsFileColumnsAndRequestedFields(): Unit = {
    val narrowFile = MessageTypeParser.parseMessageType("message m { optional int32 c0; optional int32 c1; }")
    val wideRequest = StructType((0 until 500).map(i => StructField(s"c$i", IntegerType)))
    assertEquals(502, HoodieParquetFileFormatHelper.entryWeight(narrowFile, wideRequest))
    assertEquals(HoodieParquetFileFormatHelper.MIN_ENTRY_WEIGHT,
      HoodieParquetFileFormatHelper.entryWeight(narrowFile, StructType.fromDDL("c0 int")))
    // Nested leaves count: s.a, s.b, the array element, the map key and the map value.
    val nestedRequest = StructType(wideRequest.fields.take(100))
      .add("s", StructType.fromDDL("a int, b string"))
      .add("arr", ArrayType(IntegerType))
      .add("m", MapType(StringType, IntegerType))
    assertEquals(2 + 105, HoodieParquetFileFormatHelper.entryWeight(narrowFile, nestedRequest))
  }

  /** The converter keys as the parquet readers set them (see SparkParquetReaderBase.read). */
  private def converterConf(binaryAsString: Boolean = false): Configuration = {
    val conf = new Configuration(false)
    conf.setBoolean("spark.sql.caseSensitive", false)
    conf.setBoolean("spark.sql.parquet.binaryAsString", binaryAsString)
    conf.setBoolean("spark.sql.parquet.int96AsTimestamp", true)
    conf.setBoolean("spark.sql.legacy.parquet.nanosAsLong", false)
    if (HoodieSparkUtils.gteqSpark3_4) {
      conf.setBoolean("spark.sql.parquet.inferTimestampNTZ.enabled", true)
    }
    conf
  }
}
