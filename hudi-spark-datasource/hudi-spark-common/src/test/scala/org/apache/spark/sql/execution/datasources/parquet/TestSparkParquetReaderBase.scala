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
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.exception.HoodieException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData, ParquetMetadata}
import org.apache.parquet.schema.{MessageType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.execution.datasources.parquet.VariantParquetTestFixtures.shreddedVariant
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, LongType, MapType, StringType, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotSame, assertNull, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.{Arrays, HashMap}

import scala.collection.JavaConverters._

/**
 * Unit tests for the parquet read conf that [[SparkParquetReaderBase]] prepares once per scan, and for the per-file
 * work the readers now skip: the conf copy, the variant struct check and the oversized vectorized batch.
 */
class TestSparkParquetReaderBase {

  private val requiredSchema = new StructType().add("a", IntegerType).add("s", new StructType().add("b", StringType))

  @Test
  def testReadConfHasTheKeysOfThePerFileConf(): Unit = {
    val source = new Configuration(false)
    source.set("custom.key", "custom-value")
    val readConf = ParquetReadConf(source, requiredSchema)

    // The keys and values the read path set on a per-file copy before the read conf was prepared once per scan.
    val expected = new Configuration(source)
    expected.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requiredSchema.json)
    expected.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requiredSchema.json)
    expected.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, false)
    expected.setBoolean(SQLConf.CASE_SENSITIVE.key, false)
    expected.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, false)
    expected.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, true)
    expected.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, false)
    if (HoodieSparkUtils.gteqSpark3_4) {
      expected.setBooleanIfUnset("spark.sql.parquet.inferTimestampNTZ.enabled", true)
    }
    ParquetWriteSupport.setSchema(requiredSchema, expected)

    assertEquals(entries(expected), entries(readConf))
    assertEquals(Map("custom.key" -> "custom-value"), entries(source), "The source conf is not modified")
    assertSame(source, readConf.source)
    assertSame(requiredSchema, readConf.requiredSchema)
  }

  @Test
  def testTaskAttemptContextUsesTheReadConfWithoutACopy(): Unit = {
    val readConf = ParquetReadConf(new Configuration(false), requiredSchema)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    assertSame(readConf, new TaskAttemptContextImpl(readConf, attemptId).getConfiguration)
  }

  @Test
  def testAttemptConfIsTheSharedConfUnlessTheFileNeedsItsOwnKeys(): Unit = {
    val footer = footerOf(Types.buildMessage()
      .addField(Types.optional(PrimitiveTypeName.INT32).named("a"))
      .named("test"))
    val intSchema = new StructType().add("a", IntegerType)
    val sharedConf = ParquetReadConf(new Configuration(false), intSchema)

    assertSame(sharedConf, evolutionUtils(sharedConf, intSchema).getFileReadConf(footer, false, writable = false),
      "A file without keys of its own reads with the shared conf")

    val writableConf = evolutionUtils(sharedConf, intSchema).getFileReadConf(footer, false, writable = true)
    assertNotSame(sharedConf, writableConf)
    assertTrue(writableConf.isInstanceOf[JobConf])
    assertEquals(intSchema.json, writableConf.get(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA))
    writableConf.set("file.only.key", "value")
    assertNull(sharedConf.get("file.only.key"), "Keys set for one file stay off the shared conf")

    // The file stores an int that the query reads as a long: the file gets its own requested schema.
    val longSchema = new StructType().add("a", LongType)
    val sharedLongConf = ParquetReadConf(new Configuration(false), longSchema)
    val typeChangeConf = evolutionUtils(sharedLongConf, longSchema).getFileReadConf(footer, false, writable = false)
    assertNotSame(sharedLongConf, typeChangeConf)
    assertEquals(intSchema.json, StructType.fromString(typeChangeConf.get(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA)).json)
    assertEquals(longSchema.json, sharedLongConf.get(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA),
      "The shared conf keeps the scan's requested schema")
  }

  @Test
  def testVariantStructCheckIsSkippedOnlyWhenNoRequestedStructCanMatch(): Unit = {
    val variantStruct = new StructType().add("value", BinaryType).add("metadata", BinaryType)
    assertFalse(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(requiredSchema))
    assertFalse(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(new StructType()))
    assertTrue(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(new StructType().add("v", variantStruct)))
    assertTrue(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(
      new StructType().add("v", new StructType().add("value", BinaryType))), "A pruned variant struct still matches")
    assertTrue(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(
      new StructType().add("s", new StructType().add("a", IntegerType).add("v", variantStruct))))
    assertTrue(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(
      new StructType().add("l", ArrayType(variantStruct))))
    assertTrue(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(
      new StructType().add("m", MapType(StringType, variantStruct))))
    // The check walks map values only, and a struct with any other member is a user struct.
    assertFalse(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(
      new StructType().add("m", MapType(variantStruct, IntegerType))))
    assertFalse(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(
      new StructType().add("v", variantStruct.add("x", IntegerType))))

    // Consistent with the check itself: it fails on a shredded file only for a request the precheck matches.
    val shreddedFile = Types.buildMessage().addField(shreddedVariant("v")).named("test")
    val userStruct = new StructType().add("v", new StructType().add("value", BinaryType).add("x", IntegerType))
    assertFalse(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(userStruct))
    ParquetSchemaEvolutionUtils.validateNoShreddedVariantStructs(userStruct, shreddedFile)
    val variantRequest = new StructType().add("v", variantStruct)
    assertTrue(ParquetSchemaEvolutionUtils.containsUnshreddedVariantStruct(variantRequest))
    assertThrows(classOf[HoodieException], () =>
      ParquetSchemaEvolutionUtils.validateNoShreddedVariantStructs(variantRequest, shreddedFile))
  }

  @Test
  def testVectorizedBatchCapacityIsBoundedByTheRowsInTheFooter(): Unit = {
    assertEquals(30, SparkParquetReaderBase.vectorizedBatchCapacity(4096, footerWithRowGroups(10, 20)))
    assertEquals(16, SparkParquetReaderBase.vectorizedBatchCapacity(16, footerWithRowGroups(10, 20)))
    assertEquals(1, SparkParquetReaderBase.vectorizedBatchCapacity(4096, footerWithRowGroups(0)))
    assertEquals(4096, SparkParquetReaderBase.vectorizedBatchCapacity(4096, footerWithRowGroups()),
      "A footer read without row groups keeps the configured capacity")
    assertEquals(4096, SparkParquetReaderBase.vectorizedBatchCapacity(4096,
      footerWithRowGroups(Int.MaxValue.toLong, Int.MaxValue.toLong)))
  }

  private def evolutionUtils(sharedConf: Configuration, schema: StructType): ParquetSchemaEvolutionUtils =
    new ParquetSchemaEvolutionUtils(sharedConf, new Path("/tmp/file.parquet"), schema, new StructType(), HOption.empty())

  private def footerOf(schema: MessageType): FileMetaData =
    new FileMetaData(schema, new HashMap[String, String](), "test")

  private def footerWithRowGroups(rowCounts: Long*): ParquetMetadata = {
    val blocks = rowCounts.map { rows =>
      val block = new BlockMetaData()
      block.setRowCount(rows)
      block
    }
    new ParquetMetadata(footerOf(Types.buildMessage().addField(Types.optional(PrimitiveTypeName.INT32).named("a"))
      .named("test")), Arrays.asList(blocks: _*))
  }

  private def entries(conf: Configuration): Map[String, String] =
    conf.iterator().asScala.map(e => e.getKey -> e.getValue).toMap
}
