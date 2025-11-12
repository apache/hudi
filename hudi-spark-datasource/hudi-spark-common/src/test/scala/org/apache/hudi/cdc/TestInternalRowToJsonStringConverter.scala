/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.cdc

import org.apache.hudi.HoodieTableSchema
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class TestInternalRowToJsonStringConverter {
  private val converter = new InternalRowToJsonStringConverter(hoodieTableSchema.structTypeSchema)

  @Test
  def emptyRow(): Unit = {
    val converter = new InternalRowToJsonStringConverter(emptyHoodieTableSchema.structTypeSchema)
    val row = InternalRow.empty
    val converted = converter.convert(row)
    assertEquals("{}", converted.toString)
  }

  @Test
  def nonEmptyRow(): Unit = {
    val row = InternalRow.fromSeq(Seq(1, UTF8String.fromString("foo")))
    val converted = converter.convert(row)
    assertEquals("""{"name":"foo","uuid":1}""", converted.toString)
  }

  @Test
  def emptyString(): Unit = {
    val row = InternalRow.fromSeq(Seq(1, UTF8String.EMPTY_UTF8))
    val converted = converter.convert(row)
    assertEquals("""{"name":"","uuid":1}""", converted.toString)
  }

  @Test
  def nullString(): Unit = {
    val row = InternalRow.fromSeq(Seq(1, null))
    val converted = converter.convert(row)
    assertTrue(converted.toString.equals("""{"uuid":1}""") || converted.toString.equals("""{"name":null,"uuid":1}"""))
  }

  private def hoodieTableSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType(Array[StructField](
      StructField("uuid", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty)))
    val avroSchemaStr: String =
      """{"type": "record", "name": "test", "fields": [
        |{"name": "uuid", "type": "int"},
        |{"name": "name", "type": "string"}
        |]}""".stripMargin
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def emptyHoodieTableSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType()
    val avroSchemaStr = """{"type": "record", "name": "test", "fields": []}"""
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }
}
