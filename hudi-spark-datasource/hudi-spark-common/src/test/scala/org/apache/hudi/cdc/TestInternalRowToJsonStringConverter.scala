package org.apache.hudi.cdc

import org.apache.hudi.HoodieTableSchema
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestInternalRowToJsonStringConverter {
  private val converter = new InternalRowToJsonStringConverter(hoodieTableSchema)

  @Test
  def emptyRow(): Unit = {
    val converter = new InternalRowToJsonStringConverter(emptyHoodieTableSchema)
    val row = InternalRow.empty
    val converted = converter.convertRowToJsonString(row)
    assertEquals("{}", converted.toString)
  }

  @Test
  def nonEmptyRow(): Unit = {
    val row = InternalRow.fromSeq(Seq(1, UTF8String.fromString("foo")))
    val converted = converter.convertRowToJsonString(row)
    assertEquals("{\"name\":\"foo\",\"uuid\":1}", converted.toString)
  }

  @Test
  def emptyString(): Unit = {
    val row = InternalRow.fromSeq(Seq(1, UTF8String.EMPTY_UTF8))
    val converted = converter.convertRowToJsonString(row)
    assertEquals("{\"name\":\"\",\"uuid\":1}", converted.toString)
  }

  @Test
  def nullString(): Unit = {
    val row = InternalRow.fromSeq(Seq(1, null))
    val converted = converter.convertRowToJsonString(row)
    assertEquals("{\"uuid\":1}", converted.toString)
  }

  private def hoodieTableSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType(Array[StructField](
      StructField("uuid", DataTypes.IntegerType, nullable = false, Metadata.empty),
      StructField("name", DataTypes.StringType, nullable = true, Metadata.empty)))
    val avroSchemaStr: String = "{\"type\": \"record\",\"name\": \"test\",\"fields\": [{\"name\": \"uuid\",\"type\": \"int\"},{\"name\": \"name\",\"type\": \"string\"}]}"
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }

  private def emptyHoodieTableSchema: HoodieTableSchema = {
    val structTypeSchema = new StructType()
    val avroSchemaStr = "{\"type\": \"record\",\"name\": \"test\",\"fields\": []}"
    HoodieTableSchema(structTypeSchema, avroSchemaStr, Option.empty[InternalSchema])
  }
}
