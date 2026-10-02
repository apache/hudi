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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertTrue}
import org.junit.jupiter.api.Test

/**
 * [[FileGroupOutputProjection]] must return UnsafeRows of the output schema for any row it is given,
 * and must not generate the full projection for UnsafeRows that only need the partition values
 * appended, since that generation is what makes a wide nested scan slow per file.
 */
class TestFileGroupOutputProjection {

  private val dataSchema = new StructType()
    .add("id", StringType)
    .add("ts", LongType)
    .add("nested", new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
      .add("c", ArrayType(IntegerType)))
  private val partitionSchema = new StructType().add("partition", StringType).add("bucket", IntegerType)
  private val outputSchema = StructType(dataSchema.fields ++ partitionSchema.fields)
  private val partitionValues = InternalRow(UTF8String.fromString("p1"), 7)

  /** Supplies the by-name projection from `from` to `to` and counts how many times it was generated. */
  private class CountingProjection(from: StructType, to: StructType) {
    var generated = 0

    def apply(): UnsafeProjection = {
      generated += 1
      UnsafeProjection.create(to.fields.toSeq.map(f => BoundReference(from.fieldIndex(f.name), f.dataType, f.nullable)))
    }
  }

  @Test
  def testAppendsPartitionValuesToUnsafeRowsWithoutTheProjection(): Unit = {
    val projection = new CountingProjection(StructType(dataSchema.fields ++ partitionSchema.fields), outputSchema)
    val toOutput = FileGroupOutputProjection.create(dataSchema, partitionSchema, partitionValues, outputSchema, projection())

    (0 until 5).foreach { i =>
      assertOutput(expected(i, withPartition = true), toOutput(unsafeDataRow(i)))
    }
    assertEquals(0, projection.generated, "UnsafeRows only need the partition values appended")
  }

  @Test
  def testPassesUnsafeRowsThroughWithoutPartitionValues(): Unit = {
    val projection = new CountingProjection(dataSchema, dataSchema)
    val toOutput = FileGroupOutputProjection.create(dataSchema, new StructType(), InternalRow.empty, dataSchema, projection())

    (0 until 5).foreach { i =>
      val row = unsafeDataRow(i)
      assertSame(row, toOutput(row))
    }
    assertEquals(0, projection.generated, "UnsafeRows of the output schema pass through")
  }

  @Test
  def testProjectsRowsThatAreNotUnsafeRows(): Unit = {
    val appending = new CountingProjection(StructType(dataSchema.fields ++ partitionSchema.fields), outputSchema)
    val appendToOutput = FileGroupOutputProjection.create(dataSchema, partitionSchema, partitionValues, outputSchema, appending())
    val identity = new CountingProjection(dataSchema, dataSchema)
    val identityToOutput = FileGroupOutputProjection.create(dataSchema, new StructType(), InternalRow.empty, dataSchema, identity())

    // A generic row, an UnsafeRow and a generic row again: each comes out right and the projection
    // is generated once, for the first row that needs it.
    Seq(dataRow(0), unsafeDataRow(1), dataRow(2)).zipWithIndex.foreach { case (row, i) =>
      assertOutput(expected(i, withPartition = true), appendToOutput(row))
      assertOutput(expected(i, withPartition = false), identityToOutput(row))
    }
    assertEquals(1, appending.generated)
    assertEquals(1, identity.generated)
  }

  @Test
  def testProjectsWhenTheOutputIsNotTheInputFollowedByThePartitionValues(): Unit = {
    // Same fields, but the output starts with the partition values.
    val reordered = StructType(partitionSchema.fields ++ dataSchema.fields)
    val projection = new CountingProjection(StructType(dataSchema.fields ++ partitionSchema.fields), reordered)
    val toOutput = FileGroupOutputProjection.create(dataSchema, partitionSchema, partitionValues, reordered, projection())

    (0 until 3).foreach { i =>
      val out = toOutput(unsafeDataRow(i))
      assertTrue(out.isInstanceOf[UnsafeRow])
      val e = expected(i, withPartition = true)
      assertEquals(Row.fromSeq(e.toSeq.takeRight(2) ++ e.toSeq.dropRight(2)), toScala(reordered, out))
    }
    assertEquals(1, projection.generated)
  }

  /** Row `i` of `dataSchema`; row 3 has a null nested struct and row 4 a null id. */
  private def dataRow(i: Int): InternalRow = {
    val nested = if (i == 3) null else InternalRow(i, UTF8String.fromString(s"b$i"), new GenericArrayData(Array[Any](i, i + 1)))
    InternalRow(if (i == 4) null else UTF8String.fromString(s"id$i"), i.toLong, nested)
  }

  private def unsafeDataRow(i: Int): UnsafeRow = UnsafeProjection.create(dataSchema)(dataRow(i)).copy()

  private def expected(i: Int, withPartition: Boolean): Row = {
    val nested = if (i == 3) null else Row(i, s"b$i", Seq(i, i + 1))
    val data = Seq(if (i == 4) null else s"id$i", i.toLong, nested)
    Row.fromSeq(if (withPartition) data ++ Seq("p1", 7) else data)
  }

  private def assertOutput(expected: Row, actual: InternalRow): Unit = {
    assertTrue(actual.isInstanceOf[UnsafeRow], s"Expected an UnsafeRow but got ${actual.getClass}")
    val schema = if (expected.length == outputSchema.length) outputSchema else dataSchema
    assertEquals(expected, toScala(schema, actual))
  }

  private def toScala(schema: StructType, row: InternalRow): Row =
    CatalystTypeConverters.createToScalaConverter(schema)(row).asInstanceOf[Row]
}
