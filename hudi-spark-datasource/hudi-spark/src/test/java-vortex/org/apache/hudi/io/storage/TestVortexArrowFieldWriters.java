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

package org.apache.hudi.io.storage;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.execution.arrow.ArrowWriter$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils$;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@link VortexArrowFieldWriters} writes the same values as the Spark Arrow writer.
 */
public class TestVortexArrowFieldWriters {

  private static final StructType SCHEMA = new StructType()
      .add("s", DataTypes.StringType)
      .add("small", DataTypes.createDecimalType(10, 2))
      .add("wide", DataTypes.createDecimalType(38, 10));

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator();
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testStringsAndDecimals() {
    byte[] offHeapBytes = "off heap ż".getBytes(StandardCharsets.UTF_8);
    BigDecimal beyondLong = new BigDecimal(BigInteger.TEN.pow(25).negate(), 10);
    BigDecimal largestLong = new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE), 10);
    BigDecimal smallestLong = new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE), 10);

    Schema arrowSchema = ArrowUtils$.MODULE$.toArrowSchema(SCHEMA, "UTC", true, false);
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
         ArrowBuf offHeap = allocator.buffer(offHeapBytes.length)) {
      offHeap.setBytes(0, offHeapBytes);
      // A string in the middle of a larger array, as in an UnsafeRow.
      byte[] padded = "xxhello ąę".getBytes(StandardCharsets.UTF_8);
      UTF8String inArray = UTF8String.fromBytes(padded, 2, padded.length - 2);
      UTF8String outsideArray = UTF8String.fromAddress(null, offHeap.memoryAddress(), offHeapBytes.length);

      ArrowWriter writer = VortexArrowFieldWriters.create(root);
      writer.write(row(inArray, dec("-12.34", 10, 2), Decimal.apply(beyondLong, 38, 10)));
      writer.write(row(outsideArray, dec("99999999.99", 10, 2), Decimal.apply(largestLong, 38, 10)));
      writer.write(row(UTF8String.EMPTY_UTF8, null, Decimal.apply(smallestLong, 38, 10)));
      writer.write(row(null, dec("0", 10, 2), null));
      // The value does not fit the precision of the column, thus it is null, as in Spark.
      writer.write(row(UTF8String.fromString("z"), Decimal.apply(new BigDecimal("123456789.5")), dec("1.5", 38, 10)));
      writer.finish();

      VarCharVector strings = (VarCharVector) root.getVector("s");
      assertEquals("hello ąę", new String(strings.get(0), StandardCharsets.UTF_8));
      assertEquals("off heap ż", new String(strings.get(1), StandardCharsets.UTF_8));
      assertEquals("", new String(strings.get(2), StandardCharsets.UTF_8));
      assertTrue(strings.isNull(3));
      assertEquals("z", new String(strings.get(4), StandardCharsets.UTF_8));

      DecimalVector small = (DecimalVector) root.getVector("small");
      assertEquals(new BigDecimal("-12.34"), small.getObject(0));
      assertEquals(new BigDecimal("99999999.99"), small.getObject(1));
      assertTrue(small.isNull(2));
      assertEquals(new BigDecimal("0.00"), small.getObject(3));
      assertTrue(small.isNull(4));

      DecimalVector wide = (DecimalVector) root.getVector("wide");
      assertEquals(beyondLong, wide.getObject(0));
      assertEquals(largestLong, wide.getObject(1));
      assertEquals(smallestLong, wide.getObject(2));
      assertNull(wide.getObject(3));
      assertEquals(new BigDecimal("1.5000000000"), wide.getObject(4));
    }
  }

  @Test
  public void testNestedValuesMatchSpark() {
    StructType inner = new StructType()
        .add("name", DataTypes.StringType)
        .add("amount", DataTypes.createDecimalType(38, 10));
    StructType schema = new StructType()
        .add("st", inner)
        .add("arr", DataTypes.createArrayType(DataTypes.StringType))
        .add("m", DataTypes.createMapType(DataTypes.StringType, DataTypes.createDecimalType(12, 3)))
        .add("structs", DataTypes.createArrayType(inner));

    InternalRow[] rows = new InternalRow[50];
    for (int i = 0; i < rows.length; i++) {
      UTF8String name = UTF8String.fromString("name-" + i);
      Decimal amount = i % 7 == 0 ? null : dec(i + ".5", 38, 10);
      InternalRow struct = i % 11 == 0 ? null : row(name, amount);
      GenericArrayData array = new GenericArrayData(new Object[] {name, null, UTF8String.fromString("ż" + i)});
      ArrayBasedMapData map = new ArrayBasedMapData(
          new GenericArrayData(new Object[] {UTF8String.fromString("k" + i)}),
          new GenericArrayData(new Object[] {i % 3 == 0 ? null : dec("-" + i + ".125", 12, 3)}));
      GenericArrayData structs = new GenericArrayData(new Object[] {row(name, dec("1", 38, 10)), null});
      rows[i] = row(struct, i % 13 == 0 ? null : array, map, structs);
    }

    assertEquals(
        write(schema, rows, ArrowWriter$.MODULE$::create),
        write(schema, rows, VortexArrowFieldWriters::create));
  }

  private String write(StructType schema, InternalRow[] rows, Function<VectorSchemaRoot, ArrowWriter> factory) {
    Schema arrowSchema = ArrowUtils$.MODULE$.toArrowSchema(schema, "UTC", true, false);
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      ArrowWriter writer = factory.apply(root);
      for (InternalRow row : rows) {
        writer.write(row);
      }
      writer.finish();
      return root.contentToTSVString();
    }
  }

  private static Decimal dec(String value, int precision, int scale) {
    return Decimal.apply(new BigDecimal(value), precision, scale);
  }

  private static GenericInternalRow row(Object... values) {
    return new GenericInternalRow(values);
  }
}
