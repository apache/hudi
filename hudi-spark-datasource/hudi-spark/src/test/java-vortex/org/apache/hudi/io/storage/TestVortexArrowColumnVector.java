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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the views that {@link VortexArrowColumnVector} gives of Arrow vectors.
 */
public class TestVortexArrowColumnVector {

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
  public void testSmallIntReadsAsIntAndLong() {
    try (SmallIntVector vector = new SmallIntVector("c", allocator)) {
      vector.allocateNew(3);
      vector.set(0, Short.MIN_VALUE);
      vector.setNull(1);
      vector.set(2, Short.MAX_VALUE);
      vector.setValueCount(3);

      VortexArrowColumnVector column = new VortexArrowColumnVector(vector);
      VortexArrowColumnVector asInt = column.conformTo(DataTypes.IntegerType);
      assertEquals(DataTypes.IntegerType, asInt.dataType());
      assertEquals(Short.MIN_VALUE, asInt.getInt(0));
      assertTrue(asInt.isNullAt(1));
      assertEquals(Short.MAX_VALUE, asInt.getInt(2));

      VortexArrowColumnVector asLong = column.conformTo(DataTypes.LongType);
      assertEquals(Short.MIN_VALUE, asLong.getLong(0));
      assertEquals(Short.MAX_VALUE, asLong.getLong(2));
    }
  }

  @Test
  public void testTinyIntReadsAsWiderIntegers() {
    try (TinyIntVector vector = new TinyIntVector("c", allocator)) {
      vector.allocateNew(2);
      vector.set(0, Byte.MIN_VALUE);
      vector.set(1, Byte.MAX_VALUE);
      vector.setValueCount(2);

      VortexArrowColumnVector column = new VortexArrowColumnVector(vector);
      assertEquals(Byte.MIN_VALUE, column.conformTo(DataTypes.ShortType).getShort(0));
      assertEquals(Byte.MIN_VALUE, column.conformTo(DataTypes.IntegerType).getInt(0));
      assertEquals(Byte.MAX_VALUE, column.conformTo(DataTypes.LongType).getLong(1));
    }
  }

  @Test
  public void testIntReadsAsLongAndDouble() {
    try (IntVector vector = new IntVector("c", allocator)) {
      vector.allocateNew(2);
      vector.set(0, Integer.MIN_VALUE);
      vector.set(1, Integer.MAX_VALUE);
      vector.setValueCount(2);

      VortexArrowColumnVector column = new VortexArrowColumnVector(vector);
      assertSame(column, column.conformTo(DataTypes.IntegerType));
      assertEquals(Integer.MIN_VALUE, column.conformTo(DataTypes.LongType).getLong(0));
      assertEquals(Integer.MAX_VALUE, column.conformTo(DataTypes.DoubleType).getDouble(1));
      assertThrows(UnsupportedOperationException.class, () -> column.conformTo(DataTypes.ShortType));
    }
  }

  @Test
  public void testFloatReadsAsDouble() {
    try (Float4Vector vector = new Float4Vector("c", allocator)) {
      vector.allocateNew(1);
      vector.set(0, 1.25f);
      vector.setValueCount(1);

      VortexArrowColumnVector column = new VortexArrowColumnVector(vector);
      assertEquals(1.25d, column.conformTo(DataTypes.DoubleType).getDouble(0));
    }
  }

  @Test
  public void testCanConformTo() {
    assertTrue(VortexArrowColumnVector.canConformTo(DataTypes.ShortType, DataTypes.IntegerType));
    assertTrue(VortexArrowColumnVector.canConformTo(DataTypes.IntegerType, DataTypes.LongType));
    assertTrue(VortexArrowColumnVector.canConformTo(DataTypes.FloatType, DataTypes.DoubleType));
    assertFalse(VortexArrowColumnVector.canConformTo(DataTypes.LongType, DataTypes.IntegerType));
    assertFalse(VortexArrowColumnVector.canConformTo(DataTypes.StringType, DataTypes.IntegerType));
    assertFalse(VortexArrowColumnVector.canConformTo(DataTypes.LongType, DataTypes.DoubleType));

    // The fields of a struct are found by name. A field that the file does not have is null.
    DataType fileStruct = new StructType().add("a", DataTypes.ShortType).add("b", DataTypes.StringType);
    DataType requested = new StructType().add("b", DataTypes.StringType).add("a", DataTypes.IntegerType)
        .add("new", DataTypes.LongType);
    assertTrue(VortexArrowColumnVector.canConformTo(fileStruct, requested));
    assertFalse(VortexArrowColumnVector.canConformTo(fileStruct,
        new StructType().add("b", DataTypes.IntegerType)));

    assertTrue(VortexArrowColumnVector.canConformTo(
        DataTypes.createArrayType(DataTypes.ShortType), DataTypes.createArrayType(DataTypes.LongType)));
    assertTrue(VortexArrowColumnVector.canConformTo(
        DataTypes.createMapType(DataTypes.StringType, DataTypes.FloatType),
        DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType)));
  }

  @Test
  public void testStructFieldsReadByNameWithWidening() {
    try (StructVector vector = StructVector.empty("c", allocator)) {
      SmallIntVector a = vector.addOrGet("a", FieldType.nullable(MinorType.SMALLINT.getType()), SmallIntVector.class);
      VarCharVector b = vector.addOrGet("b", FieldType.nullable(MinorType.VARCHAR.getType()), VarCharVector.class);
      vector.allocateNew();
      a.setSafe(0, 7);
      b.setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
      vector.setIndexDefined(0);
      a.setNull(1);
      b.setSafe(1, "y".getBytes(StandardCharsets.UTF_8));
      vector.setIndexDefined(1);
      a.setValueCount(2);
      b.setValueCount(2);
      vector.setValueCount(2);

      StructType requested = new StructType()
          .add("b", DataTypes.StringType)
          .add("a", DataTypes.LongType)
          .add("new", DataTypes.IntegerType);
      VortexArrowColumnVector column = new VortexArrowColumnVector(vector);
      assertTrue(VortexArrowColumnVector.canConformTo(column.dataType(), requested));

      VortexArrowColumnVector view = column.conformTo(requested);
      assertEquals(requested, view.dataType());
      assertEquals("x", view.getChild(0).getUTF8String(0).toString());
      assertEquals("y", view.getChild(0).getUTF8String(1).toString());
      assertEquals(7L, view.getChild(1).getLong(0));
      assertTrue(view.getChild(1).isNullAt(1));
      assertTrue(view.getChild(2).isNullAt(0));
      assertTrue(view.getChild(2).isNullAt(1));
    }
  }

  @Test
  public void testArrayElementsReadAsWiderType() {
    try (ListVector vector = ListVector.empty("c", allocator)) {
      vector.addOrGetVector(FieldType.nullable(MinorType.SMALLINT.getType()));
      SmallIntVector elements = (SmallIntVector) vector.getDataVector();
      vector.allocateNew();

      vector.startNewValue(0);
      elements.setSafe(0, Short.MIN_VALUE);
      elements.setNull(1);
      vector.endValue(0, 2);

      vector.startNewValue(1);
      elements.setSafe(2, Short.MAX_VALUE);
      vector.endValue(1, 1);

      elements.setValueCount(3);
      vector.setValueCount(2);

      DataType requested = DataTypes.createArrayType(DataTypes.LongType);
      VortexArrowColumnVector column = new VortexArrowColumnVector(vector);
      assertTrue(VortexArrowColumnVector.canConformTo(column.dataType(), requested));

      VortexArrowColumnVector view = column.conformTo(requested);
      ColumnarArray first = view.getArray(0);
      assertEquals(2, first.numElements());
      assertEquals(Short.MIN_VALUE, first.getLong(0));
      assertTrue(first.isNullAt(1));

      ColumnarArray second = view.getArray(1);
      assertEquals(1, second.numElements());
      assertEquals(Short.MAX_VALUE, second.getLong(0));
    }
  }

  @Test
  public void testMapEntriesReadAsWiderTypes() {
    try (MapVector vector = MapVector.empty("c", allocator, false)) {
      UnionMapWriter writer = vector.getWriter();
      writer.setPosition(0);
      writer.startMap();
      writer.startEntry();
      writer.key().integer().writeInt(1);
      writer.value().float4().writeFloat4(1.5f);
      writer.endEntry();
      writer.startEntry();
      writer.key().integer().writeInt(2);
      writer.value().float4().writeFloat4(-2.25f);
      writer.endEntry();
      writer.endMap();
      writer.setValueCount(1);

      VortexArrowColumnVector column = new VortexArrowColumnVector(vector);
      DataType requested = DataTypes.createMapType(DataTypes.LongType, DataTypes.DoubleType,
          ((MapType) column.dataType()).valueContainsNull());
      assertTrue(VortexArrowColumnVector.canConformTo(column.dataType(), requested));

      ColumnarMap map = column.conformTo(requested).getMap(0);
      assertEquals(2, map.numElements());
      assertEquals(1L, map.keyArray().getLong(0));
      assertEquals(2L, map.keyArray().getLong(1));
      assertEquals(1.5d, map.valueArray().getDouble(0));
      assertEquals(-2.25d, map.valueArray().getDouble(1));
    }
  }

  @Test
  public void testWideDecimalValues() {
    BigDecimal small = new BigDecimal("-123456.7890123456");
    BigDecimal largest = new BigDecimal("99999999.9999999999");
    BigDecimal beyondLong = new BigDecimal(BigInteger.TEN.pow(20).negate(), 10);
    BigDecimal wide = new BigDecimal(BigInteger.TEN.pow(37).subtract(BigInteger.ONE), 10);

    try (DecimalVector vector = new DecimalVector("c", allocator, 38, 10)) {
      vector.allocateNew(5);
      vector.set(0, small);
      vector.set(1, largest);
      vector.set(2, beyondLong);
      vector.set(3, wide);
      vector.setNull(4);
      vector.setValueCount(5);

      VortexArrowColumnVector column = new VortexArrowColumnVector(vector);
      assertDecimal(small, column.getDecimal(0, 38, 10));
      assertDecimal(largest, column.getDecimal(1, 38, 10));
      assertDecimal(beyondLong, column.getDecimal(2, 38, 10));
      assertDecimal(wide, column.getDecimal(3, 38, 10));
      assertNull(column.getDecimal(4, 38, 10));
    }
  }

  private static void assertDecimal(BigDecimal expected, Decimal actual) {
    assertEquals(Decimal.apply(expected, 38, 10), actual);
    assertEquals(0, expected.compareTo(actual.toJavaBigDecimal()));
    assertEquals(10, actual.scale());
    assertEquals(38, actual.precision());
  }
}
