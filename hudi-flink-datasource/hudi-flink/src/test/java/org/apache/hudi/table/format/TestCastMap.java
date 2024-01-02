/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link CastMap}.
 */
public class TestCastMap {
  @Test
  public void testCastInt() {
    CastMap castMap = new CastMap();
    castMap.add(0, new IntType(), new BigIntType());
    castMap.add(1, new IntType(), new FloatType());
    castMap.add(2, new IntType(), new DoubleType());
    castMap.add(3, new IntType(), new DecimalType());
    castMap.add(4, new IntType(), new VarCharType());
    int val = 1;
    assertEquals(1L, castMap.castIfNeeded(0, val));
    assertEquals(1.0F, castMap.castIfNeeded(1, val));
    assertEquals(1.0, castMap.castIfNeeded(2, val));
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeeded(3, val));
    assertEquals(BinaryStringData.fromString("1"), castMap.castIfNeeded(4, val));
  }

  @Test
  public void testCastLong() {
    CastMap castMap = new CastMap();
    castMap.add(0, new BigIntType(), new FloatType());
    castMap.add(1, new BigIntType(), new DoubleType());
    castMap.add(2, new BigIntType(), new DecimalType());
    castMap.add(3, new BigIntType(), new VarCharType());
    long val = 1L;
    assertEquals(1.0F, castMap.castIfNeeded(0, val));
    assertEquals(1.0, castMap.castIfNeeded(1, val));
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeeded(2, val));
    assertEquals(BinaryStringData.fromString("1"), castMap.castIfNeeded(3, val));
  }

  @Test
  public void testCastFloat() {
    CastMap castMap = new CastMap();
    castMap.add(0, new FloatType(), new DoubleType());
    castMap.add(1, new FloatType(), new DecimalType());
    castMap.add(2, new FloatType(), new VarCharType());
    float val = 1F;
    assertEquals(1.0, castMap.castIfNeeded(0, val));
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeeded(1, val));
    assertEquals(BinaryStringData.fromString("1.0"), castMap.castIfNeeded(2, val));
  }

  @Test
  public void testCastDouble() {
    CastMap castMap = new CastMap();
    castMap.add(0, new DoubleType(), new DecimalType());
    castMap.add(1, new DoubleType(), new VarCharType());
    double val = 1;
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeeded(0, val));
    assertEquals(BinaryStringData.fromString("1.0"), castMap.castIfNeeded(1, val));
  }

  @Test
  public void testCastDecimal() {
    CastMap castMap = new CastMap();
    castMap.add(0, new DecimalType(2, 1), new DecimalType(3, 2));
    castMap.add(1, new DecimalType(), new VarCharType());
    DecimalData val = DecimalData.fromBigDecimal(BigDecimal.ONE, 2, 1);
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 3, 2), castMap.castIfNeeded(0, val));
    assertEquals(BinaryStringData.fromString("1.0"), castMap.castIfNeeded(1, val));
  }

  @Test
  public void testCastString() {
    CastMap castMap = new CastMap();
    castMap.add(0, new VarCharType(), new DecimalType());
    castMap.add(1, new VarCharType(), new DateType());
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeeded(0, BinaryStringData.fromString("1.0")));
    assertEquals((int) LocalDate.parse("2022-05-12").toEpochDay(), castMap.castIfNeeded(1, BinaryStringData.fromString("2022-05-12")));
  }

  @Test
  public void testCastDate() {
    CastMap castMap = new CastMap();
    castMap.add(0, new DateType(), new VarCharType());
    assertEquals(BinaryStringData.fromString("2022-05-12"), castMap.castIfNeeded(0, (int) LocalDate.parse("2022-05-12").toEpochDay()));
  }

  @Test
  public void testCastArrayType() {
    LogicalType fromArrayType = new ArrayType(true, new IntType());
    LogicalType toArrayType = new ArrayType(true, new DoubleType());
    CastMap castMap = new CastMap();
    castMap.add(0, fromArrayType, toArrayType);

    // cannot use primitive class as primitive types cannot be null
    // inserting a null to test null-handling
    ArrayData inputArrayData = new GenericArrayData(new Integer[]{1, null, 3});
    ArrayData expectedArrayData = new GenericArrayData(new Double[]{1.0, null, 3.0});

    assertEquals(expectedArrayData, castMap.castIfNeeded(0, inputArrayData));
  }

  @Test
  public void testCastMapType() {
    LogicalType fromMapType = new MapType(true, new VarCharType(), new IntType());
    LogicalType toMapType = new MapType(true, new VarCharType(), new DoubleType());
    CastMap castMap = new CastMap();
    castMap.add(0, fromMapType, toMapType);

    Map<BinaryStringData, Integer> inputMap = new HashMap<>();
    inputMap.put(BinaryStringData.fromString("alex"), 11);
    inputMap.put(BinaryStringData.fromString("ben"), null);
    inputMap.put(BinaryStringData.fromString("jack"), 12);

    Map<BinaryStringData, Double> expectedMap = new HashMap<>();
    expectedMap.put(BinaryStringData.fromString("alex"), 11.0);
    expectedMap.put(BinaryStringData.fromString("ben"), null);
    expectedMap.put(BinaryStringData.fromString("jack"), 12.0);

    MapData inputMapData = new GenericMapData(inputMap);
    MapData expectedMapData = new GenericMapData(expectedMap);

    assertEquals(expectedMapData, castMap.castIfNeeded(0, inputMapData));
  }

  @Test
  public void testCastNestedRow() {
    RowType innerRowType = new RowType(
        true,
        Arrays.asList(
            new RowType.RowField("f0_int", new IntType()),
            new RowType.RowField("f1_int", new IntType()),
            new RowType.RowField("f2_int", new IntType()),
            new RowType.RowField("f3_int", new IntType())));

    // adding not nulls to test handling of not-null constraints
    RowType innerEvolvedRowType = new RowType(
        true,
        Arrays.asList(
            new RowType.RowField("f0_double", new DoubleType()),
            new RowType.RowField("f1_int", new IntType(false)),
            new RowType.RowField("f2_double", new DoubleType()),
            new RowType.RowField("f3_bigint", new BigIntType())));

    RowType fromRowType = new RowType(true,
        Arrays.asList(
            new RowType.RowField("f0_row", innerRowType),
            new RowType.RowField("f1_int", new IntType())));

    RowType toRowType = new RowType(true,
        Arrays.asList(
            new RowType.RowField("f0_row", innerEvolvedRowType),
            new RowType.RowField("f1_double", new DoubleType())));

    CastMap castMap = new CastMap();
    castMap.add(0, fromRowType, toRowType);

    // initialise row data to perform cast on
    GenericRowData innerRowData = new GenericRowData(4);
    innerRowData.setField(0, 333);
    innerRowData.setField(1, 444);
    innerRowData.setField(2, null);
    innerRowData.setField(3, 555);
    GenericRowData inputRowData = new GenericRowData(2);
    inputRowData.setField(0, innerRowData);
    inputRowData.setField(1, 2);

    // initialise row data as ground-truth to check against
    GenericRowData evolvedInnerRowData = new GenericRowData(4);
    evolvedInnerRowData.setField(0, 333.0);
    evolvedInnerRowData.setField(1, 444);
    evolvedInnerRowData.setField(2, null);
    evolvedInnerRowData.setField(3, 555L);
    GenericRowData expectedRowData = new GenericRowData(2);
    expectedRowData.setField(0, evolvedInnerRowData);
    expectedRowData.setField(1, 2.0);

    assertEquals(expectedRowData, castMap.castIfNeeded(0, inputRowData));
  }
}
