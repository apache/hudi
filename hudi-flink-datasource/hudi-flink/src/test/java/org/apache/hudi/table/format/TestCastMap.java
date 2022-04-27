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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;

import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

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
    assertEquals(1L, castMap.castIfNeed(0, val));
    assertEquals(1.0F, castMap.castIfNeed(1, val));
    assertEquals(1.0, castMap.castIfNeed(2, val));
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeed(3, val));
    assertEquals(BinaryStringData.fromString("1"), castMap.castIfNeed(4, val));
  }

  @Test
  public void testCastLong() {
    CastMap castMap = new CastMap();
    castMap.add(0, new BigIntType(), new FloatType());
    castMap.add(1, new BigIntType(), new DoubleType());
    castMap.add(2, new BigIntType(), new DecimalType());
    castMap.add(3, new BigIntType(), new VarCharType());
    long val = 1L;
    assertEquals(1.0F, castMap.castIfNeed(0, val));
    assertEquals(1.0, castMap.castIfNeed(1, val));
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeed(2, val));
    assertEquals(BinaryStringData.fromString("1"), castMap.castIfNeed(3, val));
  }

  @Test
  public void testCastFloat() {
    CastMap castMap = new CastMap();
    castMap.add(0, new FloatType(), new DoubleType());
    castMap.add(1, new FloatType(), new DecimalType());
    castMap.add(2, new FloatType(), new VarCharType());
    float val = 1F;
    assertEquals(1.0, castMap.castIfNeed(0, val));
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeed(1, val));
    assertEquals(BinaryStringData.fromString("1.0"), castMap.castIfNeed(2, val));
  }

  @Test
  public void testCastDouble() {
    CastMap castMap = new CastMap();
    castMap.add(0, new DoubleType(), new DecimalType());
    castMap.add(1, new DoubleType(), new VarCharType());
    double val = 1;
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeed(0, val));
    assertEquals(BinaryStringData.fromString("1.0"), castMap.castIfNeed(1, val));
  }

  @Test
  public void testCastDecimal() {
    CastMap castMap = new CastMap();
    castMap.add(0, new DecimalType(2, 1), new DecimalType(3, 2));
    castMap.add(1, new DecimalType(), new VarCharType());
    DecimalData val = DecimalData.fromBigDecimal(BigDecimal.ONE, 2, 1);
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 3, 2), castMap.castIfNeed(0, val));
    assertEquals(BinaryStringData.fromString("1.0"), castMap.castIfNeed(1, val));
  }

  @Test
  public void testCastString() {
    CastMap castMap = new CastMap();
    castMap.add(0, new VarCharType(), new DecimalType());
    castMap.add(1, new VarCharType(), new DateType());
    assertEquals(DecimalData.fromBigDecimal(BigDecimal.ONE, 1, 0), castMap.castIfNeed(0, BinaryStringData.fromString("1.0")));
    assertEquals((int) LocalDate.parse("2022-05-12").toEpochDay(), castMap.castIfNeed(1, BinaryStringData.fromString("2022-05-12")));
  }

  @Test
  public void testCastDate() {
    CastMap castMap = new CastMap();
    castMap.add(0, new DateType(), new VarCharType());
    assertEquals(BinaryStringData.fromString("2022-05-12"), castMap.castIfNeed(0, (int) LocalDate.parse("2022-05-12").toEpochDay()));
  }
}
