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

package org.apache.hudi.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestTypeConverters {

  @ParameterizedTest
  @ValueSource(longs = {0L, 9007199254740993L, -9007199254740993L, Long.MAX_VALUE, Long.MIN_VALUE})
  void testLongToDecimalPreservesPrecision(long value) {
    assertDecimal(DataTypes.BIGINT().getLogicalType(), value, 19, 0, Long.toString(value));
  }

  @ParameterizedTest
  @ValueSource(strings = {"9007199254740993.01", "-9007199254740993.01", "12345678901234567890.12"})
  void testStringToDecimalPreservesPrecision(String value) {
    assertDecimal(DataTypes.STRING().getLogicalType(), StringData.fromString(value), 22, 2, value);
  }

  @Test
  void testDecimalScaleAndNumericConversions() {
    assertDecimal(DataTypes.INT().getLogicalType(), Integer.MAX_VALUE, 12, 2, "2147483647.00");
    assertDecimal(DataTypes.BIGINT().getLogicalType(), 9007199254740993L, 18, 2, "9007199254740993.00");
    assertDecimal(DataTypes.FLOAT().getLogicalType(), 1.23f, 5, 2, "1.23");
    assertDecimal(DataTypes.DOUBLE().getLogicalType(), 1.23d, 5, 2, "1.23");
    assertDecimal(DataTypes.STRING().getLogicalType(), StringData.fromString("9007199254740993.015"),
        18, 2, "9007199254740993.02");
  }

  @Test
  void testDecimalOverflow() {
    assertNull(TypeConverters.getInstance(DataTypes.BIGINT().getLogicalType(), DataTypes.DECIMAL(18, 0).getLogicalType())
        .convert(Long.MAX_VALUE));
    assertNull(TypeConverters.getInstance(DataTypes.STRING().getLogicalType(), DataTypes.DECIMAL(18, 2).getLogicalType())
        .convert(StringData.fromString("12345678901234567890.12")));
  }

  private static void assertDecimal(LogicalType fromType, Object value, int precision, int scale, String expected) {
    DecimalData result = (DecimalData) TypeConverters.getInstance(fromType, DataTypes.DECIMAL(precision, scale).getLogicalType())
        .convert(value);
    assertEquals(new BigDecimal(expected), result.toBigDecimal());
  }
}
