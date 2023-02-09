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

package org.apache.hudi.utils;

import org.apache.hudi.client.model.HoodieRowData;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests {@link HoodieRowData}.
 */
public class TestHoodieRowData {
  private final int metaColumnsNum = HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.size();
  private static final Random RANDOM = new Random();
  private static final int INTEGER_INDEX = 0;
  private static final int STRING_INDEX = 1;
  private static final int BOOLEAN_INDEX = 2;
  private static final int SHORT_INDEX = 3;
  private static final int BYTE_INDEX = 4;
  private static final int LONG_INDEX = 5;
  private static final int FLOAT_INDEX = 6;
  private static final int DOUBLE_INDEX = 7;
  private static final int DECIMAL_INDEX = 8;
  private static final int BINARY_INDEX = 9;
  private static final int ROW_INDEX = 10;

  private static final DataType BASIC_DATA_TYPE = DataTypes.ROW(
          DataTypes.FIELD("integer", DataTypes.INT()),
          DataTypes.FIELD("string", DataTypes.STRING()),
          DataTypes.FIELD("boolean", DataTypes.BOOLEAN()),
          DataTypes.FIELD("short", DataTypes.SMALLINT()),
          DataTypes.FIELD("byte", DataTypes.TINYINT()),
          DataTypes.FIELD("long", DataTypes.BIGINT()),
          DataTypes.FIELD("float", DataTypes.FLOAT()),
          DataTypes.FIELD("double", DataTypes.DOUBLE()),
          DataTypes.FIELD("decimal", DataTypes.DECIMAL(10, 4)),
          DataTypes.FIELD("binary", DataTypes.BYTES()),
          DataTypes.FIELD("row", DataTypes.ROW()))
      .notNull();
  private static final RowType ROW_TYPE = (RowType) BASIC_DATA_TYPE.getLogicalType();

  @Test
  public void testGet() {
    Object[] values = getRandomValue(true);
    RowData rowData = TestData.insertRow(ROW_TYPE, values);

    HoodieRowData hoodieRowData = new HoodieRowData("commitTime", "commitSeqNo", "recordKey", "partitionPath", "fileName",
        rowData, true);
    assertValues(hoodieRowData, "commitTime", "commitSeqNo", "recordKey", "partitionPath",
        "fileName", values);
  }

  /**
   * Fetches a random Object[] of values for testing.
   *
   * @param haveRowType true if rowType need to be added as one of the elements in the Object[]
   * @return the random Object[] thus generated
   */
  private Object[] getRandomValue(boolean haveRowType) {
    Object[] values = new Object[11];
    values[INTEGER_INDEX] = RANDOM.nextInt();
    values[STRING_INDEX] = StringData.fromString(UUID.randomUUID().toString());
    values[BOOLEAN_INDEX] = RANDOM.nextBoolean();
    values[SHORT_INDEX] = (short) RANDOM.nextInt(2);
    byte[] bytes = new byte[1];
    RANDOM.nextBytes(bytes);
    values[BYTE_INDEX] = bytes[0];
    values[LONG_INDEX] = RANDOM.nextLong();
    values[FLOAT_INDEX] = RANDOM.nextFloat();
    values[DOUBLE_INDEX] = RANDOM.nextDouble();
    values[DECIMAL_INDEX] = DecimalData.fromBigDecimal(new BigDecimal("1005.12313"), 10, 4);
    bytes = new byte[20];
    RANDOM.nextBytes(bytes);
    values[BINARY_INDEX] = bytes;
    if (haveRowType) {
      Object[] rowField = getRandomValue(false);
      values[ROW_INDEX] = TestData.insertRow(ROW_TYPE, rowField);
    }
    return values;
  }

  private void assertValues(HoodieRowData hoodieRowData, String commitTime, String commitSeqNo, String recordKey, String partitionPath,
                            String filename, Object[] values) {
    assertEquals(commitTime, hoodieRowData.getString(0).toString());
    assertEquals(commitSeqNo, hoodieRowData.getString(1).toString());
    assertEquals(recordKey, hoodieRowData.getString(2).toString());
    assertEquals(partitionPath, hoodieRowData.getString(3).toString());
    assertEquals(filename, hoodieRowData.getString(4).toString());
    assertEquals("I", hoodieRowData.getString(5).toString());
    // row data.
    assertEquals(values[INTEGER_INDEX], hoodieRowData.getInt(INTEGER_INDEX + metaColumnsNum));
    assertEquals(values[STRING_INDEX], hoodieRowData.getString(STRING_INDEX + metaColumnsNum));
    assertEquals(values[BOOLEAN_INDEX], hoodieRowData.getBoolean(BOOLEAN_INDEX + metaColumnsNum));
    assertEquals(values[SHORT_INDEX], hoodieRowData.getShort(SHORT_INDEX + metaColumnsNum));
    assertEquals(values[BYTE_INDEX], hoodieRowData.getByte(BYTE_INDEX + metaColumnsNum));
    assertEquals(values[LONG_INDEX], hoodieRowData.getLong(LONG_INDEX + metaColumnsNum));
    assertEquals(values[FLOAT_INDEX], hoodieRowData.getFloat(FLOAT_INDEX + metaColumnsNum));
    assertEquals(values[DOUBLE_INDEX], hoodieRowData.getDouble(DOUBLE_INDEX + metaColumnsNum));
    assertEquals(values[DECIMAL_INDEX], hoodieRowData.getDecimal(DECIMAL_INDEX + metaColumnsNum, 10, 4));
    byte[] exceptBinary = (byte[]) values[BINARY_INDEX];
    byte[] binary = hoodieRowData.getBinary(BINARY_INDEX + metaColumnsNum);
    for (int i = 0; i < exceptBinary.length; i++) {
      assertEquals(exceptBinary[i], binary[i]);
    }
    assertEquals(values[ROW_INDEX], hoodieRowData.getRow(ROW_INDEX + metaColumnsNum, values.length));
  }
}
