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

package org.apache.hudi.client.model;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link HoodieInternalRow}.
 */
public class TestHoodieInternalRow {

  private static final Random RANDOM = new Random();
  private static final int INTEGER_INDEX = 5;
  private static final int STRING_INDEX = 6;
  private static final int BOOLEAN_INDEX = 7;
  private static final int SHORT_INDEX = 8;
  private static final int BYTE_INDEX = 9;
  private static final int LONG_INDEX = 10;
  private static final int FLOAT_INDEX = 11;
  private static final int DOUBLE_INDEX = 12;
  private static final int DECIMAL_INDEX = 13;
  private static final int BINARY_INDEX = 14;
  private static final int STRUCT_INDEX = 15;
  // to do array and map
  private static final int ARRAY_INDEX = 16;
  private static final int MAP_INDEX = 17;

  private List<Integer> nullIndices;

  public TestHoodieInternalRow() {
    this.nullIndices = new ArrayList<>();
  }

  @Test
  public void testGet() {
    Object[] values = getRandomValue(true);

    InternalRow row = new GenericInternalRow(values);
    HoodieInternalRow hoodieInternalRow = SparkAdapterSupport$.MODULE$.sparkAdapter().createInternalRow(
        new UTF8String[] {
            UTF8String.fromString("commitTime"),
            UTF8String.fromString("commitSeqNo"),
            UTF8String.fromString("recordKey"),
            UTF8String.fromString("partitionPath"),
            UTF8String.fromString("fileName")
        },
        row,
        true);

    assertValues(hoodieInternalRow, "commitTime", "commitSeqNo", "recordKey", "partitionPath",
        "fileName", values, nullIndices);
  }

  @Test
  public void testUpdate() {
    Object[] values = getRandomValue(true);
    InternalRow row = new GenericInternalRow(values);
    HoodieInternalRow hoodieInternalRow = SparkAdapterSupport$.MODULE$.sparkAdapter().createInternalRow(
        new UTF8String[] {
            UTF8String.fromString("commitTime"),
            UTF8String.fromString("commitSeqNo"),
            UTF8String.fromString("recordKey"),
            UTF8String.fromString("partitionPath"),
            UTF8String.fromString("fileName")
        },
        row,
        true);

    hoodieInternalRow.update(0, "commitTime_updated");
    hoodieInternalRow.update(1, "commitSeqNo_updated");
    hoodieInternalRow.update(2, "recordKey_updated");
    hoodieInternalRow.update(3, "partitionPath_updated");
    hoodieInternalRow.update(4, "fileName_updated");

    values = getRandomValue(true);
    hoodieInternalRow.update(INTEGER_INDEX, values[INTEGER_INDEX]);
    hoodieInternalRow.update(BOOLEAN_INDEX, values[BOOLEAN_INDEX]);
    hoodieInternalRow.update(SHORT_INDEX, values[SHORT_INDEX]);
    hoodieInternalRow.update(BYTE_INDEX, values[BYTE_INDEX]);
    hoodieInternalRow.update(LONG_INDEX, values[LONG_INDEX]);
    hoodieInternalRow.update(FLOAT_INDEX, values[FLOAT_INDEX]);
    hoodieInternalRow.update(DOUBLE_INDEX, values[DOUBLE_INDEX]);
    //hoodieInternalRow.update(decimalIndex, values[decimalIndex]);
    hoodieInternalRow.update(BINARY_INDEX, values[BINARY_INDEX]);
    hoodieInternalRow.update(STRUCT_INDEX, values[STRUCT_INDEX]);
    hoodieInternalRow.update(STRING_INDEX, values[STRING_INDEX].toString());

    assertValues(hoodieInternalRow, "commitTime_updated", "commitSeqNo_updated", "recordKey_updated", "partitionPath_updated",
        "fileName_updated", values, nullIndices);
  }

  @Test
  public void testNumFields() {
    Object[] values = getRandomValue(true);
    InternalRow row = new GenericInternalRow(values);
    HoodieInternalRow hoodieInternalRow1 = SparkAdapterSupport$.MODULE$.sparkAdapter().createInternalRow(
        new UTF8String[] {
            UTF8String.fromString("commitTime"),
            UTF8String.fromString("commitSeqNo"),
            UTF8String.fromString("recordKey"),
            UTF8String.fromString("partitionPath"),
            UTF8String.fromString("fileName")
        },
        row,
        true);
    HoodieInternalRow hoodieInternalRow2 = SparkAdapterSupport$.MODULE$.sparkAdapter().createInternalRow(
        new UTF8String[] {
            UTF8String.fromString("commitTime"),
            UTF8String.fromString("commitSeqNo"),
            UTF8String.fromString("recordKey"),
            UTF8String.fromString("partitionPath"),
            UTF8String.fromString("fileName")
        },
        row,
        false);
    assertEquals(row.numFields(), hoodieInternalRow1.numFields());
    assertEquals(row.numFields() + 5, hoodieInternalRow2.numFields());
  }

  @Test
  public void testIsNullCheck() {

    for (int i = 0; i < 16; i++) {
      Object[] values = getRandomValue(true);

      InternalRow row = new GenericInternalRow(values);
      HoodieInternalRow hoodieInternalRow = SparkAdapterSupport$.MODULE$.sparkAdapter().createInternalRow(
          new UTF8String[] {
              UTF8String.fromString("commitTime"),
              UTF8String.fromString("commitSeqNo"),
              UTF8String.fromString("recordKey"),
              UTF8String.fromString("partitionPath"),
              UTF8String.fromString("fileName")
          },
          row,
          true);

      hoodieInternalRow.setNullAt(i);
      nullIndices.clear();
      nullIndices.add(i);
      assertValues(hoodieInternalRow, "commitTime", "commitSeqNo", "recordKey", "partitionPath",
          "fileName", values, nullIndices);
    }

    // try setting multiple values as null
    // run it for 5 rounds
    for (int i = 0; i < 5; i++) {
      int numNullValues = 1 + RANDOM.nextInt(4);
      List<Integer> nullsSoFar = new ArrayList<>();
      while (nullsSoFar.size() < numNullValues) {
        int randomIndex = RANDOM.nextInt(16);
        if (!nullsSoFar.contains(randomIndex)) {
          nullsSoFar.add(randomIndex);
        }
      }

      Object[] values = getRandomValue(true);
      InternalRow row = new GenericInternalRow(values);
      HoodieInternalRow hoodieInternalRow = SparkAdapterSupport$.MODULE$.sparkAdapter().createInternalRow(
          new UTF8String[] {
              UTF8String.fromString("commitTime"),
              UTF8String.fromString("commitSeqNo"),
              UTF8String.fromString("recordKey"),
              UTF8String.fromString("partitionPath"),
              UTF8String.fromString("fileName")
          },
          row,
          true);

      nullIndices.clear();

      for (Integer index : nullsSoFar) {
        hoodieInternalRow.setNullAt(index);
        nullIndices.add(index);
      }
      assertValues(hoodieInternalRow, "commitTime", "commitSeqNo", "recordKey", "partitionPath",
          "fileName", values, nullIndices);
    }
  }

  /**
   * Fetches a random Object[] of values for testing.
   *
   * @param withStructType true if structType need to be added as one of the elements in the Object[]
   * @return the random Object[] thus generated
   */
  private Object[] getRandomValue(boolean withStructType) {
    Object[] values = new Object[16];
    values[INTEGER_INDEX] = RANDOM.nextInt();
    values[STRING_INDEX] = UUID.randomUUID().toString();
    values[BOOLEAN_INDEX] = RANDOM.nextBoolean();
    values[SHORT_INDEX] = (short) RANDOM.nextInt(2);
    byte[] bytes = new byte[1];
    RANDOM.nextBytes(bytes);
    values[BYTE_INDEX] = bytes[0];
    values[LONG_INDEX] = RANDOM.nextLong();
    values[FLOAT_INDEX] = RANDOM.nextFloat();
    values[DOUBLE_INDEX] = RANDOM.nextDouble();
    // TODO fix decimal type.
    values[DECIMAL_INDEX] = RANDOM.nextFloat();
    bytes = new byte[20];
    RANDOM.nextBytes(bytes);
    values[BINARY_INDEX] = bytes;
    if (withStructType) {
      Object[] structField = getRandomValue(false);
      values[STRUCT_INDEX] = new GenericInternalRow(structField);
    }
    return values;
  }

  private void assertValues(HoodieInternalRow hoodieInternalRow, String commitTime, String commitSeqNo, String recordKey, String partitionPath, String filename, Object[] values,
      List<Integer> nullIndexes) {
    for (Integer index : nullIndexes) {
      assertTrue(hoodieInternalRow.isNullAt(index));
    }
    for (int i = 0; i < 16; i++) {
      if (!nullIndexes.contains(i)) {
        assertFalse(hoodieInternalRow.isNullAt(i));
      }
    }
    if (!nullIndexes.contains(0)) {
      assertEquals(commitTime, hoodieInternalRow.get(0, DataTypes.StringType).toString());
    }
    if (!nullIndexes.contains(1)) {
      assertEquals(commitSeqNo, hoodieInternalRow.get(1, DataTypes.StringType).toString());
    }
    if (!nullIndexes.contains(2)) {
      assertEquals(recordKey, hoodieInternalRow.get(2, DataTypes.StringType).toString());
    }
    if (!nullIndexes.contains(3)) {
      assertEquals(partitionPath, hoodieInternalRow.get(3, DataTypes.StringType).toString());
    }
    if (!nullIndexes.contains(4)) {
      assertEquals(filename, hoodieInternalRow.get(4, DataTypes.StringType).toString());
    }
    if (!nullIndexes.contains(INTEGER_INDEX)) {
      assertEquals(values[INTEGER_INDEX], hoodieInternalRow.getInt(INTEGER_INDEX));
      assertEquals(values[INTEGER_INDEX], hoodieInternalRow.get(INTEGER_INDEX, DataTypes.IntegerType));
    }
    if (!nullIndexes.contains(STRING_INDEX)) {
      assertEquals(values[STRING_INDEX].toString(), hoodieInternalRow.get(STRING_INDEX, DataTypes.StringType));
    }
    if (!nullIndexes.contains(BOOLEAN_INDEX)) {
      assertEquals(values[BOOLEAN_INDEX], hoodieInternalRow.getBoolean(BOOLEAN_INDEX));
      assertEquals(values[BOOLEAN_INDEX], hoodieInternalRow.get(BOOLEAN_INDEX, DataTypes.BooleanType));
    }
    if (!nullIndexes.contains(SHORT_INDEX)) {
      assertEquals(values[SHORT_INDEX], hoodieInternalRow.getShort(SHORT_INDEX));
      assertEquals(values[SHORT_INDEX], hoodieInternalRow.get(SHORT_INDEX, DataTypes.ShortType));
    }
    if (!nullIndexes.contains(BYTE_INDEX)) {
      assertEquals(values[BYTE_INDEX], hoodieInternalRow.getByte(BYTE_INDEX));
      assertEquals(values[BYTE_INDEX], hoodieInternalRow.get(BYTE_INDEX, DataTypes.ByteType));
    }
    if (!nullIndexes.contains(LONG_INDEX)) {
      assertEquals(values[LONG_INDEX], hoodieInternalRow.getLong(LONG_INDEX));
      assertEquals(values[LONG_INDEX], hoodieInternalRow.get(LONG_INDEX, DataTypes.LongType));
    }
    if (!nullIndexes.contains(FLOAT_INDEX)) {
      assertEquals(values[FLOAT_INDEX], hoodieInternalRow.getFloat(FLOAT_INDEX));
      assertEquals(values[FLOAT_INDEX], hoodieInternalRow.get(FLOAT_INDEX, DataTypes.FloatType));
    }
    if (!nullIndexes.contains(DOUBLE_INDEX)) {
      assertEquals(values[DOUBLE_INDEX], hoodieInternalRow.getDouble(DOUBLE_INDEX));
      assertEquals(values[DOUBLE_INDEX], hoodieInternalRow.get(DOUBLE_INDEX, DataTypes.DoubleType));
    }
    if (!nullIndexes.contains(BINARY_INDEX)) {
      assertEquals(values[BINARY_INDEX], hoodieInternalRow.getBinary(BINARY_INDEX));
      assertEquals(values[BINARY_INDEX], hoodieInternalRow.get(BINARY_INDEX, DataTypes.BinaryType));
    }
    if (!nullIndexes.contains(STRUCT_INDEX)) {
      assertEquals(values[STRUCT_INDEX], hoodieInternalRow.getStruct(STRUCT_INDEX, 18));
    }
  }
}
