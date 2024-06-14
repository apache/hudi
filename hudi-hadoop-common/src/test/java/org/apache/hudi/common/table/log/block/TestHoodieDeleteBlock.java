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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests serialization and deserialization of Hudi delete log block.
 */
public class TestHoodieDeleteBlock {
  private static String KEY_PREFIX = "key";
  private static String PARTITION_PATH = "2023-01-01";

  private static Random random = new Random();

  @Test
  public void testSerializeAndDeserializeV3DeleteBlock() throws IOException {
    DeleteRecord[] deleteRecords = IntStream.range(0, 100)
        .mapToObj(i -> DeleteRecord.create(KEY_PREFIX + i, PARTITION_PATH, random.nextLong()))
        .toArray(DeleteRecord[]::new);
    testDeleteBlockWithValidation(deleteRecords);
  }

  @Test
  public void testDeserializeV2DeleteBlock() {
    // The content is Kryo serialized with V2 delete block format
    byte[] contentBytes = new byte[] {
        0, 0, 0, 2, 0, 0, 0, -88, 1, 0, 91, 76, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 104,
        117, 100, 105, 46, 99, 111, 109, 109, 111, 110, 46, 109, 111, 100, 101, 108, 46, 68, 101, 108,
        101, 116, 101, 82, 101, 99, 111, 114, 100, -69, 1, 3, 1, 1, 111, 114, 103, 46, 97, 112, 97, 99,
        104, 101, 46, 104, 117, 100, 105, 46, 99, 111, 109, 109, 111, 110, 46, 109, 111, 100, 101, 108,
        46, 68, 101, 108, 101, 116, 101, 82, 101, 99, 111, 114, -28, 1, 1, 2, 111, 114, 103, 46, 97, 112,
        97, 99, 104, 101, 46, 104, 117, 100, 105, 46, 99, 111, 109, 109, 111, 110, 46, 109, 111, 100,
        101, 108, 46, 72, 111, 111, 100, 105, 101, 75, 101, -7, 1, 1, 50, 48, 50, 51, 45, 48, 49, 45,
        48, -79, 1, 107, 101, 121, -79, 2, -30, 91, 1, 1, 1, 1, 2, 1, 5, 1, 107, 101, 121, -78, 2, -60,
        -73, 1
    };

    DeleteRecord[] deleteRecords = IntStream.range(1, 3)
        .mapToObj(i -> DeleteRecord.create(KEY_PREFIX + i, PARTITION_PATH, i * 5873))
        .toArray(DeleteRecord[]::new);
    HoodieDeleteBlock deserializeDeleteBlock = new HoodieDeleteBlock(
        Option.of(contentBytes), null, true, Option.empty(), new HashMap<>(), new HashMap<>());
    DeleteRecord[] deserializedDeleteRecords = deserializeDeleteBlock.getRecordsToDelete();
    assertEquals(Arrays.stream(deleteRecords).sorted(Comparator.comparing(DeleteRecord::getRecordKey))
            .collect(Collectors.toList()),
        Arrays.stream(deserializedDeleteRecords).sorted(Comparator.comparing(DeleteRecord::getRecordKey))
            .collect(Collectors.toList()));
  }

  public static Stream<Arguments> orderingValueParams() {
    Object[][] data =
        new Object[][] {
            {new Boolean[] {false, true}},
            {new Integer[] {Integer.MIN_VALUE, 14235, 2147465340, Integer.MAX_VALUE}},
            {new Long[] {Long.MIN_VALUE, -233498L, 2930275823L, Long.MAX_VALUE}},
            {new Float[] {Float.MIN_VALUE, 0.125f, Float.MAX_VALUE}},
            {new Double[] {Double.MIN_VALUE, 0.125, 809.25, Double.MAX_VALUE}},
            {new String[] {"val1", "val2", "val3", null}},
            {new Timestamp[] {new Timestamp(1690766971000L), new Timestamp(1672536571000L)}},
            {new LocalDate[] {LocalDate.of(2023, 1, 1), LocalDate.of(1980, 7, 1)}},
            {new BigDecimal[] {new BigDecimal("12345678901234.2948"),
                new BigDecimal("23456789012345.4856")}}
        };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("orderingValueParams")
  public void testOrderingValueInDeleteRecords(Comparable[] orderingValues) throws IOException {
    DeleteRecord[] deleteRecords = new DeleteRecord[orderingValues.length];
    for (int i = 0; i < orderingValues.length; i++) {
      deleteRecords[i] = DeleteRecord.create(
          KEY_PREFIX + i, PARTITION_PATH, orderingValues[i]);
    }
    testDeleteBlockWithValidation(deleteRecords);
  }

  public void testDeleteBlockWithValidation(DeleteRecord[] deleteRecords) throws IOException {
    HoodieDeleteBlock deleteBlock = new HoodieDeleteBlock(deleteRecords, new HashMap<>());
    byte[] contentBytes = deleteBlock.getContentBytes(HoodieTestUtils.getDefaultStorage());
    HoodieDeleteBlock deserializeDeleteBlock = new HoodieDeleteBlock(
        Option.of(contentBytes), null, true, Option.empty(), new HashMap<>(), new HashMap<>());
    DeleteRecord[] deserializedDeleteRecords = deserializeDeleteBlock.getRecordsToDelete();
    assertEquals(deleteRecords.length, deserializedDeleteRecords.length);
    for (int i = 0; i < deleteRecords.length; i++) {
      assertEquals(deleteRecords[i].getHoodieKey(), deserializedDeleteRecords[i].getHoodieKey());
      if (deleteRecords[i].getOrderingValue() != null) {
        if (deleteRecords[i].getOrderingValue() instanceof Timestamp) {
          assertEquals(((Timestamp) deleteRecords[i].getOrderingValue()).getTime(),
              ((Instant) deserializedDeleteRecords[i].getOrderingValue()).toEpochMilli());
        } else if (deleteRecords[i].getOrderingValue() instanceof BigDecimal) {
          assertEquals("0.000000000000000",
              ((BigDecimal) deleteRecords[i].getOrderingValue())
                  .subtract((BigDecimal) deserializedDeleteRecords[i].getOrderingValue()).toPlainString());
        } else {
          assertEquals(deleteRecords[i].getOrderingValue(),
              deserializedDeleteRecords[i].getOrderingValue());
        }
      } else {
        assertNull(deserializedDeleteRecords[i].getOrderingValue());
      }
    }
  }
}
