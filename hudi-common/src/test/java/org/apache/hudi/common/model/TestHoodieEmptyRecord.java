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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.OrderingValues;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieEmptyRecord {

  @ParameterizedTest
  @MethodSource("orderedDeletes")
  void testKeyCopyPreservesDeleteOrdering(HoodieOperation operation, Comparable<?> orderingValue,
                                         HoodieRecord.HoodieRecordType recordType) {
    HoodieKey originalKey = new HoodieKey("old-key", "old-partition");
    HoodieKey newKey = new HoodieKey("new-key", "new-partition");
    Properties props = new Properties();
    String[] orderingFields = {"ts"};
    HoodieEmptyRecord<Object> original = new HoodieEmptyRecord<>(originalKey, operation, orderingValue, recordType);
    assertEquals(orderingValue, original.getOrderingValue(null, props, orderingFields));

    HoodieRecord<Object> copy = original.newInstance(newKey);

    assertAll(
        () -> assertNotSame(original, copy),
        () -> assertEquals(newKey, copy.getKey()),
        () -> assertEquals(originalKey, original.getKey()),
        () -> assertEquals(operation, copy.getOperation()),
        () -> assertEquals(orderingValue, copy.getOrderingValue(null, props, orderingFields)),
        () -> assertEquals(recordType, copy.getRecordType()),
        () -> assertTrue(copy.isDelete(null, props)),
        () -> assertNull(copy.getData()));
  }

  private static Stream<Arguments> orderedDeletes() {
    return Stream.of(
        Arguments.of(HoodieOperation.DELETE, 60L, HoodieRecord.HoodieRecordType.FLINK),
        Arguments.of(HoodieOperation.UPDATE_BEFORE, 60L, HoodieRecord.HoodieRecordType.FLINK),
        Arguments.of(HoodieOperation.DELETE, "060", HoodieRecord.HoodieRecordType.AVRO),
        Arguments.of(HoodieOperation.UPDATE_BEFORE, "060", HoodieRecord.HoodieRecordType.SPARK),
        Arguments.of(HoodieOperation.DELETE, OrderingValues.getDefault(), HoodieRecord.HoodieRecordType.HIVE),
        Arguments.of(HoodieOperation.UPDATE_BEFORE, OrderingValues.getDefault(), HoodieRecord.HoodieRecordType.FLINK));
  }

  @ParameterizedTest
  @EnumSource(HoodieRecord.HoodieRecordType.class)
  void testKeyCopyPreservesNaturalOrderDelete(HoodieRecord.HoodieRecordType recordType) {
    HoodieEmptyRecord<Object> original = new HoodieEmptyRecord<>(new HoodieKey("key", "partition"), recordType);
    HoodieKey newKey = new HoodieKey("key", "new-partition");
    Properties props = new Properties();

    HoodieRecord<Object> copy = original.newInstance(newKey);

    assertEquals(newKey, copy.getKey());
    assertEquals(recordType, copy.getRecordType());
    assertNull(copy.getOperation());
    assertTrue(OrderingValues.isDefault(copy.getOrderingValue(null, props, new String[0])));
    assertTrue(copy.isDelete(null, props));
  }
}
