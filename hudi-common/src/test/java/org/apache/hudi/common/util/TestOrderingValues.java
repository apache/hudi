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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestOrderingValues {

  @Test
  void testNullIsDistinctFromLegacyDeleteSentinel() {
    assertFalse(OrderingValues.isDefault(null));
    assertTrue(OrderingValues.isDefault(0));
    assertFalse(OrderingValues.isDefault(0L));
    assertTrue(OrderingValues.isCommitTimeOrderingValue(null));
    assertTrue(OrderingValues.isCommitTimeOrderingValue(0));
    assertNull(OrderingValues.create(new String[] {"ts"}, field -> null));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieRecordType.class, names = {"AVRO", "SPARK", "FLINK"})
  void testDeleteStatementRetainsIntegerSentinel(HoodieRecordType type) {
    HoodieEmptyRecord<?> record = new HoodieEmptyRecord<>(new HoodieKey("id", "partition"), type);
    assertEquals(Integer.valueOf(0), record.getOrderingValue(null, new Properties(), new String[] {"ts"}));
  }
}
