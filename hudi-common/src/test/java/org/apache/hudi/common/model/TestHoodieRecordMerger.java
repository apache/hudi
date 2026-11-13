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

import org.apache.hudi.common.table.read.BufferedRecord;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.model.HoodieRecordMerger.maxOrderingValue;
import static org.apache.hudi.common.util.OrderingValues.getDefault;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestHoodieRecordMerger {

  @Test
  void testMaxOrderingValueRanksStoredNullLowest() {
    assertEquals(5L, maxOrderingValue(record(null), record(5L)));
    assertEquals(5L, maxOrderingValue(record(getDefault()), record(5L)));
    assertEquals(9L, maxOrderingValue(record(9L), record(5L)));
    assertEquals(5L, maxOrderingValue(record(3L), record(5L)));
  }

  private static BufferedRecord<Object> record(Comparable orderingValue) {
    return new BufferedRecord<>("key0", orderingValue, null, 0, null);
  }
}
