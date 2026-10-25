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

package org.apache.hudi.source.stats;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestColumnStatsModels {

  @Test
  void testColumnStatsValueSemantics() {
    ColumnStats stats = new ColumnStats(1, 9, 2);

    assertEquals(1, stats.getMinVal());
    assertEquals(9, stats.getMaxVal());
    assertEquals(2, stats.getNullCnt());
    assertEquals(stats, new ColumnStats(1, 9, 2));
    assertEquals(stats.hashCode(), new ColumnStats(1, 9, 2).hashCode());
    assertNotEquals(stats, new ColumnStats(null, 9, 2));
    assertNull(new ColumnStats(null, null, 0).getMinVal());
  }

  @Test
  void testColumnStatsSchemaConstantsResolveExpectedFields() {
    assertNotNull(ColumnStatsSchemas.METADATA_SCHEMA);
    assertNotNull(ColumnStatsSchemas.METADATA_DATA_TYPE);
    assertNotNull(ColumnStatsSchemas.COL_STATS_DATA_TYPE);
    assertEquals(6, ColumnStatsSchemas.COL_STATS_TARGET_POS.length);
    int[] expectedSourcePositions = new int[6];
    expectedSourcePositions[ColumnStatsSchemas.ORD_FILE_NAME] = 0;
    expectedSourcePositions[ColumnStatsSchemas.ORD_MIN_VAL] = 2;
    expectedSourcePositions[ColumnStatsSchemas.ORD_MAX_VAL] = 3;
    expectedSourcePositions[ColumnStatsSchemas.ORD_NULL_CNT] = 5;
    expectedSourcePositions[ColumnStatsSchemas.ORD_VAL_CNT] = 4;
    expectedSourcePositions[ColumnStatsSchemas.ORD_COL_NAME] = 1;
    assertArrayEquals(expectedSourcePositions, ColumnStatsSchemas.COL_STATS_TARGET_POS);
  }
}
