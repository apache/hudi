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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieWriteStat;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the plain client-side stat holders {@link SecondaryIndexStats} and {@link TableWriteStats}.
 */
public class TestClientStatsPojos {

  @Test
  void secondaryIndexStatsExposesConstructorValues() {
    SecondaryIndexStats deleted = new SecondaryIndexStats("rk1", "sk1", true);
    assertEquals("rk1", deleted.getRecordKey());
    assertEquals("sk1", deleted.getSecondaryKeyValue());
    assertTrue(deleted.isDeleted());

    SecondaryIndexStats live = new SecondaryIndexStats("rk2", "sk2", false);
    assertEquals("rk2", live.getRecordKey());
    assertEquals("sk2", live.getSecondaryKeyValue());
    assertFalse(live.isDeleted());
  }

  @Test
  void secondaryIndexStatsSetterMutatesRecordKey() {
    SecondaryIndexStats stats = new SecondaryIndexStats("rk1", "sk1", false);
    stats.setRecordKey("rk2");
    stats.setSecondaryKeyValue("sk2");
    assertEquals("rk2", stats.getRecordKey());
    assertEquals("sk2", stats.getSecondaryKeyValue());
  }

  @Test
  void secondaryIndexStatsEqualityUsesAllFields() {
    SecondaryIndexStats a = new SecondaryIndexStats("rk", "sk", false);
    SecondaryIndexStats same = new SecondaryIndexStats("rk", "sk", false);
    SecondaryIndexStats deleteDiffers = new SecondaryIndexStats("rk", "sk", true);
    assertEquals(a, same);
    assertEquals(a.hashCode(), same.hashCode());
    assertNotEquals(a, deleteDiffers);
  }

  @Test
  void tableWriteStatsSingleArgDefaultsMetadataToEmpty() {
    List<HoodieWriteStat> dataStats = Collections.singletonList(new HoodieWriteStat());
    TableWriteStats stats = new TableWriteStats(dataStats);
    assertEquals(dataStats, stats.getDataTableWriteStats());
    assertTrue(stats.getMetadataTableWriteStats().isEmpty());
    assertFalse(stats.isEmptyDataTableWriteStats());
  }

  @Test
  void tableWriteStatsReportsEmptyDataStats() {
    TableWriteStats empty = new TableWriteStats(Collections.emptyList());
    assertTrue(empty.isEmptyDataTableWriteStats());
  }

  @Test
  void tableWriteStatsRetainsBothLists() {
    List<HoodieWriteStat> dataStats = Arrays.asList(new HoodieWriteStat(), new HoodieWriteStat());
    List<HoodieWriteStat> metadataStats = Collections.singletonList(new HoodieWriteStat());
    TableWriteStats stats = new TableWriteStats(dataStats, metadataStats);
    assertEquals(2, stats.getDataTableWriteStats().size());
    assertEquals(1, stats.getMetadataTableWriteStats().size());
  }
}
