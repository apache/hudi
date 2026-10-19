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

package org.apache.hudi.hive;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieHiveSyncMetrics}.
 */
class TestHoodieHiveSyncMetrics {

  @Test
  void testNothingRecordedBeforeASync() {
    HoodieHiveSyncMetrics stats = new HoodieHiveSyncMetrics(true);
    assertFalse(stats.getSchemaReadMs().isPresent());
    assertFalse(stats.getPartitionScanMs().isPresent());
    assertFalse(stats.getRemainingMs().isPresent());
    assertEquals(0, stats.getPartitionsAdded());
    assertFalse(stats.isSchemaEvolved());
  }

  @Test
  void testMetastoreTimeIsWhatTheOtherStepsLeave() {
    HoodieHiveSyncMetrics stats = new HoodieHiveSyncMetrics(true);
    stats.addSchemaReadMs(10);
    stats.addSchemaReadMs(5);
    stats.addPartitionScanMs(30);
    stats.setTotalMs(100);

    assertEquals(15L, stats.getSchemaReadMs().get());
    assertEquals(30L, stats.getPartitionScanMs().get());
    assertEquals(55L, stats.getRemainingMs().get());
  }

  @Test
  void testMetastoreTimeWithoutOtherSteps() {
    HoodieHiveSyncMetrics stats = new HoodieHiveSyncMetrics(true);
    stats.setTotalMs(40);

    assertFalse(stats.getSchemaReadMs().isPresent());
    assertFalse(stats.getPartitionScanMs().isPresent());
    assertEquals(40L, stats.getRemainingMs().get());
  }

  @Test
  void testPartitionsAddedAndSchemaEvolvedDescribeOneTable() {
    HoodieHiveSyncMetrics stats = new HoodieHiveSyncMetrics(true);
    stats.recordPartitionsAdded(3);
    stats.recordPartitionsAdded(3);
    stats.markSchemaEvolved();
    stats.markSchemaEvolved();

    assertEquals(3, stats.getPartitionsAdded());
    assertTrue(stats.isSchemaEvolved());
  }

  @Test
  void testPartitionsAddedIsTheMostAddedToOneTable() {
    HoodieHiveSyncMetrics stats = new HoodieHiveSyncMetrics(true);
    stats.recordPartitionsAdded(2);
    stats.recordPartitionsAdded(3);
    stats.recordPartitionsAdded(1);

    assertEquals(3, stats.getPartitionsAdded());
  }

  @Test
  void testTimedStepsRecordTheirDurations() {
    HoodieHiveSyncMetrics stats = new HoodieHiveSyncMetrics(true);
    stats.timeTotal(() -> {
      assertEquals("schema", stats.timeSchemaRead(() -> "schema"));
      assertEquals("partitions", stats.timePartitionScan(() -> "partitions"));
    });

    assertTrue(stats.getSchemaReadMs().isPresent());
    assertTrue(stats.getPartitionScanMs().isPresent());
    assertTrue(stats.getRemainingMs().isPresent());
  }

  @Test
  void testTimedStepsThatFailAreStillRecorded() {
    HoodieHiveSyncMetrics stats = new HoodieHiveSyncMetrics(true);
    assertThrows(IllegalStateException.class, () -> stats.timeTotal(() -> stats.timePartitionScan(() -> {
      throw new IllegalStateException("listing fails");
    })));

    assertTrue(stats.getPartitionScanMs().isPresent());
    assertTrue(stats.getRemainingMs().isPresent());
  }

  @Test
  void testDisabledStatsRunTheStepsAndRecordNothing() {
    HoodieHiveSyncMetrics stats = new HoodieHiveSyncMetrics(false);
    stats.timeTotal(() -> {
      assertEquals("schema", stats.timeSchemaRead(() -> "schema"));
      assertEquals("partitions", stats.timePartitionScan(() -> "partitions"));
    });
    stats.recordPartitionsAdded(3);
    stats.markSchemaEvolved();

    assertFalse(stats.getSchemaReadMs().isPresent());
    assertFalse(stats.getPartitionScanMs().isPresent());
    assertFalse(stats.getRemainingMs().isPresent());
    assertEquals(0, stats.getPartitionsAdded());
    assertFalse(stats.isSchemaEvolved());
  }
}
