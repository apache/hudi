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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.FALSE;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.TRUE;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.convertCompletionTimeToEpoch;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.setPropertiesBasedOnMetadataPartitions;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestUpgradeDowngradeUtils {

  @Test
  void testConvertCompletionTimeToEpoch() {
    // Mock a HoodieInstant with a completion time
    String completionTime = "20241112153045678"; // yyyyMMddHHmmssSSS
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.COMPLETED, "commit", "20231112153045678", completionTime, InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);
    //when(instant.getCompletionTime()).thenReturn(completionTime);

    // Expected epoch time
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    LocalDateTime dateTime = LocalDateTime.parse(completionTime.substring(0, completionTime.length() - 3), formatter);
    long expectedEpoch = dateTime.atZone(ZoneId.systemDefault()).toEpochSecond() * 1000
        + Long.parseLong(completionTime.substring(completionTime.length() - 3));

    assertEquals(expectedEpoch, convertCompletionTimeToEpoch(instant), "Epoch time does not match the expected value.");

    // HoodieInstant with an invalid completion time
    String invalidCompletionTime = "12345";
    HoodieInstant inValidInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, "dummy_action", "20231112153045678", invalidCompletionTime, InstantComparatorV2.COMPLETION_TIME_BASED_COMPARATOR);

    assertEquals(-1, convertCompletionTimeToEpoch(inValidInstant), "Epoch time for invalid input should be -1.");
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithEmptySet() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> emptyPartitions = new HashSet<>();

    setPropertiesBasedOnMetadataPartitions(config, emptyPartitions);
    assertEquals(FALSE, config.getString(HoodieMetadataConfig.ENABLE.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithColumnStats() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);

    setPropertiesBasedOnMetadataPartitions(config, partitions);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithBloomFilters() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS);

    setPropertiesBasedOnMetadataPartitions(config, partitions);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithPartitionStats() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS);

    setPropertiesBasedOnMetadataPartitions(config, partitions);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithSecondaryIndex() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX);

    setPropertiesBasedOnMetadataPartitions(config, partitions);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithExpressionIndex() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX);

    setPropertiesBasedOnMetadataPartitions(config, partitions);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithRecordIndex() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);

    setPropertiesBasedOnMetadataPartitions(config, partitions);

    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithMultiplePartitions() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();

    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS);
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);

    setPropertiesBasedOnMetadataPartitions(config, partitions);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key()));
  }
}
