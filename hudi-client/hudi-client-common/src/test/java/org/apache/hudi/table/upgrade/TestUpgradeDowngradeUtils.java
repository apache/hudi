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
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.FALSE;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.TRUE;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.convertCompletionTimeToEpoch;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.setPropertiesBasedOnMetadataPartitions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestUpgradeDowngradeUtils {

  private HoodieTable createMockTable(Option<HoodieIndexMetadata> indexMetadataOpt) {
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getIndexMetadata()).thenReturn(indexMetadataOpt);
    return table;
  }

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

    setPropertiesBasedOnMetadataPartitions(config, emptyPartitions, null);
    assertEquals(FALSE, config.getString(HoodieMetadataConfig.ENABLE.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithBloomFilters() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS);
    HoodieTable table = createMockTable(Option.empty());

    setPropertiesBasedOnMetadataPartitions(config, partitions, table);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithAnyOtherIndexes() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add("any_other_index");
    HoodieTable table = createMockTable(Option.empty());

    setPropertiesBasedOnMetadataPartitions(config, partitions, table);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithMultiplePartitions() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();

    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS);
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);
    HoodieTable table = createMockTable(Option.empty());

    setPropertiesBasedOnMetadataPartitions(config, partitions, table);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithColumnStatsAndSourceFields() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);

    Map<String, HoodieIndexDefinition> indexDefinitions = new HashMap<>();
    List<String> sourceFields = Arrays.asList("field1", "field2", "field3");
    HoodieIndexDefinition columnStatsDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
        .withIndexType(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)
        .withSourceFields(sourceFields)
        .build();
    indexDefinitions.put(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, columnStatsDef);
    HoodieIndexMetadata indexMetadata = new HoodieIndexMetadata(indexDefinitions);
    HoodieTable table = createMockTable(Option.of(indexMetadata));

    setPropertiesBasedOnMetadataPartitions(config, partitions, table);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key()));
    assertEquals("field1,field2,field3", config.getString(HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithRecordIndexPartitioned() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);

    Map<String, HoodieIndexDefinition> indexDefinitions = new HashMap<>();
    Map<String, String> indexOptions = new HashMap<>();
    indexOptions.put("isPartitioned", TRUE);
    HoodieIndexDefinition recordIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
        .withIndexType(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
        .withIndexOptions(indexOptions)
        .build();
    indexDefinitions.put(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX, recordIndexDef);
    HoodieIndexMetadata indexMetadata = new HoodieIndexMetadata(indexDefinitions);
    HoodieTable table = createMockTable(Option.of(indexMetadata));

    setPropertiesBasedOnMetadataPartitions(config, partitions, table);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.PARTITIONED_RECORD_INDEX_ENABLE_PROP.key()));
  }

  @Test
  void testSetPropertiesBasedOnMetadataPartitionsWithRecordIndexNonPartitioned() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test").build();
    Set<String> partitions = new HashSet<>();
    partitions.add(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);

    Map<String, HoodieIndexDefinition> indexDefinitions = new HashMap<>();
    Map<String, String> indexOptions = new HashMap<>();
    indexOptions.put("isPartitioned", FALSE);
    HoodieIndexDefinition recordIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
        .withIndexType(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
        .withIndexOptions(indexOptions)
        .build();
    indexDefinitions.put(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX, recordIndexDef);
    HoodieIndexMetadata indexMetadata = new HoodieIndexMetadata(indexDefinitions);
    HoodieTable table = createMockTable(Option.of(indexMetadata));

    setPropertiesBasedOnMetadataPartitions(config, partitions, table);
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.ENABLE.key()));
    assertEquals(TRUE, config.getString(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key()));
  }
}
