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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieIndexVersion;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.convertCompletionTimeToEpoch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  void testDropNonV1IndexPartitions() {
    // Test scenario: No metadata partitions exist
    testDropNonV1IndexPartitionsNoPartitions();
    // Test scenario: Mixed V1 and V2+ partitions
    testDropNonV1IndexPartitionsMixedVersions();
    // Test scenario: Exception handling
    testDropNonV1IndexPartitionsExceptionHandling();
  }

  private void testDropNonV1IndexPartitionsNoPartitions() {
    // Setup mocks
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    SupportsUpgradeDowngrade upgradeDowngradeHelper = mock(SupportsUpgradeDowngrade.class);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);

    // Mock behavior - no metadata partitions
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(new HashSet<>());
    when(upgradeDowngradeHelper.getWriteClient(config, context)).thenReturn(writeClient);

    // Execute
    UpgradeDowngradeUtils.dropNonV1IndexPartitions(
        config, context, metaClient, upgradeDowngradeHelper, "upgrade");

    // Verify - dropIndex should not be called when no partitions exist
    verify(writeClient, times(0)).dropIndex(any());
  }

  private void testDropNonV1IndexPartitionsMixedVersions() {
    // Setup mocks
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    SupportsUpgradeDowngrade upgradeDowngradeHelper = mock(SupportsUpgradeDowngrade.class);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    HoodieIndexDefinition v1IndexDef = mock(HoodieIndexDefinition.class);
    HoodieIndexDefinition v2IndexDef = mock(HoodieIndexDefinition.class);

    // Mock behavior - mixed V1 and V2+ partitions
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(new HashSet<>(Arrays.asList("v1_partition", "v2_partition")));
    when(metaClient.getIndexForMetadataPartition("v1_partition")).thenReturn(Option.of(v1IndexDef));
    when(metaClient.getIndexForMetadataPartition("v2_partition")).thenReturn(Option.of(v2IndexDef));
    when(v1IndexDef.getVersion()).thenReturn(HoodieIndexVersion.V1);
    when(v2IndexDef.getVersion()).thenReturn(HoodieIndexVersion.V2);
    when(upgradeDowngradeHelper.getWriteClient(config, context)).thenReturn(writeClient);

    // Execute
    UpgradeDowngradeUtils.dropNonV1IndexPartitions(
        config, context, metaClient, upgradeDowngradeHelper, "upgrade");

    // Verify - dropIndex should be called only with V2+ partitions
    ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
    verify(writeClient, times(1)).dropIndex(captor.capture());
    List<String> droppedPartitions = captor.getValue();
    assertEquals(1, droppedPartitions.size());
    assertTrue(droppedPartitions.contains("v2_partition"));
    assertFalse(droppedPartitions.contains("v1_partition"));
  }

  private void testDropNonV1IndexPartitionsExceptionHandling() {
    // Setup mocks
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    SupportsUpgradeDowngrade upgradeDowngradeHelper = mock(SupportsUpgradeDowngrade.class);
    HoodieIndexDefinition v2IndexDef = mock(HoodieIndexDefinition.class);

    // Mock behavior - V2 partition but exception when getting write client
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(new HashSet<>(Arrays.asList("partition1")));
    when(metaClient.getIndexForMetadataPartition("partition1")).thenReturn(Option.of(v2IndexDef));
    when(v2IndexDef.getVersion()).thenReturn(HoodieIndexVersion.V2);
    when(upgradeDowngradeHelper.getWriteClient(config, context)).thenThrow(
        new RuntimeException("Write client creation failed"));

    // Execute and verify exception is propagated
    try {
      UpgradeDowngradeUtils.dropNonV1IndexPartitions(
          config, context, metaClient, upgradeDowngradeHelper, "upgrade");
      // If we reach here, the test should fail
      assert false : "Expected RuntimeException to be thrown";
    } catch (RuntimeException e) {
      assertEquals("Write client creation failed", e.getMessage());
    }
  }
}
