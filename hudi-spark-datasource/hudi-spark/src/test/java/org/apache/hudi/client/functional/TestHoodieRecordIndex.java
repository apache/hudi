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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.INIT_INSTANT_TS;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.metadata.HoodieTableMetadata.getMetadataTableBasePath;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieRecordIndex extends HoodieClientTestBase {

  @Test
  public void testRecordIndexRebootstrapWhenHoodiePartitionMetadataIsMissing() throws Exception {
    String partition3 = HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
    int insertedRecords = 30;

    HoodieWriteConfig cfgWithRecordIndex = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .withEnableGlobalRecordLevelIndex(true).build())
        .build();

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfgWithRecordIndex)) {
      String commitTime = WriteClientTestUtils.createNewInstantTime();
      // dataGen.generateInserts used by insertBatch writes across HoodieTestDataGenerator default partitions.
      insertBatch(cfgWithRecordIndex, client, commitTime, INIT_INSTANT_TS, insertedRecords, SparkRDDWriteClient::insert,
          false, true, insertedRecords, insertedRecords, 1, Option.empty(), INSTANT_GENERATOR);
    }
    HoodieTableMetadata metadataBeforeRebootstrap = metaClient.getTableFormat().getMetadataFactory()
        .create(context, storage, cfgWithRecordIndex.getMetadataConfig(), cfgWithRecordIndex.getBasePath());
    List<StoragePathInfo> filesInAllPartitionsBeforeRebootstrap = getFilesInAllPartitions(metadataBeforeRebootstrap);

    List<String> recordKeys = getRecordKeys();
    assertEquals(recordKeys.size(), getRecordIndexEntries(metadataBeforeRebootstrap, recordKeys).size(),
        "Record index entries should match inserted records after first batch");

    assertTrue(storage.exists(new StoragePath(getMetadataTableBasePath(basePath))),
        "Metadata table should exist before deletion");

    // Remove _hoodie_partition_metadata for one of the partition.
    removeOnePartitionMetadataFile(partition3);

    // Delete metadata table and rebootstrap it
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTableMetadataUtil.deleteMetadataTable(metaClient, context, false);
    assertFalse(storage.exists(new StoragePath(getMetadataTableBasePath(basePath))),
        "Metadata table should be removed before rebootstrap");
    assertDoesNotThrow(() -> syncTableMetadata(cfgWithRecordIndex),
        "Metadata rebootstrap with record index enabled should succeed");

    HoodieTableMetadata metadataAfterRebootstrap = metaClient.getTableFormat().getMetadataFactory()
        .create(context, storage, cfgWithRecordIndex.getMetadataConfig(), cfgWithRecordIndex.getBasePath());

    // Verify that the record_index is created.
    StoragePath recordIndexPath = new StoragePath(
        getMetadataTableBasePath(basePath), MetadataPartitionType.RECORD_INDEX.getPartitionPath());
    assertTrue(storage.exists(recordIndexPath),
        "Record index partition should exist after metadata rebootstrap");

    // Assert that the records stored in record_index and file entries from files partition
    // are less than the previous values. This shows that the partitions without _hoodie_partition_metadata
    // are not considered by the metadata table.
    int recordIndexEntriesCount = getRecordIndexEntries(metadataAfterRebootstrap, recordKeys).size();
    assertTrue(insertedRecords > recordIndexEntriesCount,
        "Record index entries should not match inserted records after metadata rebootstrap");
    List<StoragePathInfo> filesInAllPartitionsAfterRebootstrap = getFilesInAllPartitions(metadataAfterRebootstrap);
    assertTrue(filesInAllPartitionsBeforeRebootstrap.size() > filesInAllPartitionsAfterRebootstrap.size(),
        "Metadata files partition count should be lower than data table file count after rebootstrap");
  }

  private void removeOnePartitionMetadataFile(String partition) throws Exception {
    StoragePath partitionPath = new StoragePath(basePath, partition);
    List<StoragePathInfo> entries = storage.listDirectEntries(partitionPath);
    StoragePath partitionMetadataFile = entries.stream()
        .map(StoragePathInfo::getPath)
        .filter(path -> path.getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No partition metadata file found under " + partitionPath));
    assertTrue(storage.deleteFile(partitionMetadataFile),
        "Failed to delete partition metadata file " + partitionMetadataFile);
  }

  private List<String> getRecordKeys() {
    return sparkSession.read().format("hudi").load(basePath)
        .select("_hoodie_record_key")
        .collectAsList()
        .stream()
        .map(row -> row.getAs("_hoodie_record_key").toString())
        .collect(Collectors.toList());
  }

  private Map<String, HoodieRecordGlobalLocation> getRecordIndexEntries(HoodieTableMetadata metadata, List<String> recordKeys) {
    HoodiePairData<String, HoodieRecordGlobalLocation> recordIndexData =
        metadata.readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys));
    try {
      return HoodieDataUtils.dedupeAndCollectAsMap(recordIndexData);
    } finally {
      recordIndexData.unpersistWithDependencies();
    }
  }

  private List<StoragePathInfo> getFilesInAllPartitions(HoodieTableMetadata metadata) throws IOException {
    List<String> partitionPaths = metadata.getAllPartitionPaths();
    List<String> absolutePartitionPaths = partitionPaths.stream()
        .map(partitionPath -> FSUtils.getAbsolutePartitionPath(new StoragePath(basePath), partitionPath).toString())
        .collect(Collectors.toList());
    Map<String, List<StoragePathInfo>> filesInPartitions = metadata.getAllFilesInPartitions(absolutePartitionPaths);
    return filesInPartitions.values().stream().flatMap(List::stream).collect(Collectors.toList());
  }
}
