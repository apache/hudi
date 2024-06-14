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

package org.apache.hudi.utils;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HoodieWriterClientTestHarness extends HoodieCommonTestHarness {
  protected static int timelineServicePort = FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue();

  protected void addConfigsForPopulateMetaFields(HoodieWriteConfig.Builder configBuilder, boolean populateMetaFields,
                                                 boolean isMetadataTable) {
    if (!populateMetaFields) {
      configBuilder.withProperties((isMetadataTable ? getPropertiesForMetadataTable() : getPropertiesForKeyGen()))
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build());
    }
  }

  protected void addConfigsForPopulateMetaFields(HoodieWriteConfig.Builder configBuilder, boolean populateMetaFields) {
    addConfigsForPopulateMetaFields(configBuilder, populateMetaFields, false);
  }

  public static Properties getPropertiesForKeyGen() {
    return getPropertiesForKeyGen(false);
  }

  public static Properties getPropertiesForKeyGen(boolean populateMetaFields) {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaFields));
    properties.put("hoodie.datasource.write.recordkey.field", "_row_key");
    properties.put("hoodie.datasource.write.partitionpath.field", "partition_path");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    return properties;
  }

  protected Properties getPropertiesForMetadataTable() {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    properties.put("hoodie.datasource.write.recordkey.field", "key");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "key");
    return properties;
  }

  /**
   * Get Default HoodieWriteConfig for tests.
   *
   * @return Default Hoodie Write Config for tests
   */
  public HoodieWriteConfig getConfig() {
    return getConfigBuilder().build();
  }

  public HoodieWriteConfig getConfig(HoodieIndex.IndexType indexType) {
    return getConfigBuilder(indexType).build();
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  protected HoodieWriteConfig.Builder getConfigBuilder() {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, cleaningPolicy);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(HoodieIndex.IndexType indexType) {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, indexType, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr) {
    return getConfigBuilder(schemaStr, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, HoodieIndex.IndexType indexType) {
    return getConfigBuilder(schemaStr, indexType, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, HoodieIndex.IndexType indexType,
                                                    HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(cleaningPolicy).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .forTable(RAW_TRIPS_TEST_NAME)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withRemoteServerPort(timelineServicePort).build());
    if (StringUtils.nonEmpty(schemaStr)) {
      builder.withSchema(schemaStr);
    }
    return builder;
  }

  public void assertPartitionMetadataForRecords(String basePath, List<HoodieRecord> inputRecords,
                                                HoodieStorage storage) throws IOException {
    Set<String> partitionPathSet = inputRecords.stream()
        .map(HoodieRecord::getPartitionPath)
        .collect(Collectors.toSet());
    assertPartitionMetadata(basePath, partitionPathSet.stream().toArray(String[]::new), storage);
  }

  public void assertPartitionMetadataForKeys(String basePath, List<HoodieKey> inputKeys,
                                             HoodieStorage storage) throws IOException {
    Set<String> partitionPathSet = inputKeys.stream()
        .map(HoodieKey::getPartitionPath)
        .collect(Collectors.toSet());
    assertPartitionMetadata(basePath, partitionPathSet.stream().toArray(String[]::new), storage);
  }

  /**
   * Ensure presence of partition meta-data at known depth.
   *
   * @param partitionPaths Partition paths to check
   * @param storage        {@link HoodieStorage} instance.
   * @throws IOException in case of error
   */
  public static void assertPartitionMetadata(String basePath, String[] partitionPaths,
                                             HoodieStorage storage) throws IOException {
    for (String partitionPath : partitionPaths) {
      assertTrue(
          HoodiePartitionMetadata.hasPartitionMetadata(
              storage, new StoragePath(basePath, partitionPath)));
      HoodiePartitionMetadata pmeta =
          new HoodiePartitionMetadata(storage, new StoragePath(basePath, partitionPath));
      pmeta.readFromFS();
      assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_DEPTH, pmeta.getPartitionDepth());
    }
  }

  // Functional Interfaces for passing lambda and Hoodie Write API contexts

  @FunctionalInterface
  public interface Function2<R, T1, T2> {

    R apply(T1 v1, T2 v2) throws IOException;
  }

  @FunctionalInterface
  public interface Function3<R, T1, T2, T3> {

    R apply(T1 v1, T2 v2, T3 v3) throws IOException;
  }

  /**
   * Assert that there is no duplicate key at the partition level.
   *
   * @param records List of Hoodie records
   */
  public static void assertNodupesWithinPartition(List<HoodieRecord<RawTripTestPayload>> records) {
    Map<String, Set<String>> partitionToKeys = new HashMap<>();
    for (HoodieRecord r : records) {
      String key = r.getRecordKey();
      String partitionPath = r.getPartitionPath();
      if (!partitionToKeys.containsKey(partitionPath)) {
        partitionToKeys.put(partitionPath, new HashSet<>());
      }
      assertFalse(partitionToKeys.get(partitionPath).contains(key), "key " + key + " is duplicate within partition " + partitionPath);
      partitionToKeys.get(partitionPath).add(key);
    }
  }
}
