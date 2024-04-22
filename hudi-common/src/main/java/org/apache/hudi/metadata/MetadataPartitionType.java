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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Partition types for metadata table.
 */
public enum MetadataPartitionType {
  FILES(HoodieTableMetadataUtil.PARTITION_NAME_FILES, "files-",
      HoodieMetadataConfig::enabled,
      (metaClient, partitionType) -> metaClient.getTableConfig().isMetadataPartitionAvailable(partitionType)),
  COLUMN_STATS(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, "col-stats-",
      HoodieMetadataConfig::isColumnStatsIndexEnabled,
      (metaClient, partitionType) -> metaClient.getTableConfig().isMetadataPartitionAvailable(partitionType)),
  BLOOM_FILTERS(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS, "bloom-filters-",
      HoodieMetadataConfig::isBloomFilterIndexEnabled,
      (metaClient, partitionType) -> metaClient.getTableConfig().isMetadataPartitionAvailable(partitionType)),
  RECORD_INDEX(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX, "record-index-",
      HoodieMetadataConfig::isRecordIndexEnabled,
      (metaClient, partitionType) -> metaClient.getTableConfig().isMetadataPartitionAvailable(partitionType)),
  FUNCTIONAL_INDEX(HoodieTableMetadataUtil.PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX, "func-index-",
      metadataConfig -> false, // no config for functional index, it is created using sql
      (metaClient, partitionType) -> metaClient.getFunctionalIndexMetadata().isPresent());

  // Partition path in metadata table.
  private final String partitionPath;
  // FileId prefix used for all file groups in this partition.
  private final String fileIdPrefix;
  private final Predicate<HoodieMetadataConfig> isMetadataPartitionEnabled;
  private final BiPredicate<HoodieTableMetaClient, MetadataPartitionType> isMetadataPartitionAvailable;

  MetadataPartitionType(final String partitionPath, final String fileIdPrefix,
                        Predicate<HoodieMetadataConfig> isMetadataPartitionEnabled,
                        BiPredicate<HoodieTableMetaClient, MetadataPartitionType> isMetadataPartitionAvailable) {
    this.partitionPath = partitionPath;
    this.fileIdPrefix = fileIdPrefix;
    this.isMetadataPartitionEnabled = isMetadataPartitionEnabled;
    this.isMetadataPartitionAvailable = isMetadataPartitionAvailable;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getFileIdPrefix() {
    return fileIdPrefix;
  }

  /**
   * Returns the list of metadata table partitions which require WriteStatus to track written records.
   * <p>
   * These partitions need the list of written records so that they can update their metadata.
   */
  public static List<MetadataPartitionType> getMetadataPartitionsNeedingWriteStatusTracking() {
    return Collections.singletonList(MetadataPartitionType.RECORD_INDEX);
  }

  /**
   * Returns the set of all metadata partition names.
   */
  public static Set<String> getAllPartitionPaths() {
    return Arrays.stream(values())
        .map(MetadataPartitionType::getPartitionPath)
        .collect(Collectors.toSet());
  }

  /**
   * Returns the list of metadata partition types enabled based on the metadata config and table config.
   */
  public static List<MetadataPartitionType> getEnabledPartitions(HoodieMetadataConfig metadataConfig, HoodieTableMetaClient metaClient) {
    List<MetadataPartitionType> enabledTypes = new ArrayList<>(4);
    for (MetadataPartitionType type : values()) {
      if (type.isMetadataPartitionEnabled.test(metadataConfig) || type.isMetadataPartitionAvailable.test(metaClient, type)) {
        enabledTypes.add(type);
      }
    }
    return enabledTypes;
  }

  @Override
  public String toString() {
    return "Metadata partition {"
        + "name: " + getPartitionPath()
        + ", prefix: " + getFileIdPrefix()
        + "}";
  }
}
