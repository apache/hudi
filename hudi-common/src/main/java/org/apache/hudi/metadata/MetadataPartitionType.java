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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP;

/**
 * Partition types for metadata table.
 */
public enum MetadataPartitionType {
  FILES(HoodieTableMetadataUtil.PARTITION_NAME_FILES, "files-") {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties properties) {
      return properties.getBoolean(ENABLE.key(), ENABLE.defaultValue());
    }
  },
  COLUMN_STATS(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, "col-stats-") {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties properties) {
      return properties.getBoolean(ENABLE_METADATA_INDEX_COLUMN_STATS.key(), ENABLE_METADATA_INDEX_COLUMN_STATS.defaultValue());
    }
  },
  BLOOM_FILTERS(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS, "bloom-filters-") {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties properties) {
      return properties.getBoolean(ENABLE_METADATA_INDEX_BLOOM_FILTER.key(), ENABLE_METADATA_INDEX_BLOOM_FILTER.defaultValue());
    }
  },
  RECORD_INDEX(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX, "record-index-") {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties properties) {
      return properties.getBoolean(RECORD_INDEX_ENABLE_PROP.key(), RECORD_INDEX_ENABLE_PROP.defaultValue());
    }
  },
  FUNCTIONAL_INDEX(HoodieTableMetadataUtil.PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX, "func-index-") {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties properties) {
      return false;
    }

    @Override
    public boolean isMetadataPartitionAvailable(HoodieTableMetaClient metaClient) {
      return metaClient.getFunctionalIndexMetadata().isPresent();
    }
  };

  // Partition path in metadata table.
  private final String partitionPath;
  // FileId prefix used for all file groups in this partition.
  private final String fileIdPrefix;

  /**
   * Check if the metadata partition is enabled based on the metadata config.
   */
  public abstract boolean isMetadataPartitionEnabled(TypedProperties properties);

  /**
   * Check if the metadata partition is available based on the table config.
   */
  public boolean isMetadataPartitionAvailable(HoodieTableMetaClient metaClient) {
    return metaClient.getTableConfig().isMetadataPartitionAvailable(this);
  }

  MetadataPartitionType(final String partitionPath, final String fileIdPrefix) {
    this.partitionPath = partitionPath;
    this.fileIdPrefix = fileIdPrefix;
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
  public static List<MetadataPartitionType> getEnabledPartitions(TypedProperties properties, HoodieTableMetaClient metaClient) {
    return Arrays.stream(values())
        .filter(partitionType -> partitionType.isMetadataPartitionEnabled(properties) || partitionType.isMetadataPartitionAvailable(metaClient))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "Metadata partition {"
        + "name: " + getPartitionPath()
        + ", prefix: " + getFileIdPrefix()
        + "}";
  }
}
