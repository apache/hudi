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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.config.HoodieIndexingConfig.HOODIE_ALL_PARTITIONS_INDEX_VERSION;
import static org.apache.hudi.common.config.HoodieIndexingConfig.HOODIE_BLOOM_FILTERS_INDEX_VERSION;
import static org.apache.hudi.common.config.HoodieIndexingConfig.HOODIE_COLUMN_STATS_INDEX_VERSION;
import static org.apache.hudi.common.config.HoodieIndexingConfig.HOODIE_EXPRESSION_INDEX_VERSION;
import static org.apache.hudi.common.config.HoodieIndexingConfig.HOODIE_FILES_INDEX_VERSION;
import static org.apache.hudi.common.config.HoodieIndexingConfig.HOODIE_PARTITION_STATS_VERSION;
import static org.apache.hudi.common.config.HoodieIndexingConfig.HOODIE_RECORD_INDEX_VERSION;
import static org.apache.hudi.common.config.HoodieIndexingConfig.HOODIE_SECONDARY_INDEX_VERSION;

public enum HoodieIndexVersion {
  ALL_PARTITIONS_ONE(MetadataPartitionType.ALL_PARTITIONS, 1, Arrays.asList("0.14.0")),

  PARTITION_STATS_ONE(MetadataPartitionType.PARTITION_STATS, 1, Arrays.asList("0.14.0")),

  FILES_INDEX_ONE(MetadataPartitionType.FILES, 1, Arrays.asList("0.14.0")),

  RECORD_INDEX_ONE(MetadataPartitionType.RECORD_INDEX, 1, Arrays.asList("1.0.0")),

  COLUMN_STATS_ONE(MetadataPartitionType.COLUMN_STATS, 1, Arrays.asList("1.0.0")),

  BLOOM_FILTERS_ONE(MetadataPartitionType.BLOOM_FILTERS, 1, Arrays.asList("1.0.0")),

  EXPRESSION_INDEX_ONE(MetadataPartitionType.EXPRESSION_INDEX, 1, Arrays.asList("1.0.0")),

  SECONDARY_INDEX_ONE(MetadataPartitionType.SECONDARY_INDEX, 1, Arrays.asList("1.0.0")),
  SECONDARY_INDEX_TWO(MetadataPartitionType.SECONDARY_INDEX, 2, Arrays.asList("1.1.0"));

  private final MetadataPartitionType partitionType;
  private final int versionCode;
  private final List<String> releaseVersions;

  HoodieIndexVersion(MetadataPartitionType partitionType, int versionCode, List<String> releaseVersions) {
    this.partitionType = partitionType;
    this.versionCode = versionCode;
    this.releaseVersions = releaseVersions;
  }

  public MetadataPartitionType getPartitionType() {
    return partitionType;
  }

  public int versionCode() {
    return versionCode;
  }

  public List<String> getReleaseVersions() {
    return releaseVersions;
  }

  public static HoodieIndexVersion getCurrentVersion(String partitionType) {
    return getCurrentVersion(MetadataPartitionType.valueOf(partitionType.toUpperCase()));
  }

  public static HoodieIndexVersion getInitialVersion(String partitionType) {
    return getInitialVersion(MetadataPartitionType.valueOf(partitionType.toUpperCase()));
  }

  public static HoodieIndexVersion getCurrentVersion(MetadataPartitionType partitionType) {
    String configDefault;
    if (partitionType == MetadataPartitionType.RECORD_INDEX) {
      configDefault = HOODIE_RECORD_INDEX_VERSION.defaultValue();
    } else if (partitionType == MetadataPartitionType.COLUMN_STATS) {
      configDefault = HOODIE_COLUMN_STATS_INDEX_VERSION.defaultValue();
    } else if (partitionType == MetadataPartitionType.BLOOM_FILTERS) {
      configDefault = HOODIE_BLOOM_FILTERS_INDEX_VERSION.defaultValue();
    } else if (partitionType == MetadataPartitionType.EXPRESSION_INDEX) {
      configDefault = HOODIE_EXPRESSION_INDEX_VERSION.defaultValue();
    } else if (partitionType == MetadataPartitionType.SECONDARY_INDEX) {
      configDefault = HOODIE_SECONDARY_INDEX_VERSION.defaultValue();
    } else if (partitionType == MetadataPartitionType.FILES) {
      configDefault = HOODIE_FILES_INDEX_VERSION.defaultValue();
    } else if (partitionType == MetadataPartitionType.PARTITION_STATS) {
      configDefault = HOODIE_PARTITION_STATS_VERSION.defaultValue();
    } else if (partitionType == MetadataPartitionType.ALL_PARTITIONS) {
      configDefault = HOODIE_ALL_PARTITIONS_INDEX_VERSION.defaultValue();
    } else {
      throw new HoodieException("Unknown metadata partition type: " + partitionType);
    }

    return HoodieIndexVersion.valueOf(configDefault);
  }

  public static HoodieIndexVersion getInitialVersion(MetadataPartitionType partitionType) {
    if (partitionType == MetadataPartitionType.RECORD_INDEX) {
      return RECORD_INDEX_ONE;
    } else if (partitionType == MetadataPartitionType.COLUMN_STATS) {
      return COLUMN_STATS_ONE;
    } else if (partitionType == MetadataPartitionType.BLOOM_FILTERS) {
      return BLOOM_FILTERS_ONE;
    } else if (partitionType == MetadataPartitionType.EXPRESSION_INDEX) {
      return EXPRESSION_INDEX_ONE;
    } else if (partitionType == MetadataPartitionType.SECONDARY_INDEX) {
      return SECONDARY_INDEX_ONE;
    } else if (partitionType == MetadataPartitionType.FILES) {
      return FILES_INDEX_ONE;
    } else if (partitionType == MetadataPartitionType.ALL_PARTITIONS) {
      return ALL_PARTITIONS_ONE;
    } else if (partitionType == MetadataPartitionType.PARTITION_STATS) {
      return PARTITION_STATS_ONE;
    } else {
      throw new HoodieException("Unknown metadata partition type: " + partitionType);
    }
  }

  public boolean greaterThan(HoodieIndexVersion other) {
    checkSamePartitionType(other);
    return this.versionCode > other.versionCode;
  }

  public boolean greaterThanOrEquals(HoodieIndexVersion other) {
    checkSamePartitionType(other);
    return this.versionCode >= other.versionCode;
  }

  public boolean lowerThan(HoodieIndexVersion other) {
    checkSamePartitionType(other);
    return this.versionCode < other.versionCode;
  }

  public boolean lowerThanOrEquals(HoodieIndexVersion other) {
    checkSamePartitionType(other);
    return this.versionCode <= other.versionCode;
  }

  private void checkSamePartitionType(HoodieIndexVersion other) {
    checkIsOfPartitionType(other.getPartitionType());
  }

  public void checkIsOfPartitionType(MetadataPartitionType partitionType) {
    ValidationUtils.checkArgument(this.partitionType.equals(partitionType),
        "Hoodie index version partition type mismatches with the incoming "
          + "one: Expected" + this.partitionType + ", got " + partitionType);
  }

  @Override
  public String toString() {
    return name();
  }
}
