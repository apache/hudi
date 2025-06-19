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

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import java.util.Arrays;
import java.util.List;

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

  public static HoodieIndexVersion getCurrentVersion(HoodieTableVersion tableVersion, String partitionType) {
    return getCurrentVersion(tableVersion, MetadataPartitionType.valueOf(partitionType.toUpperCase()));
  }

  public static HoodieIndexVersion getCurrentVersion(HoodieTableVersion tableVersion, MetadataPartitionType partitionType) {
    if (partitionType == MetadataPartitionType.RECORD_INDEX) {
      return RECORD_INDEX_ONE;
    } else if (partitionType == MetadataPartitionType.COLUMN_STATS) {
      return COLUMN_STATS_ONE;
    } else if (partitionType == MetadataPartitionType.BLOOM_FILTERS) {
      return BLOOM_FILTERS_ONE;
    } else if (partitionType == MetadataPartitionType.EXPRESSION_INDEX) {
      return EXPRESSION_INDEX_ONE;
    } else if (partitionType == MetadataPartitionType.SECONDARY_INDEX) {
      if (tableVersion.greaterThanOrEquals(HoodieTableVersion.NINE)) {
        return SECONDARY_INDEX_TWO;
      }
      return SECONDARY_INDEX_ONE;
    } else if (partitionType == MetadataPartitionType.FILES) {
      return FILES_INDEX_ONE;
    } else if (partitionType == MetadataPartitionType.PARTITION_STATS) {
      return PARTITION_STATS_ONE;
    } else if (partitionType == MetadataPartitionType.ALL_PARTITIONS) {
      return ALL_PARTITIONS_ONE;
    } else {
      throw new HoodieException("Unknown metadata partition type: " + partitionType);
    }
  }

  public static boolean isValidIndexDefinition(HoodieTableVersion tv, HoodieIndexDefinition idxDef) {
    HoodieIndexVersion iv = idxDef.getVersion();
    MetadataPartitionType metadataPartitionType = MetadataPartitionType.fromPartitionPath(idxDef.getIndexName());
    // Table version 8, missing version attribute is allowed.
    if (tv == HoodieTableVersion.EIGHT && iv == null) {
      return true;
    }
    // Table version eight, SI only v1 is allowed.
    if (tv == HoodieTableVersion.EIGHT && MetadataPartitionType.SECONDARY_INDEX.equals(metadataPartitionType) && iv != HoodieIndexVersion.SECONDARY_INDEX_ONE) {
      return false;
    }
    // Table version 9, SI must have none null version.
    if (tv == HoodieTableVersion.NINE && MetadataPartitionType.SECONDARY_INDEX.equals(metadataPartitionType) && iv == null) {
      return false;
    }
    // Table version 9, SI must be v2 or above.
    if (tv == HoodieTableVersion.NINE && MetadataPartitionType.SECONDARY_INDEX.equals(metadataPartitionType) && !iv.greaterThanOrEquals(HoodieIndexVersion.SECONDARY_INDEX_TWO)) {
      return false;
    }
    return true;
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
    ValidationUtils.checkArgument(this.partitionType.equals(other.partitionType),
        "Hoodie index version partition type mismatches with the incoming "
            + "one: Expected" + this.partitionType + ", got " + other.partitionType);
  }

  public void ensureVersionCanBeAssignedToPartitionType(MetadataPartitionType partitionType) {
    ValidationUtils.checkArgument(this.partitionType.equals(partitionType)
        || this.partitionType.equals(MetadataPartitionType.EXPRESSION_INDEX),
        String.format("Hoodie index version %s is not allowed to be assigned to partition type %s",
          this, partitionType));
  }

  @Override
  public String toString() {
    return name();
  }
}
