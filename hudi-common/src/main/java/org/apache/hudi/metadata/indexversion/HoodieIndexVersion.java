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

package org.apache.hudi.metadata.indexversion;

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.metadata.MetadataPartitionType;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = RecordIndexVersion.class, name = "RECORD_INDEX"),
    @JsonSubTypes.Type(value = ColumnStatsIndexVersion.class, name = "COLUMN_STATS"),
    @JsonSubTypes.Type(value = BloomFiltersIndexVersion.class, name = "BLOOM_FILTERS"),
    @JsonSubTypes.Type(value = ExpressionIndexVersion.class, name = "EXPRESSION_INDEX"),
    @JsonSubTypes.Type(value = SecondaryIndexVersion.class, name = "SECONDARY_INDEX"),
    @JsonSubTypes.Type(value = FilesIndexVersion.class, name = "FILES"),
    @JsonSubTypes.Type(value = PartitionStatsIndexVersion.class, name = "PARTITION_STATS"),
    @JsonSubTypes.Type(value = AllPartitionsIndexVersion.class, name = "ALL_PARTITIONS"),
})
public interface HoodieIndexVersion {

  MetadataPartitionType getPartitionType();

  int versionCode();

  List<String> getReleaseVersions();

  default boolean greaterThan(HoodieIndexVersion other) {
    checkSamePartitionType(other);
    return this.versionCode() > other.versionCode();
  }

  default boolean greaterThanOrEquals(HoodieIndexVersion other) {
    checkSamePartitionType(other);
    return this.versionCode() >= other.versionCode();
  }

  default boolean lowerThan(HoodieIndexVersion other) {
    checkSamePartitionType(other);
    return this.versionCode() < other.versionCode();
  }

  default boolean lowerThanOrEquals(HoodieIndexVersion other) {
    checkSamePartitionType(other);
    return this.versionCode() <= other.versionCode();
  }

  default void ensureVersionCanBeAssignedToIndexType(MetadataPartitionType partitionType) {
    if (!getPartitionType().equals(partitionType)
        && !getPartitionType().equals(MetadataPartitionType.EXPRESSION_INDEX)) {
      throw new IllegalArgumentException(String.format(
          "Hoodie index version %s is not allowed to be assigned to partition type %s",
          this, partitionType));
    }
  }

  default void checkSamePartitionType(HoodieIndexVersion other) {
    if (!this.getPartitionType().equals(other.getPartitionType())) {
      throw new IllegalArgumentException(String.format(
          "Hoodie index version partition type mismatch: expected %s, got %s",
          this.getPartitionType(), other.getPartitionType()));
    }
  }

  static HoodieIndexVersion getCurrentVersion(HoodieTableVersion tableVersion, String partitionPath) {
    MetadataPartitionType partitionType = MetadataPartitionType.fromPartitionPath(partitionPath);
    return getCurrentVersion(tableVersion, partitionType);
  }

  static HoodieIndexVersion getCurrentVersion(HoodieTableVersion tableVersion, MetadataPartitionType partitionType) {
    if (partitionType == MetadataPartitionType.RECORD_INDEX) {
      return RecordIndexVersion.V1;
    } else if (partitionType == MetadataPartitionType.COLUMN_STATS) {
      return ColumnStatsIndexVersion.V1;
    } else if (partitionType == MetadataPartitionType.BLOOM_FILTERS) {
      return BloomFiltersIndexVersion.V1;
    } else if (partitionType == MetadataPartitionType.EXPRESSION_INDEX) {
      return ExpressionIndexVersion.V1;
    } else if (partitionType == MetadataPartitionType.SECONDARY_INDEX) {
      if (tableVersion.greaterThanOrEquals(HoodieTableVersion.NINE)) {
        return SecondaryIndexVersion.V2;
      }
      return SecondaryIndexVersion.V1;
    } else if (partitionType == MetadataPartitionType.FILES) {
      return FilesIndexVersion.V1;
    } else if (partitionType == MetadataPartitionType.PARTITION_STATS) {
      return PartitionStatsIndexVersion.V1;
    } else if (partitionType == MetadataPartitionType.ALL_PARTITIONS) {
      return AllPartitionsIndexVersion.V1;
    } else {
      throw new IllegalArgumentException("Unknown metadata partition type: " + partitionType);
    }
  }

  static boolean isValidIndexDefinition(HoodieTableVersion tv, HoodieIndexDefinition idxDef) {
    HoodieIndexVersion iv = idxDef.getVersion();
    MetadataPartitionType metadataPartitionType = MetadataPartitionType.fromPartitionPath(idxDef.getIndexName());

    // Table version 8: missing version allowed
    if (tv == HoodieTableVersion.EIGHT && iv == null) {
      return true;
    }
    // Table version 8: SI must be V1
    if (tv == HoodieTableVersion.EIGHT
        && metadataPartitionType == MetadataPartitionType.SECONDARY_INDEX
        && iv != SecondaryIndexVersion.V1) {
      return false;
    }
    // Table version 9: SI version required
    if (tv == HoodieTableVersion.NINE
        && metadataPartitionType == MetadataPartitionType.SECONDARY_INDEX
        && iv == null) {
      return false;
    }
    // Table version 9: SI must be V2+
    if (tv == HoodieTableVersion.NINE
        && metadataPartitionType == MetadataPartitionType.SECONDARY_INDEX
        && !iv.greaterThanOrEquals(SecondaryIndexVersion.V2)) {
      return false;
    }
    return true;
  }
}
