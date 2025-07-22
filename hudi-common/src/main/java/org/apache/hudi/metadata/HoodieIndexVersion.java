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
import org.apache.hudi.exception.HoodieException;

/**
 * Enum representing different versions of Hoodie indexes.
 *
 * <p>This enum defines the supported versions of metadata indexes in Apache Hudi.
 * Each version may have different features, performance characteristics, and compatibility
 * requirements. The versioning system allows for evolution of the metadata index format
 * while maintaining backward compatibility.</p>
 *
 * <p>Version codes are used for comparison and determining compatibility between
 * different index versions for the same index type. Different indexes may have different
 * version codes. Versions adopted by one index is not comparable with another index.</p>
 */
public enum HoodieIndexVersion {
  V1(1),
  V2(2);

  private final int versionCode;

  /**
   * Constructor for HoodieIndexVersion.
   *
   * @param versionCode the numeric version code for this index version
   */
  HoodieIndexVersion(int versionCode) {
    this.versionCode = versionCode;
  }

  /**
   * Returns the numeric version code for this index version.
   *
   * @return the version code as an integer
   */
  public int versionCode() {
    return versionCode;
  }

  /**
   * Gets the current index version for a given table version and partition path.
   *
   * <p>This method determines the appropriate index version based on the table version
   * and the specific partition path. The partition path is used to identify the type
   * of metadata partition being accessed.</p>
   *
   * @param tableVersion the version of the Hoodie table
   * @param partitionPath the partition path to determine the metadata partition type
   * @return the appropriate HoodieIndexVersion for the given parameters
   */
  public static HoodieIndexVersion getCurrentVersion(HoodieTableVersion tableVersion, String partitionPath) {
    return getCurrentVersion(tableVersion, MetadataPartitionType.fromPartitionPath(partitionPath));
  }

  /**
   * Gets the current index version for a given table version and metadata partition type.
   *
   * <p>This method determines the appropriate index version based on the table version
   * and the specific metadata partition type. Different partition types may support
   * different index versions.</p>
   *
   * @param tableVersion the version of the Hoodie table
   * @param partitionType the type of metadata partition
   * @return the appropriate HoodieIndexVersion for the given parameters
   */
  public static HoodieIndexVersion getCurrentVersion(HoodieTableVersion tableVersion, MetadataPartitionType partitionType) {
    if (partitionType == MetadataPartitionType.RECORD_INDEX) {
      return V1;
    } else if (partitionType == MetadataPartitionType.COLUMN_STATS) {
      return V1;
    } else if (partitionType == MetadataPartitionType.BLOOM_FILTERS) {
      return V1;
    } else if (partitionType == MetadataPartitionType.EXPRESSION_INDEX) {
      return V1;
    } else if (partitionType == MetadataPartitionType.SECONDARY_INDEX) {
      if (tableVersion.greaterThanOrEquals(HoodieTableVersion.NINE)) {
        return V2;
      }
      return V1;
    } else if (partitionType == MetadataPartitionType.FILES) {
      return V1;
    } else if (partitionType == MetadataPartitionType.PARTITION_STATS) {
      return V1;
    } else if (partitionType == MetadataPartitionType.ALL_PARTITIONS) {
      return V1;
    } else {
      throw new HoodieException("Unknown metadata partition type: " + partitionType);
    }
  }

  /**
   * Validates if an index definition is valid for a given table version.
   *
   * <p>This method checks whether the provided index definition is compatible
   * with the specified table version. Different table versions may have different
   * requirements for index definitions.</p>
   *
   * @param tv the table version to validate against
   * @param idxDef the index definition to validate
   * @return true if the index definition is valid for the table version, false otherwise
   */
  public static boolean isValidIndexDefinition(HoodieTableVersion tv, HoodieIndexDefinition idxDef) {
    HoodieIndexVersion iv = idxDef.getVersion();
    MetadataPartitionType metadataPartitionType = MetadataPartitionType.fromPartitionPath(idxDef.getIndexName());
    // Table version 8, missing version attribute is allowed.
    if (tv == HoodieTableVersion.EIGHT && iv == null) {
      return true;
    }
    // Table version 9, missing version attribute NOT is allowed.
    if (tv.greaterThanOrEquals(HoodieTableVersion.NINE) && iv == null) {
      return false;
    }
    // Table version eight, SI only v1 is allowed.
    if (tv == HoodieTableVersion.EIGHT && MetadataPartitionType.SECONDARY_INDEX.equals(metadataPartitionType) && iv != HoodieIndexVersion.V1) {
      return false;
    }

    return true;
  }

  /**
   * Checks if this index version is greater than the specified version.
   *
   * @param other the index version to compare against
   * @return true if this version is greater than the other version, false otherwise
   */
  public boolean greaterThan(HoodieIndexVersion other) {
    return this.versionCode > other.versionCode;
  }

  /**
   * Checks if this index version is greater than or equal to the specified version.
   *
   * @param other the index version to compare against
   * @return true if this version is greater than or equal to the other version, false otherwise
   */
  public boolean greaterThanOrEquals(HoodieIndexVersion other) {
    return this.versionCode >= other.versionCode;
  }

  /**
   * Checks if this index version is lower than the specified version.
   *
   * @param other the index version to compare against
   * @return true if this version is lower than the other version, false otherwise
   */
  public boolean lowerThan(HoodieIndexVersion other) {
    return this.versionCode < other.versionCode;
  }

  /**
   * Checks if this index version is lower than or equal to the specified version.
   *
   * @param other the index version to compare against
   * @return true if this version is lower than or equal to the other version, false otherwise
   */
  public boolean lowerThanOrEquals(HoodieIndexVersion other) {
    return this.versionCode <= other.versionCode;
  }

  /**
   * Returns the string representation of this index version.
   *
   * @return the name of this enum constant
   */
  @Override
  public String toString() {
    return name();
  }
}
