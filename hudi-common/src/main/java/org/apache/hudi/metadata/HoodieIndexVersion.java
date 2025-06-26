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

import java.util.Arrays;
import java.util.List;

public enum HoodieIndexVersion {
  // Implementation comes in downstream commits
  DUMMY(MetadataPartitionType.PARTITION_STATS, 1, Arrays.asList(""));

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

  public static HoodieIndexVersion getCurrentVersion(HoodieTableVersion tableVersion, String partitionPath) {
    return getCurrentVersion(tableVersion, MetadataPartitionType.fromPartitionPath(partitionPath));
  }

  public static HoodieIndexVersion getCurrentVersion(HoodieTableVersion tableVersion, MetadataPartitionType partitionType) {
    return null;
  }

  public static boolean isValidIndexDefinition(HoodieTableVersion tv, HoodieIndexDefinition idxDef) {
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

  public void ensureVersionCanBeAssignedToIndexType(MetadataPartitionType partitionType) {
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
