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

package org.apache.hudi.common;

import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.List;

/**
 * Collects stats about a single partition clean operation.
 */
public class HoodieCleanStat implements Serializable {

  // Policy used
  private final HoodieCleaningPolicy policy;
  // Partition path cleaned
  private final String partitionPath;
  // The patterns that were generated for the delete operation
  private final List<String> deletePathPatterns;
  private final List<String> successDeleteFiles;
  // Files that could not be deleted
  private final List<String> failedDeleteFiles;
  // Bootstrap Base Path patterns that were generated for the delete operation
  private final List<String> deleteBootstrapBasePathPatterns;
  private final List<String> successDeleteBootstrapBaseFiles;
  // Files that could not be deleted
  private final List<String> failedDeleteBootstrapBaseFiles;
  // Earliest commit that was retained in this clean
  private final String earliestCommitToRetain;
  // Last completed commit timestamp before clean
  private final String lastCompletedCommitTimestamp;
  // set to true if partition is deleted
  private final boolean isPartitionDeleted;

  public HoodieCleanStat(HoodieCleaningPolicy policy, String partitionPath, List<String> deletePathPatterns,
      List<String> successDeleteFiles, List<String> failedDeleteFiles, String earliestCommitToRetain,String lastCompletedCommitTimestamp) {
    this(policy, partitionPath, deletePathPatterns, successDeleteFiles, failedDeleteFiles, earliestCommitToRetain,
        lastCompletedCommitTimestamp, CollectionUtils.createImmutableList(), CollectionUtils.createImmutableList(),
        CollectionUtils.createImmutableList(), false);
  }

  public HoodieCleanStat(HoodieCleaningPolicy policy, String partitionPath, List<String> deletePathPatterns,
                         List<String> successDeleteFiles, List<String> failedDeleteFiles,
                         String earliestCommitToRetain,String lastCompletedCommitTimestamp,
                         List<String> deleteBootstrapBasePathPatterns,
                         List<String> successDeleteBootstrapBaseFiles,
                         List<String> failedDeleteBootstrapBaseFiles,
                         boolean isPartitionDeleted) {
    this.policy = policy;
    this.partitionPath = partitionPath;
    this.deletePathPatterns = deletePathPatterns;
    this.successDeleteFiles = successDeleteFiles;
    this.failedDeleteFiles = failedDeleteFiles;
    this.earliestCommitToRetain = earliestCommitToRetain;
    this.lastCompletedCommitTimestamp = lastCompletedCommitTimestamp;
    this.deleteBootstrapBasePathPatterns = deleteBootstrapBasePathPatterns;
    this.successDeleteBootstrapBaseFiles = successDeleteBootstrapBaseFiles;
    this.failedDeleteBootstrapBaseFiles = failedDeleteBootstrapBaseFiles;
    this.isPartitionDeleted = isPartitionDeleted;
  }

  public HoodieCleaningPolicy getPolicy() {
    return policy;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public List<String> getDeletePathPatterns() {
    return deletePathPatterns;
  }

  public List<String> getSuccessDeleteFiles() {
    return successDeleteFiles;
  }

  public List<String> getFailedDeleteFiles() {
    return failedDeleteFiles;
  }

  public List<String> getDeleteBootstrapBasePathPatterns() {
    return deleteBootstrapBasePathPatterns;
  }

  public List<String> getSuccessDeleteBootstrapBaseFiles() {
    return successDeleteBootstrapBaseFiles;
  }

  public List<String> getFailedDeleteBootstrapBaseFiles() {
    return failedDeleteBootstrapBaseFiles;
  }

  public String getEarliestCommitToRetain() {
    return earliestCommitToRetain;
  }

  public String getLastCompletedCommitTimestamp() {
    return lastCompletedCommitTimestamp;
  }

  public boolean isPartitionDeleted() {
    return isPartitionDeleted;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder used to build {@link HoodieCleanStat}.
   */
  public static class Builder {

    private HoodieCleaningPolicy policy;
    private List<String> deletePathPatterns;
    private List<String> successDeleteFiles;
    private List<String> failedDeleteFiles;
    private String partitionPath;
    private String earliestCommitToRetain;
    private String lastCompletedCommitTimestamp;
    private List<String> deleteBootstrapBasePathPatterns;
    private List<String> successDeleteBootstrapBaseFiles;
    private List<String> failedDeleteBootstrapBaseFiles;
    private boolean isPartitionDeleted;

    public Builder withPolicy(HoodieCleaningPolicy policy) {
      this.policy = policy;
      return this;
    }

    public Builder withDeletePathPattern(List<String> deletePathPatterns) {
      this.deletePathPatterns = deletePathPatterns;
      return this;
    }

    public Builder withSuccessfulDeletes(List<String> successDeleteFiles) {
      this.successDeleteFiles = successDeleteFiles;
      return this;
    }

    public Builder withFailedDeletes(List<String> failedDeleteFiles) {
      this.failedDeleteFiles = failedDeleteFiles;
      return this;
    }

    public Builder withDeleteBootstrapBasePathPatterns(List<String> deletePathPatterns) {
      this.deleteBootstrapBasePathPatterns = deletePathPatterns;
      return this;
    }

    public Builder withSuccessfulDeleteBootstrapBaseFiles(List<String> successDeleteFiles) {
      this.successDeleteBootstrapBaseFiles = successDeleteFiles;
      return this;
    }

    public Builder withFailedDeleteBootstrapBaseFiles(List<String> failedDeleteFiles) {
      this.failedDeleteBootstrapBaseFiles = failedDeleteFiles;
      return this;
    }

    public Builder withPartitionPath(String partitionPath) {
      this.partitionPath = partitionPath;
      return this;
    }

    public Builder withEarliestCommitRetained(Option<HoodieInstant> earliestCommitToRetain) {
      this.earliestCommitToRetain =
          (earliestCommitToRetain.isPresent()) ? earliestCommitToRetain.get().requestedTime() : "";
      return this;
    }

    public Builder withLastCompletedCommitTimestamp(String lastCompletedCommitTimestamp) {
      this.lastCompletedCommitTimestamp = lastCompletedCommitTimestamp;
      return this;
    }

    public Builder isPartitionDeleted(boolean isPartitionDeleted) {
      this.isPartitionDeleted = isPartitionDeleted;
      return this;
    }

    public HoodieCleanStat build() {
      return new HoodieCleanStat(policy, partitionPath, deletePathPatterns, successDeleteFiles, failedDeleteFiles,
          earliestCommitToRetain, lastCompletedCommitTimestamp, deleteBootstrapBasePathPatterns,
        successDeleteBootstrapBaseFiles, failedDeleteBootstrapBaseFiles, isPartitionDeleted);
    }
  }

  @Override
  public String toString() {
    return "HoodieCleanStat{"
        + "policy=" + policy
        + ", partitionPath='" + partitionPath + '\''
        + ", deletePathPatterns=" + deletePathPatterns
        + ", successDeleteFiles=" + successDeleteFiles
        + ", failedDeleteFiles=" + failedDeleteFiles
        + ", earliestCommitToRetain='" + earliestCommitToRetain
        + ", deleteBootstrapBasePathPatterns=" + deleteBootstrapBasePathPatterns
        + ", successDeleteBootstrapBaseFiles=" + successDeleteBootstrapBaseFiles
        + ", failedDeleteBootstrapBaseFiles=" + failedDeleteBootstrapBaseFiles
        + ", isPartitionDeleted=" + isPartitionDeleted + '\''
        + '}';
  }
}
