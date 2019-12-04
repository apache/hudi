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
  // Earliest commit that was retained in this clean
  private final String earliestCommitToRetain;

  public HoodieCleanStat(HoodieCleaningPolicy policy, String partitionPath, List<String> deletePathPatterns,
      List<String> successDeleteFiles, List<String> failedDeleteFiles, String earliestCommitToRetain) {
    this.policy = policy;
    this.partitionPath = partitionPath;
    this.deletePathPatterns = deletePathPatterns;
    this.successDeleteFiles = successDeleteFiles;
    this.failedDeleteFiles = failedDeleteFiles;
    this.earliestCommitToRetain = earliestCommitToRetain;
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

  public String getEarliestCommitToRetain() {
    return earliestCommitToRetain;
  }

  public static HoodieCleanStat.Builder newBuilder() {
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

    public Builder withPartitionPath(String partitionPath) {
      this.partitionPath = partitionPath;
      return this;
    }

    public Builder withEarliestCommitRetained(Option<HoodieInstant> earliestCommitToRetain) {
      this.earliestCommitToRetain =
          (earliestCommitToRetain.isPresent()) ? earliestCommitToRetain.get().getTimestamp() : "";
      return this;
    }

    public HoodieCleanStat build() {
      return new HoodieCleanStat(policy, partitionPath, deletePathPatterns, successDeleteFiles, failedDeleteFiles,
          earliestCommitToRetain);
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
        + ", earliestCommitToRetain='" + earliestCommitToRetain + '\''
        + '}';
  }
}
