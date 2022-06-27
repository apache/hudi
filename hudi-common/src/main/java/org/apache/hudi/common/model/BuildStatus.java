/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.secondary.index.HoodieSecondaryIndex;

import java.util.List;

public class BuildStatus {
  private final String partition;
  private final String fileName;
  private final List<HoodieSecondaryIndex> secondaryIndexes;

  private final long totalRecords;
  private final long totalBytes;

  public BuildStatus(String partition,
                     String fileName,
                     List<HoodieSecondaryIndex> secondaryIndexes,
                     long totalRecords,
                     long totalBytes) {
    this.partition = partition;
    this.fileName = fileName;
    this.secondaryIndexes = secondaryIndexes;
    this.totalRecords = totalRecords;
    this.totalBytes = totalBytes;
  }

  public String getPartition() {
    return partition;
  }

  public String getFileName() {
    return fileName;
  }

  public List<HoodieSecondaryIndex> getSecondaryIndexes() {
    return secondaryIndexes;
  }

  public long getTotalRecords() {
    return totalRecords;
  }

  public long getTotalBytes() {
    return totalBytes;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String partition;
    private String baseFilePath;
    private List<HoodieSecondaryIndex> secondaryIndexes;
    private long totalRecords;
    private long totalBytes;

    public Builder setPartition(String partition) {
      this.partition = partition;
      return this;
    }

    public Builder setBaseFilePath(String baseFilePath) {
      this.baseFilePath = baseFilePath;
      return this;
    }

    public Builder setSecondaryIndexes(List<HoodieSecondaryIndex> secondaryIndexes) {
      this.secondaryIndexes = secondaryIndexes;
      return this;
    }

    public Builder setTotalRecords(long totalRecords) {
      this.totalRecords = totalRecords;
      return this;
    }

    public Builder setTotalBytes(long totalBytes) {
      this.totalBytes = totalBytes;
      return this;
    }

    public BuildStatus build() {
      return new BuildStatus(partition, baseFilePath, secondaryIndexes, totalRecords, totalBytes);
    }
  }
}
