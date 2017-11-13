/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CompactionWriteStat implements Serializable {

  private HoodieWriteStat writeStat;
  private String partitionPath;
  private long totalLogRecords;
  private long totalLogFiles;
  private long totalRecordsToBeUpdate;

  public CompactionWriteStat(HoodieWriteStat writeStat, String partitionPath, long totalLogFiles,
      long totalLogRecords,
      long totalRecordsToUpdate) {
    this.writeStat = writeStat;
    this.partitionPath = partitionPath;
    this.totalLogFiles = totalLogFiles;
    this.totalLogRecords = totalLogRecords;
    this.totalRecordsToBeUpdate = totalRecordsToUpdate;
  }

  public CompactionWriteStat() {
    // For de-serialization
  }

  public long getTotalLogRecords() {
    return totalLogRecords;
  }

  public long getTotalLogFiles() {
    return totalLogFiles;
  }

  public long getTotalRecordsToBeUpdate() {
    return totalRecordsToBeUpdate;
  }

  public HoodieWriteStat getHoodieWriteStat() {
    return writeStat;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private HoodieWriteStat writeStat;
    private long totalLogRecords;
    private long totalRecordsToUpdate;
    private long totalLogFiles;
    private String partitionPath;


    public Builder withHoodieWriteStat(HoodieWriteStat writeStat) {
      this.writeStat = writeStat;
      return this;
    }

    public Builder setTotalLogRecords(long records) {
      this.totalLogRecords = records;
      return this;
    }

    public Builder setTotalLogFiles(long totalLogFiles) {
      this.totalLogFiles = totalLogFiles;
      return this;
    }

    public Builder setTotalRecordsToUpdate(long records) {
      this.totalRecordsToUpdate = records;
      return this;
    }

    public Builder onPartition(String path) {
      this.partitionPath = path;
      return this;
    }

    public CompactionWriteStat build() {
      return new CompactionWriteStat(writeStat, partitionPath, totalLogFiles, totalLogRecords,
          totalRecordsToUpdate);
    }
  }
}
