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

import org.apache.hadoop.fs.FileStatus;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Collects stats about a single partition clean operation.
 */
public class HoodieRollbackStat implements Serializable {

  // Partition path
  private final String partitionPath;
  private final List<String> successDeleteFiles;
  // Files that could not be deleted
  private final List<String> failedDeleteFiles;
  // Count of HoodieLogFile to commandBlocks written for a particular rollback
  private final Map<FileStatus, Long> commandBlocksCount;
  // all log files with same base instant as instant to be rolledback
  private final Map<FileStatus, Long> writtenLogFileSizeMap;

  public HoodieRollbackStat(String partitionPath, List<String> successDeleteFiles, List<String> failedDeleteFiles,
      Map<FileStatus, Long> commandBlocksCount, Map<FileStatus, Long> writtenLogFileSizeMap) {
    this.partitionPath = partitionPath;
    this.successDeleteFiles = successDeleteFiles;
    this.failedDeleteFiles = failedDeleteFiles;
    this.commandBlocksCount = commandBlocksCount;
    this.writtenLogFileSizeMap = writtenLogFileSizeMap;
  }

  public Map<FileStatus, Long> getCommandBlocksCount() {
    return commandBlocksCount;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public List<String> getSuccessDeleteFiles() {
    return successDeleteFiles;
  }

  public List<String> getFailedDeleteFiles() {
    return failedDeleteFiles;
  }

  public Map<FileStatus, Long> getWrittenLogFileSizeMap() {
    return writtenLogFileSizeMap;
  }

  public static HoodieRollbackStat.Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder used to build {@link HoodieRollbackStat}.
   */
  public static class Builder {

    private List<String> successDeleteFiles;
    private List<String> failedDeleteFiles;
    private Map<FileStatus, Long> commandBlocksCount;
    private Map<FileStatus, Long> writtenLogFileSizeMap;
    private String partitionPath;

    public Builder withDeletedFileResults(Map<FileStatus, Boolean> deletedFiles) {
      // noinspection Convert2MethodRef
      successDeleteFiles = deletedFiles.entrySet().stream().filter(s -> s.getValue())
          .map(s -> s.getKey().getPath().toString()).collect(Collectors.toList());
      failedDeleteFiles = deletedFiles.entrySet().stream().filter(s -> !s.getValue())
          .map(s -> s.getKey().getPath().toString()).collect(Collectors.toList());
      return this;
    }

    public Builder withDeletedFileResult(String fileName, boolean isDeleted) {
      if (isDeleted) {
        successDeleteFiles = Collections.singletonList(fileName);
      } else {
        failedDeleteFiles = Collections.singletonList(fileName);
      }
      return this;
    }

    public Builder withRollbackBlockAppendResults(Map<FileStatus, Long> commandBlocksCount) {
      this.commandBlocksCount = commandBlocksCount;
      return this;
    }

    public Builder withWrittenLogFileSizeMap(Map<FileStatus, Long> writtenLogFileSizeMap) {
      this.writtenLogFileSizeMap = writtenLogFileSizeMap;
      return this;
    }

    public Builder withPartitionPath(String partitionPath) {
      this.partitionPath = partitionPath;
      return this;
    }

    public HoodieRollbackStat build() {
      if (successDeleteFiles == null) {
        successDeleteFiles = Collections.EMPTY_LIST;
      }
      if (failedDeleteFiles == null) {
        failedDeleteFiles = Collections.EMPTY_LIST;
      }
      if (commandBlocksCount == null) {
        commandBlocksCount = Collections.EMPTY_MAP;
      }
      if (writtenLogFileSizeMap == null) {
        writtenLogFileSizeMap = Collections.EMPTY_MAP;
      }
      return new HoodieRollbackStat(partitionPath, successDeleteFiles, failedDeleteFiles, commandBlocksCount, writtenLogFileSizeMap);
    }
  }
}
