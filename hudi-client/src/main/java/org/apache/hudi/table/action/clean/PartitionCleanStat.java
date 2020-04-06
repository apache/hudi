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

package org.apache.hudi.table.action.clean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class PartitionCleanStat implements Serializable {

  private final String partitionPath;
  private final List<String> deletePathPatterns = new ArrayList<>();
  private final List<String> successDeleteFiles = new ArrayList<>();
  private final List<String> failedDeleteFiles = new ArrayList<>();

  PartitionCleanStat(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  void addDeletedFileResult(String deletePathStr, Boolean deletedFileResult) {
    if (deletedFileResult) {
      successDeleteFiles.add(deletePathStr);
    } else {
      failedDeleteFiles.add(deletePathStr);
    }
  }

  void addDeleteFilePatterns(String deletePathStr) {
    deletePathPatterns.add(deletePathStr);
  }

  PartitionCleanStat merge(PartitionCleanStat other) {
    if (!this.partitionPath.equals(other.partitionPath)) {
      throw new RuntimeException(
          String.format("partitionPath is not a match: (%s, %s)", partitionPath, other.partitionPath));
    }
    successDeleteFiles.addAll(other.successDeleteFiles);
    deletePathPatterns.addAll(other.deletePathPatterns);
    failedDeleteFiles.addAll(other.failedDeleteFiles);
    return this;
  }

  public List<String> deletePathPatterns() {
    return deletePathPatterns;
  }

  public List<String> successDeleteFiles() {
    return successDeleteFiles;
  }

  public List<String> failedDeleteFiles() {
    return failedDeleteFiles;
  }
}
