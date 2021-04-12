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

package org.apache.hudi.common.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Unique ID to identify a file-group in a data-set.
 */
public class HoodieFileGroupId implements Serializable, Comparable<HoodieFileGroupId> {

  private final String partitionPath;

  private final String fileId;

  public HoodieFileGroupId(String partitionPath, String fileId) {
    this.partitionPath = partitionPath;
    this.fileId = fileId;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getFileId() {
    return fileId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieFileGroupId that = (HoodieFileGroupId) o;
    return Objects.equals(partitionPath, that.partitionPath) && Objects.equals(fileId, that.fileId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionPath, fileId);
  }

  @Override
  public String toString() {
    return "HoodieFileGroupId{partitionPath='" + partitionPath + '\'' + ", fileId='" + fileId + '\'' + '}';
  }

  @Override
  public int compareTo(HoodieFileGroupId o) {
    int ret = partitionPath.compareTo(o.partitionPath);
    if (ret == 0) {
      ret = fileId.compareTo(o.fileId);
    }
    return ret;
  }
}
