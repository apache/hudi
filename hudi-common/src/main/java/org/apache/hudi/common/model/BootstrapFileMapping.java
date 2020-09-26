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
import org.apache.hudi.avro.model.HoodieFileStatus;

/**
 * Value stored in the bootstrap index.
 */
public class BootstrapFileMapping implements Serializable, Comparable<BootstrapFileMapping> {

  private final String bootstrapBasePath;
  private final String bootstrapPartitionPath;
  private final HoodieFileStatus bootstrapFileStatus;

  private final String partitionPath;
  private final String fileId;

  public BootstrapFileMapping(String bootstrapBasePath, String bootstrapPartitionPath, String partitionPath,
                              HoodieFileStatus bootstrapFileStatus, String fileId) {
    this.bootstrapBasePath = bootstrapBasePath;
    this.bootstrapPartitionPath = bootstrapPartitionPath;
    this.partitionPath = partitionPath;
    this.bootstrapFileStatus = bootstrapFileStatus;
    this.fileId = fileId;
  }

  @Override
  public String toString() {
    return "BootstrapFileMapping{"
        + "bootstrapBasePath='" + bootstrapBasePath + '\''
        + ", bootstrapPartitionPath='" + bootstrapPartitionPath + '\''
        + ", bootstrapFileStatus=" + bootstrapFileStatus
        + ", partitionPath='" + partitionPath + '\''
        + ", fileId='" + fileId + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BootstrapFileMapping mapping = (BootstrapFileMapping) o;
    return Objects.equals(bootstrapBasePath, mapping.bootstrapBasePath)
        && Objects.equals(bootstrapPartitionPath, mapping.bootstrapPartitionPath)
        && Objects.equals(partitionPath, mapping.partitionPath)
        && Objects.equals(bootstrapFileStatus, mapping.bootstrapFileStatus)
        && Objects.equals(fileId, mapping.fileId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bootstrapBasePath, bootstrapPartitionPath, partitionPath, bootstrapFileStatus, fileId);
  }

  public String getBootstrapBasePath() {
    return bootstrapBasePath;
  }

  public String getBootstrapPartitionPath() {
    return bootstrapPartitionPath;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public HoodieFileStatus getBootstrapFileStatus() {
    return bootstrapFileStatus;
  }

  public String getFileId() {
    return fileId;
  }

  public HoodieFileGroupId getFileGroupId() {
    return new HoodieFileGroupId(partitionPath, fileId);
  }

  @Override
  public int compareTo(BootstrapFileMapping o) {
    int ret = partitionPath.compareTo(o.partitionPath);
    if (ret == 0) {
      ret = fileId.compareTo(o.fileId);
    }
    return ret;
  }
}
