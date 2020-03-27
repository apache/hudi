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

public class BootstrapSourceFileMapping implements Serializable, Comparable<BootstrapSourceFileMapping> {

  private final String sourceBasePath;
  private final String sourcePartitionPath;
  private final String hudiPartitionPath;
  private final HoodieFileStatus sourceFileStatus;
  private final String hudiFileId;

  public BootstrapSourceFileMapping(String sourceBasePath, String sourcePartitionPath,
      String hudiPartitionPath, HoodieFileStatus sourceFileStatus, String hudiFileId) {
    this.sourceBasePath = sourceBasePath;
    this.sourcePartitionPath = sourcePartitionPath;
    this.hudiPartitionPath = hudiPartitionPath;
    this.sourceFileStatus = sourceFileStatus;
    this.hudiFileId = hudiFileId;
  }

  @Override
  public String toString() {
    return "BootstrapSourceFileMapping{"
        + "sourceBasePath='" + sourceBasePath + '\''
        + ", sourcePartitionPath='" + sourcePartitionPath + '\''
        + ", hudiPartitionPath='" + hudiPartitionPath + '\''
        + ", sourceFileStatus='" + sourceFileStatus + '\''
        + ", hudiFileId='" + hudiFileId + '\''
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
    BootstrapSourceFileMapping mapping = (BootstrapSourceFileMapping) o;
    return Objects.equals(sourceBasePath, mapping.sourceBasePath)
        && Objects.equals(sourcePartitionPath, mapping.sourcePartitionPath)
        && Objects.equals(hudiPartitionPath, mapping.hudiPartitionPath)
        && Objects.equals(sourceFileStatus, mapping.sourceFileStatus)
        && Objects.equals(hudiFileId, mapping.hudiFileId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceBasePath, sourcePartitionPath, hudiPartitionPath, sourceFileStatus, hudiFileId);
  }

  public String getSourceBasePath() {
    return sourceBasePath;
  }

  public String getSourcePartitionPath() {
    return sourcePartitionPath;
  }

  public String getHudiPartitionPath() {
    return hudiPartitionPath;
  }

  public HoodieFileStatus getSourceFileStatus() {
    return sourceFileStatus;
  }

  public String getHudiFileId() {
    return hudiFileId;
  }

  public HoodieFileGroupId getFileGroupId() {
    return new HoodieFileGroupId(hudiPartitionPath, hudiFileId);
  }

  @Override
  public int compareTo(BootstrapSourceFileMapping o) {
    int ret = hudiPartitionPath.compareTo(o.hudiPartitionPath);
    if (ret == 0) {
      ret = hudiFileId.compareTo(o.hudiFileId);
    }
    return ret;
  }
}
