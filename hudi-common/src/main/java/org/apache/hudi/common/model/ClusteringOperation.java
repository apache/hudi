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

import org.apache.hudi.avro.model.HoodieSliceInfo;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Encapsulates all the needed information about a clustering file slice. This is needed because spark serialization
 * does not work with avro objects.
 */
@Setter
@Getter
public class ClusteringOperation implements Serializable {

  private String dataFilePath;
  private List<String> deltaFilePaths;
  private String fileId;
  private String partitionPath;
  private String bootstrapFilePath;
  private int version;

  public static ClusteringOperation create(HoodieSliceInfo sliceInfo) {
    return new ClusteringOperation(sliceInfo.getDataFilePath(), new ArrayList<>(sliceInfo.getDeltaFilePaths()), sliceInfo.getFileId(),
        sliceInfo.getPartitionPath(), sliceInfo.getBootstrapFilePath(), sliceInfo.getVersion());
  }

  // Only for serialization/de-serialization
  @Deprecated
  public ClusteringOperation() {
  }

  private ClusteringOperation(final String dataFilePath, final List<String> deltaFilePaths, final String fileId,
                             final String partitionPath, final String bootstrapFilePath, final int version) {
    this.dataFilePath = dataFilePath;
    this.deltaFilePaths = deltaFilePaths;
    this.fileId = fileId;
    this.partitionPath = partitionPath;
    this.bootstrapFilePath = bootstrapFilePath;
    this.version = version;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ClusteringOperation that = (ClusteringOperation) o;
    return getVersion() == that.getVersion()
        && Objects.equals(getDataFilePath(), that.getDataFilePath())
        && Objects.equals(getDeltaFilePaths(), that.getDeltaFilePaths())
        && Objects.equals(getFileId(), that.getFileId())
        && Objects.equals(getPartitionPath(), that.getPartitionPath())
        && Objects.equals(getBootstrapFilePath(), that.getBootstrapFilePath());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDataFilePath(), getDeltaFilePaths(), getFileId(), getPartitionPath(), getBootstrapFilePath(), getVersion());
  }
}
