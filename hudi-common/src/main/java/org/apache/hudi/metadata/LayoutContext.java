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

package org.apache.hudi.metadata;

import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/**
 * Per-file-group context handed to a {@link HoodieMetadataTableLayout}.
 *
 * <p>Carries everything a layout needs to compute the on-disk relative path
 * and fileId for a single file group: the metadata partition type, the
 * relative MDT partition path (which for secondary/expression indexes is the
 * full partition name like {@code secondary_index_idx0}, not the static
 * prefix on the partition type), the global file-group index, the total
 * number of file groups in that partition, and the data-table partition name
 * (set only for partitioned RLI).
 */
public final class LayoutContext implements Serializable {

  private final MetadataPartitionType partitionType;
  private final String relativePartitionPath;
  private final int fileGroupIndex;
  private final int fileGroupCount;
  private final Option<String> dataPartitionName;

  public LayoutContext(MetadataPartitionType partitionType,
                       String relativePartitionPath,
                       int fileGroupIndex,
                       int fileGroupCount,
                       Option<String> dataPartitionName) {
    this.partitionType = partitionType;
    this.relativePartitionPath = relativePartitionPath;
    this.fileGroupIndex = fileGroupIndex;
    this.fileGroupCount = fileGroupCount;
    this.dataPartitionName = dataPartitionName;
  }

  public MetadataPartitionType getPartitionType() {
    return partitionType;
  }

  public String getRelativePartitionPath() {
    return relativePartitionPath;
  }

  public int getFileGroupIndex() {
    return fileGroupIndex;
  }

  public int getFileGroupCount() {
    return fileGroupCount;
  }

  public Option<String> getDataPartitionName() {
    return dataPartitionName;
  }
}
