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

import org.apache.hudi.common.table.timeline.HoodieInstant;

import java.util.List;
import java.util.Objects;

public class HoodiePartitionSnapshot {

  HoodiePartitionDescriptor partitionDescriptor;

  // (Latest) Instant T as of which partition's state is represented
  HoodieInstant latestInstant;

  // File-slices current at the instant T
  List<FileSlice> fileSlices;

  public HoodiePartitionSnapshot(HoodiePartitionDescriptor partitionDescriptor, HoodieInstant latestInstant, List<FileSlice> fileSlices) {
    this.partitionDescriptor = partitionDescriptor;
    this.latestInstant = latestInstant;
    this.fileSlices = fileSlices;
  }

  public HoodiePartitionDescriptor getPartitionDescriptor() {
    return partitionDescriptor;
  }

  public HoodieInstant getLatestInstant() {
    return latestInstant;
  }

  public List<FileSlice> getFileSlices() {
    return fileSlices;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodiePartitionSnapshot that = (HoodiePartitionSnapshot) o;
    return Objects.equals(partitionDescriptor, that.partitionDescriptor)
        && Objects.equals(latestInstant, that.latestInstant)
        && Objects.equals(fileSlices, that.fileSlices);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionDescriptor, latestInstant, fileSlices);
  }

  @Override
  public String toString() {
    return "HoodiePartitionSnapshot{"
        + "partitionDescriptor=" + partitionDescriptor
        + ", latestInstant=" + latestInstant
        + ", fileSlices=" + fileSlices
        + '}';
  }
}
