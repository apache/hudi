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

import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;

import java.util.Objects;

/**
 * Represents a raw key for bloom filter metadata.
 */
public class BloomFilterIndexRawKey implements MetadataRawKey {
  private final String partitionName;
  private final String fileName;

  public BloomFilterIndexRawKey(String partitionName, String fileName) {
    this.partitionName = Objects.requireNonNull(partitionName);
    this.fileName = Objects.requireNonNull(fileName);
  }

  @Override
  public String encode() {
    return HoodieMetadataPayload.getBloomFilterIndexKey(
        new PartitionIndexID(HoodieTableMetadataUtil.getBloomFilterIndexPartitionIdentifier(partitionName)),
        new FileIndexID(fileName));
  }

  public String getPartitionName() {
    return partitionName;
  }

  public String getFileName() {
    return fileName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BloomFilterIndexRawKey that = (BloomFilterIndexRawKey) o;
    return Objects.equals(partitionName, that.partitionName) && Objects.equals(fileName, that.fileName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionName, fileName);
  }

  @Override
  public String toString() {
    return "BloomFilterRawKey{" + "partitionName='" + partitionName + '\'' + ", fileName='" + fileName + '\'' + '}';
  }
}