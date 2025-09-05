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
 * <p>
 * The BLOOM_FILTERS partition stores bloom filters for each data file to enable
 * efficient filtering of files that might contain a specific record key.
 * Each bloom filter helps avoid reading files that definitely don't contain a record.
 * <p>
 * Raw key format: base64(partition_identifier) + base64(file_name)
 * - partition_identifier: The partition path (or "." for non-partitioned tables)
 * - file_name: Name of the data file (base file)
 * <p>
 * Examples:
 * - For file "f1.parquet" in partition "2023/01/15":
 *   key = base64("2023/01/15") + base64("f1.parquet")
 *   Example encoded: "MjAyMy8wMS8xNQ==" + "ZjEucGFycXVldA=="
 * <p>
 * - For file "base_001.parquet" in non-partitioned table:
 *   key = base64(".") + base64("base_001.parquet")
 *   Example encoded: "X19ISVZFX0RFRkFVTFRfUEFSVElUSU9OX18=" + "YmFzZV8wMDEucGFycXVldA=="
 * <p>
 * - For file "data.parquet" in partition "country=US/state=CA":
 *   key = base64("country=US/state=CA") + base64("data.parquet")
 *   Example encoded: "Y291bnRyeT1VUy9zdGF0ZT1DQQ==" + "ZGF0YS5wYXJxdWV0"
 * <p>
 * The value contains the serialized bloom filter for efficient key lookups in that file.
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

  public String partitionName() {
    return partitionName;
  }

  public String fileName() {
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