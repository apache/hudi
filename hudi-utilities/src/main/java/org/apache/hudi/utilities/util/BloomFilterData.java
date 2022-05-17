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

package org.apache.hudi.utilities.util;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Includes partition path, filename and bloom filter for validation
 */
public class BloomFilterData implements Comparable<BloomFilterData> {
  private final String partitionPath;
  private final String filename;
  private final ByteBuffer bloomFilter;

  private BloomFilterData(
      String partitionPath, String filename, ByteBuffer bloomFilter) {
    this.partitionPath = partitionPath;
    this.filename = filename;
    this.bloomFilter = bloomFilter;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public int compareTo(@NotNull BloomFilterData o) {
    return this.toString().compareTo(o.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BloomFilterData that = (BloomFilterData) o;
    return partitionPath.equals(that.partitionPath) && filename.equals(that.filename)
        && bloomFilter.equals(that.bloomFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionPath, filename, bloomFilter);
  }

  @Override
  public String toString() {
    String bloomFilterString = new String(bloomFilter.array());
    return "BloomFilterData{"
        + "partitionPath='" + partitionPath + '\''
        + ", filename='" + filename + '\''
        + ", bloomFilter="
        + (bloomFilterString.length() > 50 ? bloomFilterString.substring(0, 50) + "..." : bloomFilterString)
        + '}';
  }

  public static class Builder {
    private String partitionPath;
    private String filename;
    private ByteBuffer bloomFilter;

    public Builder setPartitionPath(String partitionPath) {
      this.partitionPath = partitionPath;
      return this;
    }

    public Builder setFilename(String filename) {
      this.filename = filename;
      return this;
    }

    public Builder setBloomFilter(ByteBuffer bloomFilter) {
      this.bloomFilter = bloomFilter;
      return this;
    }

    public BloomFilterData build() {
      return new BloomFilterData(partitionPath, filename, bloomFilter);
    }
  }
}
