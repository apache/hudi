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

package org.apache.hudi.metadata;

import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * Immutable stats for a single record index shard (file group) read during one lookup.
 *
 * <p>Instances are merged field-wise by {@code max} rather than summed. That is what makes
 * collection idempotent: a retried or recomputed engine task re-reports the same shard, and taking
 * the maximum overwrites the earlier report instead of accumulating a second time. A task that
 * failed part-way through a read and then succeeded resolves to the complete read, because the
 * complete read observed at least as many keys and hits.
 *
 * <p>{@code max} is also commutative and associative, which the Spark accumulator contract
 * requires: executor-local copies are merged on the driver in an unspecified order.
 */
public class RecordIndexShardLookupStats implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int shardIndex;
  private final String fileGroupId;
  private final long keysSubmitted;
  private final long keysHit;
  private final long logFilesRead;
  private final long bytesInShard;
  private final long lookupMillis;

  public RecordIndexShardLookupStats(int shardIndex, String fileGroupId, long keysSubmitted,
                                     long keysHit, long logFilesRead, long bytesInShard,
                                     long lookupMillis) {
    // fileGroupId is the identity of this value and the key of the aggregate map, so a null would
    // not fail here but silently become a map key that swallows every other null-id shard.
    ValidationUtils.checkArgument(fileGroupId != null, "fileGroupId is required, it identifies the shard");
    ValidationUtils.checkArgument(shardIndex >= 0, "shardIndex must be non-negative");
    ValidationUtils.checkArgument(keysHit <= keysSubmitted, "keysHit cannot exceed keysSubmitted");
    this.shardIndex = shardIndex;
    this.fileGroupId = fileGroupId;
    this.keysSubmitted = keysSubmitted;
    this.keysHit = keysHit;
    this.logFilesRead = logFilesRead;
    this.bytesInShard = bytesInShard;
    this.lookupMillis = lookupMillis;
  }

  public int getShardIndex() {
    return shardIndex;
  }

  public String getFileGroupId() {
    return fileGroupId;
  }

  public long getKeysSubmitted() {
    return keysSubmitted;
  }

  public long getKeysHit() {
    return keysHit;
  }

  public long getLogFilesRead() {
    return logFilesRead;
  }

  /**
   * Footprint of the shard read: base file size plus the sum of log file sizes.
   *
   * <p>This is an upper bound on I/O, not I/O. Record index lookups push the key set into the file
   * reader, so only a fraction of the shard is actually read. True bytes read would be reported as
   * a separate value rather than by redefining this one.
   */
  public long getBytesInShard() {
    return bytesInShard;
  }

  public long getLookupMillis() {
    return lookupMillis;
  }

  /**
   * Field-wise maximum of this and {@code other}, which must describe the same file group.
   *
   * <p>Identity is the file group id, not the shard index. A shard index is only unique within one
   * lookup call: the partitioned record index resolves it against the slices of a single data table
   * partition, so two different partitions both yield index 0 for two different file groups. Merging
   * those would silently collapse two real shards into one. File group ids embed the data table
   * partition and are globally unique, so keying on them keeps retry idempotence without conflating
   * distinct shards.
   */
  public RecordIndexShardLookupStats merge(RecordIndexShardLookupStats other) {
    ValidationUtils.checkArgument(Objects.equals(fileGroupId, other.fileGroupId),
        "cannot merge stats for different file groups: " + fileGroupId + " vs " + other.fileGroupId);
    return new RecordIndexShardLookupStats(
        Math.max(shardIndex, other.shardIndex),
        fileGroupId,
        Math.max(keysSubmitted, other.keysSubmitted),
        Math.max(keysHit, other.keysHit),
        Math.max(logFilesRead, other.logFilesRead),
        Math.max(bytesInShard, other.bytesInShard),
        Math.max(lookupMillis, other.lookupMillis));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RecordIndexShardLookupStats that = (RecordIndexShardLookupStats) o;
    return shardIndex == that.shardIndex
        && keysSubmitted == that.keysSubmitted
        && keysHit == that.keysHit
        && logFilesRead == that.logFilesRead
        && bytesInShard == that.bytesInShard
        && lookupMillis == that.lookupMillis
        && Objects.equals(fileGroupId, that.fileGroupId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardIndex, fileGroupId, keysSubmitted, keysHit, logFilesRead,
        bytesInShard, lookupMillis);
  }

  @Override
  public String toString() {
    return "RecordIndexShardLookupStats{shard=" + shardIndex + ", fileGroup=" + fileGroupId
        + ", keysSubmitted=" + keysSubmitted + ", keysHit=" + keysHit
        + ", logFilesRead=" + logFilesRead + ", bytesInShard=" + bytesInShard
        + ", lookupMillis=" + lookupMillis + '}';
  }
}
