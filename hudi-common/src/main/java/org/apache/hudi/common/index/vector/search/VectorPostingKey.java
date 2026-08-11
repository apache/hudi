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

package org.apache.hudi.common.index.vector.search;

import java.io.Serializable;
import java.util.Objects;

/** Canonical identity of one posting entry within a generation's cluster and shard. */
public final class VectorPostingKey implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int clusterId;
  private final int shardId;
  private final String recordKey;

  public VectorPostingKey(int clusterId, int shardId, String recordKey) {
    this.clusterId = clusterId;
    this.shardId = shardId;
    this.recordKey = Objects.requireNonNull(recordKey, "recordKey");
  }

  public static VectorPostingKey fromCandidate(VectorCandidate candidate) {
    return new VectorPostingKey(
        candidate.getClusterId(), candidate.getShardId(), candidate.getRecordKey());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof VectorPostingKey)) {
      return false;
    }
    VectorPostingKey that = (VectorPostingKey) other;
    return clusterId == that.clusterId && shardId == that.shardId && recordKey.equals(that.recordKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, shardId, recordKey);
  }
}
