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

/**
 * Physical hint for where a candidate's packed posting row lives (RFC-109 v3 §4). It combines the
 * logical index coordinates (generation/cluster/shard/block/ordinal) with an optional data-table
 * location hint (partition/file/rowPosition) decoded from the posting.
 *
 * <p>Per the architectural rule, the data-table location is a <em>hint</em> only: it is trusted for
 * a positional read solely after RLI arbitration returns {@link VectorCandidateState#SERVE}. When
 * the base instant recorded here diverges from the live RLI location the candidate is STALE and the
 * fetch planner falls back to a key-based read.
 */
public final class VectorPostingLocator implements Serializable {

  private static final long serialVersionUID = 1L;

  // Logical index coordinates.
  private final int generationId;
  private final int clusterId;
  private final int shardId;
  private final long blockId;
  private final int vectorOrdinal;

  // Data-table location hint (may be null when only key-fallback is possible).
  private final String partitionPath;
  private final String fileId;
  private final String baseInstant;
  private final long rowPosition;

  public VectorPostingLocator(int generationId,
                              int clusterId,
                              int shardId,
                              long blockId,
                              int vectorOrdinal,
                              String partitionPath,
                              String fileId,
                              String baseInstant,
                              long rowPosition) {
    this.generationId = generationId;
    this.clusterId = clusterId;
    this.shardId = shardId;
    this.blockId = blockId;
    this.vectorOrdinal = vectorOrdinal;
    this.partitionPath = partitionPath;
    this.fileId = fileId;
    this.baseInstant = baseInstant;
    this.rowPosition = rowPosition;
  }

  public int getGenerationId() {
    return generationId;
  }

  public int getClusterId() {
    return clusterId;
  }

  public int getShardId() {
    return shardId;
  }

  public long getBlockId() {
    return blockId;
  }

  public int getVectorOrdinal() {
    return vectorOrdinal;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getFileId() {
    return fileId;
  }

  public String getBaseInstant() {
    return baseInstant;
  }

  public long getRowPosition() {
    return rowPosition;
  }

  /** Whether a positional data-table hint is present (still subject to RLI arbitration). */
  public boolean hasDataLocationHint() {
    return fileId != null && rowPosition >= 0;
  }
}
