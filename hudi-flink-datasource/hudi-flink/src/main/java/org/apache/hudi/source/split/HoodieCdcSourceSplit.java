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

package org.apache.hudi.source.split;

import lombok.ToString;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.util.Option;

import lombok.Getter;

/**
 * A {@link HoodieSourceSplit} for CDC (Change Data Capture) reads.
 *
 * <p>Extends the base split with an ordered list of {@link HoodieCDCFileSplit} entries
 * that describe the CDC changes for a single file group, and the memory budget used
 * for spillable image maps.
 */
@ToString
public class HoodieCdcSourceSplit extends HoodieSourceSplit {
  private static final long serialVersionUID = 1L;

  /** Ordered CDC file splits for one file group, sorted by instant time. */
  @Getter
  private final HoodieCDCFileSplit[] changes;

  /** Maximum memory in bytes available for compaction/merge operations. */
  @Getter
  private final long maxCompactionMemoryInBytes;

  public HoodieCdcSourceSplit(
      int splitNum,
      String tablePath,
      long maxCompactionMemoryInBytes,
      String fileId,
      String partitionPath,
      HoodieCDCFileSplit[] changes,
      String mergeType,
      String lastCommit) {
    super(splitNum, null, Option.empty(), tablePath, partitionPath, mergeType, lastCommit, fileId, Option.empty());
    this.changes = changes;
    this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
  }
}
