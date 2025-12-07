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

package org.apache.hudi.table.action;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;

import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Contains metadata, write-statuses and latency times corresponding to a commit/delta-commit action.
 */
public class HoodieWriteMetadata<O> {

  @Getter
  @Setter
  private O writeStatuses;
  @Getter
  private Option<Duration> indexLookupDuration = Option.empty();
  @Getter
  private Option<Long> sourceReadAndIndexDurationMs = Option.empty();

  // Will be set when auto-commit happens
  @Getter
  private boolean isCommitted;
  @Getter
  @Setter
  private Option<HoodieCommitMetadata> commitMetadata = Option.empty();
  @Getter
  private Option<List<HoodieWriteStat>> writeStats = Option.empty();
  @Getter
  private Option<Duration> indexUpdateDuration = Option.empty();
  @Getter
  private Option<Duration> finalizeDuration = Option.empty();
  private Option<Map<String, List<String>>> partitionToReplaceFileIds = Option.empty();

  public HoodieWriteMetadata() {
  }

  /**
   * Clones the write metadata with transformed write statuses.
   *
   * @param transformedWriteStatuses transformed write statuses
   * @param <T>                      type of transformed write statuses
   * @return Cloned {@link HoodieWriteMetadata<T>} instance
   */
  public <T> HoodieWriteMetadata<T> clone(T transformedWriteStatuses) {
    HoodieWriteMetadata<T> newMetadataInstance = new HoodieWriteMetadata<>();
    newMetadataInstance.setWriteStatuses(transformedWriteStatuses);
    if (indexLookupDuration.isPresent()) {
      newMetadataInstance.setIndexLookupDuration(indexLookupDuration.get());
    }
    if (sourceReadAndIndexDurationMs.isPresent()) {
      newMetadataInstance.setSourceReadAndIndexDurationMs(sourceReadAndIndexDurationMs.get());
    }
    newMetadataInstance.setCommitted(isCommitted);
    newMetadataInstance.setCommitMetadata(commitMetadata);
    if (writeStats.isPresent()) {
      newMetadataInstance.setWriteStats(writeStats.get());
    }
    if (indexUpdateDuration.isPresent()) {
      newMetadataInstance.setIndexUpdateDuration(indexUpdateDuration.get());
    }
    if (finalizeDuration.isPresent()) {
      newMetadataInstance.setFinalizeDuration(finalizeDuration.get());
    }
    if (partitionToReplaceFileIds.isPresent()) {
      newMetadataInstance.setPartitionToReplaceFileIds(partitionToReplaceFileIds.get());
    }
    return newMetadataInstance;
  }

  public void setFinalizeDuration(Duration finalizeDuration) {
    this.finalizeDuration = Option.ofNullable(finalizeDuration);
  }

  public void setIndexUpdateDuration(Duration indexUpdateDuration) {
    this.indexUpdateDuration = Option.ofNullable(indexUpdateDuration);
  }

  public void setCommitted(boolean committed) {
    isCommitted = committed;
  }

  public void setWriteStats(List<HoodieWriteStat> writeStats) {
    this.writeStats = Option.of(writeStats);
  }

  public void setIndexLookupDuration(Duration indexLookupDuration) {
    this.indexLookupDuration = Option.ofNullable(indexLookupDuration);
  }

  public void setSourceReadAndIndexDurationMs(Long sourceReadAndIndexDurationMs) {
    this.sourceReadAndIndexDurationMs = Option.of(sourceReadAndIndexDurationMs);
  }

  public Map<String, List<String>> getPartitionToReplaceFileIds() {
    return partitionToReplaceFileIds.orElse(Collections.emptyMap());
  }

  public void setPartitionToReplaceFileIds(Map<String, List<String>> partitionToReplaceFileIds) {
    this.partitionToReplaceFileIds = Option.ofNullable(partitionToReplaceFileIds);
  }
}
