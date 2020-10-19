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

import java.util.List;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;

import org.apache.spark.api.java.JavaRDD;

import java.time.Duration;

/**
 * Contains metadata, write-statuses and latency times corresponding to a commit/delta-commit action.
 */
public class HoodieWriteMetadata {

  private JavaRDD<WriteStatus> writeStatuses;
  private Option<Duration> indexLookupDuration = Option.empty();

  // Will be set when auto-commit happens
  private boolean isCommitted;
  private Option<HoodieCommitMetadata> commitMetadata = Option.empty();
  private Option<List<HoodieWriteStat>> writeStats = Option.empty();
  private Option<Duration> indexUpdateDuration = Option.empty();
  private Option<Duration> finalizeDuration = Option.empty();

  public HoodieWriteMetadata() {
  }

  public JavaRDD<WriteStatus> getWriteStatuses() {
    return writeStatuses;
  }

  public Option<HoodieCommitMetadata> getCommitMetadata() {
    return commitMetadata;
  }

  public void setWriteStatuses(JavaRDD<WriteStatus> writeStatuses) {
    this.writeStatuses = writeStatuses;
  }

  public void setCommitMetadata(Option<HoodieCommitMetadata> commitMetadata) {
    this.commitMetadata = commitMetadata;
  }

  public Option<Duration> getFinalizeDuration() {
    return finalizeDuration;
  }

  public void setFinalizeDuration(Duration finalizeDuration) {
    this.finalizeDuration = Option.ofNullable(finalizeDuration);
  }

  public Option<Duration> getIndexUpdateDuration() {
    return indexUpdateDuration;
  }

  public void setIndexUpdateDuration(Duration indexUpdateDuration) {
    this.indexUpdateDuration = Option.ofNullable(indexUpdateDuration);
  }

  public boolean isCommitted() {
    return isCommitted;
  }

  public void setCommitted(boolean committed) {
    isCommitted = committed;
  }

  public Option<List<HoodieWriteStat>> getWriteStats() {
    return writeStats;
  }

  public void setWriteStats(List<HoodieWriteStat> writeStats) {
    this.writeStats = Option.of(writeStats);
  }

  public Option<Duration> getIndexLookupDuration() {
    return indexLookupDuration;
  }

  public void setIndexLookupDuration(Duration indexLookupDuration) {
    this.indexLookupDuration = Option.ofNullable(indexLookupDuration);
  }
}
