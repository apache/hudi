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

package org.apache.hudi.hive;

import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;

import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * What one {@link HiveSyncTool#syncHoodieTable()} did, for callers that report on their syncs.
 *
 * <p>A MERGE_ON_READ table is registered as several Hive tables, all synced by one call. The
 * durations add up over those tables. The partition count is the most partitions added to any one
 * of them, and the schema flag is set when any of them had its schema evolved.
 *
 * <p>After a failed sync, the metrics cover the steps that ran before the failure.
 *
 * <p>Unless {@code hoodie.datasource.hive_sync.detailed_metrics.enabled} is set, the sync steps run
 * untimed and nothing is recorded, so the metrics stay empty.
 */
public class HoodieHiveSyncMetrics {

  private final boolean detailedMetricsEnabled;
  private long totalMs = -1;
  private long schemaReadMs = -1;
  private long partitionScanMs = -1;
  private int partitionsAdded = 0;
  private boolean schemaEvolved = false;

  /**
   * @param detailedMetricsEnabled whether to time the sync steps and keep what is recorded; disabled
   *     metrics run the steps untimed and stay empty
   */
  public HoodieHiveSyncMetrics(boolean detailedMetricsEnabled) {
    this.detailedMetricsEnabled = detailedMetricsEnabled;
  }

  /**
   * Time spent reading the table schema from storage, or empty when no step read it, as when the
   * metastore is already up to date with the latest commit.
   */
  public Option<Long> getSchemaReadMs() {
    return schemaReadMs < 0 ? Option.empty() : Option.of(schemaReadMs);
  }

  /**
   * Time spent finding the partitions to sync, from the timeline or by listing storage, or empty
   * when no step looked for them.
   */
  public Option<Long> getPartitionScanMs() {
    return partitionScanMs < 0 ? Option.empty() : Option.of(partitionScanMs);
  }

  /**
   * Time spent on the rest of the sync: mostly the calls to the metastore, checking for the database
   * and tables, creating or altering them, and reading and writing their partitions, but also the
   * local work between them, such as comparing the partitions and schemas found. Empty when the sync
   * did not run, because the metastore client could not be created.
   */
  public Option<Long> getRemainingMs() {
    if (totalMs < 0) {
      return Option.empty();
    }
    return Option.of(Math.max(0, totalMs - Math.max(0, schemaReadMs) - Math.max(0, partitionScanMs)));
  }

  /**
   * The most partitions added to any one Hive table of the sync. They are counted once adding them
   * all succeeds, so a sync that fails while adding partitions counts none of them.
   *
   * <p>The Hive tables of a MERGE_ON_READ table normally get the same partitions, which this counts
   * once. It is not a count of distinct partitions: when the tables were missing different
   * partitions, as after one of them was repaired by hand, it counts the larger of their additions.
   */
  public int getPartitionsAdded() {
    return partitionsAdded;
  }

  /** Whether the storage schema differed from the metastore's and was pushed to it. */
  public boolean isSchemaEvolved() {
    return schemaEvolved;
  }

  void timeTotal(Runnable sync) {
    time(() -> {
      sync.run();
      return null;
    }, this::setTotalMs);
  }

  <T> T timeSchemaRead(Supplier<T> step) {
    return time(step, this::addSchemaReadMs);
  }

  <T> T timePartitionScan(Supplier<T> step) {
    return time(step, this::addPartitionScanMs);
  }

  void recordPartitionsAdded(int count) {
    if (detailedMetricsEnabled) {
      partitionsAdded = Math.max(partitionsAdded, count);
    }
  }

  void markSchemaEvolved() {
    if (detailedMetricsEnabled) {
      schemaEvolved = true;
    }
  }

  void setTotalMs(long totalMs) {
    this.totalMs = totalMs;
  }

  void addSchemaReadMs(long durationMs) {
    schemaReadMs = Math.max(0, schemaReadMs) + durationMs;
  }

  void addPartitionScanMs(long durationMs) {
    partitionScanMs = Math.max(0, partitionScanMs) + durationMs;
  }

  private <T> T time(Supplier<T> step, LongConsumer recorder) {
    if (!detailedMetricsEnabled) {
      return step.get();
    }
    HoodieTimer timer = HoodieTimer.start();
    try {
      return step.get();
    } finally {
      recorder.accept(timer.endTimer());
    }
  }
}
