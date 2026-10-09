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

import org.apache.hudi.common.util.Option;

/**
 * What one {@link HiveSyncTool#syncHoodieTable()} did, for callers that report on their syncs.
 *
 * <p>A MERGE_ON_READ table is registered as several Hive tables, all synced by one call. The
 * durations add up over those tables. The partition count and the schema flag describe one of them,
 * since each gets the same partitions and the same schema.
 *
 * <p>After a failed sync, the stats cover the steps that ran before the failure.
 */
public class HiveSyncStats {

  private long totalMs = -1;
  private long schemaReadMs = -1;
  private long partitionScanMs = -1;
  private int partitionsAdded = 0;
  private boolean schemaEvolved = false;

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

  /** Number of partitions added to the metastore. */
  public int getPartitionsAdded() {
    return partitionsAdded;
  }

  /** Whether the storage schema differed from the metastore's and was pushed to it. */
  public boolean isSchemaEvolved() {
    return schemaEvolved;
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

  /**
   * Keeps the largest count rather than the sum: the Hive tables of a MERGE_ON_READ table each get
   * the same partitions, so summing would count them once per table.
   */
  void recordPartitionsAdded(int count) {
    partitionsAdded = Math.max(partitionsAdded, count);
  }

  void markSchemaEvolved() {
    schemaEvolved = true;
  }
}
