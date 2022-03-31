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

package org.apache.hudi.index;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;

/**
 * Base class for different types of indexes to determine the mapping from uuid.
 *
 * @param <T> Sub type of HoodieRecordPayload
 * @param <I> Type of inputs for deprecated APIs
 * @param <K> Type of keys for deprecated APIs
 * @param <O> Type of outputs for deprecated APIs
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class HoodieIndex<T extends HoodieRecordPayload, I, K, O> implements Serializable {
  // Metrics for updateLocation
  public static final String UPDATE_LOC_DURATION = "update_loc_duration";
  public static final String UPDATE_LOC_NUM_PARTITIONS = "update_loc_num_partitions";
  public static final String TOTAL_UPDATE = "total_updates";
  public static final String TOTAL_INSERT = "total_inserts";
  public static final String TOTAL_DELETE = "total_deletes";
  public static final String TOTAL_WRITES = "total_writes";

  // Metrics for tagLocation
  public static final String TAG_LOC_DURATION = "tag_loc_duration";
  public static final String TAG_LOC_NUM_PARTITIONS = "tag_num_partitions";
  public static final String TAG_LOC_RECORD_COUNT = "tag_count";
  public static final String TAG_LOC_HITS = "tag_hits";

  // Metric registry
  public Option<Registry> registry = Option.empty();

  public final HoodieWriteConfig config;

  protected HoodieIndex(HoodieWriteConfig config) {
    this.config = config;
    if ((config.getTableName() != null) && !config.getTableName().isEmpty()) {
      this.registry = Option.of(Registry.getRegistry(config.getTableName() + "." + this.getClass().getSimpleName()));
    }
  }

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains the row (if it is actually
   * present).
   */
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  public I tagLocation(I records, HoodieEngineContext context,
                       HoodieTable<T, I, K, O> hoodieTable) throws HoodieIndexException {
    throw new HoodieNotSupportedException("Deprecated API should not be called");
  }

  /**
   * Extracts the location of written records, and updates the index.
   */
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  public O updateLocation(O writeStatuses, HoodieEngineContext context,
                          HoodieTable<T, I, K, O> hoodieTable) throws HoodieIndexException {
    throw new HoodieNotSupportedException("Deprecated API should not be called");
  }

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains
   * the row (if it is actually present).
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieData<HoodieRecord<T>> tagLocation(
      HoodieData<HoodieRecord<T>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) throws HoodieIndexException;

  /**
   * Extracts the location of written records, and updates the index.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) throws HoodieIndexException;

  /**
   * Rollback the effects of the commit made at instantTime.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract boolean rollbackCommit(String instantTime);

  /**
   * An index is `global` if {@link HoodieKey} to fileID mapping, does not depend on the `partitionPath`. Such an
   * implementation is able to obtain the same mapping, for two hoodie keys with same `recordKey` but different
   * `partitionPath`
   *
   * @return whether or not, the index implementation is global in nature
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract boolean isGlobal();

  /**
   * This is used by storage to determine, if its safe to send inserts, straight to the log, i.e having a
   * {@link FileSlice}, with no data file.
   *
   * @return Returns true/false depending on whether the impl has this capability
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean canIndexLogFiles();

  /**
   * An index is "implicit" with respect to storage, if just writing new data to a file slice, updates the index as
   * well. This is used by storage, to save memory footprint in certain cases.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract boolean isImplicitWithStorage();

  /**
   * Each index type should implement it's own logic to release any resources acquired during the process.
   */
  public void close() {
  }

  /**
   * Check if the given commit timestamp is valid.
   *
   * Commit timestamp is considered valid if either it is present in the timeline or is less than the first
   * commit ts in the timeline.
   *
   * @param metaClient
   * @param commitTs the commit timestamp to check
   * @returns true if the commit timestamp is valid, false otherwise
   */
  protected boolean checkIfValidCommit(HoodieTableMetaClient metaClient, String commitTs) {
    HoodieTimeline commitTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
    return !commitTimeline.empty() && commitTimeline.containsOrBeforeTimelineStarts(commitTs);
  }

  public enum IndexType {
    HBASE, INMEMORY, BLOOM, GLOBAL_BLOOM, SIMPLE, GLOBAL_SIMPLE, RECORD_INDEX
  }
}
