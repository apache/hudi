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

package org.apache.hudi.table.action.index;

import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_CONCURRENCY_MODE;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataPartition;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightAndCompletedMetadataPartitions;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.metadataPartitionExists;

/**
 * Schedules INDEX action.
 * <li>
 * 1. Fetch last completed instant on data timeline.
 * 2. Write the index plan to the <instant>.index.requested.
 * 3. Initialize file groups for the enabled partition types within a transaction.
 * </li>
 */
public class ScheduleIndexActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieIndexPlan>> {

  private static final Logger LOG = LogManager.getLogger(ScheduleIndexActionExecutor.class);
  private static final Integer INDEX_PLAN_VERSION_1 = 1;
  private static final Integer LATEST_INDEX_PLAN_VERSION = INDEX_PLAN_VERSION_1;

  private final List<MetadataPartitionType> partitionIndexTypes;
  private final TransactionManager txnManager;

  public ScheduleIndexActionExecutor(HoodieEngineContext context,
                                     HoodieWriteConfig config,
                                     HoodieTable<T, I, K, O> table,
                                     String instantTime,
                                     List<MetadataPartitionType> partitionIndexTypes) {
    super(context, config, table, instantTime);
    this.partitionIndexTypes = partitionIndexTypes;
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
  }

  @Override
  public Option<HoodieIndexPlan> execute() {
    validateBeforeScheduling();
    // make sure that it is idempotent, check with previously pending index operations.
    Set<String> indexesInflightOrCompleted = getInflightAndCompletedMetadataPartitions(table.getMetaClient().getTableConfig());
    Set<String> requestedPartitions = partitionIndexTypes.stream().map(MetadataPartitionType::getPartitionPath).collect(Collectors.toSet());
    requestedPartitions.removeAll(indexesInflightOrCompleted);
    if (!requestedPartitions.isEmpty()) {
      LOG.warn(String.format("Following partitions already exist or inflight: %s. Going to schedule indexing of only these partitions: %s",
          indexesInflightOrCompleted, requestedPartitions));
    } else {
      LOG.error("All requested index types are inflight or completed: " + partitionIndexTypes);
      return Option.empty();
    }
    List<MetadataPartitionType> finalPartitionsToIndex = partitionIndexTypes.stream()
        .filter(p -> requestedPartitions.contains(p.getPartitionPath())).collect(Collectors.toList());
    final HoodieInstant indexInstant = HoodieTimeline.getIndexRequestedInstant(instantTime);
    try {
      this.txnManager.beginTransaction(Option.of(indexInstant), Option.empty());
      // get last completed instant
      Option<HoodieInstant> indexUptoInstant = table.getActiveTimeline().getContiguousCompletedWriteTimeline().lastInstant();
      if (indexUptoInstant.isPresent()) {
        // start initializing file groups
        // in case FILES partition itself was not initialized before (i.e. metadata was never enabled), this will initialize synchronously
        HoodieTableMetadataWriter metadataWriter = table.getMetadataWriter(instantTime)
            .orElseThrow(() -> new HoodieIndexException(String.format("Could not get metadata writer to initialize filegroups for indexing for instant: %s", instantTime)));
        if (!finalPartitionsToIndex.get(0).getPartitionPath().equals(MetadataPartitionType.FILES.getPartitionPath())) {
          // initialize metadata partition only if not for FILES partition.
          metadataWriter.initializeMetadataPartitions(table.getMetaClient(), finalPartitionsToIndex, indexUptoInstant.get().getTimestamp());
        }

        // for each partitionToIndex add that time to the plan
        List<HoodieIndexPartitionInfo> indexPartitionInfos = finalPartitionsToIndex.stream()
            .map(p -> new HoodieIndexPartitionInfo(LATEST_INDEX_PLAN_VERSION, p.getPartitionPath(), indexUptoInstant.get().getTimestamp()))
            .collect(Collectors.toList());
        HoodieIndexPlan indexPlan = new HoodieIndexPlan(LATEST_INDEX_PLAN_VERSION, indexPartitionInfos);
        // update data timeline with requested instant
        table.getActiveTimeline().saveToPendingIndexAction(indexInstant, TimelineMetadataUtils.serializeIndexPlan(indexPlan));
        return Option.of(indexPlan);
      }
    } catch (IOException e) {
      LOG.error("Could not initialize file groups", e);
      // abort gracefully
      abort(indexInstant);
      throw new HoodieIOException(e.getMessage(), e);
    } finally {
      this.txnManager.endTransaction(Option.of(indexInstant));
    }

    return Option.empty();
  }

  private void validateBeforeScheduling() {
    if (!EnumSet.allOf(MetadataPartitionType.class).containsAll(partitionIndexTypes)) {
      throw new HoodieIndexException("Not all index types are valid: " + partitionIndexTypes);
    }
    // ensure lock provider configured
    if (!config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl() || StringUtils.isNullOrEmpty(config.getLockProviderClass())) {
      throw new HoodieIndexException(String.format("Need to set %s as %s and configure lock provider class",
          WRITE_CONCURRENCY_MODE.key(), OPTIMISTIC_CONCURRENCY_CONTROL.name()));
    }
  }

  private void abort(HoodieInstant indexInstant) {
    // delete metadata partition
    partitionIndexTypes.forEach(partitionType -> {
      if (metadataPartitionExists(table.getMetaClient().getBasePath(), context, partitionType)) {
        deleteMetadataPartition(table.getMetaClient().getBasePath(), context, partitionType);
      }
    });
    // delete requested instant
    table.getMetaClient().reloadActiveTimeline().deleteInstantFileIfExists(indexInstant);
  }
}
