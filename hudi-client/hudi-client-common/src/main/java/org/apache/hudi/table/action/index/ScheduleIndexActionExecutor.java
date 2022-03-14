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
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Schedules INDEX action.
 * <li>
 *   1. Fetch last completed instant on data timeline.
 *   2. Write the index plan to the <instant>.index.requested.
 *   3. Initialize filegroups for the enabled partition types within a transaction.
 * </li>
 */
public class ScheduleIndexActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieIndexPlan>> {

  private static final Logger LOG = LogManager.getLogger(ScheduleIndexActionExecutor.class);
  private static final Integer INDEX_PLAN_VERSION_1 = 1;
  private static final Integer LATEST_INDEX_PLAN_VERSION = INDEX_PLAN_VERSION_1;

  private final List<MetadataPartitionType> partitionsToIndex;
  private final TransactionManager txnManager;

  public ScheduleIndexActionExecutor(HoodieEngineContext context,
                                     HoodieWriteConfig config,
                                     HoodieTable<T, I, K, O> table,
                                     String instantTime,
                                     List<MetadataPartitionType> partitionsToIndex) {
    super(context, config, table, instantTime);
    this.partitionsToIndex = partitionsToIndex;
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
  }

  @Override
  public Option<HoodieIndexPlan> execute() {
    // validate partitionsToIndex
    if (!MetadataPartitionType.allPaths().containsAll(partitionsToIndex)) {
      throw new HoodieIndexException("Not all partitions are valid: " + partitionsToIndex);
    }
    // get last completed instant
    Option<HoodieInstant> indexUptoInstant = table.getActiveTimeline().filterCompletedInstants().lastInstant();
    if (indexUptoInstant.isPresent()) {
      final HoodieInstant indexInstant = HoodieTimeline.getIndexRequestedInstant(instantTime);
      // for each partitionToIndex add that time to the plan
      List<HoodieIndexPartitionInfo> indexPartitionInfos = partitionsToIndex.stream()
          .map(p -> new HoodieIndexPartitionInfo(LATEST_INDEX_PLAN_VERSION, p.getPartitionPath(), indexUptoInstant.get().getTimestamp()))
          .collect(Collectors.toList());
      HoodieIndexPlan indexPlan = new HoodieIndexPlan(LATEST_INDEX_PLAN_VERSION, indexPartitionInfos);
      try {
        table.getActiveTimeline().saveToPendingIndexCommit(indexInstant, TimelineMetadataUtils.serializeIndexPlan(indexPlan));
      } catch (IOException e) {
        LOG.error("Error while saving index requested file", e);
        throw new HoodieIOException(e.getMessage(), e);
      }
      table.getMetaClient().reloadActiveTimeline();

      // start initializing filegroups
      // 1. get metadata writer
      HoodieTableMetadataWriter metadataWriter = table.getMetadataWriter(instantTime)
          .orElseThrow(() -> new HoodieIndexException(String.format("Could not get metadata writer to run index action for instant: %s", instantTime)));
      // 2. take a lock --> begin tx (data table)
      try {
        this.txnManager.beginTransaction(Option.of(indexInstant), Option.empty());
        // 3. initialize filegroups as per plan for the enabled partition types
        for (MetadataPartitionType partitionType : partitionsToIndex) {
          metadataWriter.initializeFileGroups(table.getMetaClient(), partitionType, indexInstant.getTimestamp(), 1);
        }
      } catch (IOException e) {
        LOG.error("Could not initialize file groups");
        throw new HoodieIOException(e.getMessage(), e);
      } finally {
        this.txnManager.endTransaction(Option.of(indexInstant));
      }
      return Option.of(indexPlan);
    }
    return Option.empty();
  }
}
