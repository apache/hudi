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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL;
import static org.apache.hudi.common.model.WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_CONCURRENCY_MODE;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataPartition;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getInflightAndCompletedMetadataPartitions;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getSecondaryOrExpressionIndexName;
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

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleIndexActionExecutor.class);
  private static final Integer INDEX_PLAN_VERSION_1 = 1;
  private static final Integer LATEST_INDEX_PLAN_VERSION = INDEX_PLAN_VERSION_1;

  private final List<MetadataPartitionType> partitionIndexTypes;

  private final List<String> partitionPaths;

  public ScheduleIndexActionExecutor(HoodieEngineContext context,
                                     HoodieWriteConfig config,
                                     HoodieTable<T, I, K, O> table,
                                     String instantTime,
                                     List<MetadataPartitionType> partitionIndexTypes,
                                     List<String> partitionPaths) {
    super(context, config, table, instantTime);
    this.partitionIndexTypes = partitionIndexTypes;
    this.partitionPaths = partitionPaths;
  }

  @Override
  public Option<HoodieIndexPlan> execute() {
    validateBeforeScheduling();
    // make sure that it is idempotent, check with previously pending index operations.
    Set<String> indexesInflightOrCompleted = getInflightAndCompletedMetadataPartitions(table.getMetaClient().getTableConfig());
    InstantGenerator instantGenerator = table.getMetaClient().getInstantGenerator();

    Set<String> requestedPartitions = partitionIndexTypes.stream().map(p -> {
      HoodieMetadataConfig metadataConfig = config.getMetadataConfig();
      if (MetadataPartitionType.EXPRESSION_INDEX.equals(p)) {
        return getSecondaryOrExpressionIndexName(metadataConfig::getExpressionIndexName, PARTITION_NAME_EXPRESSION_INDEX_PREFIX, metadataConfig.getExpressionIndexColumn());
      } else if (MetadataPartitionType.SECONDARY_INDEX.equals(p)) {
        return getSecondaryOrExpressionIndexName(metadataConfig::getSecondaryIndexName, PARTITION_NAME_SECONDARY_INDEX_PREFIX, metadataConfig.getSecondaryIndexColumn());
      }
      return p.getPartitionPath();
    }).collect(Collectors.toSet());
    requestedPartitions.addAll(partitionPaths);
    requestedPartitions.removeAll(indexesInflightOrCompleted);

    if (!requestedPartitions.isEmpty()) {
      LOG.info("Some index partitions already exist: {}. Scheduling indexing of only these remaining partitions: {}",
          indexesInflightOrCompleted, requestedPartitions);
    } else {
      LOG.info("All requested index partitions exist (either inflight or built): {}", partitionIndexTypes);
      return Option.empty();
    }
    List<MetadataPartitionType> finalPartitionsToIndex = partitionIndexTypes.stream()
        .filter(p -> {
          HoodieMetadataConfig metadataConfig = config.getMetadataConfig();
          String partitionName;
          if (MetadataPartitionType.EXPRESSION_INDEX.equals(p)) {
            partitionName = getSecondaryOrExpressionIndexName(metadataConfig::getExpressionIndexName, PARTITION_NAME_EXPRESSION_INDEX_PREFIX, metadataConfig.getExpressionIndexColumn());
          } else if (MetadataPartitionType.SECONDARY_INDEX.equals(p)) {
            partitionName = getSecondaryOrExpressionIndexName(metadataConfig::getSecondaryIndexName, PARTITION_NAME_SECONDARY_INDEX_PREFIX, metadataConfig.getSecondaryIndexColumn());
          } else {
            partitionName = p.getPartitionPath();
          }
          return requestedPartitions.contains(partitionName);
        }).collect(Collectors.toList());
    final HoodieInstant indexInstant = instantGenerator.getIndexRequestedInstant(instantTime);
    try {
      // get last completed instant
      Option<HoodieInstant> indexUptoInstant = table.getActiveTimeline().getContiguousCompletedWriteTimeline().lastInstant();
      if (indexUptoInstant.isPresent()) {
        // for each partitionToIndex add that time to the plan
        List<HoodieIndexPartitionInfo> indexPartitionInfos = finalPartitionsToIndex.stream()
            .map(p -> buildIndexPartitionInfo(p, indexUptoInstant.get()))
            .collect(Collectors.toList());
        HoodieIndexPlan indexPlan = new HoodieIndexPlan(LATEST_INDEX_PLAN_VERSION, indexPartitionInfos);
        // update data timeline with requested instant
        table.getActiveTimeline().saveToPendingIndexAction(indexInstant, indexPlan);
        return Option.of(indexPlan);
      }
    } catch (HoodieIOException e) {
      LOG.error("Could not initialize file groups", e);
      // abort gracefully
      abort(indexInstant);
    }

    return Option.empty();
  }

  private HoodieIndexPartitionInfo buildIndexPartitionInfo(MetadataPartitionType partitionType, HoodieInstant indexUptoInstant) {
    String partitionName = partitionType.getPartitionPath();
    HoodieMetadataConfig metadataConfig = config.getMetadataConfig();
    // for expression or secondary index, we need to pass the metadata config to derive the partition name
    if (MetadataPartitionType.EXPRESSION_INDEX.equals(partitionType)) {
      partitionName = getSecondaryOrExpressionIndexName(metadataConfig::getExpressionIndexName, PARTITION_NAME_EXPRESSION_INDEX_PREFIX, metadataConfig.getExpressionIndexColumn());
    }
    if (MetadataPartitionType.SECONDARY_INDEX.equals(partitionType)) {
      partitionName = getSecondaryOrExpressionIndexName(metadataConfig::getSecondaryIndexName, PARTITION_NAME_SECONDARY_INDEX_PREFIX, metadataConfig.getSecondaryIndexColumn());
    }
    return new HoodieIndexPartitionInfo(LATEST_INDEX_PLAN_VERSION, partitionName, indexUptoInstant.requestedTime(), Collections.emptyMap());
  }

  private void validateBeforeScheduling() {
    if (!EnumSet.allOf(MetadataPartitionType.class).containsAll(partitionIndexTypes)) {
      throw new HoodieIndexException("Not all index types are valid: " + partitionIndexTypes);
    }
    // ensure lock provider configured
    if (!config.getWriteConcurrencyMode().supportsMultiWriter() || StringUtils.isNullOrEmpty(config.getLockProviderClass())) {
      throw new HoodieIndexException(String.format("Need to set %s as %s or %s and configure lock provider class",
          WRITE_CONCURRENCY_MODE.key(), OPTIMISTIC_CONCURRENCY_CONTROL.name(), NON_BLOCKING_CONCURRENCY_CONTROL.name()));
    }
  }

  private void abort(HoodieInstant indexInstant) {
    // delete metadata partition
    partitionIndexTypes.forEach(partitionType -> {
      if (metadataPartitionExists(table.getMetaClient().getBasePath(), context, partitionType.getPartitionPath())) {
        deleteMetadataPartition(table.getMetaClient().getBasePath(), context, partitionType.getPartitionPath());
      }
    });
    // delete requested instant
    table.getMetaClient().reloadActiveTimeline().deleteInstantFileIfExists(indexInstant);
  }
}
