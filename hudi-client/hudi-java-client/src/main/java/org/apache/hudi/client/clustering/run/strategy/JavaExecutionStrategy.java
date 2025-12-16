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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.JavaTaskContextSupplier;
import org.apache.hudi.client.utils.LazyConcatenatingIterator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.JavaBulkInsertInternalPartitionerFactory;
import org.apache.hudi.execution.bulkinsert.JavaCustomColumnsSortPartitioner;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering strategy for Java engine.
 */
@Slf4j
public abstract class JavaExecutionStrategy<T>
    extends ClusteringExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {

  public JavaExecutionStrategy(
      HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> performClustering(
      HoodieClusteringPlan clusteringPlan, Schema schema, String instantTime) {
    // execute clustering for each group and collect WriteStatus
    List<WriteStatus> writeStatusList = new ArrayList<>();
    clusteringPlan.getInputGroups().forEach(
        inputGroup -> writeStatusList.addAll(runClusteringForGroup(
            inputGroup, clusteringPlan.getStrategy().getStrategyParams(),
            Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(true),
            instantTime)));
    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
    writeMetadata.setWriteStatuses(HoodieListData.eager(writeStatusList));
    return writeMetadata;
  }

  /**
   * Execute clustering to write inputRecords into new files as defined by rules in strategy parameters.
   * The number of new file groups created is bounded by numOutputGroups.
   * Note that commit is not done as part of strategy. commit is callers responsibility.
   *
   * @param inputRecords           List of {@link HoodieRecord}.
   * @param numOutputGroups        Number of output file groups.
   * @param instantTime            Clustering (replace commit) instant time.
   * @param strategyParams         Strategy parameters containing columns to sort the data by when clustering.
   * @param schema                 Schema of the data including metadata fields.
   * @param fileGroupIdList        File group id corresponding to each out group.
   * @param preserveHoodieMetadata Whether to preserve commit metadata while clustering.
   * @return List of {@link WriteStatus}.
   */
  public abstract List<WriteStatus> performClusteringWithRecordList(
      final List<HoodieRecord<T>> inputRecords, final int numOutputGroups, final String instantTime,
      final Map<String, String> strategyParams, final Schema schema,
      final List<HoodieFileGroupId> fileGroupIdList, final boolean preserveHoodieMetadata);

  /**
   * Create {@link BulkInsertPartitioner} based on strategy params.
   *
   * @param strategyParams Strategy parameters containing columns to sort the data by when clustering.
   * @param schema         Schema of the data including metadata fields.
   * @return partitioner for the java engine
   */
  protected BulkInsertPartitioner<List<HoodieRecord<T>>> getPartitioner(Map<String, String> strategyParams, Schema schema) {
    if (strategyParams.containsKey(PLAN_STRATEGY_SORT_COLUMNS.key())) {
      return new JavaCustomColumnsSortPartitioner(
          strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()).split(","),
          HoodieAvroUtils.addMetadataFields(schema), getWriteConfig());
    } else {
      return JavaBulkInsertInternalPartitionerFactory.get(getWriteConfig().getBulkInsertSortMode());
    }
  }

  /**
   * Executes clustering for the group.
   */
  private List<WriteStatus> runClusteringForGroup(
      HoodieClusteringGroup clusteringGroup, Map<String, String> strategyParams,
      boolean preserveHoodieMetadata, String instantTime) {
    List<HoodieRecord<T>> inputRecords = readRecordsForGroup(clusteringGroup, instantTime);
    Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
    List<HoodieFileGroupId> inputFileIds = clusteringGroup.getSlices().stream()
        .map(info -> new HoodieFileGroupId(info.getPartitionPath(), info.getFileId()))
        .collect(Collectors.toList());
    return performClusteringWithRecordList(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams, readerSchema, inputFileIds, preserveHoodieMetadata);
  }

  /**
   * Get a list of all records for the group. This includes all records from file slice
   * (Apply updates from log files, if any).
   */
  private List<HoodieRecord<T>> readRecordsForGroup(HoodieClusteringGroup clusteringGroup, String instantTime) {
    List<ClusteringOperation> clusteringOps = clusteringGroup.getSlices().stream().map(ClusteringOperation::create).collect(Collectors.toList());
    HoodieWriteConfig config = getWriteConfig();
    List<HoodieRecord<T>> records = new ArrayList<>();
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(new JavaTaskContextSupplier(), config);
    log.info("MaxMemoryPerCompaction run as part of clustering => {}", maxMemoryPerCompaction);

    List<Supplier<ClosableIterator<HoodieRecord<T>>>> suppliers = new ArrayList<>(clusteringOps.size());
    clusteringOps.forEach(op -> suppliers.add(() -> getRecordIterator(getEngineContext().getReaderContextFactory(getHoodieTable().getMetaClient()), op, instantTime, maxMemoryPerCompaction)));
    LazyConcatenatingIterator<HoodieRecord<T>> lazyIterator = new LazyConcatenatingIterator<>(suppliers);

    lazyIterator.forEachRemaining(records::add);
    lazyIterator.close();
    return records;
  }
}
