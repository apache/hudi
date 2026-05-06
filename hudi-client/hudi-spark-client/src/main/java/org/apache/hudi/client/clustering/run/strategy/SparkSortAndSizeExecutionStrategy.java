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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Clustering Strategy based on following.
 * 1) Spark execution engine.
 * 2) Uses bulk_insert to write data into new files.
 *
 * <p>When a {@link ClusteringGroupWriter} provider is registered on the runtime classpath
 * and reports {@link ClusteringGroupWriter#isEnabled()}, this strategy delegates each input
 * group to the writer. The Row path is forced (subject to the standard Row-writer
 * compatibility check) so the writer always sees the Dataset-based pipeline. Per-group
 * fallback to the default Spark path happens whenever the writer returns
 * {@link Option#empty()}.
 */
@Slf4j
public class SparkSortAndSizeExecutionStrategy<T>
    extends MultipleSparkJobExecutionStrategy<T> {

  /** Lazily initialized; write config schema is constant for a given strategy instance. */
  private final AtomicReference<HoodieSchema> cachedSchema = new AtomicReference<>();

  public SparkSortAndSizeExecutionStrategy(HoodieTable table,
                                           HoodieEngineContext engineContext,
                                           HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected boolean shouldForceRowWriter() {
    Option<ClusteringGroupWriter> writer = ClusteringGroupWriterRegistry.get();
    return writer.isPresent() && writer.get().isEnabled();
  }

  @Override
  protected CompletableFuture<HoodieData<WriteStatus>> runClusteringForGroupAsyncAsRow(
      HoodieClusteringGroup clusteringGroup,
      Map<String, String> strategyParams,
      boolean shouldPreserveHoodieMetadata,
      String instantTime,
      ExecutorService clusteringExecutorService) {
    Option<CompletableFuture<HoodieData<WriteStatus>>> delegated = tryDelegateToGroupWriter(
        clusteringGroup, strategyParams, shouldPreserveHoodieMetadata, instantTime, clusteringExecutorService);
    if (delegated.isPresent()) {
      return delegated.get();
    }
    return runSuperRunClusteringForGroupAsyncAsRow(
        clusteringGroup, strategyParams, shouldPreserveHoodieMetadata, instantTime, clusteringExecutorService);
  }

  /**
   * Indirection over {@code super.runClusteringForGroupAsyncAsRow} so unit tests can
   * verify the fallback contract without bootstrapping a real {@link HoodieTable}. Tests
   * override only this method; the production {@code runClusteringForGroupAsyncAsRow}
   * routing logic is exercised unchanged.
   */
  CompletableFuture<HoodieData<WriteStatus>> runSuperRunClusteringForGroupAsyncAsRow(
      HoodieClusteringGroup clusteringGroup,
      Map<String, String> strategyParams,
      boolean shouldPreserveHoodieMetadata,
      String instantTime,
      ExecutorService clusteringExecutorService) {
    return super.runClusteringForGroupAsyncAsRow(
        clusteringGroup, strategyParams, shouldPreserveHoodieMetadata, instantTime, clusteringExecutorService);
  }

  /**
   * Routing for the {@link ClusteringGroupWriter} SPI. Returns the writer's future when a
   * writer is registered, reports enabled, AND can serve the group. Returns
   * {@link Option#empty()} otherwise so the caller falls back to the default path.
   *
   * <p>Package-private so tests can exercise the routing logic directly without needing a
   * real {@link HoodieTable} to drive {@code super.runClusteringForGroupAsyncAsRow}.
   */
  Option<CompletableFuture<HoodieData<WriteStatus>>> tryDelegateToGroupWriter(
      HoodieClusteringGroup clusteringGroup,
      Map<String, String> strategyParams,
      boolean shouldPreserveHoodieMetadata,
      String instantTime,
      ExecutorService clusteringExecutorService) {
    Option<ClusteringGroupWriter> writerOpt = ClusteringGroupWriterRegistry.get();
    if (!writerOpt.isPresent() || !writerOpt.get().isEnabled()) {
      return Option.empty();
    }
    ClusteringGroupWriter writer = writerOpt.get();
    log.info("Delegating clustering group (firstFileId={}, instant={}) to ClusteringGroupWriter '{}'",
        firstFileGroupId(clusteringGroup), instantTime, writer.name());
    ClusteringGroupWriteContext context = ClusteringGroupWriteContext.builder()
        .clusteringGroup(clusteringGroup)
        .strategyParams(strategyParams)
        .shouldPreserveHoodieMetadata(shouldPreserveHoodieMetadata)
        .instantTime(instantTime)
        .clusteringExecutorService(clusteringExecutorService)
        .schema(getCachedSchema())
        .table(getHoodieTable())
        .writeConfig(getWriteConfig())
        .build();
    Option<CompletableFuture<HoodieData<WriteStatus>>> result = writer.runClusteringForGroupAsync(context);
    if (!result.isPresent()) {
      log.info("ClusteringGroupWriter '{}' declined to serve group (firstFileId={}, instant={}); "
              + "falling back to the default Spark bulk-insert path.",
          writer.name(), firstFileGroupId(clusteringGroup), instantTime);
    }
    return result;
  }

  private static String firstFileGroupId(HoodieClusteringGroup clusteringGroup) {
    if (clusteringGroup.getSlices() == null || clusteringGroup.getSlices().isEmpty()) {
      return "<empty-group>";
    }
    return clusteringGroup.getSlices().get(0).getFileId();
  }

  private HoodieSchema getCachedSchema() {
    HoodieSchema local = cachedSchema.get();
    if (local != null) {
      return local;
    }
    HoodieSchema parsed = HoodieSchema.parse(getWriteConfig().getSchema());
    return cachedSchema.compareAndSet(null, parsed) ? parsed : cachedSchema.get();
  }

  @Override
  public HoodieData<WriteStatus> performClusteringWithRecordsAsRow(Dataset<Row> inputRecords,
                                                                   int numOutputGroups,
                                                                   String instantTime, Map<String, String> strategyParams,
                                                                   HoodieSchema schema,
                                                                   List<HoodieFileGroupId> fileGroupIdList,
                                                                   boolean shouldPreserveHoodieMetadata,
                                                                   Map<String, String> extraMetadata) {
    log.info("Starting clustering for a group, parallelism:{} commit:{}", numOutputGroups, instantTime);
    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder()
        .withBulkInsertParallelism(numOutputGroups)
        .withProps(getWriteConfig().getProps()).build();

    newConfig.setValue(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE, String.valueOf(getWriteConfig().getClusteringTargetFileMaxBytes()));

    BulkInsertPartitioner<Dataset<Row>> partitioner = getRowPartitioner(strategyParams, schema);
    Dataset<Row> repartitionedRecords = partitioner.repartitionRecords(inputRecords, numOutputGroups);

    return HoodieDatasetBulkInsertHelper.bulkInsert(repartitionedRecords, instantTime, getHoodieTable(), newConfig,
        partitioner.arePartitionRecordsSorted(), shouldPreserveHoodieMetadata);
  }

  @Override
  public HoodieData<WriteStatus> performClusteringWithRecordsRDD(final HoodieData<HoodieRecord<T>> inputRecords,
                                                                 final int numOutputGroups,
                                                                 final String instantTime,
                                                                 final Map<String, String> strategyParams,
                                                                 final HoodieSchema schema,
                                                                 final List<HoodieFileGroupId> fileGroupIdList,
                                                                 final boolean shouldPreserveHoodieMetadata,
                                                                 final Map<String, String> extraMetadata) {
    log.info("Starting clustering for a group, parallelism:{} commit:{}", numOutputGroups, instantTime);

    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder()
        .withBulkInsertParallelism(numOutputGroups)
        .withProps(getWriteConfig().getProps()).build();

    newConfig.setValue(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE, String.valueOf(getWriteConfig().getClusteringTargetFileMaxBytes()));

    return (HoodieData<WriteStatus>) SparkBulkInsertHelper.newInstance().bulkInsert(inputRecords, instantTime, getHoodieTable(),
        newConfig, false, getRDDPartitioner(strategyParams, schema), true, numOutputGroups, new CreateHandleFactory(shouldPreserveHoodieMetadata));
  }
}
