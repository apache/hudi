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

package org.apache.hudi.commit;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.update.strategy.SparkAllowUpdateStrategy;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.BucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerWithRowsFactory;
import org.apache.hudi.execution.bulkinsert.ConsistentBucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.NonSortPartitionerWithRows;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE;

@Slf4j
public abstract class BaseDatasetBulkInsertCommitActionExecutor implements Serializable {

  protected final transient HoodieWriteConfig writeConfig;
  protected final transient SparkRDDWriteClient writeClient;
  @Getter
  protected String instantTime;
  protected HoodieTable table;

  public BaseDatasetBulkInsertCommitActionExecutor(HoodieWriteConfig config,
                                                   SparkRDDWriteClient writeClient, String instantTime) {
    this.writeConfig = config;
    this.writeClient = writeClient;
    this.instantTime = instantTime;
  }

  protected void preExecute() {
    table = writeClient.initTable(getWriteOperationType(), Option.ofNullable(instantTime));
    table.validateInsertSchema();
    writeClient.preWrite(instantTime, getWriteOperationType(), table.getMetaClient());
  }

  protected Option<HoodieData<WriteStatus>> doExecute(Dataset<Row> records, boolean arePartitionRecordsSorted) {
    table.getActiveTimeline().transitionRequestedToInflight(table.getMetaClient().createNewInstant(HoodieInstant.State.REQUESTED,
        getCommitActionType(), instantTime), Option.empty());
    return Option.of(HoodieDatasetBulkInsertHelper
        .bulkInsert(records, instantTime, table, writeConfig, arePartitionRecordsSorted, false));
  }

  protected void afterExecute(HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {
    writeClient.postWrite(result, instantTime, table);
  }

  private HoodieWriteMetadata<JavaRDD<WriteStatus>> buildHoodieWriteMetadata(Option<HoodieData<WriteStatus>> writeStatuses) {
    table.getMetaClient().reloadActiveTimeline();
    return writeStatuses.map(statuses -> {
      // cache writeStatusRDD, so that all actions before this are not triggered again for future
      statuses.persist(writeConfig.getString(WRITE_STATUS_STORAGE_LEVEL_VALUE), writeClient.getEngineContext(), HoodieData.HoodieDataCacheKey.of(writeConfig.getBasePath(), instantTime));
      HoodieWriteMetadata<JavaRDD<WriteStatus>> hoodieWriteMetadata = new HoodieWriteMetadata<>();
      hoodieWriteMetadata.setWriteStatuses(HoodieJavaRDD.getJavaRDD(statuses));
      hoodieWriteMetadata.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(statuses));
      return hoodieWriteMetadata;
    }).orElseGet(HoodieWriteMetadata::new);
  }

  public final HoodieWriteResult execute(Dataset<Row> records, boolean isTablePartitioned) {
    if (writeConfig.getBoolean(DataSourceWriteOptions.INSERT_DROP_DUPS())) {
      throw new HoodieException("Dropping duplicates with bulk_insert in row writer path is not supported yet");
    }

    boolean populateMetaFields = writeConfig.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS);
    preExecute();

    BulkInsertPartitioner<Dataset<Row>> bulkInsertPartitionerRows = getPartitioner(populateMetaFields, isTablePartitioned);
    Dataset<Row> hoodieDF = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(records, writeConfig, table.getMetaClient().getTableConfig(), bulkInsertPartitionerRows, instantTime);

    // Reject INSERT_OVERWRITE / INSERT_OVERWRITE_TABLE against partitions with pending
    // clustering before any writes materialize. Subclasses override getFileGroupsBeingReplaced;
    // default is a no-op (empty set) which preserves the non-overwrite paths.
    rejectIfOverlappingPendingClustering(hoodieDF);

    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = buildHoodieWriteMetadata(doExecute(hoodieDF, bulkInsertPartitionerRows.arePartitionRecordsSorted()));
    afterExecute(result);

    return new HoodieWriteResult(result.getWriteStatuses(), result.getPartitionToReplaceFileIds());
  }

  public abstract WriteOperationType getWriteOperationType();

  public String getCommitActionType() {
    return CommitUtils.getCommitActionType(getWriteOperationType(), writeClient.getConfig().getTableType());
  }

  protected BulkInsertPartitioner<Dataset<Row>> getPartitioner(boolean populateMetaFields, boolean isTablePartitioned) {
    if (populateMetaFields) {
      if (writeConfig.getIndexType() == HoodieIndex.IndexType.BUCKET) {
        if (writeConfig.getBucketIndexEngineType() == HoodieIndex.BucketIndexEngineType.SIMPLE) {
          return new BucketIndexBulkInsertPartitionerWithRows(writeConfig.getBucketIndexHashFieldWithDefault(), table.getConfig());
        } else {
          return new ConsistentBucketIndexBulkInsertPartitionerWithRows(table, Collections.emptyMap(), true);
        }
      } else {
        return DataSourceUtils
            .createUserDefinedBulkInsertPartitionerWithRows(writeConfig)
            .orElseGet(() -> BulkInsertInternalPartitionerWithRowsFactory.get(writeConfig, isTablePartitioned));
      }
    } else {
      // Sort modes are not yet supported when meta fields are disabled
      return new NonSortPartitionerWithRows();
    }
  }

  protected abstract Map<String, List<String>> getPartitionToReplacedFileIds(HoodieData<WriteStatus> writeStatuses);

  /**
   * Returns the file groups this operation will replace. Default is empty (non-overwrite paths).
   * Bulk-insert overwrite executors override this so the overlap-with-pending-clustering check
   * can fire before any writes materialize.
   *
   * @param preparedRecords the dataset after {@code HoodieDatasetBulkInsertHelper.prepareForBulkInsert}
   *                        has populated the {@code _hoodie_partition_path} meta field, so dynamic
   *                        partition resolution can read it.
   */
  protected Set<HoodieFileGroupId> getFileGroupsBeingReplaced(Dataset<Row> preparedRecords) {
    return Collections.emptySet();
  }

  /**
   * Mirrors {@code BaseSparkCommitActionExecutor#clusteringHandleUpdate} for the bulk-insert row
   * path: if any of the file groups this operation will replace are in pending clustering, route
   * through the configured {@code hoodie.clustering.updates.strategy}. With the default
   * {@code SparkRejectUpdateStrategy} this throws {@code HoodieClusteringUpdateException}; with
   * {@code SparkAllowUpdateStrategy} (and {@code !isRollbackPendingClustering()}) the overlap is
   * deferred to the conflict-resolution strategy, matching the existing Spark-side behavior.
   */
  protected void rejectIfOverlappingPendingClustering(Dataset<Row> preparedRecords) {
    Set<HoodieFileGroupId> fileGroupsInPendingClustering = table.getFileSystemView()
        .getFileGroupsInPendingClustering().map(Pair::getKey).collect(Collectors.toSet());
    if (fileGroupsInPendingClustering.isEmpty()) {
      return;
    }
    Set<HoodieFileGroupId> fileGroupsToBeReplaced = getFileGroupsBeingReplaced(preparedRecords);
    if (fileGroupsToBeReplaced.isEmpty()) {
      return;
    }

    HoodieEngineContext engineContext = writeClient.getEngineContext();
    UpdateStrategy<HoodieRecord, HoodieData<HoodieRecord>> updateStrategy = loadClusteringUpdateStrategy(
        engineContext, fileGroupsInPendingClustering, fileGroupsToBeReplaced);
    if (updateStrategy instanceof SparkAllowUpdateStrategy && !writeConfig.isRollbackPendingClustering()) {
      return;
    }
    // handleUpdate consumes only fileGroupsToBeReplaced on this path (no tagged records to
    // inspect for the bulk-insert overwrite case), so pass an empty HoodieData.
    updateStrategy.handleUpdate(engineContext.emptyHoodieData());
  }

  /**
   * Loads {@code hoodie.clustering.updates.strategy} via reflection, preferring the 4-arg
   * constructor (with {@code fileGroupsToBeReplaced}) and falling back to the legacy 3-arg
   * constructor for custom strategies that pre-date this PR.
   */
  @SuppressWarnings("unchecked")
  private UpdateStrategy<HoodieRecord, HoodieData<HoodieRecord>> loadClusteringUpdateStrategy(
      HoodieEngineContext engineContext,
      Set<HoodieFileGroupId> fileGroupsInPendingClustering,
      Set<HoodieFileGroupId> fileGroupsToBeReplaced) {
    String strategyClass = writeConfig.getClusteringUpdatesStrategyClass();
    try {
      return (UpdateStrategy<HoodieRecord, HoodieData<HoodieRecord>>) ReflectionUtils.loadClass(
          strategyClass,
          new Class<?>[] {HoodieEngineContext.class, HoodieTable.class, Set.class, Set.class},
          engineContext, table, fileGroupsInPendingClustering, fileGroupsToBeReplaced);
    } catch (HoodieException ex) {
      if (!(ex.getCause() instanceof NoSuchMethodException)) {
        throw ex;
      }
      log.warn("Clustering update strategy {} is missing the 4-arg constructor with "
          + "fileGroupsToBeReplaced; falling back to the 3-arg constructor. INSERT_OVERWRITE "
          + "overlap with pending clustering will not be detected for this strategy.", strategyClass);
      return (UpdateStrategy<HoodieRecord, HoodieData<HoodieRecord>>) ReflectionUtils.loadClass(
          strategyClass,
          new Class<?>[] {HoodieEngineContext.class, HoodieTable.class, Set.class},
          engineContext, table, fileGroupsInPendingClustering);
    }
  }
}
