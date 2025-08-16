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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.update.strategy.SparkAllowUpdateStrategy;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.SparkPartitionUtils;
import org.apache.hudi.client.utils.SparkValidatorUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.SparkLazyInsertIterable;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieSparkIndexClient;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.FileGroupReaderBasedMergeHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieMergeHandleFactory;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.common.util.ClusteringUtils.getAllFileGroupsInPendingClusteringPlans;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE;

public abstract class BaseSparkCommitActionExecutor<T> extends
    BaseCommitActionExecutor<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>, HoodieWriteMetadata<HoodieData<WriteStatus>>> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseSparkCommitActionExecutor.class);
  protected final Option<BaseKeyGenerator> keyGeneratorOpt;
  private final ReaderContextFactory<T> readerContextFactory;

  public BaseSparkCommitActionExecutor(HoodieEngineContext context,
                                       HoodieWriteConfig config,
                                       HoodieTable table,
                                       String instantTime,
                                       WriteOperationType operationType) {
    this(context, config, table, instantTime, operationType, Option.empty());
  }

  public BaseSparkCommitActionExecutor(HoodieEngineContext context,
                                       HoodieWriteConfig config,
                                       HoodieTable table,
                                       String instantTime,
                                       WriteOperationType operationType,
                                       Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, operationType, extraMetadata);
    keyGeneratorOpt = HoodieSparkKeyGeneratorFactory.createBaseKeyGenerator(config);
    readerContextFactory = WriteOperationType.isChangingRecords(operationType) ? table.getReaderContextFactoryForWrite() : null;
  }

  protected HoodieData<HoodieRecord<T>> clusteringHandleUpdate(HoodieData<HoodieRecord<T>> inputRecords) {
    context.setJobStatus(this.getClass().getSimpleName(), "Handling updates which are under clustering: " + config.getTableName());
    Set<HoodieFileGroupId> fileGroupsInPendingClustering =
        table.getFileSystemView().getFileGroupsInPendingClustering().map(Pair::getKey).collect(Collectors.toSet());
    // Skip processing if there is no inflight clustering
    if (fileGroupsInPendingClustering.isEmpty()) {
      return inputRecords;
    }

    UpdateStrategy<T, HoodieData<HoodieRecord<T>>> updateStrategy = (UpdateStrategy<T, HoodieData<HoodieRecord<T>>>) ReflectionUtils
        .loadClass(config.getClusteringUpdatesStrategyClass(), new Class<?>[] {HoodieEngineContext.class, HoodieTable.class, Set.class},
            this.context, table, fileGroupsInPendingClustering);
    // For SparkAllowUpdateStrategy with rollback pending clustering as false, need not handle
    // the file group intersection between current ingestion and pending clustering file groups.
    // This will be handled at the conflict resolution strategy.
    if (updateStrategy instanceof SparkAllowUpdateStrategy && !config.isRollbackPendingClustering()) {
      return inputRecords;
    }
    Pair<HoodieData<HoodieRecord<T>>, Set<HoodieFileGroupId>> recordsAndPendingClusteringFileGroups =
        updateStrategy.handleUpdate(inputRecords);

    Set<HoodieFileGroupId> fileGroupsWithUpdatesAndPendingClustering = recordsAndPendingClusteringFileGroups.getRight();
    if (fileGroupsWithUpdatesAndPendingClustering.isEmpty()) {
      return recordsAndPendingClusteringFileGroups.getLeft();
    }
    // there are file groups pending clustering and receiving updates, so rollback the pending clustering instants
    // there could be race condition, for example, if the clustering completes after instants are fetched but before rollback completed
    if (config.isRollbackPendingClustering()) {
      Set<HoodieInstant> pendingClusteringInstantsToRollback = getAllFileGroupsInPendingClusteringPlans(table.getMetaClient()).entrySet().stream()
          .filter(e -> fileGroupsWithUpdatesAndPendingClustering.contains(e.getKey()))
          .map(Map.Entry::getValue)
          .collect(Collectors.toSet());
      pendingClusteringInstantsToRollback.forEach(instant -> {
        try (TransactionManager transactionManager = new TransactionManager(table.getConfig(), table.getStorage())) {
          transactionManager.beginStateChange(Option.empty(), Option.empty());
          String commitTime;
          try {
            commitTime = table.getMetaClient().createNewInstantTime(false);
            table.scheduleRollback(context, commitTime, instant, false, config.shouldRollbackUsingMarkers(), false);
          } finally {
            transactionManager.endStateChange(Option.empty());
          }
          table.rollback(context, commitTime, instant, true, true);
        }
      });
      table.getMetaClient().reloadActiveTimeline();
    }
    return recordsAndPendingClusteringFileGroups.getLeft();
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(HoodieData<HoodieRecord<T>> inputRecords) {
    return this.execute(inputRecords, Option.empty());
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(HoodieData<HoodieRecord<T>> inputRecords, Option<HoodieTimer> sourceReadAndIndexTimer) {
    // Cache the tagged records, so we don't end up computing both
    JavaRDD<HoodieRecord<T>> inputRDD = HoodieJavaRDD.getJavaRDD(inputRecords);
    if (shouldPersistInputRecords(inputRDD)) {
      HoodieJavaRDD.of(inputRDD).persist(config.getTaggedRecordStorageLevel(),
          context, HoodieDataCacheKey.of(config.getBasePath(), instantTime));
    } else {
      LOG.info("RDD PreppedRecords was persisted at: {}", inputRDD.getStorageLevel());
    }

    // Handle records update with clustering
    HoodieData<HoodieRecord<T>> inputRecordsWithClusteringUpdate = clusteringHandleUpdate(inputRecords);
    LOG.info("Num spark partitions for inputRecords before triggering workload profile {}", inputRecordsWithClusteringUpdate.getNumPartitions());

    WorkloadProfile workloadProfile = prepareWorkloadProfile(inputRecordsWithClusteringUpdate);
    Long sourceReadAndIndexDurationMs = null;
    if (sourceReadAndIndexTimer.isPresent()) {
      sourceReadAndIndexDurationMs = sourceReadAndIndexTimer.get().endTimer();
      LOG.info("Source read and index timer {}", sourceReadAndIndexDurationMs);
    }
    // partition using the insert partitioner
    final Partitioner partitioner = getPartitioner(workloadProfile);

    saveWorkloadProfileMetadataToInflight(workloadProfile, instantTime);

    context.setJobStatus(this.getClass().getSimpleName(), "Doing partition and writing data: " + config.getTableName());
    HoodieData<WriteStatus> writeStatuses = mapPartitionsAsRDD(inputRecordsWithClusteringUpdate, partitioner);
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();
    updateIndexAndMaybeRunPreCommitValidations(writeStatuses, result);
    if (sourceReadAndIndexTimer.isPresent()) {
      result.setSourceReadAndIndexDurationMs(sourceReadAndIndexDurationMs);
    }
    return result;
  }

  /**
   * Prepares workload profile.
   * @param inputRecordsWithClusteringUpdate input records of interest.
   * @return {@link WorkloadProfile} thus prepared.
   */
  protected WorkloadProfile prepareWorkloadProfile(HoodieData<HoodieRecord<T>> inputRecordsWithClusteringUpdate) {
    context.setJobStatus(this.getClass().getSimpleName(), "Building workload profile:" + config.getTableName());
    WorkloadProfile workloadProfile =
        new WorkloadProfile(buildProfile(inputRecordsWithClusteringUpdate), operationType, table.getIndex().canIndexLogFiles());
    LOG.debug("Input workload profile :{}", workloadProfile);
    return workloadProfile;
  }

  protected boolean shouldPersistInputRecords(JavaRDD<HoodieRecord<T>> inputRDD) {
    return !config.isSourceRddPersisted() || inputRDD.getStorageLevel() == StorageLevel.NONE();
  }

  /**
   * Count the number of updates/inserts for each file in each partition.
   */
  private Pair<Map<String, WorkloadStat>, WorkloadStat> buildProfile(HoodieData<HoodieRecord<T>> inputRecords) {
    HashMap<String, WorkloadStat> partitionPathStatMap = new HashMap<>();
    WorkloadStat globalStat = new WorkloadStat();

    // group the records by partitionPath + currentLocation combination, count the number of
    // records in each partition
    Map<Tuple2<String, Option<HoodieRecordLocation>>, Long> partitionLocationCounts = inputRecords
        .mapToPair(record -> Pair.of(
            new Tuple2<>(record.getPartitionPath(), Option.ofNullable(record.getCurrentLocation())), record))
        .countByKey();

    // count the number of both inserts and updates in each partition, update the counts to workLoadStats
    for (Map.Entry<Tuple2<String, Option<HoodieRecordLocation>>, Long> e : partitionLocationCounts.entrySet()) {
      String partitionPath = e.getKey()._1();
      Long count = e.getValue();
      Option<HoodieRecordLocation> locOption = e.getKey()._2();

      if (!partitionPathStatMap.containsKey(partitionPath)) {
        partitionPathStatMap.put(partitionPath, new WorkloadStat());
      }

      if (locOption.isPresent()) {
        // update
        partitionPathStatMap.get(partitionPath).addUpdates(locOption.get(), count);
        globalStat.addUpdates(locOption.get(), count);
      } else {
        // insert
        partitionPathStatMap.get(partitionPath).addInserts(count);
        globalStat.addInserts(count);
      }
    }
    return Pair.of(partitionPathStatMap, globalStat);
  }

  protected Partitioner getPartitioner(WorkloadProfile profile) {
    Option<String> layoutPartitionerClass = table.getStorageLayout().layoutPartitionerClass();
    if (layoutPartitionerClass.isPresent()) {
      return getLayoutPartitioner(profile, layoutPartitionerClass.get());
    } else if (WriteOperationType.isChangingRecords(operationType)) {
      return getUpsertPartitioner(profile);
    } else {
      return getInsertPartitioner(profile);
    }
  }

  protected HoodieData<WriteStatus> mapPartitionsAsRDD(HoodieData<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
    JavaPairRDD<Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>> mappedRDD = HoodieJavaPairRDD.getJavaPairRDD(
        dedupedRecords.mapToPair(record -> Pair.of(new Tuple2<>(record.getKey(), Option.ofNullable(record.getCurrentLocation())), record)));

    JavaPairRDD<Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>> partitionedRDD;
    if (table.requireSortedRecords()) {
      // Partition and sort within each partition as a single step. This is faster than partitioning first and then
      // applying a sort.
      Comparator<Tuple2<HoodieKey, Option<HoodieRecordLocation>>> comparator = (Comparator<Tuple2<HoodieKey, Option<HoodieRecordLocation>>> & Serializable) (t1, t2) -> {
        HoodieKey key1 = t1._1;
        HoodieKey key2 = t2._1;
        return key1.getRecordKey().compareTo(key2.getRecordKey());
      };

      partitionedRDD = mappedRDD.repartitionAndSortWithinPartitions(partitioner, comparator);
    } else {
      // Partition only
      partitionedRDD = mappedRDD.partitionBy(partitioner);
    }

    Broadcast<SparkBucketInfoGetter> bucketInfoGetter = ((HoodieSparkEngineContext) this.context)
        .getJavaSparkContext().broadcast(((SparkHoodiePartitioner) partitioner).getSparkBucketInfoGetter());
    return HoodieJavaRDD.of(partitionedRDD.map(Tuple2::_2).mapPartitionsWithIndex((partition, recordItr) -> {
      if (WriteOperationType.isChangingRecords(operationType)) {
        return handleUpsertPartition(instantTime, partition, recordItr, bucketInfoGetter);
      } else {
        return handleInsertPartition(instantTime, partition, recordItr, bucketInfoGetter);
      }
    }, true).flatMap(List::iterator));
  }

  protected HoodieData<WriteStatus> updateIndex(HoodieData<WriteStatus> writeStatuses, HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    // cache writeStatusRDD before updating index, so that all actions before this are not triggered again for future
    // RDD actions that are performed after updating the index.
    writeStatuses.persist(config.getString(WRITE_STATUS_STORAGE_LEVEL_VALUE), context, HoodieDataCacheKey.of(config.getBasePath(), instantTime));
    Instant indexStartTime = Instant.now();
    // Update the index back
    HoodieData<WriteStatus> statuses = table.getIndex().updateLocation(writeStatuses, context, table, instantTime);
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
    return statuses;
  }

  protected void updateIndexAndMaybeRunPreCommitValidations(HoodieData<WriteStatus> writeStatusRDD, HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    updateIndex(writeStatusRDD, result);
    result.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(result));
    runPrecommitValidators(result);
  }

  @Override
  protected String getCommitActionType() {
    return table.getMetaClient().getCommitActionType();
  }

  @Override
  protected void setCommitMetadata(HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    List<HoodieWriteStat> writeStats = result.getWriteStatuses().map(WriteStatus::getStat).collectAsList();
    result.setWriteStats(writeStats);
    result.setCommitMetadata(Option.of(CommitUtils.buildMetadata(writeStats,
        result.getPartitionToReplaceFileIds(),
        extraMetadata, operationType, getSchemaToStoreInCommit(), getCommitActionType())));
  }

  @Override
  protected void commit(HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    context.setJobStatus(this.getClass().getSimpleName(), "Commit write status collect: " + config.getTableName());
    commit(result, result.getWriteStats().isPresent()
        ? result.getWriteStats().get() : result.getWriteStatuses().map(WriteStatus::getStat).collectAsList());
  }

  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieWriteMetadata<HoodieData<WriteStatus>> writeStatuses) {
    return Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  protected Iterator<List<WriteStatus>> handleUpsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                              Broadcast<SparkBucketInfoGetter> bucketInfoGetter) {
    BucketInfo binfo = bucketInfoGetter.getValue().getBucketInfo(partition);
    BucketType btype = binfo.bucketType;
    try {
      if (btype.equals(BucketType.INSERT)) {
        return handleInsert(binfo.fileIdPrefix, recordItr);
      } else if (btype.equals(BucketType.UPDATE)) {
        return handleUpdate(binfo.partitionPath, binfo.fileIdPrefix, recordItr);
      } else {
        throw new HoodieUpsertException("Unknown bucketType " + btype + " for partition :" + partition);
      }
    } catch (Throwable t) {
      String msg = "Error upserting bucketType " + btype + " for partition :" + partition;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  protected Iterator<List<WriteStatus>> handleInsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                              Broadcast<SparkBucketInfoGetter> bucketInfoGetter) {
    return handleUpsertPartition(instantTime, partition, recordItr, bucketInfoGetter);
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
                                                  Iterator<HoodieRecord<T>> recordItr)
      throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition with fileId => {}", fileId);
      return Collections.emptyIterator();
    }

    // Pre-check: if the old file does not exist (which may happen in bucket index case), fallback to insert
    if (!table.getBaseFileOnlyView().getLatestBaseFile(partitionPath, fileId).isPresent()
        && HoodieIndex.IndexType.BUCKET.equals(config.getIndexType())) {
      return handleInsert(fileId, recordItr);
    }

    // these are updates
    HoodieMergeHandle mergeHandle = getUpdateHandle(partitionPath, fileId, recordItr);
    return IOUtils.runMerge(mergeHandle, instantTime, fileId);
  }

  protected HoodieMergeHandle getUpdateHandle(String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    HoodieMergeHandle mergeHandle = HoodieMergeHandleFactory.create(operationType, config, instantTime, table, recordItr, partitionPath, fileId,
          taskContextSupplier, keyGeneratorOpt);
    if (mergeHandle.getOldFilePath() != null && mergeHandle.baseFileForMerge().getBootstrapBaseFile().isPresent()) {
      Option<String[]> partitionFields = table.getMetaClient().getTableConfig().getPartitionFields();
      Object[] partitionValues = SparkPartitionUtils.getPartitionFieldVals(partitionFields, mergeHandle.getPartitionPath(),
          table.getMetaClient().getTableConfig().getBootstrapBasePath().get(),
          mergeHandle.getWriterSchema(), (Configuration) table.getStorageConf().unwrap());
      mergeHandle.setPartitionFields(partitionFields);
      mergeHandle.setPartitionValues(partitionValues);
    }
    if (readerContextFactory != null && mergeHandle instanceof FileGroupReaderBasedMergeHandle) {
      ((FileGroupReaderBasedMergeHandle) mergeHandle).setReaderContext(readerContextFactory.getContext());
    }
    return mergeHandle;
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr) {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition");
      return Collections.emptyIterator();
    }
    return new SparkLazyInsertIterable<>(recordItr, true, config, instantTime, table, idPfx,
        taskContextSupplier, new CreateHandleFactory<>());
  }

  public Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    return new UpsertPartitioner<>(profile, context, table, config, operationType);
  }

  public Partitioner getInsertPartitioner(WorkloadProfile profile) {
    return getUpsertPartitioner(profile);
  }

  public Partitioner getLayoutPartitioner(WorkloadProfile profile, String layoutPartitionerClass) {
    return (Partitioner) ReflectionUtils.loadClass(layoutPartitionerClass,
        new Class[] {WorkloadProfile.class, HoodieEngineContext.class, HoodieTable.class, HoodieWriteConfig.class},
        profile, context, table, config);
  }

  @Override
  protected void runPrecommitValidators(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    SparkValidatorUtils.runValidators(config, writeMetadata, context, table, instantTime);
  }

  @Override
  protected void updateColumnsToIndexForColumnStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    new HoodieSparkIndexClient(config, context).createOrUpdateColumnStatsIndexDefinition(metaClient, columnsToIndex);
  }
}
