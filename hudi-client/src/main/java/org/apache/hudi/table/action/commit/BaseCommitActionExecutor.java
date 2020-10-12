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

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.SparkConfigUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class BaseCommitActionExecutor<T extends HoodieRecordPayload<T>, R>
    extends BaseActionExecutor<R> {

  private static final Logger LOG = LogManager.getLogger(BaseCommitActionExecutor.class);

  protected final Option<Map<String, String>> extraMetadata;
  private final WriteOperationType operationType;
  protected final SparkTaskContextSupplier sparkTaskContextSupplier = new SparkTaskContextSupplier();

  public BaseCommitActionExecutor(JavaSparkContext jsc, HoodieWriteConfig config,
      HoodieTable table, String instantTime, WriteOperationType operationType,
      Option<Map<String, String>> extraMetadata) {
    super(jsc, config, table, instantTime);
    this.operationType = operationType;
    this.extraMetadata = extraMetadata;
  }

  public HoodieWriteMetadata execute(JavaRDD<HoodieRecord<T>> inputRecordsRDD) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();
    // Cache the tagged records, so we don't end up computing both
    // TODO: Consistent contract in HoodieWriteClient regarding preppedRecord storage level handling
    if (inputRecordsRDD.getStorageLevel() == StorageLevel.NONE()) {
      inputRecordsRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
    } else {
      LOG.info("RDD PreppedRecords was persisted at: " + inputRecordsRDD.getStorageLevel());
    }

    WorkloadProfile profile = null;
    if (isWorkloadProfileNeeded()) {
      profile = new WorkloadProfile(inputRecordsRDD);
      LOG.info("Workload profile :" + profile);
      saveWorkloadProfileMetadataToInflight(profile, instantTime);
    }

    // partition using the insert partitioner
    final Partitioner partitioner = getPartitioner(profile);
    JavaRDD<HoodieRecord<T>> partitionedRecords = partition(inputRecordsRDD, partitioner);
    JavaRDD<WriteStatus> writeStatusRDD = partitionedRecords.mapPartitionsWithIndex((partition, recordItr) -> {
      if (WriteOperationType.isChangingRecords(operationType)) {
        return handleUpsertPartition(instantTime, partition, recordItr, partitioner);
      } else {
        return handleInsertPartition(instantTime, partition, recordItr, partitioner);
      }
    }, true).flatMap(List::iterator);

    updateIndexAndCommitIfNeeded(writeStatusRDD, result);
    return result;
  }

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful when performing
   * rollback for MOR tables. Only updates are recorded in the workload profile metadata since updates to log blocks
   * are unknown across batches Inserts (which are new parquet files) are rolled back based on commit time. // TODO :
   * Create a new WorkloadProfile metadata file instead of using HoodieCommitMetadata
   */
  void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, String instantTime)
      throws HoodieCommitException {
    try {
      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
      profile.getPartitionPaths().forEach(path -> {
        WorkloadStat partitionStat = profile.getWorkloadStat(path.toString());
        HoodieWriteStat insertStat = new HoodieWriteStat();
        insertStat.setNumInserts(partitionStat.getNumInserts());
        insertStat.setFileId("");
        insertStat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
        metadata.addWriteStat(path.toString(), insertStat);

        partitionStat.getUpdateLocationToCount().forEach((key, value) -> {
          HoodieWriteStat writeStat = new HoodieWriteStat();
          writeStat.setFileId(key);
          // TODO : Write baseCommitTime is possible here ?
          writeStat.setPrevCommit(value.getKey());
          writeStat.setNumUpdateWrites(value.getValue());
          metadata.addWriteStat(path.toString(), writeStat);
        });
      });
      metadata.setOperationType(operationType);

      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = getCommitActionType();
      HoodieInstant requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime);
      activeTimeline.transitionRequestedToInflight(requested,
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)),
          config.shouldAllowMultiWriteOnSameInstant());
    } catch (IOException io) {
      throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", io);
    }
  }

  protected Partitioner getPartitioner(WorkloadProfile profile) {
    if (WriteOperationType.isChangingRecords(operationType)) {
      return getUpsertPartitioner(profile);
    } else {
      return getInsertPartitioner(profile);
    }
  }

  private JavaRDD<HoodieRecord<T>> partition(JavaRDD<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
    JavaPairRDD<Tuple2, HoodieRecord<T>> mappedRDD = dedupedRecords.mapToPair(
        record -> new Tuple2<>(new Tuple2<>(record.getKey(), Option.ofNullable(record.getCurrentLocation())), record));

    JavaPairRDD<Tuple2, HoodieRecord<T>> partitionedRDD;
    if (table.requireSortedRecords()) {
      // Partition and sort within each partition as a single step. This is faster than partitioning first and then
      // applying a sort.
      Comparator<Tuple2> comparator = (Comparator<Tuple2> & Serializable)(t1, t2) -> {
        HoodieKey key1 = (HoodieKey) t1._1;
        HoodieKey key2 = (HoodieKey) t2._1;
        return key1.getRecordKey().compareTo(key2.getRecordKey());
      };

      partitionedRDD = mappedRDD.repartitionAndSortWithinPartitions(partitioner, comparator);
    } else {
      // Partition only
      partitionedRDD = mappedRDD.partitionBy(partitioner);
    }

    return partitionedRDD.map(Tuple2::_2);
  }

  protected void updateIndexAndCommitIfNeeded(JavaRDD<WriteStatus> writeStatusRDD, HoodieWriteMetadata result) {
    // cache writeStatusRDD before updating index, so that all actions before this are not triggered again for future
    // RDD actions that are performed after updating the index.
    writeStatusRDD = writeStatusRDD.persist(SparkConfigUtils.getWriteStatusStorageLevel(config.getProps()));
    Instant indexStartTime = Instant.now();
    // Update the index back
    JavaRDD<WriteStatus> statuses = ((HoodieTable<T>)table).getIndex().updateLocation(writeStatusRDD, jsc,
        (HoodieTable<T>)table);
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
    result.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(statuses));
    commitOnAutoCommit(result);
  }

  protected void commitOnAutoCommit(HoodieWriteMetadata result) {
    if (config.shouldAutoCommit()) {
      LOG.info("Auto commit enabled: Committing " + instantTime);
      commit(extraMetadata, result);
    } else {
      LOG.info("Auto commit disabled for " + instantTime);
    }
  }

  protected String getCommitActionType() {
    return  table.getMetaClient().getCommitActionType();
  }

  protected void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata result) {
    commit(extraMetadata, result, result.getWriteStatuses().map(WriteStatus::getStat).collect());
  }

  protected void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata result, List<HoodieWriteStat> writeStats) {
    String actionType = getCommitActionType();
    LOG.info("Committing " + instantTime + ", action Type " + actionType);
    result.setCommitted(true);
    result.setWriteStats(writeStats);
    // Finalize write
    finalizeWrite(instantTime, writeStats, result);

    try {
      LOG.info("Committing " + instantTime + ", action Type " + getCommitActionType());
      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      HoodieCommitMetadata metadata = CommitUtils.buildMetadata(writeStats, result.getPartitionToReplaceFileIds(),
          extraMetadata, operationType, getSchemaToStoreInCommit(), getCommitActionType());

      table.metadata().update(jsc, metadata, instantTime);

      activeTimeline.saveAsComplete(new HoodieInstant(true, getCommitActionType(), instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      LOG.info("Committed " + instantTime);
      result.setCommitMetadata(Option.of(metadata));
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
  }

  protected Map<String, List<String>> getPartitionToReplacedFileIds(JavaRDD<WriteStatus> writeStatuses) {
    return Collections.emptyMap();
  }

  /**
   * Finalize Write operation.
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(String instantTime, List<HoodieWriteStat> stats, HoodieWriteMetadata result) {
    try {
      Instant start = Instant.now();
      table.finalizeWrite(jsc, instantTime, stats);
      result.setFinalizeDuration(Duration.between(start, Instant.now()));
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  /**
   * By default, return the writer schema in Write Config for storing in commit.
   */
  protected String getSchemaToStoreInCommit() {
    return config.getSchema();
  }

  protected boolean isWorkloadProfileNeeded() {
    return true;
  }

  @SuppressWarnings("unchecked")
  protected Iterator<List<WriteStatus>> handleUpsertPartition(String instantTime, Integer partition, Iterator recordItr,
      Partitioner partitioner) {
    UpsertPartitioner upsertPartitioner = (UpsertPartitioner) partitioner;
    BucketInfo binfo = upsertPartitioner.getBucketInfo(partition);
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
      Partitioner partitioner) {
    return handleUpsertPartition(instantTime, partition, recordItr, partitioner);
  }

  /**
   * Provides a partitioner to perform the upsert operation, based on the workload profile.
   */
  protected abstract Partitioner getUpsertPartitioner(WorkloadProfile profile);

  /**
   * Provides a partitioner to perform the insert operation, based on the workload profile.
   */
  protected abstract Partitioner getInsertPartitioner(WorkloadProfile profile);

  protected abstract Iterator<List<WriteStatus>> handleInsert(String idPfx,
      Iterator<HoodieRecord<T>> recordItr) throws Exception;

  protected abstract Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) throws IOException;
}
