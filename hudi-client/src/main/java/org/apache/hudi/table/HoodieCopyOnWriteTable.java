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

package org.apache.hudi.table;

import org.apache.hudi.WriteStatus;
import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.func.CopyOnWriteLazyInsertIterable;
import org.apache.hudi.func.ParquetReaderIterator;
import org.apache.hudi.func.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieCleanHelper;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;

import com.google.common.hash.Hashing;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Implementation of a very heavily read-optimized Hoodie Table where.
 * <p>
 * INSERTS - Produce new files, block aligned to desired size (or) Merge with the smallest existing file, to expand it
 * <p>
 * UPDATES - Produce a new version of the file, just replacing the updated records with new values
 */
public class HoodieCopyOnWriteTable<T extends HoodieRecordPayload> extends HoodieTable<T> {

  private static Logger logger = LogManager.getLogger(HoodieCopyOnWriteTable.class);

  public HoodieCopyOnWriteTable(HoodieWriteConfig config, JavaSparkContext jsc) {
    super(config, jsc);
  }

  private static PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, PartitionCleanStat> deleteFilesFunc(
      HoodieTable table) {
    return (PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, PartitionCleanStat>) iter -> {
      Map<String, PartitionCleanStat> partitionCleanStatMap = new HashMap<>();

      FileSystem fs = table.getMetaClient().getFs();
      Path basePath = new Path(table.getMetaClient().getBasePath());
      while (iter.hasNext()) {
        Tuple2<String, String> partitionDelFileTuple = iter.next();
        String partitionPath = partitionDelFileTuple._1();
        String delFileName = partitionDelFileTuple._2();
        String deletePathStr = new Path(new Path(basePath, partitionPath), delFileName).toString();
        Boolean deletedFileResult = deleteFileAndGetResult(fs, deletePathStr);
        if (!partitionCleanStatMap.containsKey(partitionPath)) {
          partitionCleanStatMap.put(partitionPath, new PartitionCleanStat(partitionPath));
        }
        PartitionCleanStat partitionCleanStat = partitionCleanStatMap.get(partitionPath);
        partitionCleanStat.addDeleteFilePatterns(deletePathStr);
        partitionCleanStat.addDeletedFileResult(deletePathStr, deletedFileResult);
      }
      return partitionCleanStatMap.entrySet().stream().map(e -> new Tuple2<>(e.getKey(), e.getValue()))
          .collect(Collectors.toList()).iterator();
    };
  }

  private static Boolean deleteFileAndGetResult(FileSystem fs, String deletePathStr) throws IOException {
    Path deletePath = new Path(deletePathStr);
    logger.debug("Working on delete path :" + deletePath);
    try {
      boolean deleteResult = fs.delete(deletePath, false);
      if (deleteResult) {
        logger.debug("Cleaned file at path :" + deletePath);
      }
      return deleteResult;
    } catch (FileNotFoundException fio) {
      // With cleanPlan being used for retried cleaning operations, its possible to clean a file twice
      return false;
    }
  }

  @Override
  public Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    return new UpsertPartitioner(profile);
  }

  @Override
  public Partitioner getInsertPartitioner(WorkloadProfile profile) {
    return getUpsertPartitioner(profile);
  }

  @Override
  public boolean isWorkloadProfileNeeded() {
    return true;
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(JavaSparkContext jsc, String commitTime) {
    throw new HoodieNotSupportedException("Compaction is not supported from a CopyOnWrite table");
  }

  @Override
  public JavaRDD<WriteStatus> compact(JavaSparkContext jsc, String compactionInstantTime,
      HoodieCompactionPlan compactionPlan) {
    throw new HoodieNotSupportedException("Compaction is not supported from a CopyOnWrite table");
  }

  public Iterator<List<WriteStatus>> handleUpdate(String commitTime, String fileId, Iterator<HoodieRecord<T>> recordItr)
      throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      logger.info("Empty partition with fileId => " + fileId);
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(commitTime, fileId, recordItr);
    return handleUpdateInternal(upsertHandle, commitTime, fileId);
  }

  public Iterator<List<WriteStatus>> handleUpdate(String commitTime, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieDataFile oldDataFile) throws IOException {
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(commitTime, fileId, keyToNewRecords, oldDataFile);
    return handleUpdateInternal(upsertHandle, commitTime, fileId);
  }

  protected Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle upsertHandle, String commitTime,
      String fileId) throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + commitTime + " for fileId: " + fileId);
    } else {
      AvroReadSupport.setAvroReadSchema(getHadoopConf(), upsertHandle.getWriterSchema());
      BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
      try (ParquetReader<IndexedRecord> reader =
          AvroParquetReader.<IndexedRecord>builder(upsertHandle.getOldFilePath()).withConf(getHadoopConf()).build()) {
        wrapper = new SparkBoundedInMemoryExecutor(config, new ParquetReaderIterator(reader),
            new UpdateHandler(upsertHandle), x -> x);
        wrapper.execute();
      } catch (Exception e) {
        throw new HoodieException(e);
      } finally {
        upsertHandle.close();
        if (null != wrapper) {
          wrapper.shutdownNow();
        }
      }
    }

    // TODO(vc): This needs to be revisited
    if (upsertHandle.getWriteStatus().getPartitionPath() == null) {
      logger.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
          + upsertHandle.getWriteStatus());
    }
    return Collections.singletonList(Collections.singletonList(upsertHandle.getWriteStatus())).iterator();
  }

  protected HoodieMergeHandle getUpdateHandle(String commitTime, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    return new HoodieMergeHandle<>(config, commitTime, this, recordItr, fileId);
  }

  protected HoodieMergeHandle getUpdateHandle(String commitTime, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieDataFile dataFileToBeMerged) {
    return new HoodieMergeHandle<>(config, commitTime, this, keyToNewRecords, fileId, dataFileToBeMerged);
  }

  public Iterator<List<WriteStatus>> handleInsert(String commitTime, String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      logger.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    return new CopyOnWriteLazyInsertIterable<>(recordItr, config, commitTime, this, idPfx);
  }

  public Iterator<List<WriteStatus>> handleInsert(String commitTime, String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) {
    HoodieCreateHandle createHandle =
        new HoodieCreateHandle(config, commitTime, this, partitionPath, fileId, recordItr);
    createHandle.write();
    return Collections.singletonList(Collections.singletonList(createHandle.close())).iterator();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<List<WriteStatus>> handleUpsertPartition(String commitTime, Integer partition, Iterator recordItr,
      Partitioner partitioner) {
    UpsertPartitioner upsertPartitioner = (UpsertPartitioner) partitioner;
    BucketInfo binfo = upsertPartitioner.getBucketInfo(partition);
    BucketType btype = binfo.bucketType;
    try {
      if (btype.equals(BucketType.INSERT)) {
        return handleInsert(commitTime, binfo.fileIdPrefix, recordItr);
      } else if (btype.equals(BucketType.UPDATE)) {
        return handleUpdate(commitTime, binfo.fileIdPrefix, recordItr);
      } else {
        throw new HoodieUpsertException("Unknown bucketType " + btype + " for partition :" + partition);
      }
    } catch (Throwable t) {
      String msg = "Error upserting bucketType " + btype + " for partition :" + partition;
      logger.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsertPartition(String commitTime, Integer partition, Iterator recordItr,
      Partitioner partitioner) {
    return handleUpsertPartition(commitTime, partition, recordItr, partitioner);
  }

  /**
   * Generates List of files to be cleaned.
   * 
   * @param jsc JavaSparkContext
   * @return Cleaner Plan
   */
  public HoodieCleanerPlan scheduleClean(JavaSparkContext jsc) {
    try {
      HoodieCleanHelper cleaner = new HoodieCleanHelper(this, config);
      Option<HoodieInstant> earliestInstant = cleaner.getEarliestCommitToRetain();

      List<String> partitionsToClean = cleaner.getPartitionPathsToClean(earliestInstant);

      if (partitionsToClean.isEmpty()) {
        logger.info("Nothing to clean here. It is already clean");
        return HoodieCleanerPlan.newBuilder().setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name()).build();
      }
      logger.info(
          "Total Partitions to clean : " + partitionsToClean.size() + ", with policy " + config.getCleanerPolicy());
      int cleanerParallelism = Math.min(partitionsToClean.size(), config.getCleanerParallelism());
      logger.info("Using cleanerParallelism: " + cleanerParallelism);

      Map<String, List<String>> cleanOps = jsc.parallelize(partitionsToClean, cleanerParallelism)
          .map(partitionPathToClean -> Pair.of(partitionPathToClean, cleaner.getDeletePaths(partitionPathToClean)))
          .collect().stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      return new HoodieCleanerPlan(earliestInstant
          .map(x -> new HoodieActionInstant(x.getTimestamp(), x.getAction(), x.getState().name())).orElse(null),
          config.getCleanerPolicy().name(), cleanOps, 1);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to schedule clean operation", e);
    }
  }

  /**
   * Performs cleaning of partition paths according to cleaning policy and returns the number of files cleaned. Handles
   * skews in partitions to clean by making files to clean as the unit of task distribution.
   *
   * @throws IllegalArgumentException if unknown cleaning policy is provided
   */
  @Override
  public List<HoodieCleanStat> clean(JavaSparkContext jsc, HoodieInstant cleanInstant) {
    try {
      HoodieCleanerPlan cleanerPlan = AvroUtils.deserializeCleanerPlan(getActiveTimeline()
          .getInstantAuxiliaryDetails(HoodieTimeline.getCleanRequestedInstant(cleanInstant.getTimestamp())).get());

      int cleanerParallelism = Math.min(
          (int) (cleanerPlan.getFilesToBeDeletedPerPartition().values().stream().mapToInt(x -> x.size()).count()),
          config.getCleanerParallelism());
      logger.info("Using cleanerParallelism: " + cleanerParallelism);
      List<Tuple2<String, PartitionCleanStat>> partitionCleanStats = jsc
          .parallelize(cleanerPlan.getFilesToBeDeletedPerPartition().entrySet().stream()
              .flatMap(x -> x.getValue().stream().map(y -> new Tuple2<String, String>(x.getKey(), y)))
              .collect(Collectors.toList()), cleanerParallelism)
          .mapPartitionsToPair(deleteFilesFunc(this)).reduceByKey((e1, e2) -> e1.merge(e2)).collect();

      Map<String, PartitionCleanStat> partitionCleanStatsMap =
          partitionCleanStats.stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

      // Return PartitionCleanStat for each partition passed.
      return cleanerPlan.getFilesToBeDeletedPerPartition().keySet().stream().map(partitionPath -> {
        PartitionCleanStat partitionCleanStat =
            (partitionCleanStatsMap.containsKey(partitionPath)) ? partitionCleanStatsMap.get(partitionPath)
                : new PartitionCleanStat(partitionPath);
        HoodieActionInstant actionInstant = cleanerPlan.getEarliestInstantToRetain();
        return HoodieCleanStat.newBuilder().withPolicy(config.getCleanerPolicy()).withPartitionPath(partitionPath)
            .withEarliestCommitRetained(Option.ofNullable(
                actionInstant != null
                    ? new HoodieInstant(HoodieInstant.State.valueOf(actionInstant.getState()),
                        actionInstant.getAction(), actionInstant.getTimestamp())
                    : null))
            .withDeletePathPattern(partitionCleanStat.deletePathPatterns)
            .withSuccessfulDeletes(partitionCleanStat.successDeleteFiles)
            .withFailedDeletes(partitionCleanStat.failedDeleteFiles).build();
      }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to clean up after commit", e);
    }
  }

  @Override
  public List<HoodieRollbackStat> rollback(JavaSparkContext jsc, String commit, boolean deleteInstants)
      throws IOException {
    String actionType = metaClient.getCommitActionType();
    HoodieActiveTimeline activeTimeline = this.getActiveTimeline();
    List<String> inflights =
        this.getInflightCommitTimeline().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    // Atomically unpublish the commits
    if (!inflights.contains(commit)) {
      logger.info("Unpublishing " + commit);
      activeTimeline.revertToInflight(new HoodieInstant(false, actionType, commit));
    }

    HoodieInstant instantToRollback = new HoodieInstant(false, actionType, commit);
    Long startTime = System.currentTimeMillis();

    // delete all the data files for this commit
    logger.info("Clean out all parquet files generated for commit: " + commit);
    List<RollbackRequest> rollbackRequests = generateRollbackRequests(instantToRollback);

    // TODO: We need to persist this as rollback workload and use it in case of partial failures
    List<HoodieRollbackStat> stats =
        new RollbackExecutor(metaClient, config).performRollback(jsc, instantToRollback, rollbackRequests);

    // Delete Inflight instant if enabled
    deleteInflightInstant(deleteInstants, activeTimeline, new HoodieInstant(true, actionType, commit));
    logger.info("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));
    return stats;
  }

  private List<RollbackRequest> generateRollbackRequests(HoodieInstant instantToRollback) throws IOException {
    return FSUtils.getAllPartitionPaths(this.metaClient.getFs(), this.getMetaClient().getBasePath(),
        config.shouldAssumeDatePartitioning()).stream().map(partitionPath -> {
          return RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback);
        }).collect(Collectors.toList());
  }

  /**
   * Delete Inflight instant if enabled.
   *
   * @param deleteInstant Enable Deletion of Inflight instant
   * @param activeTimeline Hoodie active timeline
   * @param instantToBeDeleted Instant to be deleted
   */
  protected void deleteInflightInstant(boolean deleteInstant, HoodieActiveTimeline activeTimeline,
      HoodieInstant instantToBeDeleted) {
    // Remove marker files always on rollback
    deleteMarkerDir(instantToBeDeleted.getTimestamp());

    // Remove the rolled back inflight commits
    if (deleteInstant) {
      activeTimeline.deleteInflight(instantToBeDeleted);
      logger.info("Deleted inflight commit " + instantToBeDeleted);
    } else {
      logger.warn("Rollback finished without deleting inflight instant file. Instant=" + instantToBeDeleted);
    }
  }

  enum BucketType {
    UPDATE, INSERT
  }

  /**
   * Consumer that dequeues records from queue and sends to Merge Handle.
   */
  private static class UpdateHandler extends BoundedInMemoryQueueConsumer<GenericRecord, Void> {

    private final HoodieMergeHandle upsertHandle;

    private UpdateHandler(HoodieMergeHandle upsertHandle) {
      this.upsertHandle = upsertHandle;
    }

    @Override
    protected void consumeOneRecord(GenericRecord record) {
      upsertHandle.write(record);
    }

    @Override
    protected void finish() {}

    @Override
    protected Void getResult() {
      return null;
    }
  }

  private static class PartitionCleanStat implements Serializable {

    private final String partitionPath;
    private final List<String> deletePathPatterns = new ArrayList<>();
    private final List<String> successDeleteFiles = new ArrayList<>();
    private final List<String> failedDeleteFiles = new ArrayList<>();

    private PartitionCleanStat(String partitionPath) {
      this.partitionPath = partitionPath;
    }

    private void addDeletedFileResult(String deletePathStr, Boolean deletedFileResult) {
      if (deletedFileResult) {
        successDeleteFiles.add(deletePathStr);
      } else {
        failedDeleteFiles.add(deletePathStr);
      }
    }

    private void addDeleteFilePatterns(String deletePathStr) {
      deletePathPatterns.add(deletePathStr);
    }

    private PartitionCleanStat merge(PartitionCleanStat other) {
      if (!this.partitionPath.equals(other.partitionPath)) {
        throw new RuntimeException(
            String.format("partitionPath is not a match: (%s, %s)", partitionPath, other.partitionPath));
      }
      successDeleteFiles.addAll(other.successDeleteFiles);
      deletePathPatterns.addAll(other.deletePathPatterns);
      failedDeleteFiles.addAll(other.failedDeleteFiles);
      return this;
    }
  }

  /**
   * Helper class for a small file's location and its actual size on disk.
   */
  static class SmallFile implements Serializable {

    HoodieRecordLocation location;
    long sizeBytes;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("SmallFile {");
      sb.append("location=").append(location).append(", ");
      sb.append("sizeBytes=").append(sizeBytes);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Helper class for an insert bucket along with the weight [0.0, 0.1] that defines the amount of incoming inserts that
   * should be allocated to the bucket.
   */
  class InsertBucket implements Serializable {

    int bucketNumber;
    // fraction of total inserts, that should go into this bucket
    double weight;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("WorkloadStat {");
      sb.append("bucketNumber=").append(bucketNumber).append(", ");
      sb.append("weight=").append(weight);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Helper class for a bucket's type (INSERT and UPDATE) and its file location.
   */
  class BucketInfo implements Serializable {

    BucketType bucketType;
    String fileIdPrefix;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BucketInfo {");
      sb.append("bucketType=").append(bucketType).append(", ");
      sb.append("fileIdPrefix=").append(fileIdPrefix);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Packs incoming records to be upserted, into buckets (1 bucket = 1 RDD partition).
   */
  class UpsertPartitioner extends Partitioner {

    /**
     * List of all small files to be corrected.
     */
    List<SmallFile> smallFiles = new ArrayList<SmallFile>();
    /**
     * Total number of RDD partitions, is determined by total buckets we want to pack the incoming workload into.
     */
    private int totalBuckets = 0;
    /**
     * Stat for the current workload. Helps in determining total inserts, upserts etc.
     */
    private WorkloadStat globalStat;
    /**
     * Helps decide which bucket an incoming update should go to.
     */
    private HashMap<String, Integer> updateLocationToBucket;
    /**
     * Helps us pack inserts into 1 or more buckets depending on number of incoming records.
     */
    private HashMap<String, List<InsertBucket>> partitionPathToInsertBuckets;
    /**
     * Remembers what type each bucket is for later.
     */
    private HashMap<Integer, BucketInfo> bucketInfoMap;

    /**
     * Rolling stats for files.
     */
    protected HoodieRollingStatMetadata rollingStatMetadata;
    protected long averageRecordSize;

    UpsertPartitioner(WorkloadProfile profile) {
      updateLocationToBucket = new HashMap<>();
      partitionPathToInsertBuckets = new HashMap<>();
      bucketInfoMap = new HashMap<>();
      globalStat = profile.getGlobalStat();
      rollingStatMetadata = getRollingStats();
      assignUpdates(profile);
      assignInserts(profile);

      logger.info("Total Buckets :" + totalBuckets + ", " + "buckets info => " + bucketInfoMap + ", \n"
          + "Partition to insert buckets => " + partitionPathToInsertBuckets + ", \n"
          + "UpdateLocations mapped to buckets =>" + updateLocationToBucket);
    }

    private void assignUpdates(WorkloadProfile profile) {
      // each update location gets a partition
      WorkloadStat gStat = profile.getGlobalStat();
      for (Map.Entry<String, Pair<String, Long>> updateLocEntry : gStat.getUpdateLocationToCount().entrySet()) {
        addUpdateBucket(updateLocEntry.getKey());
      }
    }

    private int addUpdateBucket(String fileIdHint) {
      int bucket = totalBuckets;
      updateLocationToBucket.put(fileIdHint, bucket);
      BucketInfo bucketInfo = new BucketInfo();
      bucketInfo.bucketType = BucketType.UPDATE;
      bucketInfo.fileIdPrefix = fileIdHint;
      bucketInfoMap.put(totalBuckets, bucketInfo);
      totalBuckets++;
      return bucket;
    }

    private void assignInserts(WorkloadProfile profile) {
      // for new inserts, compute buckets depending on how many records we have for each partition
      Set<String> partitionPaths = profile.getPartitionPaths();
      long averageRecordSize =
          averageBytesPerRecord(metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
              config.getCopyOnWriteRecordSizeEstimate());
      logger.info("AvgRecordSize => " + averageRecordSize);
      for (String partitionPath : partitionPaths) {
        WorkloadStat pStat = profile.getWorkloadStat(partitionPath);
        if (pStat.getNumInserts() > 0) {

          List<SmallFile> smallFiles = getSmallFiles(partitionPath);
          logger.info("For partitionPath : " + partitionPath + " Small Files => " + smallFiles);

          long totalUnassignedInserts = pStat.getNumInserts();
          List<Integer> bucketNumbers = new ArrayList<>();
          List<Long> recordsPerBucket = new ArrayList<>();

          // first try packing this into one of the smallFiles
          for (SmallFile smallFile : smallFiles) {
            long recordsToAppend = Math.min((config.getParquetMaxFileSize() - smallFile.sizeBytes) / averageRecordSize,
                totalUnassignedInserts);
            if (recordsToAppend > 0 && totalUnassignedInserts > 0) {
              // create a new bucket or re-use an existing bucket
              int bucket;
              if (updateLocationToBucket.containsKey(smallFile.location.getFileId())) {
                bucket = updateLocationToBucket.get(smallFile.location.getFileId());
                logger.info("Assigning " + recordsToAppend + " inserts to existing update bucket " + bucket);
              } else {
                bucket = addUpdateBucket(smallFile.location.getFileId());
                logger.info("Assigning " + recordsToAppend + " inserts to new update bucket " + bucket);
              }
              bucketNumbers.add(bucket);
              recordsPerBucket.add(recordsToAppend);
              totalUnassignedInserts -= recordsToAppend;
            }
          }

          // if we have anything more, create new insert buckets, like normal
          if (totalUnassignedInserts > 0) {
            long insertRecordsPerBucket = config.getCopyOnWriteInsertSplitSize();
            if (config.shouldAutoTuneInsertSplits()) {
              insertRecordsPerBucket = config.getParquetMaxFileSize() / averageRecordSize;
            }

            int insertBuckets = (int) Math.ceil((1.0 * totalUnassignedInserts) / insertRecordsPerBucket);
            logger.info("After small file assignment: unassignedInserts => " + totalUnassignedInserts
                + ", totalInsertBuckets => " + insertBuckets + ", recordsPerBucket => " + insertRecordsPerBucket);
            for (int b = 0; b < insertBuckets; b++) {
              bucketNumbers.add(totalBuckets);
              recordsPerBucket.add(totalUnassignedInserts / insertBuckets);
              BucketInfo bucketInfo = new BucketInfo();
              bucketInfo.bucketType = BucketType.INSERT;
              bucketInfo.fileIdPrefix = FSUtils.createNewFileIdPfx();
              bucketInfoMap.put(totalBuckets, bucketInfo);
              totalBuckets++;
            }
          }

          // Go over all such buckets, and assign weights as per amount of incoming inserts.
          List<InsertBucket> insertBuckets = new ArrayList<>();
          for (int i = 0; i < bucketNumbers.size(); i++) {
            InsertBucket bkt = new InsertBucket();
            bkt.bucketNumber = bucketNumbers.get(i);
            bkt.weight = (1.0 * recordsPerBucket.get(i)) / pStat.getNumInserts();
            insertBuckets.add(bkt);
          }
          logger.info("Total insert buckets for partition path " + partitionPath + " => " + insertBuckets);
          partitionPathToInsertBuckets.put(partitionPath, insertBuckets);
        }
      }
    }

    /**
     * Returns a list of small files in the given partition path.
     */
    protected List<SmallFile> getSmallFiles(String partitionPath) {

      // smallFiles only for partitionPath
      List<SmallFile> smallFileLocations = new ArrayList<>();

      HoodieTimeline commitTimeline = getCompletedCommitsTimeline();

      if (!commitTimeline.empty()) { // if we have some commits
        HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
        List<HoodieDataFile> allFiles = getROFileSystemView()
            .getLatestDataFilesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).collect(Collectors.toList());

        for (HoodieDataFile file : allFiles) {
          if (file.getFileSize() < config.getParquetSmallFileLimit()) {
            String filename = file.getFileName();
            SmallFile sf = new SmallFile();
            sf.location = new HoodieRecordLocation(FSUtils.getCommitTime(filename), FSUtils.getFileId(filename));
            sf.sizeBytes = file.getFileSize();
            smallFileLocations.add(sf);
            // Update the global small files list
            smallFiles.add(sf);
          }
        }
      }

      return smallFileLocations;
    }

    public BucketInfo getBucketInfo(int bucketNumber) {
      return bucketInfoMap.get(bucketNumber);
    }

    public List<InsertBucket> getInsertBuckets(String partitionPath) {
      return partitionPathToInsertBuckets.get(partitionPath);
    }

    @Override
    public int numPartitions() {
      return totalBuckets;
    }

    @Override
    public int getPartition(Object key) {
      Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation =
          (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
      if (keyLocation._2().isPresent()) {
        HoodieRecordLocation location = keyLocation._2().get();
        return updateLocationToBucket.get(location.getFileId());
      } else {
        List<InsertBucket> targetBuckets = partitionPathToInsertBuckets.get(keyLocation._1().getPartitionPath());
        // pick the target bucket to use based on the weights.
        double totalWeight = 0.0;
        final long totalInserts = Math.max(1, globalStat.getNumInserts());
        final long hashOfKey =
            Hashing.md5().hashString(keyLocation._1().getRecordKey(), StandardCharsets.UTF_8).asLong();
        final double r = 1.0 * Math.floorMod(hashOfKey, totalInserts) / totalInserts;
        for (InsertBucket insertBucket : targetBuckets) {
          totalWeight += insertBucket.weight;
          if (r <= totalWeight) {
            return insertBucket.bucketNumber;
          }
        }
        // return first one, by default
        return targetBuckets.get(0).bucketNumber;
      }
    }
  }

  protected HoodieRollingStatMetadata getRollingStats() {
    return null;
  }

  /**
   * Obtains the average record size based on records written during previous commits. Used for estimating how many
   * records pack into one file.
   */
  protected static long averageBytesPerRecord(HoodieTimeline commitTimeline, int defaultRecordSizeEstimate) {
    long avgSize = defaultRecordSizeEstimate;
    try {
      if (!commitTimeline.empty()) {
        // Go over the reverse ordered commits to get a more recent estimate of average record size.
        Iterator<HoodieInstant> instants = commitTimeline.getReverseOrderedInstants().iterator();
        while (instants.hasNext()) {
          HoodieInstant instant = instants.next();
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(commitTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          long totalBytesWritten = commitMetadata.fetchTotalBytesWritten();
          long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
          if (totalBytesWritten > 0 && totalRecordsWritten > 0) {
            avgSize = (long) Math.ceil((1.0 * totalBytesWritten) / totalRecordsWritten);
            break;
          }
        }
      }
    } catch (Throwable t) {
      // make this fail safe.
      logger.error("Error trying to compute average bytes/record ", t);
    }
    return avgSize;
  }
}
