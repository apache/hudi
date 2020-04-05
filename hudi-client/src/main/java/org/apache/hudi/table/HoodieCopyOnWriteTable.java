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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.ParquetReaderIterator;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.CopyOnWriteLazyInsertIterable;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.action.clean.CleanActionExecutor;
import org.apache.hudi.table.rollback.RollbackHelper;
import org.apache.hudi.table.rollback.RollbackRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of a very heavily read-optimized Hoodie Table where, all data is stored in base files, with
 * zero read amplification.
 *
 * <p>
 * INSERTS - Produce new files, block aligned to desired size (or) Merge with the smallest existing file, to expand it
 * <p>
 * UPDATES - Produce a new version of the file, just replacing the updated records with new values
 */
public class HoodieCopyOnWriteTable<T extends HoodieRecordPayload> extends HoodieTable<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieCopyOnWriteTable.class);

  public HoodieCopyOnWriteTable(HoodieWriteConfig config, JavaSparkContext jsc, HoodieTableMetaClient metaClient) {
    super(config, jsc, metaClient);
  }

  @Override
  public Partitioner getUpsertPartitioner(WorkloadProfile profile, JavaSparkContext jsc) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    return new UpsertPartitioner(profile, jsc);
  }

  @Override
  public Partitioner getInsertPartitioner(WorkloadProfile profile, JavaSparkContext jsc) {
    return getUpsertPartitioner(profile, jsc);
  }

  @Override
  public boolean isWorkloadProfileNeeded() {
    return true;
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(JavaSparkContext jsc, String instantTime) {
    throw new HoodieNotSupportedException("Compaction is not supported from a CopyOnWrite table");
  }

  @Override
  public JavaRDD<WriteStatus> compact(JavaSparkContext jsc, String compactionInstantTime,
      HoodieCompactionPlan compactionPlan) {
    throw new HoodieNotSupportedException("Compaction is not supported from a CopyOnWrite table");
  }

  public Iterator<List<WriteStatus>> handleUpdate(String instantTime, String partitionPath, String fileId,
                                                  Iterator<HoodieRecord<T>> recordItr)
      throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition with fileId => " + fileId);
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(instantTime, partitionPath, fileId, recordItr);
    return handleUpdateInternal(upsertHandle, instantTime, fileId);
  }

  public Iterator<List<WriteStatus>> handleUpdate(String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile oldDataFile) throws IOException {
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(instantTime, partitionPath, fileId, keyToNewRecords, oldDataFile);
    return handleUpdateInternal(upsertHandle, instantTime, fileId);
  }

  protected Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle upsertHandle, String instantTime,
      String fileId) throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
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
      LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
          + upsertHandle.getWriteStatus());
    }
    return Collections.singletonList(Collections.singletonList(upsertHandle.getWriteStatus())).iterator();
  }

  protected HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    return new HoodieMergeHandle<>(config, instantTime, this, recordItr, partitionPath, fileId, sparkTaskContextSupplier);
  }

  protected HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile dataFileToBeMerged) {
    return new HoodieMergeHandle<>(config, instantTime, this, keyToNewRecords,
            partitionPath, fileId, dataFileToBeMerged, sparkTaskContextSupplier);
  }

  public Iterator<List<WriteStatus>> handleInsert(String instantTime, String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    return new CopyOnWriteLazyInsertIterable<>(recordItr, config, instantTime, this, idPfx, sparkTaskContextSupplier);
  }

  public Iterator<List<WriteStatus>> handleInsert(String instantTime, String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) {
    HoodieCreateHandle createHandle =
        new HoodieCreateHandle(config, instantTime, this, partitionPath, fileId, recordItr, sparkTaskContextSupplier);
    createHandle.write();
    return Collections.singletonList(Collections.singletonList(createHandle.close())).iterator();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<List<WriteStatus>> handleUpsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                           Partitioner partitioner) {
    UpsertPartitioner upsertPartitioner = (UpsertPartitioner) partitioner;
    BucketInfo binfo = upsertPartitioner.getBucketInfo(partition);
    BucketType btype = binfo.bucketType;
    try {
      if (btype.equals(BucketType.INSERT)) {
        return handleInsert(instantTime, binfo.fileIdPrefix, recordItr);
      } else if (btype.equals(BucketType.UPDATE)) {
        return handleUpdate(instantTime, binfo.partitionPath, binfo.fileIdPrefix, recordItr);
      } else {
        throw new HoodieUpsertException("Unknown bucketType " + btype + " for partition :" + partition);
      }
    } catch (Throwable t) {
      String msg = "Error upserting bucketType " + btype + " for partition :" + partition;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                           Partitioner partitioner) {
    return handleUpsertPartition(instantTime, partition, recordItr, partitioner);
  }

  @Override
  public HoodieCleanMetadata clean(JavaSparkContext jsc, String cleanInstantTime) {
    return new CleanActionExecutor(jsc, config, this, cleanInstantTime).execute();
  }

  @Override
  public List<HoodieRollbackStat> rollback(JavaSparkContext jsc, HoodieInstant instant, boolean deleteInstants)
      throws IOException {
    long startTime = System.currentTimeMillis();
    List<HoodieRollbackStat> stats = new ArrayList<>();
    HoodieActiveTimeline activeTimeline = this.getActiveTimeline();

    if (instant.isCompleted()) {
      LOG.info("Unpublishing instant " + instant);
      instant = activeTimeline.revertToInflight(instant);
    }

    // For Requested State (like failure during index lookup), there is nothing to do rollback other than
    // deleting the timeline file
    if (!instant.isRequested()) {
      String commit = instant.getTimestamp();

      // delete all the data files for this commit
      LOG.info("Clean out all parquet files generated for commit: " + commit);
      List<RollbackRequest> rollbackRequests = generateRollbackRequests(instant);

      //TODO: We need to persist this as rollback workload and use it in case of partial failures
      stats = new RollbackHelper(metaClient, config).performRollback(jsc, instant, rollbackRequests);
    }
    // Delete Inflight instant if enabled
    deleteInflightAndRequestedInstant(deleteInstants, activeTimeline, instant);
    LOG.info("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));
    return stats;
  }

  private List<RollbackRequest> generateRollbackRequests(HoodieInstant instantToRollback)
      throws IOException {
    return FSUtils.getAllPartitionPaths(this.metaClient.getFs(), this.getMetaClient().getBasePath(),
        config.shouldAssumeDatePartitioning()).stream().map(partitionPath -> RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback))
            .collect(Collectors.toList());
  }


  /**
   * Delete Inflight instant if enabled.
   *
   * @param deleteInstant Enable Deletion of Inflight instant
   * @param activeTimeline Hoodie active timeline
   * @param instantToBeDeleted Instant to be deleted
   */
  protected void deleteInflightAndRequestedInstant(boolean deleteInstant, HoodieActiveTimeline activeTimeline,
      HoodieInstant instantToBeDeleted) {
    // Remove marker files always on rollback
    deleteMarkerDir(instantToBeDeleted.getTimestamp());

    // Remove the rolled back inflight commits
    if (deleteInstant) {
      LOG.info("Deleting instant=" + instantToBeDeleted);
      activeTimeline.deletePending(instantToBeDeleted);
      if (instantToBeDeleted.isInflight() && !metaClient.getTimelineLayoutVersion().isNullVersion()) {
        // Delete corresponding requested instant
        instantToBeDeleted = new HoodieInstant(State.REQUESTED, instantToBeDeleted.getAction(),
            instantToBeDeleted.getTimestamp());
        activeTimeline.deletePending(instantToBeDeleted);
      }
      LOG.info("Deleted pending commit " + instantToBeDeleted);
    } else {
      LOG.warn("Rollback finished without deleting inflight instant file. Instant=" + instantToBeDeleted);
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
   * Helper class for an insert bucket along with the weight [0.0, 1.0] that defines the amount of incoming inserts that
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
    String partitionPath;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BucketInfo {");
      sb.append("bucketType=").append(bucketType).append(", ");
      sb.append("fileIdPrefix=").append(fileIdPrefix).append(", ");
      sb.append("partitionPath=").append(partitionPath);
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
    List<SmallFile> smallFiles = new ArrayList<>();
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

    UpsertPartitioner(WorkloadProfile profile, JavaSparkContext jsc) {
      updateLocationToBucket = new HashMap<>();
      partitionPathToInsertBuckets = new HashMap<>();
      bucketInfoMap = new HashMap<>();
      globalStat = profile.getGlobalStat();
      rollingStatMetadata = getRollingStats();
      assignUpdates(profile);
      assignInserts(profile, jsc);

      LOG.info("Total Buckets :" + totalBuckets + ", buckets info => " + bucketInfoMap + ", \n"
          + "Partition to insert buckets => " + partitionPathToInsertBuckets + ", \n"
          + "UpdateLocations mapped to buckets =>" + updateLocationToBucket);
    }

    private void assignUpdates(WorkloadProfile profile) {
      // each update location gets a partition
      Set<Map.Entry<String, WorkloadStat>> partitionStatEntries = profile.getPartitionPathStatMap().entrySet();
      for (Map.Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
        for (Map.Entry<String, Pair<String, Long>> updateLocEntry :
                partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
          addUpdateBucket(partitionStat.getKey(), updateLocEntry.getKey());
        }
      }
    }

    private int addUpdateBucket(String partitionPath, String fileIdHint) {
      int bucket = totalBuckets;
      updateLocationToBucket.put(fileIdHint, bucket);
      BucketInfo bucketInfo = new BucketInfo();
      bucketInfo.bucketType = BucketType.UPDATE;
      bucketInfo.fileIdPrefix = fileIdHint;
      bucketInfo.partitionPath = partitionPath;
      bucketInfoMap.put(totalBuckets, bucketInfo);
      totalBuckets++;
      return bucket;
    }

    private void assignInserts(WorkloadProfile profile, JavaSparkContext jsc) {
      // for new inserts, compute buckets depending on how many records we have for each partition
      Set<String> partitionPaths = profile.getPartitionPaths();
      long averageRecordSize =
          averageBytesPerRecord(metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
              config.getCopyOnWriteRecordSizeEstimate());
      LOG.info("AvgRecordSize => " + averageRecordSize);

      Map<String, List<SmallFile>> partitionSmallFilesMap =
              getSmallFilesForPartitions(new ArrayList<String>(partitionPaths), jsc);

      for (String partitionPath : partitionPaths) {
        WorkloadStat pStat = profile.getWorkloadStat(partitionPath);
        if (pStat.getNumInserts() > 0) {

          List<SmallFile> smallFiles = partitionSmallFilesMap.get(partitionPath);
          this.smallFiles.addAll(smallFiles);

          LOG.info("For partitionPath : " + partitionPath + " Small Files => " + smallFiles);

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
                LOG.info("Assigning " + recordsToAppend + " inserts to existing update bucket " + bucket);
              } else {
                bucket = addUpdateBucket(partitionPath, smallFile.location.getFileId());
                LOG.info("Assigning " + recordsToAppend + " inserts to new update bucket " + bucket);
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
            LOG.info("After small file assignment: unassignedInserts => " + totalUnassignedInserts
                + ", totalInsertBuckets => " + insertBuckets + ", recordsPerBucket => " + insertRecordsPerBucket);
            for (int b = 0; b < insertBuckets; b++) {
              bucketNumbers.add(totalBuckets);
              recordsPerBucket.add(totalUnassignedInserts / insertBuckets);
              BucketInfo bucketInfo = new BucketInfo();
              bucketInfo.bucketType = BucketType.INSERT;
              bucketInfo.partitionPath = partitionPath;
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
          LOG.info("Total insert buckets for partition path " + partitionPath + " => " + insertBuckets);
          partitionPathToInsertBuckets.put(partitionPath, insertBuckets);
        }
      }
    }

    private Map<String, List<SmallFile>> getSmallFilesForPartitions(List<String> partitionPaths, JavaSparkContext jsc) {

      Map<String, List<SmallFile>> partitionSmallFilesMap = new HashMap<>();
      if (partitionPaths != null && partitionPaths.size() > 0) {
        JavaRDD<String> partitionPathRdds = jsc.parallelize(partitionPaths, partitionPaths.size());
        partitionSmallFilesMap = partitionPathRdds.mapToPair((PairFunction<String, String, List<SmallFile>>)
            partitionPath -> new Tuple2<>(partitionPath, getSmallFiles(partitionPath))).collectAsMap();
      }

      return partitionSmallFilesMap;
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
        List<HoodieBaseFile> allFiles = getBaseFileOnlyView()
            .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).collect(Collectors.toList());

        for (HoodieBaseFile file : allFiles) {
          if (file.getFileSize() < config.getParquetSmallFileLimit()) {
            String filename = file.getFileName();
            SmallFile sf = new SmallFile();
            sf.location = new HoodieRecordLocation(FSUtils.getCommitTime(filename), FSUtils.getFileId(filename));
            sf.sizeBytes = file.getFileSize();
            smallFileLocations.add(sf);
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
        final long hashOfKey = NumericUtils.getMessageDigestHash("MD5", keyLocation._1().getRecordKey());
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
      LOG.error("Error trying to compute average bytes/record ", t);
    }
    return avgSize;
  }
}
