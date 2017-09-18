/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.table;

import com.google.common.hash.Hashing;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieCompactionMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.func.LazyInsertIterable;
import com.uber.hoodie.io.HoodieCleanHelper;
import com.uber.hoodie.io.HoodieMergeHandle;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Option;
import scala.Tuple2;

/**
 * Implementation of a very heavily read-optimized Hoodie Table where
 *
 * INSERTS - Produce new files, block aligned to desired size (or)
 *           Merge with the smallest existing file, to expand it
 *
 * UPDATES - Produce a new version of the file, just replacing the updated records with new values
 *
 */
public class HoodieCopyOnWriteTable<T extends HoodieRecordPayload> extends HoodieTable {
    public HoodieCopyOnWriteTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
        super(config, metaClient);
    }

    private static Logger logger = LogManager.getLogger(HoodieCopyOnWriteTable.class);

    enum BucketType {
        UPDATE,
        INSERT
    }

    /**
     * Helper class for a small file's location and its actual size on disk
     */
    class SmallFile implements Serializable {
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
     * Helper class for an insert bucket along with the weight [0.0, 0.1]
     * that defines the amount of incoming inserts that should be allocated to
     * the bucket
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
     * Helper class for a bucket's type (INSERT and UPDATE) and its file location
     */
    class BucketInfo implements Serializable {
        BucketType bucketType;
        String fileLoc;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("BucketInfo {");
            sb.append("bucketType=").append(bucketType).append(", ");
            sb.append("fileLoc=").append(fileLoc);
            sb.append('}');
            return sb.toString();
        }
    }


    /**
     * Packs incoming records to be upserted, into buckets (1 bucket = 1 RDD partition)
     */
    class UpsertPartitioner extends Partitioner {

        /**
         * Total number of RDD partitions, is determined by total buckets we want to
         * pack the incoming workload into
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
         * Helps us pack inserts into 1 or more buckets depending on number of
         * incoming records.
         */
        private HashMap<String, List<InsertBucket>> partitionPathToInsertBuckets;


        /**
         * Remembers what type each bucket is for later.
         */
        private HashMap<Integer, BucketInfo> bucketInfoMap;

        UpsertPartitioner(WorkloadProfile profile) {
            updateLocationToBucket = new HashMap<>();
            partitionPathToInsertBuckets = new HashMap<>();
            bucketInfoMap = new HashMap<>();
            globalStat = profile.getGlobalStat();

            assignUpdates(profile);
            assignInserts(profile);

            logger.info("Total Buckets :" + totalBuckets + ", " +
                    "buckets info => " + bucketInfoMap + ", \n" +
                    "Partition to insert buckets => " + partitionPathToInsertBuckets + ", \n" +
                    "UpdateLocations mapped to buckets =>" + updateLocationToBucket);
        }

        private void assignUpdates(WorkloadProfile profile) {
            // each update location gets a partition
            WorkloadStat gStat = profile.getGlobalStat();
            for (Map.Entry<String, Long> updateLocEntry : gStat.getUpdateLocationToCount().entrySet()) {
                addUpdateBucket(updateLocEntry.getKey());
            }
        }

        private int addUpdateBucket(String fileLoc) {
            int bucket = totalBuckets;
            updateLocationToBucket.put(fileLoc, bucket);
            BucketInfo bucketInfo = new BucketInfo();
            bucketInfo.bucketType = BucketType.UPDATE;
            bucketInfo.fileLoc = fileLoc;
            bucketInfoMap.put(totalBuckets, bucketInfo);
            totalBuckets++;
            return bucket;
        }

        private void assignInserts(WorkloadProfile profile) {
            // for new inserts, compute buckets depending on how many records we have for each partition
            Set<String> partitionPaths = profile.getPartitionPaths();
            long averageRecordSize = averageBytesPerRecord();
            logger.info("AvgRecordSize => " + averageRecordSize);
            for (String partitionPath : partitionPaths) {
                WorkloadStat pStat = profile.getWorkloadStat(partitionPath);
                if (pStat.getNumInserts() > 0) {

                    List<SmallFile> smallFiles = getSmallFiles(partitionPath);
                    logger.info("For partitionPath : "+ partitionPath + " Small Files => " + smallFiles);

                    long totalUnassignedInserts = pStat.getNumInserts();
                    List<Integer> bucketNumbers = new ArrayList<>();
                    List<Long> recordsPerBucket = new ArrayList<>();

                    // first try packing this into one of the smallFiles
                    for (SmallFile smallFile: smallFiles) {
                        long recordsToAppend = Math.min((config.getParquetMaxFileSize() - smallFile.sizeBytes)/ averageRecordSize, totalUnassignedInserts);
                        if (recordsToAppend > 0 && totalUnassignedInserts > 0){
                            // create a new bucket or re-use an existing bucket
                            int bucket;
                            if (updateLocationToBucket.containsKey(smallFile.location.getFileId())) {
                                bucket = updateLocationToBucket.get(smallFile.location.getFileId());
                                logger.info("Assigning " + recordsToAppend + " inserts to existing update bucket "+ bucket);
                            } else {
                                bucket = addUpdateBucket(smallFile.location.getFileId());
                                logger.info("Assigning " + recordsToAppend + " inserts to new update bucket "+ bucket);
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
                            insertRecordsPerBucket = config.getParquetMaxFileSize()/averageRecordSize;
                        }

                        int insertBuckets = (int) Math.max(totalUnassignedInserts / insertRecordsPerBucket, 1L);
                        logger.info("After small file assignment: unassignedInserts => " + totalUnassignedInserts
                                + ", totalInsertBuckets => " + insertBuckets
                                + ", recordsPerBucket => " + insertRecordsPerBucket);
                        for (int b = 0; b < insertBuckets; b++) {
                            bucketNumbers.add(totalBuckets);
                            recordsPerBucket.add(totalUnassignedInserts/insertBuckets);
                            BucketInfo bucketInfo = new BucketInfo();
                            bucketInfo.bucketType = BucketType.INSERT;
                            bucketInfoMap.put(totalBuckets, bucketInfo);
                            totalBuckets++;
                        }
                    }

                    // Go over all such buckets, and assign weights as per amount of incoming inserts.
                    List<InsertBucket> insertBuckets = new ArrayList<>();
                    for (int i = 0; i < bucketNumbers.size(); i++) {
                        InsertBucket bkt = new InsertBucket();
                        bkt.bucketNumber = bucketNumbers.get(i);
                        bkt.weight = (1.0 * recordsPerBucket.get(i))/pStat.getNumInserts();
                        insertBuckets.add(bkt);
                    }
                    logger.info("Total insert buckets for partition path "+ partitionPath + " => " + insertBuckets);
                    partitionPathToInsertBuckets.put(partitionPath, insertBuckets);
                }
            }
        }


        /**
         * Returns a list  of small files in the given partition path
         *
         * @param partitionPath
         * @return
         */
        private List<SmallFile> getSmallFiles(String partitionPath) {
            List<SmallFile> smallFileLocations = new ArrayList<>();

            HoodieTimeline commitTimeline = getCompletedCommitTimeline();

            if (!commitTimeline.empty()) { // if we have some commits
                HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
                List<HoodieDataFile> allFiles = getROFileSystemView()
                    .getLatestDataFilesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp())
                    .collect(Collectors.toList());

                for (HoodieDataFile file : allFiles) {
                    if (file.getFileSize() < config.getParquetSmallFileLimit()) {
                        String filename = file.getFileName();
                        SmallFile sf = new SmallFile();
                        sf.location = new HoodieRecordLocation(FSUtils.getCommitTime(filename),
                            FSUtils.getFileId(filename));
                        sf.sizeBytes = file.getFileSize();
                        smallFileLocations.add(sf);
                    }
                }
            }

            return smallFileLocations;
        }

        /**
         * Obtains the average record size based on records written during last commit.
         * Used for estimating how many records pack into one file.
         *
         * @return
         */
        private long averageBytesPerRecord() {
            long avgSize = 0L;
            HoodieTimeline commitTimeline =
                metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();
            try {
                if (!commitTimeline.empty()) {
                    HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
                    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                        .fromBytes(commitTimeline.getInstantDetails(latestCommitTime).get());
                    avgSize = (long) Math.ceil(
                        (1.0 * commitMetadata.fetchTotalBytesWritten()) / commitMetadata
                            .fetchTotalRecordsWritten());
                }
            } catch (Throwable t) {
                // make this fail safe.
                logger.error("Error trying to compute average bytes/record ", t);
            }
            return avgSize <= 0L ? config.getCopyOnWriteRecordSizeEstimate() : avgSize;
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
            Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation = (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
            if (keyLocation._2().isDefined()) {
                HoodieRecordLocation location = keyLocation._2().get();
                return updateLocationToBucket.get(location.getFileId());
            } else {
                List<InsertBucket> targetBuckets = partitionPathToInsertBuckets.get(keyLocation._1().getPartitionPath());
                // pick the target bucket to use based on the weights.
                double totalWeight = 0.0;
                final long totalInserts = Math.max(1, globalStat.getNumInserts());
                final double r = 1.0 * Math.floorMod(Hashing.md5().hashUnencodedChars(keyLocation._1().getRecordKey()).asLong(),
                    totalInserts) / totalInserts;
                for (InsertBucket insertBucket: targetBuckets) {
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



    public Iterator<List<WriteStatus>> handleUpdate(String commitTime, String fileLoc, Iterator<HoodieRecord<T>> recordItr)
        throws IOException {
        // these are updates
        HoodieMergeHandle upsertHandle = getUpdateHandle(commitTime, fileLoc, recordItr);
        if (upsertHandle.getOldFilePath() == null) {
            throw new HoodieUpsertException("Error in finding the old file path at commit " +
                    commitTime +" at fileLoc: " + fileLoc);
        } else {
            Configuration conf = FSUtils.getFs().getConf();
            AvroReadSupport.setAvroReadSchema(conf, upsertHandle.getSchema());
            ParquetReader<IndexedRecord> reader =
                    AvroParquetReader.builder(upsertHandle.getOldFilePath()).withConf(conf).build();
            try {
                IndexedRecord record;
                while ((record = reader.read()) != null) {
                    // Two types of writes here (new record, and old record).
                    // We have already catch the exception during writing new records.
                    // But for old records, we should fail if any exception happens.
                    upsertHandle.write((GenericRecord) record);
                }
            } catch (IOException e) {
                throw new HoodieUpsertException(
                        "Failed to read record from " + upsertHandle.getOldFilePath()
                                + " with new Schema " + upsertHandle.getSchema(), e);
            } finally {
                reader.close();
                upsertHandle.close();
            }
        }
        //TODO(vc): This needs to be revisited
        if (upsertHandle.getWriteStatus().getPartitionPath() == null) {
            logger.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath()
                            + ", " + upsertHandle.getWriteStatus());
        }
        return Collections.singletonList(Collections.singletonList(upsertHandle.getWriteStatus())).iterator();
    }

    protected HoodieMergeHandle getUpdateHandle(String commitTime, String fileLoc, Iterator<HoodieRecord<T>> recordItr) {
        return new HoodieMergeHandle<>(config, commitTime, this, recordItr, fileLoc);
    }

    public Iterator<List<WriteStatus>> handleInsert(String commitTime, Iterator<HoodieRecord<T>> recordItr) throws Exception {
        return new LazyInsertIterable<>(recordItr, config, commitTime, this);
    }


    @SuppressWarnings("unchecked")
    @Override
    public Iterator<List<WriteStatus>> handleUpsertPartition(String commitTime, Integer partition,
        Iterator recordItr, Partitioner partitioner) {
        UpsertPartitioner upsertPartitioner = (UpsertPartitioner) partitioner;
        BucketInfo binfo = upsertPartitioner.getBucketInfo(partition);
        BucketType btype = binfo.bucketType;
        try {
            if (btype.equals(BucketType.INSERT)) {
                return handleInsert(commitTime, recordItr);
            } else if (btype.equals(BucketType.UPDATE)) {
                return handleUpdate(commitTime, binfo.fileLoc, recordItr);
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
    public Iterator<List<WriteStatus>> handleInsertPartition(String commitTime, Integer partition,
        Iterator recordItr,
        Partitioner partitioner) {
        return handleUpsertPartition(commitTime, partition, recordItr, partitioner);
    }

    @Override
    public Optional<HoodieCompactionMetadata> compact(JavaSparkContext jsc) {
        logger.info("Nothing to compact in COW storage format");
        return Optional.empty();
    }

    /**
     * Performs cleaning of partition paths according to cleaning policy and returns the number
     * of files cleaned. Handles skews in partitions to clean by making files to clean as the
     * unit of task distribution.
     *
     * @throws IllegalArgumentException if unknown cleaning policy is provided
     */
    @Override
    public List<HoodieCleanStat> clean(JavaSparkContext jsc) {
        try {
            List<String> partitionsToClean =
                FSUtils.getAllPartitionPaths(getFs(), getMetaClient().getBasePath(), config.shouldAssumeDatePartitioning());
            logger.info("Partitions to clean up : " + partitionsToClean + ", with policy " + config
                .getCleanerPolicy());
            if (partitionsToClean.isEmpty()) {
                logger.info("Nothing to clean here mom. It is already clean");
                return Collections.emptyList();
            }
            return cleanPartitionPaths(partitionsToClean, jsc);
        } catch (IOException e) {
            throw new HoodieIOException("Failed to clean up after commit", e);
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
                throw new RuntimeException(String.format(
                    "partitionPath is not a match: (%s, %s)",
                    partitionPath, other.partitionPath));
            }
            successDeleteFiles.addAll(other.successDeleteFiles);
            deletePathPatterns.addAll(other.deletePathPatterns);
            failedDeleteFiles.addAll(other.failedDeleteFiles);
            return this;
        }
    }

    private List<HoodieCleanStat> cleanPartitionPaths(List<String> partitionsToClean, JavaSparkContext jsc) {
        int cleanerParallelism = Math.min(partitionsToClean.size(), config.getCleanerParallelism());
        logger.info("Using cleanerParallelism: " + cleanerParallelism);
        List<Tuple2<String, PartitionCleanStat>> partitionCleanStats = jsc
            .parallelize(partitionsToClean, cleanerParallelism)
            .flatMapToPair(getFilesToDeleteFunc(this, config))
            .repartition(cleanerParallelism)                    // repartition to remove skews
            .mapPartitionsToPair(deleteFilesFunc(this, config))
            .reduceByKey(                                       // merge partition level clean stats below
                (Function2<PartitionCleanStat, PartitionCleanStat, PartitionCleanStat>) (e1, e2) -> e1
                    .merge(e2))
            .collect();

        Map<String, PartitionCleanStat> partitionCleanStatsMap = partitionCleanStats
            .stream().collect(Collectors.toMap(e -> e._1(), e -> e._2()));

        HoodieCleanHelper cleaner = new HoodieCleanHelper(this, config);
        // Return PartitionCleanStat for each partition passed.
        return partitionsToClean.stream().map(partitionPath -> {
            PartitionCleanStat partitionCleanStat =
                (partitionCleanStatsMap.containsKey(partitionPath)) ?
                    partitionCleanStatsMap.get(partitionPath)
                    : new PartitionCleanStat(partitionPath);
            return HoodieCleanStat.newBuilder()
                .withPolicy(config.getCleanerPolicy())
                .withPartitionPath(partitionPath)
                .withEarliestCommitRetained(cleaner.getEarliestCommitToRetain())
                .withDeletePathPattern(partitionCleanStat.deletePathPatterns)
                .withSuccessfulDeletes(partitionCleanStat.successDeleteFiles)
                .withFailedDeletes(partitionCleanStat.failedDeleteFiles)
                .build();
        }).collect(Collectors.toList());
    }

    private PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, PartitionCleanStat> deleteFilesFunc(
        HoodieTable table, HoodieWriteConfig config) {
        return (PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, PartitionCleanStat>) iter -> {
            HoodieCleanHelper cleaner = new HoodieCleanHelper(table, config);
            Map<String, PartitionCleanStat> partitionCleanStatMap = new HashMap<>();

            while (iter.hasNext()) {
                Tuple2<String, String> partitionDelFileTuple = iter.next();
                String partitionPath = partitionDelFileTuple._1();
                String deletePathStr = partitionDelFileTuple._2();
                Boolean deletedFileResult = deleteFileAndGetResult(deletePathStr);
                if (!partitionCleanStatMap.containsKey(partitionPath)) {
                    partitionCleanStatMap.put(partitionPath,
                        new PartitionCleanStat(partitionPath));
                }
                PartitionCleanStat partitionCleanStat = partitionCleanStatMap.get(partitionPath);
                partitionCleanStat.addDeleteFilePatterns(deletePathStr);
                partitionCleanStat.addDeletedFileResult(deletePathStr, deletedFileResult);
            }

            return partitionCleanStatMap.entrySet().stream()
                .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                .collect(Collectors.toList()).iterator();
        };
    }

    private static PairFlatMapFunction<String, String, String> getFilesToDeleteFunc(
        HoodieTable table, HoodieWriteConfig config) {
        return (PairFlatMapFunction<String, String, String>) partitionPathToClean -> {
            HoodieCleanHelper cleaner = new HoodieCleanHelper(table, config);
            return cleaner.getDeletePaths(partitionPathToClean).stream()
                .map(deleteFile -> new Tuple2<>(partitionPathToClean, deleteFile.toString()))
                .iterator();
        };
    }

    private Boolean deleteFileAndGetResult(String deletePathStr) throws IOException {
        Path deletePath = new Path(deletePathStr);
        logger.debug("Working on delete path :" + deletePath);
        boolean deleteResult = getFs().delete(deletePath, false);
        if (deleteResult) {
            logger.debug("Cleaned file at path :" + deletePath);
        }
        return deleteResult;
    }
}
