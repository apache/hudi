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

import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieInsertException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.func.LazyInsertIterable;
import com.uber.hoodie.io.HoodieUpdateHandle;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.Partitioner;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import scala.Option;
import scala.Tuple2;

/**
 * Implementation of a very heavily read-optimized Hoodie Table where
 *
 * INSERTS - Produce new files, block aligned to desired size (or)
 *           Merge with the smallest existing file, to expand it
 *
 * UPDATES - Produce a new version of the file containing the invalidated records
 *
 */
public class HoodieCopyOnWriteTable<T extends HoodieRecordPayload> extends HoodieTable {

    // seed for random number generator. No particular significance, just makes testing deterministic
    private static final long RANDOM_NUMBER_SEED = 356374L;


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


    public HoodieCopyOnWriteTable(String commitTime, HoodieWriteConfig config, HoodieTableMetadata metadata) {
        super(commitTime, config, metadata);
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


        /**
         * Random number generator to use for splitting inserts into buckets by weight
         */
        private Random rand = new Random(RANDOM_NUMBER_SEED);


        UpsertPartitioner(WorkloadProfile profile) {
            updateLocationToBucket = new HashMap<>();
            partitionPathToInsertBuckets = new HashMap<>();
            bucketInfoMap = new HashMap<>();

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
            FileSystem fs = FSUtils.getFs();
            List<SmallFile> smallFileLocations = new ArrayList<>();

            if (metadata.getAllCommits().getNumCommits() > 0) { // if we have some commits
                String latestCommitTime = metadata.getAllCommits().lastCommit();
                FileStatus[] allFiles = metadata.getLatestVersionInPartition(fs, partitionPath, latestCommitTime);

                if (allFiles != null && allFiles.length > 0) {
                    for (FileStatus fileStatus : allFiles) {
                        if (fileStatus.getLen() < config.getParquetSmallFileLimit()) {
                            String filename = fileStatus.getPath().getName();
                            SmallFile sf = new SmallFile();
                            sf.location = new HoodieRecordLocation(
                                    FSUtils.getCommitTime(filename),
                                    FSUtils.getFileId(filename));
                            sf.sizeBytes = fileStatus.getLen();
                            smallFileLocations.add(sf);
                        }
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
            try {
                if (metadata.getAllCommits().getNumCommits() > 0) {
                    String latestCommitTime = metadata.getAllCommits().lastCommit();
                    HoodieCommitMetadata commitMetadata = metadata.getCommitMetadata(latestCommitTime);
                    avgSize =(long) Math.ceil((1.0 * commitMetadata.fetchTotalBytesWritten())/commitMetadata.fetchTotalRecordsWritten());
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
                double r = rand.nextDouble();
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


    public Iterator<List<WriteStatus>> handleUpdate(String fileLoc, Iterator<HoodieRecord<T>> recordItr) throws Exception {
        // these are updates
        HoodieUpdateHandle upsertHandle =
                new HoodieUpdateHandle<>(config, commitTime, metadata, recordItr, fileLoc);
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

    public Iterator<List<WriteStatus>> handleInsert(Iterator<HoodieRecord<T>> recordItr) throws Exception {
        return new LazyInsertIterable<>(recordItr, config, commitTime, metadata);
    }


    @Override
    public Iterator<List<WriteStatus>> handleUpsertPartition(Integer partition,
                                                             Iterator recordItr,
                                                             Partitioner partitioner) {
        UpsertPartitioner upsertPartitioner = (UpsertPartitioner) partitioner;
        BucketInfo binfo = upsertPartitioner.getBucketInfo(partition);
        BucketType btype = binfo.bucketType;
        try {
            if (btype.equals(BucketType.INSERT)) {
                return handleInsert(recordItr);
            } else if (btype.equals(BucketType.UPDATE)) {
                return handleUpdate(binfo.fileLoc, recordItr);
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
    public Iterator<List<WriteStatus>> handleInsertPartition(Integer partition,
                                                             Iterator recordItr,
                                                             Partitioner partitioner) {
        return handleUpsertPartition(partition, recordItr, partitioner);
    }
}
