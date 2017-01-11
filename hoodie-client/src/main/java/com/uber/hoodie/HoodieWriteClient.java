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

package com.uber.hoodie;

import com.codahale.metrics.Timer;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieInsertException;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.func.InsertMapFunction;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.HoodieCleaner;
import com.uber.hoodie.io.HoodieCommitArchiveLog;
import com.uber.hoodie.metrics.HoodieMetrics;
import com.uber.hoodie.table.HoodieTable;
import com.uber.hoodie.table.WorkloadProfile;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import scala.Option;
import scala.Tuple2;

/**
 * Hoodie Write Client helps you build datasets on HDFS [insert()] and then
 * perform efficient mutations on a HDFS dataset [upsert()]
 *
 * Note that, at any given time, there can only be one Spark job performing
 * these operatons on a Hoodie dataset.
 *
 */
public class HoodieWriteClient<T extends HoodieRecordPayload> implements Serializable {

    private static Logger logger = LogManager.getLogger(HoodieWriteClient.class);
    private transient final FileSystem fs;
    private transient final JavaSparkContext jsc;
    private final HoodieWriteConfig config;
    private transient final HoodieMetrics metrics;
    private transient final HoodieIndex<T> index;
    private transient final HoodieCommitArchiveLog archiveLog;
    private transient Timer.Context writeContext = null;

    private final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * @param jsc
     * @param clientConfig
     * @throws Exception
     */
    public HoodieWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig) throws Exception {
        this(jsc, clientConfig, false);
    }

    /**
     * @param jsc
     * @param clientConfig
     * @param rollbackInFlight
     */
    public HoodieWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig, boolean rollbackInFlight) {
        this.fs = FSUtils.getFs();
        this.jsc = jsc;
        this.config = clientConfig;
        this.index = HoodieIndex.createIndex(config, jsc);
        this.metrics = new HoodieMetrics(config, config.getTableName());
        this.archiveLog = new HoodieCommitArchiveLog(clientConfig);
        if (rollbackInFlight) {
            rollbackInflightCommits();
        }
    }

    /**
     * Filter out HoodieRecords that already exists in the output folder. This is useful in
     * deduplication.
     *
     * @param hoodieRecords Input RDD of Hoodie records.
     * @return A subset of hoodieRecords RDD, with existing records filtered out.
     */
    public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
        final HoodieTableMetadata metadata =
                new HoodieTableMetadata(fs, config.getBasePath(), config.getTableName());
        JavaRDD<HoodieRecord<T>> recordsWithLocation = index.tagLocation(hoodieRecords, metadata);
        return recordsWithLocation.filter(new Function<HoodieRecord<T>, Boolean>() {
            @Override
            public Boolean call(HoodieRecord<T> v1) throws Exception {
                return !v1.isCurrentLocationKnown();
            }
        });
    }

    /**
     * Upserts a bunch of new records into the Hoodie table, at the supplied commitTime
     */
    public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
        final HoodieTableMetadata metadata =
            new HoodieTableMetadata(fs, config.getBasePath(), config.getTableName());
        writeContext = metrics.getCommitCtx();
        final HoodieTable table =
            HoodieTable.getHoodieTable(metadata.getTableType(), commitTime, config, metadata);

        try {
            // De-dupe/merge if needed
            JavaRDD<HoodieRecord<T>> dedupedRecords =
                combineOnCondition(config.shouldCombineBeforeUpsert(), records,
                    config.getUpsertShuffleParallelism());

            // perform index loop up to get existing location of records
            JavaRDD<HoodieRecord<T>> taggedRecords = index.tagLocation(dedupedRecords, metadata);

            // Cache the tagged records, so we don't end up computing both
            taggedRecords.persist(StorageLevel.MEMORY_AND_DISK_SER());


            WorkloadProfile profile = null;
            if (table.isWorkloadProfileNeeded()) {
                profile = new WorkloadProfile(taggedRecords);
                logger.info("Workload profile :" + profile);
            }

            // obtain the upsert partitioner, and the run the tagger records through that & get a partitioned RDD.
            final Partitioner upsertPartitioner = table.getUpsertPartitioner(profile);
            JavaRDD<HoodieRecord<T>> partitionedRecords = taggedRecords.mapToPair(
                new PairFunction<HoodieRecord<T>, Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>>() {
                    @Override
                    public Tuple2<Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>> call(
                        HoodieRecord<T> record) throws Exception {
                        return new Tuple2<>(new Tuple2<>(record.getKey(),
                            Option.apply(record.getCurrentLocation())), record);
                    }
                }).partitionBy(upsertPartitioner).map(
                new Function<Tuple2<Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>>, HoodieRecord<T>>() {
                    @Override
                    public HoodieRecord<T> call(
                        Tuple2<Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>> tuple)
                        throws Exception {
                        return tuple._2();
                    }
                });


            // Perform the actual writing.
            JavaRDD<WriteStatus> upsertStatusRDD = partitionedRecords.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<List<WriteStatus>>>() {
                    @Override
                    public Iterator<List<WriteStatus>> call(Integer partition,
                        Iterator<HoodieRecord<T>> recordItr) throws Exception {
                        return table.handleUpsertPartition(partition, recordItr, upsertPartitioner);
                    }
                }, true).flatMap(new FlatMapFunction<List<WriteStatus>, WriteStatus>() {
                @Override
                public Iterable<WriteStatus> call(List<WriteStatus> writeStatuses)
                    throws Exception {
                    return writeStatuses;
                }
            });

            // Update the index back.
            JavaRDD<WriteStatus> resultRDD = index.updateLocation(upsertStatusRDD, metadata);
            resultRDD = resultRDD.persist(config.getWriteStatusStorageLevel());
            commitOnAutoCommit(commitTime, resultRDD);
            return resultRDD;
        } catch (Throwable e) {
            if (e instanceof HoodieUpsertException) {
                throw (HoodieUpsertException) e;
            }
            throw new HoodieUpsertException("Failed to upsert for commit time " + commitTime, e);
        }
    }

    private void commitOnAutoCommit(String commitTime, JavaRDD<WriteStatus> resultRDD) {
        if(config.shouldAutoCommit()) {
            logger.info("Auto commit enabled: Committing " + commitTime);
            boolean commitResult = commit(commitTime, resultRDD);
            if (!commitResult) {
                throw new HoodieCommitException("Failed to commit " + commitTime);
            }
        } else {
            logger.info("Auto commit disabled for " + commitTime);
        }
    }

    private JavaRDD<HoodieRecord<T>> combineOnCondition(boolean condition,
        JavaRDD<HoodieRecord<T>> records, int parallelism) {
        if(condition) {
            return deduplicateRecords(records, parallelism);
        }
        return records;
    }

    /**
     * Loads the given HoodieRecords, as inserts into the table.
     * (This implementation uses sortBy and attempts to control the numbers of files with less memory)
     *
     * @param records HoodieRecords to insert
     * @param commitTime Commit Time handle
     * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
     *
     */
    public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
        final HoodieTableMetadata metadata =
            new HoodieTableMetadata(fs, config.getBasePath(), config.getTableName());
        writeContext = metrics.getCommitCtx();
        try {
            // De-dupe/merge if needed
            JavaRDD<HoodieRecord<T>> dedupedRecords =
                combineOnCondition(config.shouldCombineBeforeInsert(), records,
                    config.getInsertShuffleParallelism());

            // Now, sort the records and line them up nicely for loading.
            JavaRDD<HoodieRecord<T>> sortedRecords =
                dedupedRecords.sortBy(new Function<HoodieRecord<T>, String>() {
                    @Override
                    public String call(HoodieRecord<T> record) {
                        // Let's use "partitionPath + key" as the sort key. Spark, will ensure
                        // the records split evenly across RDD partitions, such that small partitions fit
                        // into 1 RDD partition, while big ones spread evenly across multiple RDD partitions
                        return String
                            .format("%s+%s", record.getPartitionPath(), record.getRecordKey());
                    }
                }, true, config.getInsertShuffleParallelism());
            JavaRDD<WriteStatus> writeStatusRDD = sortedRecords
                .mapPartitionsWithIndex(new InsertMapFunction<T>(commitTime, config, metadata),
                    true).flatMap(new FlatMapFunction<List<WriteStatus>, WriteStatus>() {
                    @Override
                    public Iterable<WriteStatus> call(List<WriteStatus> writeStatuses)
                        throws Exception {
                        return writeStatuses;
                    }
                });
            // Update the index back
            JavaRDD<WriteStatus> statuses = index.updateLocation(writeStatusRDD, metadata);
            // Trigger the insert and collect statuses
            statuses = statuses.persist(config.getWriteStatusStorageLevel());
            commitOnAutoCommit(commitTime, statuses);
            return statuses;
        } catch (Throwable e) {
            if (e instanceof HoodieInsertException) {
                throw e;
            }
            throw new HoodieInsertException("Failed to insert for commit time " + commitTime, e);
        }
    }

    /**
     * Commit changes performed at the given commitTime marker
     */
    public boolean commit(String commitTime, JavaRDD<WriteStatus> writeStatuses) {
        logger.info("Comitting " + commitTime);
        Path commitFile =
            new Path(config.getBasePath() + "/.hoodie/" + FSUtils.makeCommitFileName(commitTime));
        try {

            if (fs.exists(commitFile)) {
                throw new HoodieCommitException("Duplicate commit found. " + commitTime);
            }

            List<Tuple2<String, HoodieWriteStat>> stats =
                writeStatuses.mapToPair(new PairFunction<WriteStatus, String, HoodieWriteStat>() {
                    @Override
                    public Tuple2<String, HoodieWriteStat> call(WriteStatus writeStatus)
                        throws Exception {
                        return new Tuple2<>(writeStatus.getPartitionPath(), writeStatus.getStat());
                    }
                }).collect();

            HoodieCommitMetadata metadata = new HoodieCommitMetadata();
            for (Tuple2<String, HoodieWriteStat> stat : stats) {
                metadata.addWriteStat(stat._1(), stat._2());
            }

            // open a new file and write the commit metadata in
            Path inflightCommitFile = new Path(config.getBasePath() + "/.hoodie/" + FSUtils
                .makeInflightCommitFileName(commitTime));
            FSDataOutputStream fsout = fs.create(inflightCommitFile, true);
            fsout.writeBytes(new String(metadata.toJsonString().getBytes(StandardCharsets.UTF_8),
                StandardCharsets.UTF_8));
            fsout.close();

            boolean success = fs.rename(inflightCommitFile, commitFile);
            if (success) {
                // We cannot have unbounded commit files. Archive commits if we have to archive
                archiveLog.archiveIfRequired();
                // Call clean to cleanup if there is anything to cleanup after the commit,
                clean();
                if(writeContext != null) {
                    long durationInMs = metrics.getDurationInMs(writeContext.stop());
                    metrics.updateCommitMetrics(FORMATTER.parse(commitTime).getTime(), durationInMs,
                        metadata);
                    writeContext = null;
                }
            }
            logger.info("Status of the commit " + commitTime + ": " + success);
            return success;
        } catch (IOException e) {
            throw new HoodieCommitException(
                "Failed to commit " + config.getBasePath() + " at time " + commitTime, e);
        } catch (ParseException e) {
            throw new HoodieCommitException(
                "Commit time is not of valid format.Failed to commit " + config.getBasePath()
                    + " at time " + commitTime, e);
        }
    }

    /**
     * Rollback the (inflight/committed) record changes with the given commit time.
     * Three steps:
     * (0) Obtain the commit or rollback file
     * (1) clean indexing data,
     * (2) clean new generated parquet files.
     * (3) Finally delete .commit or .inflight file,
     */
    public boolean rollback(final String commitTime) throws HoodieRollbackException {

        final Timer.Context context = metrics.getRollbackCtx();
        final HoodieTableMetadata metadata =
                new HoodieTableMetadata(fs, config.getBasePath(), config.getTableName());
        final String metaPath = config.getBasePath() + "/" + HoodieTableMetadata.METAFOLDER_NAME;
        try {
            // 0. Obtain the commit/.inflight file, to work on
            FileStatus[] commitFiles =
                    fs.globStatus(new Path(metaPath + "/" + commitTime + ".*"));
            if (commitFiles.length != 1) {
                throw new HoodieRollbackException("Expected exactly one .commit or .inflight file for commitTime: " + commitTime);
            }

            // we first need to unpublish the commit by making it .inflight again. (this will ensure no future queries see this data)
            Path filePath = commitFiles[0].getPath();
            if (filePath.getName().endsWith(HoodieTableMetadata.COMMIT_FILE_SUFFIX)) {
                if (metadata.findCommitsAfter(commitTime, Integer.MAX_VALUE).size() > 0) {
                    throw new HoodieRollbackException("Found commits after time :" + commitTime +
                            ", please rollback greater commits first");
                }
                Path newInflightPath = new Path(metaPath + "/" + commitTime + HoodieTableMetadata.INFLIGHT_FILE_SUFFIX);
                if (!fs.rename(filePath, newInflightPath)) {
                    throw new HoodieRollbackException("Unable to rename .commit file to .inflight for commitTime:" + commitTime);
                }
                filePath = newInflightPath;
            }

            // 1. Revert the index changes
            logger.info("Clean out index changes at time: " + commitTime);
            if (!index.rollbackCommit(commitTime)) {
                throw new HoodieRollbackException("Clean out index changes failed, for time :" + commitTime);
            }

            // 2. Delete the new generated parquet files
            logger.info("Clean out all parquet files generated at time: " + commitTime);
            final Accumulator<Integer> numFilesDeletedAccu = jsc.accumulator(0);
            jsc.parallelize(FSUtils.getAllPartitionPaths(fs, metadata.getBasePath()))
                    .foreach(new VoidFunction<String>() {
                        @Override
                        public void call(String partitionPath) throws Exception {
                            // Scan all partitions files with this commit time
                            FileSystem fs = FSUtils.getFs();
                            FileStatus[] toBeDeleted =
                                    fs.listStatus(new Path(config.getBasePath(), partitionPath),
                                            new PathFilter() {
                                                @Override
                                                public boolean accept(Path path) {
                                                    return commitTime
                                                            .equals(FSUtils.getCommitTime(path.getName()));
                                                }
                                            });
                            for (FileStatus file : toBeDeleted) {
                                boolean success = fs.delete(file.getPath(), false);
                                logger.info("Delete file " + file.getPath() + "\t" + success);
                                if (success) {
                                    numFilesDeletedAccu.add(1);
                                }
                            }
                        }
                    });

            // 3. Clean out metadata (.commit or .tmp)
            logger.info("Clean out metadata files at time: " + commitTime);
            if (!fs.delete(filePath, false)) {
                logger.error("Deleting file " + filePath + " failed.");
                throw new HoodieRollbackException("Delete file " + filePath + " failed.");
            }

            if (context != null) {
                long durationInMs = metrics.getDurationInMs(context.stop());
                int numFilesDeleted = numFilesDeletedAccu.value();
                metrics.updateRollbackMetrics(durationInMs, numFilesDeleted);
            }

            return true;
        } catch (IOException e) {
            throw new HoodieRollbackException("Failed to rollback " +
                    config.getBasePath() + " at commit time" + commitTime, e);
        }
    }

    /**
     * Releases any resources used by the client.
     */
    public void close() {
        // UNDER CONSTRUCTION
    }

    /**
     * Clean up any stale/old files/data lying around (either on file storage or index storage)
     */
    private void clean() throws HoodieIOException  {
        try {
            logger.info("Cleaner started");
            final Timer.Context context = metrics.getCleanCtx();
            final HoodieTableMetadata metadata = new HoodieTableMetadata(fs, config.getBasePath(), config.getTableName());
            List<String> partitionsToClean = FSUtils.getAllPartitionPaths(fs, metadata.getBasePath());
            // shuffle to distribute cleaning work across partitions evenly
            Collections.shuffle(partitionsToClean);
            logger.info("Partitions to clean up : " + partitionsToClean + ", with policy " + config.getCleanerPolicy());
            if(partitionsToClean.isEmpty()) {
                logger.info("Nothing to clean here mom. It is already clean");
                return;
            }

            int cleanerParallelism = Math.min(partitionsToClean.size(), config.getCleanerParallelism());
            int numFilesDeleted = jsc.parallelize(partitionsToClean, cleanerParallelism)
                .map(new Function<String, Integer>() {
                    @Override
                    public Integer call(String partitionPathToClean) throws Exception {
                        FileSystem fs = FSUtils.getFs();
                        HoodieCleaner cleaner = new HoodieCleaner(metadata, config, fs);
                        return cleaner.clean(partitionPathToClean);
                    }
                }).reduce(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
            logger.info("Cleaned " + numFilesDeleted + " files");
            // Emit metrics (duration, numFilesDeleted) if needed
            if (context != null) {
                long durationInMs = metrics.getDurationInMs(context.stop());
                logger.info("cleanerElaspsedTime (Minutes): " + durationInMs / (1000 * 60));
                metrics.updateCleanMetrics(durationInMs, numFilesDeleted);
            }
        } catch (IOException e) {
            throw new HoodieIOException("Failed to clean up after commit", e);
        }
    }

    /**
     * Provides a new commit time for a write operation (insert/update)
     */
    public String startCommit() {
        String commitTime = FORMATTER.format(new Date());
        startCommitWithTime(commitTime);
        return commitTime;
    }

    public void startCommitWithTime(String commitTime) {
        logger.info("Generate a new commit time " + commitTime);
        // Create the in-flight commit file
        Path inflightCommitFilePath = new Path(
            config.getBasePath() + "/.hoodie/" + FSUtils.makeInflightCommitFileName(commitTime));
        try {
            if (fs.createNewFile(inflightCommitFilePath)) {
                logger.info("Create an inflight commit file " + inflightCommitFilePath);
                return;
            }
            throw new HoodieCommitException(
                "Failed to create the inflight commit file " + inflightCommitFilePath);
        } catch (IOException e) {
            // handled below
            throw new HoodieCommitException(
                "Failed to create the inflight commit file " + inflightCommitFilePath, e);
        }
    }

    public static SparkConf registerClasses(SparkConf conf) {
        conf.registerKryoClasses(new Class[]{HoodieWriteConfig.class, HoodieRecord.class, HoodieKey.class});
        return conf;
    }

    /**
     * Deduplicate Hoodie records, using the given deduplication funciton.
     */
    private JavaRDD<HoodieRecord<T>> deduplicateRecords(JavaRDD<HoodieRecord<T>> records, int parallelism) {
        return records.mapToPair(new PairFunction<HoodieRecord<T>, HoodieKey, HoodieRecord<T>>() {
            @Override
            public Tuple2<HoodieKey, HoodieRecord<T>> call(HoodieRecord<T> record) {
                return new Tuple2<>(record.getKey(), record);
            }
        }).reduceByKey(new Function2<HoodieRecord<T>, HoodieRecord<T>, HoodieRecord<T>>() {
            @Override
            public HoodieRecord<T> call(HoodieRecord<T> rec1, HoodieRecord<T> rec2) {
                @SuppressWarnings("unchecked")
                T reducedData = (T) rec1.getData().preCombine(rec2.getData());
                // we cannot allow the user to change the key or partitionPath, since that will affect everything
                // so pick it from one of the records.
                return new HoodieRecord<T>(rec1.getKey(), reducedData);
            }
        }, parallelism).map(new Function<Tuple2<HoodieKey, HoodieRecord<T>>, HoodieRecord<T>>() {
            @Override
            public HoodieRecord<T> call(Tuple2<HoodieKey, HoodieRecord<T>> recordTuple) {
                return recordTuple._2();
            }
        });
    }

    /**
     * Cleanup all inflight commits
     * @throws IOException
     */
    private void rollbackInflightCommits() {
        final HoodieTableMetadata metadata = new HoodieTableMetadata(fs, config.getBasePath(), config.getTableName());
        for (String commit : metadata.getAllInflightCommits()) {
            rollback(commit);
        }
    }
}
