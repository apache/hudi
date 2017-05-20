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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.uber.hoodie.avro.model.HoodieCleanMetadata;
import com.uber.hoodie.avro.model.HoodieRollbackMetadata;
import com.uber.hoodie.avro.model.HoodieSavepointMetadata;
import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieCompactionMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieInsertException;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.exception.HoodieSavepointException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.func.BulkInsertMapFunction;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.HoodieCleaner;
import com.uber.hoodie.io.HoodieCommitArchiveLog;
import com.uber.hoodie.metrics.HoodieMetrics;
import com.uber.hoodie.table.HoodieTable;
import com.uber.hoodie.table.WorkloadProfile;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
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
        this.archiveLog = new HoodieCommitArchiveLog(clientConfig, fs);

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
        // Create a Hoodie table which encapsulated the commits and files visible
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);

        JavaRDD<HoodieRecord<T>> recordsWithLocation = index.tagLocation(hoodieRecords, table);
        return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
    }

    /**
     * Upserts a bunch of new records into the Hoodie table, at the supplied commitTime
     */
    public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
        writeContext = metrics.getCommitCtx();
        // Create a Hoodie table which encapsulated the commits and files visible
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);

        try {
            // De-dupe/merge if needed
            JavaRDD<HoodieRecord<T>> dedupedRecords =
                combineOnCondition(config.shouldCombineBeforeUpsert(), records,
                    config.getUpsertShuffleParallelism());

            // perform index loop up to get existing location of records
            JavaRDD<HoodieRecord<T>> taggedRecords = index.tagLocation(dedupedRecords, table);
            return upsertRecordsInternal(taggedRecords, commitTime, table, true);
        } catch (Throwable e) {
            if (e instanceof HoodieUpsertException) {
                throw (HoodieUpsertException) e;
            }
            throw new HoodieUpsertException("Failed to upsert for commit time " + commitTime, e);
        }
    }

    /**
     * Inserts the given HoodieRecords, into the table. This API is intended to be used for normal
     * writes.
     *
     * This implementation skips the index check and is able to leverage benefits such as
     * small file handling/blocking alignment, as with upsert(), by profiling the workload
     *
     * @param records    HoodieRecords to insert
     * @param commitTime Commit Time handle
     * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
     */
    public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
        writeContext = metrics.getCommitCtx();
        // Create a Hoodie table which encapsulated the commits and files visible
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        try {
            // De-dupe/merge if needed
            JavaRDD<HoodieRecord<T>> dedupedRecords =
                combineOnCondition(config.shouldCombineBeforeInsert(), records,
                    config.getInsertShuffleParallelism());

            return upsertRecordsInternal(dedupedRecords, commitTime, table, false);
        } catch (Throwable e) {
            if (e instanceof HoodieInsertException) {
                throw e;
            }
            throw new HoodieInsertException("Failed to insert for commit time " + commitTime, e);
        }
    }

    /**
     * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk
     * loads into a Hoodie table for the very first time (e.g: converting an existing dataset to
     * Hoodie).
     *
     * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and
     * attempts to control the numbers of files with less memory compared to the {@link
     * HoodieWriteClient#insert(JavaRDD, String)}
     *
     * @param records    HoodieRecords to insert
     * @param commitTime Commit Time handle
     * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
     */
    public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
        writeContext = metrics.getCommitCtx();
        // Create a Hoodie table which encapsulated the commits and files visible
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);

        try {
            // De-dupe/merge if needed
            JavaRDD<HoodieRecord<T>> dedupedRecords =
                combineOnCondition(config.shouldCombineBeforeInsert(), records, config.getInsertShuffleParallelism());

            // Now, sort the records and line them up nicely for loading.
            JavaRDD<HoodieRecord<T>> sortedRecords = dedupedRecords
                    .sortBy(record -> {
                        // Let's use "partitionPath + key" as the sort key. Spark, will ensure
                        // the records split evenly across RDD partitions, such that small partitions fit
                        // into 1 RDD partition, while big ones spread evenly across multiple RDD partitions
                        return String
                            .format("%s+%s", record.getPartitionPath(), record.getRecordKey());
                    }, true, config.getInsertShuffleParallelism());
            JavaRDD<WriteStatus> writeStatusRDD = sortedRecords
                    .mapPartitionsWithIndex(new BulkInsertMapFunction<T>(commitTime, config, table), true)
                    .flatMap(writeStatuses -> writeStatuses.iterator());

            return updateIndexAndCommitIfNeeded(writeStatusRDD, table, commitTime);
        } catch (Throwable e) {
            if (e instanceof HoodieInsertException) {
                throw e;
            }
            throw new HoodieInsertException("Failed to bulk insert for commit time " + commitTime, e);
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

    private JavaRDD<WriteStatus> upsertRecordsInternal(JavaRDD<HoodieRecord<T>> preppedRecords,
        String commitTime,
        HoodieTable<T> hoodieTable,
        final boolean isUpsert) {

        // Cache the tagged records, so we don't end up computing both
        preppedRecords.persist(StorageLevel.MEMORY_AND_DISK_SER());

        WorkloadProfile profile = null;
        if (hoodieTable.isWorkloadProfileNeeded()) {
            profile = new WorkloadProfile(preppedRecords);
            logger.info("Workload profile :" + profile);
        }

        // partition using the insert partitioner
        final Partitioner partitioner = getPartitioner(hoodieTable, isUpsert, profile);
        JavaRDD<HoodieRecord<T>> partitionedRecords = partition(preppedRecords, partitioner);
        JavaRDD<WriteStatus> writeStatusRDD = partitionedRecords
                .mapPartitionsWithIndex((partition, recordItr) -> {
                    if (isUpsert) {
                        return hoodieTable
                            .handleUpsertPartition(commitTime, partition, recordItr, partitioner);
                    } else {
                        return hoodieTable
                            .handleInsertPartition(commitTime, partition, recordItr, partitioner);
                    }
                }, true)
                .flatMap(writeStatuses -> writeStatuses.iterator());

        return updateIndexAndCommitIfNeeded(writeStatusRDD, hoodieTable, commitTime);
    }

    private Partitioner getPartitioner(HoodieTable table, boolean isUpsert, WorkloadProfile profile) {
        if (isUpsert) {
            return table.getUpsertPartitioner(profile);
        } else {
            return table.getInsertPartitioner(profile);
        }
    }

    private JavaRDD<WriteStatus> updateIndexAndCommitIfNeeded(JavaRDD<WriteStatus> writeStatusRDD, HoodieTable<T> table, String commitTime) {
        // Update the index back
        JavaRDD<WriteStatus> statuses = index.updateLocation(writeStatusRDD, table);
        // Trigger the insert and collect statuses
        statuses = statuses.persist(config.getWriteStatusStorageLevel());
        commitOnAutoCommit(commitTime, statuses);
        return statuses;
    }

    private JavaRDD<HoodieRecord<T>> partition(JavaRDD<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
        return dedupedRecords
                .mapToPair((PairFunction<HoodieRecord<T>, Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>>) record ->
                        new Tuple2<>(new Tuple2<>(record.getKey(), Option.apply(record.getCurrentLocation())), record))
                .partitionBy(partitioner)
                .map((Function<Tuple2<Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>>, HoodieRecord<T>>) tuple -> tuple._2());
    }

    /**
     * Commit changes performed at the given commitTime marker
     */
    public boolean commit(String commitTime, JavaRDD<WriteStatus> writeStatuses) {
        return commit(commitTime, writeStatuses, Optional.empty());
    }

    /**
     * Commit changes performed at the given commitTime marker
     */
    public boolean commit(String commitTime,
                          JavaRDD<WriteStatus> writeStatuses,
                          Optional<HashMap<String, String>> extraMetadata) {

        logger.info("Comitting " + commitTime);
        // Create a Hoodie table which encapsulated the commits and files visible
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);

        HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

        List<Tuple2<String, HoodieWriteStat>> stats = writeStatuses
                    .mapToPair((PairFunction<WriteStatus, String, HoodieWriteStat>) writeStatus ->
                            new Tuple2<String, HoodieWriteStat>(writeStatus.getPartitionPath(), writeStatus.getStat()))
                    .collect();

        HoodieCommitMetadata metadata = new HoodieCommitMetadata();
        for (Tuple2<String, HoodieWriteStat> stat : stats) {
            metadata.addWriteStat(stat._1(), stat._2());
        }
        // add in extra metadata
        if (extraMetadata.isPresent()) {
            extraMetadata.get().forEach((k, v) -> metadata.addMetadata(k, v));
        }

        try {
            String actionType = table.getCommitActionType();
            activeTimeline.saveAsComplete(
                new HoodieInstant(true, actionType, commitTime),
                Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
            // Save was a success
            // Do a inline compaction if enabled
            if (config.isInlineCompaction()) {
                Optional<HoodieCompactionMetadata> compactionMetadata = table.compact(jsc);
                if (compactionMetadata.isPresent()) {
                    logger.info("Compacted successfully on commit " + commitTime);
                    metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true");
                } else {
                    logger.info("Compaction did not run for commit " + commitTime);
                    metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
                }
            }

            // We cannot have unbounded commit files. Archive commits if we have to archive
            archiveLog.archiveIfRequired();
            if(config.isAutoClean()) {
                // Call clean to cleanup if there is anything to cleanup after the commit,
                logger.info("Auto cleaning is enabled. Running cleaner now");
                clean(commitTime);
            } else {
                logger.info("Auto cleaning is not enabled. Not running cleaner now");
            }
            if (writeContext != null) {
                long durationInMs = metrics.getDurationInMs(writeContext.stop());
                metrics.updateCommitMetrics(
                    HoodieActiveTimeline.COMMIT_FORMATTER.parse(commitTime).getTime(), durationInMs,
                    metadata);
                writeContext = null;
            }
            logger.info("Committed " + commitTime);
        } catch (IOException e) {
            throw new HoodieCommitException(
                "Failed to commit " + config.getBasePath() + " at time " + commitTime, e);
        } catch (ParseException e) {
            throw new HoodieCommitException(
                "Commit time is not of valid format.Failed to commit " + config.getBasePath()
                    + " at time " + commitTime, e);
        }
        return true;
    }

    /**
     * Savepoint a specific commit. Latest version of data files as of the passed in commitTime
     * will be referenced in the savepoint and will never be cleaned. The savepointed commit
     * will never be rolledback or archived.
     *
     * This gives an option to rollback the state to the savepoint anytime.
     * Savepoint needs to be manually created and deleted.
     *
     * Savepoint should be on a commit that could not have been cleaned.
     *
     * @param user - User creating the savepoint
     * @param comment - Comment for the savepoint
     * @return true if the savepoint was created successfully
     */
    public boolean savepoint(String user, String comment) {
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        if (table.getCompletedCommitTimeline().empty()) {
            throw new HoodieSavepointException("Could not savepoint. Commit timeline is empty");
        }

        String latestCommit = table.getCompletedCommitTimeline().lastInstant().get().getTimestamp();
        logger.info("Savepointing latest commit " + latestCommit);
        return savepoint(latestCommit, user, comment);
    }

    /**
     * Savepoint a specific commit. Latest version of data files as of the passed in commitTime
     * will be referenced in the savepoint and will never be cleaned. The savepointed commit
     * will never be rolledback or archived.
     *
     * This gives an option to rollback the state to the savepoint anytime.
     * Savepoint needs to be manually created and deleted.
     *
     * Savepoint should be on a commit that could not have been cleaned.
     *
     * @param commitTime - commit that should be savepointed
     * @param user - User creating the savepoint
     * @param comment - Comment for the savepoint
     * @return true if the savepoint was created successfully
     */
    public boolean savepoint(String commitTime, String user, String comment) {
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        Optional<HoodieInstant> cleanInstant = table.getCompletedCleanTimeline().lastInstant();

        HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);
        if(!table.getCompletedCommitTimeline().containsInstant(commitInstant)) {
            throw new HoodieSavepointException("Could not savepoint non-existing commit " + commitInstant);
        }

        try {
            // Check the last commit that was not cleaned and check if savepoint time is > that commit
            String lastCommitRetained;
            if (cleanInstant.isPresent()) {
                HoodieCleanMetadata cleanMetadata = AvroUtils.deserializeHoodieCleanMetadata(
                    table.getActiveTimeline().getInstantDetails(cleanInstant.get()).get());
                lastCommitRetained = cleanMetadata.getEarliestCommitToRetain();
            } else {
                lastCommitRetained =
                    table.getCompletedCommitTimeline().firstInstant().get().getTimestamp();
            }

            // Cannot allow savepoint time on a commit that could have been cleaned
            Preconditions.checkArgument(HoodieTimeline
                    .compareTimestamps(commitTime, lastCommitRetained, HoodieTimeline.GREATER_OR_EQUAL),
                "Could not savepoint commit " + commitTime + " as this is beyond the lookup window "
                    + lastCommitRetained);

            Map<String, List<String>> latestFilesMap = jsc.parallelize(
                FSUtils.getAllPartitionPaths(fs, table.getMetaClient().getBasePath(), config.shouldAssumeDatePartitioning()))
                .mapToPair((PairFunction<String, String, List<String>>) partitionPath -> {
                    // Scan all partitions files with this commit time
                    logger.info("Collecting latest files in partition path " + partitionPath);
                    TableFileSystemView view = table.getFileSystemView();
                    List<String> latestFiles =
                        view.getLatestVersionInPartition(partitionPath, commitTime)
                            .map(HoodieDataFile::getFileName).collect(Collectors.toList());
                    return new Tuple2<>(partitionPath, latestFiles);
                }).collectAsMap();

            HoodieSavepointMetadata metadata =
                AvroUtils.convertSavepointMetadata(user, comment, latestFilesMap);
            // Nothing to save in the savepoint
            table.getActiveTimeline().saveAsComplete(
                new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, commitTime),
                AvroUtils.serializeSavepointMetadata(metadata));
            logger.info("Savepoint " + commitTime + " created");
            return true;
        } catch (IOException e) {
            throw new HoodieSavepointException("Failed to savepoint " + commitTime, e);
        }
    }

    /**
     * Delete a savepoint that was created. Once the savepoint is deleted, the commit can be rolledback
     * and cleaner may clean up data files.
     *
     * @param savepointTime - delete the savepoint
     * @return true if the savepoint was deleted successfully
     */
    public void deleteSavepoint(String savepointTime) {
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

        HoodieInstant savePoint =
            new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
        boolean isSavepointPresent =
            table.getCompletedSavepointTimeline().containsInstant(savePoint);
        if (!isSavepointPresent) {
            logger.warn("No savepoint present " + savepointTime);
            return;
        }

        activeTimeline.revertToInflight(savePoint);
        activeTimeline.deleteInflight(
            new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, savepointTime));
        logger.info("Savepoint " + savepointTime + " deleted");
    }

    /**
     * Rollback the state to the savepoint.
     * WARNING: This rollsback recent commits and deleted data files. Queries accessing the files
     * will mostly fail. This should be done during a downtime.
     *
     * @param savepointTime - savepoint time to rollback to
     * @return true if the savepoint was rollecback to successfully
     */
    public boolean rollbackToSavepoint(String savepointTime) {
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
        HoodieTimeline commitTimeline = table.getCompletedCommitTimeline();

        HoodieInstant savePoint =
            new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
        boolean isSavepointPresent =
            table.getCompletedSavepointTimeline().containsInstant(savePoint);
        if (!isSavepointPresent) {
            throw new HoodieRollbackException("No savepoint for commitTime " + savepointTime);
        }

        List<String> commitsToRollback =
            commitTimeline.findInstantsAfter(savepointTime, Integer.MAX_VALUE).getInstants()
                .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
        logger.info("Rolling back commits " + commitsToRollback);

        rollback(commitsToRollback);

        // Make sure the rollback was successful
        Optional<HoodieInstant> lastInstant =
            activeTimeline.reload().getCommitsAndCompactionsTimeline().filterCompletedInstants().lastInstant();
        Preconditions.checkArgument(lastInstant.isPresent());
        Preconditions.checkArgument(lastInstant.get().getTimestamp().equals(savepointTime),
            savepointTime + "is not the last commit after rolling back " + commitsToRollback
                + ", last commit was " + lastInstant.get().getTimestamp());
        return true;
    }

    /**
     * Rollback the (inflight/committed) record changes with the given commit time.
     * Three steps:
     * (1) Atomically unpublish this commit
     * (2) clean indexing data,
     * (3) clean new generated parquet files.
     * (4) Finally delete .commit or .inflight file,
     */
    public boolean rollback(final String commitTime) throws HoodieRollbackException {
        rollback(Lists.newArrayList(commitTime));
        return true;
    }


    private void rollback(List<String> commits) {
        if(commits.isEmpty()) {
            logger.info("List of commits to rollback is empty");
            return;
        }

        final Timer.Context context = metrics.getRollbackCtx();
        String startRollbackTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());

        // Create a Hoodie table which encapsulated the commits and files visible
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
        HoodieTimeline inflightTimeline = table.getInflightCommitTimeline();
        HoodieTimeline commitTimeline = table.getCompletedCommitTimeline();

        // Check if any of the commits is a savepoint - do not allow rollback on those commits
        List<String> savepoints =
            table.getCompletedSavepointTimeline().getInstants().map(HoodieInstant::getTimestamp)
                .collect(Collectors.toList());
        commits.forEach(s -> {
            if (savepoints.contains(s)) {
                throw new HoodieRollbackException(
                    "Could not rollback a savepointed commit. Delete savepoint first before rolling back"
                        + s);
            }
        });

        try {
            if (commitTimeline.empty() && inflightTimeline.empty()) {
                // nothing to rollback
                logger.info("No commits to rollback " + commits);
            }

            // Make sure only the last n commits are being rolled back
            // If there is a commit in-between or after that is not rolled back, then abort
            String lastCommit = commits.get(commits.size() - 1);
            if (!commitTimeline.empty() && !commitTimeline
                .findInstantsAfter(lastCommit, Integer.MAX_VALUE).empty()) {
                throw new HoodieRollbackException("Found commits after time :" + lastCommit +
                    ", please rollback greater commits first");
            }

            List<String> inflights = inflightTimeline.getInstants().map(HoodieInstant::getTimestamp)
                .collect(Collectors.toList());
            if (!inflights.isEmpty() && inflights.indexOf(lastCommit) != inflights.size() - 1) {
                throw new HoodieRollbackException(
                    "Found in-flight commits after time :" + lastCommit +
                        ", please rollback greater commits first");
            }

            // Atomically unpublish all the commits
            commits.stream().filter(s -> !inflights.contains(s))
                .map(s -> new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, s))
                .forEach(activeTimeline::revertToInflight);
            logger.info("Unpublished " + commits);

            // cleanup index entries
            commits.stream().forEach(s -> {
                if (!index.rollbackCommit(s)) {
                    throw new HoodieRollbackException(
                        "Clean out index changes failed, for time :" + s);
                }
            });
            logger.info("Index rolled back for commits " + commits);

            // delete all the data files for all these commits
            logger.info("Clean out all parquet files generated for commits: " + commits);
            final LongAccumulator numFilesDeletedCounter = jsc.sc().longAccumulator();
            List<HoodieRollbackStat> stats = jsc.parallelize(
                FSUtils.getAllPartitionPaths(fs, table.getMetaClient().getBasePath(), config.shouldAssumeDatePartitioning()))
                .map((Function<String, HoodieRollbackStat>) partitionPath -> {
                    // Scan all partitions files with this commit time
                    logger.info("Cleaning path " + partitionPath);
                    FileSystem fs1 = FSUtils.getFs();
                    FileStatus[] toBeDeleted =
                        fs1.listStatus(new Path(config.getBasePath(), partitionPath), path -> {
                            if(!path.toString().contains(".parquet")) {
                                return false;
                            }
                            String fileCommitTime = FSUtils.getCommitTime(path.getName());
                            return commits.contains(fileCommitTime);
                        });
                    Map<FileStatus, Boolean> results = Maps.newHashMap();
                    for (FileStatus file : toBeDeleted) {
                        boolean success = fs1.delete(file.getPath(), false);
                        results.put(file, success);
                        logger.info("Delete file " + file.getPath() + "\t" + success);
                        if (success) {
                            numFilesDeletedCounter.add(1);
                        }
                    }
                    return HoodieRollbackStat.newBuilder().withPartitionPath(partitionPath)
                        .withDeletedFileResults(results).build();
                }).collect();

            // Remove the rolled back inflight commits
            commits.stream().map(s -> new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, s))
                .forEach(activeTimeline::deleteInflight);
            logger.info("Deleted inflight commits " + commits);

            Optional<Long> durationInMs = Optional.empty();
            if (context != null) {
                durationInMs = Optional.of(metrics.getDurationInMs(context.stop()));
                Long numFilesDeleted = numFilesDeletedCounter.value();
                metrics.updateRollbackMetrics(durationInMs.get(), numFilesDeleted);
            }
            HoodieRollbackMetadata rollbackMetadata =
                AvroUtils.convertRollbackMetadata(startRollbackTime, durationInMs, commits, stats);
            table.getActiveTimeline().saveAsComplete(
                new HoodieInstant(true, HoodieTimeline.ROLLBACK_ACTION, startRollbackTime),
                AvroUtils.serializeRollbackMetadata(rollbackMetadata));
            logger.info("Commits " + commits + " rollback is complete");

            if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
                logger.info("Cleaning up older rollback meta files");
                // Cleanup of older cleaner meta files
                // TODO - make the commit archival generic and archive rollback metadata
                FSUtils.deleteOlderRollbackMetaFiles(fs, table.getMetaClient().getMetaPath(),
                    table.getActiveTimeline().getRollbackTimeline().getInstants());
            }
        } catch (IOException e) {
            throw new HoodieRollbackException("Failed to rollback " +
                config.getBasePath() + " commits " + commits, e);
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
     * based on the configurations and CleaningPolicy used. (typically files that no longer can be used
     * by a running query can be cleaned)
     */
    public void clean() throws HoodieIOException {
        String startCleanTime = HoodieActiveTimeline.createNewCommitTime();
        clean(startCleanTime);
    }

    /**
     * Clean up any stale/old files/data lying around (either on file storage or index storage)
     * based on the configurations and CleaningPolicy used. (typically files that no longer can be used
     * by a running query can be cleaned)
     */
    private void clean(String startCleanTime) throws HoodieIOException  {
        try {
            logger.info("Cleaner started");
            final Timer.Context context = metrics.getCleanCtx();

            // Create a Hoodie table which encapsulated the commits and files visible
            HoodieTable<T> table = HoodieTable
                .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);

            List<String> partitionsToClean =
                FSUtils.getAllPartitionPaths(fs, table.getMetaClient().getBasePath(), config.shouldAssumeDatePartitioning());
            // shuffle to distribute cleaning work across partitions evenly
            Collections.shuffle(partitionsToClean);
            logger.info("Partitions to clean up : " + partitionsToClean + ", with policy " + config
                .getCleanerPolicy());
            if (partitionsToClean.isEmpty()) {
                logger.info("Nothing to clean here mom. It is already clean");
                return;
            }

            int cleanerParallelism = Math.min(partitionsToClean.size(), config.getCleanerParallelism());
            List<HoodieCleanStat> cleanStats = jsc.parallelize(partitionsToClean, cleanerParallelism)
                    .map((Function<String, HoodieCleanStat>) partitionPathToClean -> {
                        HoodieCleaner cleaner = new HoodieCleaner(table, config);
                        return cleaner.clean(partitionPathToClean);
                    })
                .collect();

            // Emit metrics (duration, numFilesDeleted) if needed
            Optional<Long> durationInMs = Optional.empty();
            if (context != null) {
                durationInMs = Optional.of(metrics.getDurationInMs(context.stop()));
                logger.info("cleanerElaspsedTime (Minutes): " + durationInMs.get() / (1000 * 60));
            }

            // Create the metadata and save it
            HoodieCleanMetadata metadata =
                AvroUtils.convertCleanMetadata(startCleanTime, durationInMs, cleanStats);
            logger.info("Cleaned " + metadata.getTotalFilesDeleted() + " files");
            metrics.updateCleanMetrics(durationInMs.orElseGet(() -> -1L),
                metadata.getTotalFilesDeleted());

            table.getActiveTimeline().saveAsComplete(
                new HoodieInstant(true, HoodieTimeline.CLEAN_ACTION, startCleanTime),
                AvroUtils.serializeCleanMetadata(metadata));
            logger.info("Marked clean started on " + startCleanTime + " as complete");

            if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
                // Cleanup of older cleaner meta files
                // TODO - make the commit archival generic and archive clean metadata
                FSUtils.deleteOlderCleanMetaFiles(fs, table.getMetaClient().getMetaPath(),
                    table.getActiveTimeline().getCleanerTimeline().getInstants());
            }
        } catch (IOException e) {
            throw new HoodieIOException("Failed to clean up after commit", e);
        }
    }

    /**
     * Provides a new commit time for a write operation (insert/update)
     */
    public String startCommit() {
        String commitTime = HoodieActiveTimeline.createNewCommitTime();
        startCommitWithTime(commitTime);
        return commitTime;
    }

    public void startCommitWithTime(String commitTime) {
        logger.info("Generate a new commit time " + commitTime);
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
        String commitActionType = table.getCommitActionType();
        activeTimeline.createInflight(
            new HoodieInstant(true, commitActionType, commitTime));
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
     *
     * @throws IOException
     */
    private void rollbackInflightCommits() {
        HoodieTable<T> table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        HoodieTimeline inflightTimeline = table.getCommitTimeline().filterInflights();
        List<String> commits = inflightTimeline.getInstants().map(HoodieInstant::getTimestamp)
            .collect(Collectors.toList());
        Collections.reverse(commits);
        for (String commit : commits) {
            rollback(commit);
        }
    }
}
