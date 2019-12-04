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

package org.apache.hudi;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRollingStat;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView.ReadOptimizedView;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.func.BulkInsertMapFunction;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.HoodieCommitArchiveLog;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple2;

/**
 * Hoodie Write Client helps you build datasets on HDFS [insert()] and then perform efficient mutations on a HDFS
 * dataset [upsert()]
 * <p>
 * Note that, at any given time, there can only be one Spark job performing these operatons on a Hoodie dataset.
 */
public class HoodieWriteClient<T extends HoodieRecordPayload> extends AbstractHoodieClient {

  private static Logger logger = LogManager.getLogger(HoodieWriteClient.class);
  private static final String UPDATE_STR = "update";
  private static final String LOOKUP_STR = "lookup";
  private final boolean rollbackInFlight;
  private final transient HoodieMetrics metrics;
  private final transient HoodieIndex<T> index;
  private final transient HoodieCleanClient<T> cleanClient;
  private transient Timer.Context writeContext = null;
  private transient Timer.Context compactionTimer;
  private transient Timer.Context indexTimer = null;

  /**
   *
   */
  public HoodieWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig) throws Exception {
    this(jsc, clientConfig, false);
  }

  /**
   *
   */
  public HoodieWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig, boolean rollbackInFlight) {
    this(jsc, clientConfig, rollbackInFlight, HoodieIndex.createIndex(clientConfig, jsc));
  }

  @VisibleForTesting
  HoodieWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig, boolean rollbackInFlight, HoodieIndex index) {
    this(jsc, clientConfig, rollbackInFlight, index, Option.empty());
  }

  public HoodieWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig, boolean rollbackInFlight,
      HoodieIndex index, Option<EmbeddedTimelineService> timelineService) {
    super(jsc, clientConfig, timelineService);
    this.index = index;
    this.metrics = new HoodieMetrics(config, config.getTableName());
    this.rollbackInFlight = rollbackInFlight;
    this.cleanClient = new HoodieCleanClient<>(jsc, config, metrics, timelineService);
  }

  public static SparkConf registerClasses(SparkConf conf) {
    conf.registerKryoClasses(new Class[]{HoodieWriteConfig.class, HoodieRecord.class, HoodieKey.class});
    return conf;
  }

  /**
   * Filter out HoodieRecords that already exists in the output folder. This is useful in deduplication.
   *
   * @param hoodieRecords Input RDD of Hoodie records.
   * @return A subset of hoodieRecords RDD, with existing records filtered out.
   */
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    indexTimer = metrics.getIndexCtx();
    JavaRDD<HoodieRecord<T>> recordsWithLocation = index.tagLocation(hoodieRecords, jsc, table);
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    indexTimer = null;
    return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
  }

  /**
   * Upserts a bunch of new records into the Hoodie table, at the supplied commitTime.
   */
  public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.UPSERT);
    try {
      // De-dupe/merge if needed
      JavaRDD<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeUpsert(), records, config.getUpsertShuffleParallelism());

      indexTimer = metrics.getIndexCtx();
      // perform index loop up to get existing location of records
      JavaRDD<HoodieRecord<T>> taggedRecords = index.tagLocation(dedupedRecords, jsc, table);
      metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
      indexTimer = null;
      return upsertRecordsInternal(taggedRecords, commitTime, table, true);
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + commitTime, e);
    }
  }

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied commitTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * @param preppedRecords Prepared HoodieRecords to upsert
   * @param commitTime Commit Time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.UPSERT_PREPPED);
    try {
      return upsertRecordsInternal(preppedRecords, commitTime, table, true);
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert prepared records for commit time " + commitTime, e);
    }
  }

  /**
   * Inserts the given HoodieRecords, into the table. This API is intended to be used for normal writes.
   * <p>
   * This implementation skips the index check and is able to leverage benefits such as small file handling/blocking
   * alignment, as with upsert(), by profiling the workload
   *
   * @param records HoodieRecords to insert
   * @param commitTime Commit Time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.INSERT);
    try {
      // De-dupe/merge if needed
      JavaRDD<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeInsert(), records, config.getInsertShuffleParallelism());

      return upsertRecordsInternal(dedupedRecords, commitTime, table, false);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to insert for commit time " + commitTime, e);
    }
  }

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied commitTime.
   * <p>
   * This implementation skips the index check, skips de-duping and is able to leverage benefits such as small file
   * handling/blocking alignment, as with insert(), by profiling the workload. The prepared HoodieRecords should be
   * de-duped if needed.
   *
   * @param preppedRecords HoodieRecords to insert
   * @param commitTime Commit Time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> insertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.INSERT_PREPPED);
    try {
      return upsertRecordsInternal(preppedRecords, commitTime, table, false);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to insert prepared records for commit time " + commitTime, e);
    }
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing dataset to Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link HoodieWriteClient#insert(JavaRDD, String)}
   *
   * @param records HoodieRecords to insert
   * @param commitTime Commit Time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
    return bulkInsert(records, commitTime, Option.empty());
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing dataset to Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link HoodieWriteClient#insert(JavaRDD, String)}. Optionally
   * it allows users to specify their own partitioner. If specified then it will be used for repartitioning records. See
   * {@link UserDefinedBulkInsertPartitioner}.
   *
   * @param records HoodieRecords to insert
   * @param commitTime Commit Time handle
   * @param bulkInsertPartitioner If specified then it will be used to partition input records before they are inserted
   * into hoodie.
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, final String commitTime,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.BULK_INSERT);
    try {
      // De-dupe/merge if needed
      JavaRDD<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeInsert(), records, config.getInsertShuffleParallelism());

      return bulkInsertInternal(dedupedRecords, commitTime, table, bulkInsertPartitioner);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert for commit time " + commitTime, e);
    }
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing dataset to Hoodie). The input records should contain no
   * duplicates if needed.
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and attempts to control
   * the numbers of files with less memory compared to the {@link HoodieWriteClient#insert(JavaRDD, String)}. Optionally
   * it allows users to specify their own partitioner. If specified then it will be used for repartitioning records. See
   * {@link UserDefinedBulkInsertPartitioner}.
   *
   * @param preppedRecords HoodieRecords to insert
   * @param commitTime Commit Time handle
   * @param bulkInsertPartitioner If specified then it will be used to partition input records before they are inserted
   * into hoodie.
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> bulkInsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, final String commitTime,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.BULK_INSERT_PREPPED);
    try {
      return bulkInsertInternal(preppedRecords, commitTime, table, bulkInsertPartitioner);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert prepared records for commit time " + commitTime, e);
    }
  }

  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied commitTime {@link HoodieKey}s will be
   * deduped and non existant keys will be removed before deleting.
   *
   * @param keys {@link List} of {@link HoodieKey}s to be deleted
   * @param commitTime Commit time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> delete(JavaRDD<HoodieKey> keys, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx(OperationType.DELETE);
    try {
      // De-dupe/merge if needed
      JavaRDD<HoodieKey> dedupedKeys =
          config.shouldCombineBeforeDelete() ? deduplicateKeys(keys, config.getDeleteShuffleParallelism()) : keys;

      JavaRDD<HoodieRecord<T>> dedupedRecords =
          dedupedKeys.map(key -> new HoodieRecord(key, new EmptyHoodieRecordPayload()));
      indexTimer = metrics.getIndexCtx();
      // perform index loop up to get existing location of records
      JavaRDD<HoodieRecord<T>> taggedRecords = index.tagLocation(dedupedRecords, jsc, table);
      // filter out non existant keys/records
      JavaRDD<HoodieRecord<T>> taggedValidRecords = taggedRecords.filter(record -> record.isCurrentLocationKnown());
      if (!taggedValidRecords.isEmpty()) {
        metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
        indexTimer = null;
        return upsertRecordsInternal(taggedValidRecords, commitTime, table, true);
      } else {
        // if entire set of keys are non existent
        JavaRDD<WriteStatus> writeStatusRDD = jsc.parallelize(Collections.EMPTY_LIST, 1);
        commitOnAutoCommit(commitTime, writeStatusRDD, table.getMetaClient().getCommitActionType());
        return writeStatusRDD;
      }
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to delete for commit time " + commitTime, e);
    }
  }

  private JavaRDD<WriteStatus> bulkInsertInternal(JavaRDD<HoodieRecord<T>> dedupedRecords, String commitTime,
      HoodieTable<T> table, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    final JavaRDD<HoodieRecord<T>> repartitionedRecords;
    final int parallelism = config.getBulkInsertShuffleParallelism();
    if (bulkInsertPartitioner.isPresent()) {
      repartitionedRecords = bulkInsertPartitioner.get().repartitionRecords(dedupedRecords, parallelism);
    } else {
      // Now, sort the records and line them up nicely for loading.
      repartitionedRecords = dedupedRecords.sortBy(record -> {
        // Let's use "partitionPath + key" as the sort key. Spark, will ensure
        // the records split evenly across RDD partitions, such that small partitions fit
        // into 1 RDD partition, while big ones spread evenly across multiple RDD partitions
        return String.format("%s+%s", record.getPartitionPath(), record.getRecordKey());
      }, true, parallelism);
    }

    // generate new file ID prefixes for each output partition
    final List<String> fileIDPrefixes =
        IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());

    JavaRDD<WriteStatus> writeStatusRDD = repartitionedRecords
        .mapPartitionsWithIndex(new BulkInsertMapFunction<T>(commitTime, config, table, fileIDPrefixes), true)
        .flatMap(writeStatuses -> writeStatuses.iterator());

    return updateIndexAndCommitIfNeeded(writeStatusRDD, table, commitTime);
  }

  private void commitOnAutoCommit(String commitTime, JavaRDD<WriteStatus> resultRDD, String actionType) {
    if (config.shouldAutoCommit()) {
      logger.info("Auto commit enabled: Committing " + commitTime);
      boolean commitResult = commit(commitTime, resultRDD, Option.empty(), actionType);
      if (!commitResult) {
        throw new HoodieCommitException("Failed to commit " + commitTime);
      }
    } else {
      logger.info("Auto commit disabled for " + commitTime);
    }
  }

  private JavaRDD<HoodieRecord<T>> combineOnCondition(boolean condition, JavaRDD<HoodieRecord<T>> records,
      int parallelism) {
    return condition ? deduplicateRecords(records, parallelism) : records;
  }

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful when performing
   * rollback for MOR datasets. Only updates are recorded in the workload profile metadata since updates to log blocks
   * are unknown across batches Inserts (which are new parquet files) are rolled back based on commit time. // TODO :
   * Create a new WorkloadProfile metadata file instead of using HoodieCommitMetadata
   */
  private void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, HoodieTable<T> table, String commitTime)
      throws HoodieCommitException {
    try {
      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
      profile.getPartitionPaths().stream().forEach(path -> {
        WorkloadStat partitionStat = profile.getWorkloadStat(path.toString());
        partitionStat.getUpdateLocationToCount().entrySet().stream().forEach(entry -> {
          HoodieWriteStat writeStat = new HoodieWriteStat();
          writeStat.setFileId(entry.getKey());
          // TODO : Write baseCommitTime is possible here ?
          writeStat.setPrevCommit(entry.getValue().getKey());
          writeStat.setNumUpdateWrites(entry.getValue().getValue());
          metadata.addWriteStat(path.toString(), writeStat);
        });
      });

      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      Option<HoodieInstant> instant =
          activeTimeline.getCommitsTimeline().filterInflightsExcludingCompaction().lastInstant();
      activeTimeline.saveToInflight(instant.get(), Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException io) {
      throw new HoodieCommitException("Failed to commit " + commitTime + " unable to save inflight metadata ", io);
    }
  }

  private JavaRDD<WriteStatus> upsertRecordsInternal(JavaRDD<HoodieRecord<T>> preppedRecords, String commitTime,
      HoodieTable<T> hoodieTable, final boolean isUpsert) {

    // Cache the tagged records, so we don't end up computing both
    // TODO: Consistent contract in HoodieWriteClient regarding preppedRecord storage level handling
    if (preppedRecords.getStorageLevel() == StorageLevel.NONE()) {
      preppedRecords.persist(StorageLevel.MEMORY_AND_DISK_SER());
    } else {
      logger.info("RDD PreppedRecords was persisted at: " + preppedRecords.getStorageLevel());
    }

    WorkloadProfile profile = null;
    if (hoodieTable.isWorkloadProfileNeeded()) {
      profile = new WorkloadProfile(preppedRecords);
      logger.info("Workload profile :" + profile);
      saveWorkloadProfileMetadataToInflight(profile, hoodieTable, commitTime);
    }

    // partition using the insert partitioner
    final Partitioner partitioner = getPartitioner(hoodieTable, isUpsert, profile);
    JavaRDD<HoodieRecord<T>> partitionedRecords = partition(preppedRecords, partitioner);
    JavaRDD<WriteStatus> writeStatusRDD = partitionedRecords.mapPartitionsWithIndex((partition, recordItr) -> {
      if (isUpsert) {
        return hoodieTable.handleUpsertPartition(commitTime, partition, recordItr, partitioner);
      } else {
        return hoodieTable.handleInsertPartition(commitTime, partition, recordItr, partitioner);
      }
    }, true).flatMap(List::iterator);

    return updateIndexAndCommitIfNeeded(writeStatusRDD, hoodieTable, commitTime);
  }

  private Partitioner getPartitioner(HoodieTable table, boolean isUpsert, WorkloadProfile profile) {
    if (isUpsert) {
      return table.getUpsertPartitioner(profile);
    } else {
      return table.getInsertPartitioner(profile);
    }
  }

  private JavaRDD<WriteStatus> updateIndexAndCommitIfNeeded(JavaRDD<WriteStatus> writeStatusRDD, HoodieTable<T> table,
      String commitTime) {
    // cache writeStatusRDD before updating index, so that all actions before this are not triggered again for future
    // RDD actions that are performed after updating the index.
    writeStatusRDD = writeStatusRDD.persist(config.getWriteStatusStorageLevel());
    indexTimer = metrics.getIndexCtx();
    // Update the index back
    JavaRDD<WriteStatus> statuses = index.updateLocation(writeStatusRDD, jsc, table);
    metrics.updateIndexMetrics(UPDATE_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    indexTimer = null;
    // Trigger the insert and collect statuses
    commitOnAutoCommit(commitTime, statuses, table.getMetaClient().getCommitActionType());
    return statuses;
  }

  private JavaRDD<HoodieRecord<T>> partition(JavaRDD<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
    return dedupedRecords.mapToPair(
        record -> new Tuple2<>(new Tuple2<>(record.getKey(), Option.ofNullable(record.getCurrentLocation())), record))
        .partitionBy(partitioner).map(Tuple2::_2);
  }

  /**
   * Commit changes performed at the given commitTime marker.
   */
  public boolean commit(String commitTime, JavaRDD<WriteStatus> writeStatuses) {
    return commit(commitTime, writeStatuses, Option.empty());
  }

  /**
   * Commit changes performed at the given commitTime marker.
   */
  public boolean commit(String commitTime, JavaRDD<WriteStatus> writeStatuses,
      Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    return commit(commitTime, writeStatuses, extraMetadata, metaClient.getCommitActionType());
  }

  private boolean commit(String commitTime, JavaRDD<WriteStatus> writeStatuses,
      Option<Map<String, String>> extraMetadata, String actionType) {

    logger.info("Commiting " + commitTime);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    List<HoodieWriteStat> stats = writeStatuses.map(WriteStatus::getStat).collect();

    updateMetadataAndRollingStats(actionType, metadata, stats);

    // Finalize write
    finalizeWrite(table, commitTime, stats);

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, commitTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      // Save was a success & Do a inline compaction if enabled
      if (config.isInlineCompaction()) {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true");
        forceCompact(extraMetadata);
      } else {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
      }

      // We cannot have unbounded commit files. Archive commits if we have to archive
      HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(config, createMetaClient(true));
      archiveLog.archiveIfRequired(jsc);
      if (config.isAutoClean()) {
        // Call clean to cleanup if there is anything to cleanup after the commit,
        logger.info("Auto cleaning is enabled. Running cleaner now");
        clean(commitTime);
      } else {
        logger.info("Auto cleaning is not enabled. Not running cleaner now");
      }
      if (writeContext != null) {
        long durationInMs = metrics.getDurationInMs(writeContext.stop());
        metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(commitTime).getTime(), durationInMs,
            metadata, actionType);
        writeContext = null;
      }
      logger.info("Committed " + commitTime);
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + commitTime,
          e);
    } catch (ParseException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + commitTime
          + "Instant time is not of valid format", e);
    }
    return true;
  }

  /**
   * Savepoint a specific commit. Latest version of data files as of the passed in commitTime will be referenced in the
   * savepoint and will never be cleaned. The savepointed commit will never be rolledback or archived.
   * <p>
   * This gives an option to rollback the state to the savepoint anytime. Savepoint needs to be manually created and
   * deleted.
   * <p>
   * Savepoint should be on a commit that could not have been cleaned.
   *
   * @param user - User creating the savepoint
   * @param comment - Comment for the savepoint
   * @return true if the savepoint was created successfully
   */
  public boolean savepoint(String user, String comment) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    if (table.getCompletedCommitsTimeline().empty()) {
      throw new HoodieSavepointException("Could not savepoint. Commit timeline is empty");
    }
    if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
      throw new UnsupportedOperationException("Savepointing is not supported or MergeOnRead table types");
    }

    String latestCommit = table.getCompletedCommitsTimeline().lastInstant().get().getTimestamp();
    logger.info("Savepointing latest commit " + latestCommit);
    return savepoint(latestCommit, user, comment);
  }

  /**
   * Savepoint a specific commit. Latest version of data files as of the passed in commitTime will be referenced in the
   * savepoint and will never be cleaned. The savepointed commit will never be rolledback or archived.
   * <p>
   * This gives an option to rollback the state to the savepoint anytime. Savepoint needs to be manually created and
   * deleted.
   * <p>
   * Savepoint should be on a commit that could not have been cleaned.
   *
   * @param commitTime - commit that should be savepointed
   * @param user - User creating the savepoint
   * @param comment - Comment for the savepoint
   * @return true if the savepoint was created successfully
   */
  public boolean savepoint(String commitTime, String user, String comment) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
      throw new UnsupportedOperationException("Savepointing is not supported or MergeOnRead table types");
    }
    Option<HoodieInstant> cleanInstant = table.getCompletedCleanTimeline().lastInstant();

    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);
    if (!table.getCompletedCommitsTimeline().containsInstant(commitInstant)) {
      throw new HoodieSavepointException("Could not savepoint non-existing commit " + commitInstant);
    }

    try {
      // Check the last commit that was not cleaned and check if savepoint time is > that commit
      String lastCommitRetained;
      if (cleanInstant.isPresent()) {
        HoodieCleanMetadata cleanMetadata = AvroUtils
            .deserializeHoodieCleanMetadata(table.getActiveTimeline().getInstantDetails(cleanInstant.get()).get());
        lastCommitRetained = cleanMetadata.getEarliestCommitToRetain();
      } else {
        lastCommitRetained = table.getCompletedCommitsTimeline().firstInstant().get().getTimestamp();
      }

      // Cannot allow savepoint time on a commit that could have been cleaned
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(commitTime, lastCommitRetained, HoodieTimeline.GREATER_OR_EQUAL),
          "Could not savepoint commit " + commitTime + " as this is beyond the lookup window " + lastCommitRetained);

      Map<String, List<String>> latestFilesMap = jsc
          .parallelize(FSUtils.getAllPartitionPaths(fs, table.getMetaClient().getBasePath(),
              config.shouldAssumeDatePartitioning()))
          .mapToPair((PairFunction<String, String, List<String>>) partitionPath -> {
            // Scan all partitions files with this commit time
            logger.info("Collecting latest files in partition path " + partitionPath);
            ReadOptimizedView view = table.getROFileSystemView();
            List<String> latestFiles = view.getLatestDataFilesBeforeOrOn(partitionPath, commitTime)
                .map(HoodieDataFile::getFileName).collect(Collectors.toList());
            return new Tuple2<>(partitionPath, latestFiles);
          }).collectAsMap();

      HoodieSavepointMetadata metadata = AvroUtils.convertSavepointMetadata(user, comment, latestFilesMap);
      // Nothing to save in the savepoint
      table.getActiveTimeline().saveAsComplete(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, commitTime),
          AvroUtils.serializeSavepointMetadata(metadata));
      logger.info("Savepoint " + commitTime + " created");
      return true;
    } catch (IOException e) {
      throw new HoodieSavepointException("Failed to savepoint " + commitTime, e);
    }
  }

  /**
   * Delete a savepoint that was created. Once the savepoint is deleted, the commit can be rolledback and cleaner may
   * clean up data files.
   *
   * @param savepointTime - delete the savepoint
   * @return true if the savepoint was deleted successfully
   */
  public void deleteSavepoint(String savepointTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
      throw new UnsupportedOperationException("Savepointing is not supported or MergeOnRead table types");
    }
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      logger.warn("No savepoint present " + savepointTime);
      return;
    }

    activeTimeline.revertToInflight(savePoint);
    activeTimeline.deleteInflight(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, savepointTime));
    logger.info("Savepoint " + savepointTime + " deleted");
  }

  /**
   * Delete a compaction request that is pending.
   *
   * NOTE - This is an Admin operation. With async compaction, this is expected to be called with async compaction and
   * write shutdown. Otherwise, async compactor could fail with errors
   *
   * @param compactionTime - delete the compaction time
   */
  private void deleteRequestedCompaction(String compactionTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieInstant compactionRequestedInstant =
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionTime);
    boolean isCompactionInstantInRequestedState =
        table.getActiveTimeline().filterPendingCompactionTimeline().containsInstant(compactionRequestedInstant);
    HoodieTimeline commitTimeline = table.getCompletedCommitTimeline();
    if (commitTimeline.empty() && !commitTimeline.findInstantsAfter(compactionTime, Integer.MAX_VALUE).empty()) {
      throw new HoodieRollbackException(
          "Found commits after time :" + compactionTime + ", please rollback greater commits first");
    }
    if (isCompactionInstantInRequestedState) {
      activeTimeline.deleteCompactionRequested(compactionRequestedInstant);
    } else {
      throw new IllegalArgumentException("Compaction is not in requested state " + compactionTime);
    }
    logger.info("Compaction " + compactionTime + " deleted");
  }

  /**
   * Rollback the state to the savepoint. WARNING: This rollsback recent commits and deleted data files. Queries
   * accessing the files will mostly fail. This should be done during a downtime.
   *
   * @param savepointTime - savepoint time to rollback to
   * @return true if the savepoint was rollecback to successfully
   */
  public boolean rollbackToSavepoint(String savepointTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    // Rollback to savepoint is expected to be a manual operation and no concurrent write or compaction is expected
    // to be running. Rollback to savepoint also removes any pending compaction actions that are generated after
    // savepoint time. Allowing pending compaction to be retained is not safe as those workload could be referencing
    // file-slices that will be rolled-back as part of this operation
    HoodieTimeline commitTimeline = table.getMetaClient().getCommitsAndCompactionTimeline();

    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      throw new HoodieRollbackException("No savepoint for commitTime " + savepointTime);
    }

    List<String> commitsToRollback = commitTimeline.findInstantsAfter(savepointTime, Integer.MAX_VALUE).getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    logger.info("Rolling back commits " + commitsToRollback);

    restoreToInstant(savepointTime);

    // Make sure the rollback was successful
    Option<HoodieInstant> lastInstant =
        activeTimeline.reload().getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants().lastInstant();
    Preconditions.checkArgument(lastInstant.isPresent());
    Preconditions.checkArgument(lastInstant.get().getTimestamp().equals(savepointTime),
        savepointTime + "is not the last commit after rolling back " + commitsToRollback + ", last commit was "
            + lastInstant.get().getTimestamp());
    return true;
  }

  /**
   * Rollback the (inflight/committed) record changes with the given commit time. Three steps: (1) Atomically unpublish
   * this commit (2) clean indexing data, (3) clean new generated parquet files. (4) Finally delete .commit or .inflight
   * file,
   */
  public boolean rollback(final String commitTime) throws HoodieRollbackException {
    rollbackInternal(commitTime);
    return true;
  }

  /**
   * NOTE : This action requires all writers (ingest and compact) to a dataset to be stopped before proceeding. Revert
   * the (inflight/committed) record changes for all commits after the provided @param. Three steps: (1) Atomically
   * unpublish this commit (2) clean indexing data, (3) clean new generated parquet/log files and/or append rollback to
   * existing log files. (4) Finally delete .commit, .inflight, .compaction.inflight or .compaction.requested file
   */
  public void restoreToInstant(final String instantTime) throws HoodieRollbackException {

    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    // Get all the commits on the timeline after the provided commit time
    List<HoodieInstant> instantsToRollback = table.getActiveTimeline().getCommitsAndCompactionTimeline()
        .getReverseOrderedInstants()
        .filter(instant -> HoodieActiveTimeline.GREATER.test(instant.getTimestamp(), instantTime))
        .collect(Collectors.toList());
    // Start a rollback instant for all commits to be rolled back
    String startRollbackInstant = startInstant();
    // Start the timer
    final Timer.Context context = startContext();
    ImmutableMap.Builder<String, List<HoodieRollbackStat>> instantsToStats = ImmutableMap.builder();
    instantsToRollback.stream().forEach(instant -> {
      try {
        switch (instant.getAction()) {
          case HoodieTimeline.COMMIT_ACTION:
          case HoodieTimeline.DELTA_COMMIT_ACTION:
            List<HoodieRollbackStat> statsForInstant = doRollbackAndGetStats(instant.getTimestamp());
            instantsToStats.put(instant.getTimestamp(), statsForInstant);
            break;
          case HoodieTimeline.COMPACTION_ACTION:
            // TODO : Get file status and create a rollback stat and file
            // TODO : Delete the .aux files along with the instant file, okay for now since the archival process will
            // delete these files when it does not see a corresponding instant file under .hoodie
            List<HoodieRollbackStat> statsForCompaction = doRollbackAndGetStats(instant.getTimestamp());
            instantsToStats.put(instant.getTimestamp(), statsForCompaction);
            logger.info("Deleted compaction instant " + instant);
            break;
          default:
            throw new IllegalArgumentException("invalid action name " + instant.getAction());
        }
      } catch (IOException io) {
        throw new HoodieRollbackException("unable to rollback instant " + instant, io);
      }
    });
    try {
      finishRestore(context, instantsToStats.build(),
          instantsToRollback.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList()),
          startRollbackInstant, instantTime);
    } catch (IOException io) {
      throw new HoodieRollbackException("unable to rollback instants " + instantsToRollback, io);
    }
  }

  private String startInstant() {
    return HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
  }

  private Timer.Context startContext() {
    return metrics.getRollbackCtx();
  }

  private List<HoodieRollbackStat> doRollbackAndGetStats(final String commitToRollback) throws IOException {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    HoodieTimeline inflightCommitTimeline = table.getInflightCommitTimeline();
    HoodieTimeline commitTimeline = table.getCompletedCommitsTimeline();
    // Check if any of the commits is a savepoint - do not allow rollback on those commits
    List<String> savepoints = table.getCompletedSavepointTimeline().getInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    savepoints.stream().forEach(s -> {
      if (s.contains(commitToRollback)) {
        throw new HoodieRollbackException(
            "Could not rollback a savepointed commit. Delete savepoint first before rolling back" + s);
      }
    });

    if (commitTimeline.empty() && inflightCommitTimeline.empty()) {
      // nothing to rollback
      logger.info("No commits to rollback " + commitToRollback);
    }

    // Make sure only the last n commits are being rolled back
    // If there is a commit in-between or after that is not rolled back, then abort
    String lastCommit = commitToRollback;

    if ((lastCommit != null) && !commitTimeline.empty()
        && !commitTimeline.findInstantsAfter(lastCommit, Integer.MAX_VALUE).empty()) {
      throw new HoodieRollbackException(
          "Found commits after time :" + lastCommit + ", please rollback greater commits first");
    }

    List<String> inflights =
        inflightCommitTimeline.getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    if ((lastCommit != null) && !inflights.isEmpty() && (inflights.indexOf(lastCommit) != inflights.size() - 1)) {
      throw new HoodieRollbackException(
          "Found in-flight commits after time :" + lastCommit + ", please rollback greater commits first");
    }

    List<HoodieRollbackStat> stats = table.rollback(jsc, commitToRollback, true);

    logger.info("Deleted inflight commits " + commitToRollback);

    // cleanup index entries
    if (!index.rollbackCommit(commitToRollback)) {
      throw new HoodieRollbackException("Rollback index changes failed, for time :" + commitToRollback);
    }
    logger.info("Index rolled back for commits " + commitToRollback);
    return stats;
  }

  private void finishRollback(final Timer.Context context, List<HoodieRollbackStat> rollbackStats,
      List<String> commitsToRollback, final String startRollbackTime) throws IOException {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    Option<Long> durationInMs = Option.empty();
    Long numFilesDeleted = rollbackStats.stream().mapToLong(stat -> stat.getSuccessDeleteFiles().size()).sum();
    if (context != null) {
      durationInMs = Option.of(metrics.getDurationInMs(context.stop()));
      metrics.updateRollbackMetrics(durationInMs.get(), numFilesDeleted);
    }
    HoodieRollbackMetadata rollbackMetadata =
        AvroUtils.convertRollbackMetadata(startRollbackTime, durationInMs, commitsToRollback, rollbackStats);
    table.getActiveTimeline().saveAsComplete(new HoodieInstant(true, HoodieTimeline.ROLLBACK_ACTION, startRollbackTime),
        AvroUtils.serializeRollbackMetadata(rollbackMetadata));
    logger.info("Commits " + commitsToRollback + " rollback is complete");

    if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
      logger.info("Cleaning up older rollback meta files");
      // Cleanup of older cleaner meta files
      // TODO - make the commit archival generic and archive rollback metadata
      FSUtils.deleteOlderRollbackMetaFiles(fs, table.getMetaClient().getMetaPath(),
          table.getActiveTimeline().getRollbackTimeline().getInstants());
    }
  }

  private void finishRestore(final Timer.Context context, Map<String, List<HoodieRollbackStat>> commitToStats,
      List<String> commitsToRollback, final String startRestoreTime, final String restoreToInstant) throws IOException {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    Option<Long> durationInMs = Option.empty();
    Long numFilesDeleted = 0L;
    for (Map.Entry<String, List<HoodieRollbackStat>> commitToStat : commitToStats.entrySet()) {
      List<HoodieRollbackStat> stats = commitToStat.getValue();
      numFilesDeleted = stats.stream().mapToLong(stat -> stat.getSuccessDeleteFiles().size()).sum();
    }
    if (context != null) {
      durationInMs = Option.of(metrics.getDurationInMs(context.stop()));
      metrics.updateRollbackMetrics(durationInMs.get(), numFilesDeleted);
    }
    HoodieRestoreMetadata restoreMetadata =
        AvroUtils.convertRestoreMetadata(startRestoreTime, durationInMs, commitsToRollback, commitToStats);
    table.getActiveTimeline().saveAsComplete(new HoodieInstant(true, HoodieTimeline.RESTORE_ACTION, startRestoreTime),
        AvroUtils.serializeRestoreMetadata(restoreMetadata));
    logger.info("Commits " + commitsToRollback + " rollback is complete. Restored dataset to " + restoreToInstant);

    if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
      logger.info("Cleaning up older restore meta files");
      // Cleanup of older cleaner meta files
      // TODO - make the commit archival generic and archive rollback metadata
      FSUtils.deleteOlderRollbackMetaFiles(fs, table.getMetaClient().getMetaPath(),
          table.getActiveTimeline().getRestoreTimeline().getInstants());
    }
  }

  private void rollbackInternal(String commitToRollback) {
    if (commitToRollback.isEmpty()) {
      logger.info("List of commits to rollback is empty");
      return;
    }
    final String startRollbackTime = startInstant();
    final Timer.Context context = startContext();
    // Create a Hoodie table which encapsulated the commits and files visible
    try {
      List<HoodieRollbackStat> stats = doRollbackAndGetStats(commitToRollback);
      Map<String, List<HoodieRollbackStat>> statToCommit = new HashMap<>();
      finishRollback(context, stats, Arrays.asList(commitToRollback), startRollbackTime);
    } catch (IOException e) {
      throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitToRollback,
          e);
    }
  }

  /**
   * Releases any resources used by the client.
   */
  public void close() {
    // Stop timeline-server if running
    super.close();
    this.cleanClient.close();
    // Calling this here releases any resources used by your index, so make sure to finish any related operations
    // before this point
    this.index.close();
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  public void clean() throws HoodieIOException {
    cleanClient.clean();
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   *
   * @param startCleanTime Cleaner Instant Timestamp
   * @throws HoodieIOException in case of any IOException
   */
  protected HoodieCleanMetadata clean(String startCleanTime) throws HoodieIOException {
    return cleanClient.clean(startCleanTime);
  }

  /**
   * Provides a new commit time for a write operation (insert/update).
   */
  public String startCommit() {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackInFlight) {
      // Only rollback inflight commit/delta-commits. Do not touch compaction commits
      rollbackInflightCommits();
    }
    String commitTime = HoodieActiveTimeline.createNewCommitTime();
    startCommit(commitTime);
    return commitTime;
  }

  public void startCommitWithTime(String instantTime) {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackInFlight) {
      // Only rollback inflight commit/delta-commits. Do not touch compaction commits
      rollbackInflightCommits();
    }
    startCommit(instantTime);
  }

  private void startCommit(String instantTime) {
    logger.info("Generate a new instant time " + instantTime);
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // if there are pending compactions, their instantTime must not be greater than that of this instant time
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().ifPresent(latestPending -> {
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(latestPending.getTimestamp(), instantTime, HoodieTimeline.LESSER),
          "Latest pending compaction instant time must be earlier " + "than this instant time. Latest Compaction :"
              + latestPending + ",  Ingesting at " + instantTime);
    });
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitActionType = table.getMetaClient().getCommitActionType();
    activeTimeline.createInflight(new HoodieInstant(true, commitActionType, instantTime));
  }

  /**
   * Schedules a new compaction instant.
   */
  public Option<String> scheduleCompaction(Option<Map<String, String>> extraMetadata) throws IOException {
    String instantTime = HoodieActiveTimeline.createNewCommitTime();
    logger.info("Generate a new instant time " + instantTime);
    boolean notEmpty = scheduleCompactionAtInstant(instantTime, extraMetadata);
    return notEmpty ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new compaction instant with passed-in instant time.
   *
   * @param instantTime Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata)
      throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // if there are inflight writes, their instantTime must not be less than that of compaction instant time
    metaClient.getCommitsTimeline().filterInflightsExcludingCompaction().firstInstant().ifPresent(earliestInflight -> {
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), instantTime, HoodieTimeline.GREATER),
          "Earliest write inflight instant time must be later " + "than compaction time. Earliest :" + earliestInflight
              + ", Compaction scheduled at " + instantTime);
    });
    // Committed and pending compaction instants should have strictly lower timestamps
    List<HoodieInstant> conflictingInstants = metaClient
        .getActiveTimeline().getCommitsAndCompactionTimeline().getInstants().filter(instant -> HoodieTimeline
            .compareTimestamps(instant.getTimestamp(), instantTime, HoodieTimeline.GREATER_OR_EQUAL))
        .collect(Collectors.toList());
    Preconditions.checkArgument(conflictingInstants.isEmpty(),
        "Following instants have timestamps >= compactionInstant (" + instantTime + ") Instants :"
            + conflictingInstants);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    HoodieCompactionPlan workload = table.scheduleCompaction(jsc, instantTime);
    if (workload != null && (workload.getOperations() != null) && (!workload.getOperations().isEmpty())) {
      extraMetadata.ifPresent(workload::setExtraMetadata);
      HoodieInstant compactionInstant =
          new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
      metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
          AvroUtils.serializeCompactionPlan(workload));
      return true;
    }
    return false;
  }

  /**
   * Performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   */
  public JavaRDD<WriteStatus> compact(String compactionInstantTime) throws IOException {
    return compact(compactionInstantTime, config.shouldAutoCommit());
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   */
  public void commitCompaction(String compactionInstantTime, JavaRDD<WriteStatus> writeStatuses,
      Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieCompactionPlan compactionPlan = AvroUtils.deserializeCompactionPlan(
        timeline.getInstantAuxiliaryDetails(HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get());
    // Merge extra meta-data passed by user with the one already in inflight compaction
    Option<Map<String, String>> mergedMetaData = extraMetadata.map(m -> {
      Map<String, String> merged = new HashMap<>();
      Map<String, String> extraMetaDataFromInstantFile = compactionPlan.getExtraMetadata();
      if (extraMetaDataFromInstantFile != null) {
        merged.putAll(extraMetaDataFromInstantFile);
      }
      // Overwrite/Merge with the user-passed meta-data
      merged.putAll(m);
      return Option.of(merged);
    }).orElseGet(() -> Option.ofNullable(compactionPlan.getExtraMetadata()));
    commitCompaction(writeStatuses, table, compactionInstantTime, true, mergedMetaData);
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication funciton.
   */
  JavaRDD<HoodieRecord<T>> deduplicateRecords(JavaRDD<HoodieRecord<T>> records, int parallelism) {
    boolean isIndexingGlobal = index.isGlobal();
    return records.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      return new Tuple2<>(key, record);
    }).reduceByKey((rec1, rec2) -> {
      @SuppressWarnings("unchecked")
      T reducedData = (T) rec1.getData().preCombine(rec2.getData());
      // we cannot allow the user to change the key or partitionPath, since that will affect
      // everything
      // so pick it from one of the records.
      return new HoodieRecord<T>(rec1.getKey(), reducedData);
    }, parallelism).map(Tuple2::_2);
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication funciton.
   */
  JavaRDD<HoodieKey> deduplicateKeys(JavaRDD<HoodieKey> keys, int parallelism) {
    boolean isIndexingGlobal = index.isGlobal();
    if (isIndexingGlobal) {
      return keys.keyBy(HoodieKey::getRecordKey)
          .reduceByKey((key1, key2) -> key1)
          .values();
    } else {
      return keys.distinct();
    }
  }

  /**
   * Cleanup all inflight commits.
   */
  private void rollbackInflightCommits() {
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterInflightsExcludingCompaction();
    List<String> commits = inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    for (String commit : commits) {
      rollback(commit);
    }
  }

  private HoodieTable getTableAndInitCtx(OperationType operationType) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    if (operationType == OperationType.DELETE) {
      setWriteSchemaFromLastInstant(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeContext = metrics.getCommitCtx();
    } else {
      writeContext = metrics.getDeltaCommitCtx();
    }
    return table;
  }

  /**
   * Sets write schema from last instant since deletes may not have schema set in the config.
   */
  private void setWriteSchemaFromLastInstant(HoodieTableMetaClient metaClient) {
    try {
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
      Option<HoodieInstant> lastInstant =
          activeTimeline.filterCompletedInstants().filter(s -> s.getAction().equals(metaClient.getCommitActionType()))
              .lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            activeTimeline.getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
        if (commitMetadata.getExtraMetadata().containsKey(HoodieCommitMetadata.SCHEMA_KEY)) {
          config.setSchema(commitMetadata.getExtraMetadata().get(HoodieCommitMetadata.SCHEMA_KEY));
        } else {
          throw new HoodieIOException("Latest commit does not have any schema in commit metadata");
        }
      } else {
        throw new HoodieIOException("Deletes issued without any prior commits");
      }
    } catch (IOException e) {
      throw new HoodieIOException("IOException thrown while reading last commit metadata", e);
    }
  }
  /**
   * Compaction specific private methods
   */

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   */
  private JavaRDD<WriteStatus> compact(String compactionInstantTime, boolean autoCommit) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    HoodieTimeline pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      // inflight compaction - Needs to rollback first deleting new parquet files before we run compaction.
      rollbackInflightCompaction(inflightInstant, table);
      // refresh table
      metaClient = createMetaClient(true);
      table = HoodieTable.getHoodieTable(metaClient, config, jsc);
      pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    }

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(instant)) {
      return runCompaction(instant, metaClient.getActiveTimeline(), autoCommit);
    } else {
      throw new IllegalStateException(
          "No Compaction request available at " + compactionInstantTime + " to run compaction");
    }
  }

  /**
   * Perform compaction operations as specified in the compaction commit file.
   *
   * @param compactionInstant Compacton Instant time
   * @param activeTimeline Active Timeline
   * @param autoCommit Commit after compaction
   * @return RDD of Write Status
   */
  private JavaRDD<WriteStatus> runCompaction(HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline,
      boolean autoCommit) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.getCompactionPlan(metaClient, compactionInstant.getTimestamp());

    // Mark instant as compaction inflight
    activeTimeline.transitionCompactionRequestedToInflight(compactionInstant);
    compactionTimer = metrics.getCompactionCtx();
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    JavaRDD<WriteStatus> statuses = table.compact(jsc, compactionInstant.getTimestamp(), compactionPlan);
    // Force compaction action
    statuses.persist(config.getWriteStatusStorageLevel());
    // pass extra-metada so that it gets stored in commit file automatically
    commitCompaction(statuses, table, compactionInstant.getTimestamp(), autoCommit,
        Option.ofNullable(compactionPlan.getExtraMetadata()));
    return statuses;
  }

  /**
   * Commit Compaction and track metrics.
   *
   * @param compactedStatuses Compaction Write status
   * @param table Hoodie Table
   * @param compactionCommitTime Compaction Commit Time
   * @param autoCommit Auto Commit
   * @param extraMetadata Extra Metadata to store
   */
  protected void commitCompaction(JavaRDD<WriteStatus> compactedStatuses, HoodieTable<T> table,
      String compactionCommitTime, boolean autoCommit, Option<Map<String, String>> extraMetadata) {
    if (autoCommit) {
      HoodieCommitMetadata metadata = doCompactionCommit(table, compactedStatuses, compactionCommitTime, extraMetadata);
      if (compactionTimer != null) {
        long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
        try {
          metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(compactionCommitTime).getTime(),
              durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
        } catch (ParseException e) {
          throw new HoodieCommitException("Commit time is not of valid format.Failed to commit compaction "
              + config.getBasePath() + " at time " + compactionCommitTime, e);
        }
      }
      logger.info("Compacted successfully on commit " + compactionCommitTime);
    } else {
      logger.info("Compaction did not run for commit " + compactionCommitTime);
    }
  }

  private void finalizeWrite(HoodieTable<T> table, String instantTime, List<HoodieWriteStat> stats) {
    try {
      final Timer.Context finalizeCtx = metrics.getFinalizeCtx();
      table.finalizeWrite(jsc, instantTime, stats);
      if (finalizeCtx != null) {
        Option<Long> durationInMs = Option.of(metrics.getDurationInMs(finalizeCtx.stop()));
        durationInMs.ifPresent(duration -> {
          logger.info("Finalize write elapsed time (milliseconds): " + duration);
          metrics.updateFinalizeWriteMetrics(duration, stats.size());
        });
      }
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file to the .requested file
   *
   * @param inflightInstant Inflight Compaction Instant
   * @param table Hoodie Table
   */
  @VisibleForTesting
  void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieTable table) throws IOException {
    table.rollback(jsc, inflightInstant.getTimestamp(), false);
    // Revert instant state file
    table.getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  private HoodieCommitMetadata doCompactionCommit(HoodieTable<T> table, JavaRDD<WriteStatus> writeStatuses,
      String compactionCommitTime, Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    List<HoodieWriteStat> updateStatusMap = writeStatuses.map(WriteStatus::getStat).collect();

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }

    // Finalize write
    finalizeWrite(table, compactionCommitTime, updateStatusMap);

    // Copy extraMetadata
    extraMetadata.ifPresent(m -> {
      m.entrySet().stream().forEach(e -> {
        metadata.addMetadata(e.getKey(), e.getValue());
      });
    });

    logger.info("Committing Compaction " + compactionCommitTime + ". Finished with result " + metadata);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      activeTimeline.transitionCompactionInflightToComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + metaClient.getBasePath() + " at time " + compactionCommitTime, e);
    }
    return metadata;
  }

  /**
   * Performs a compaction operation on a dataset, serially before or after an insert/upsert action.
   */
  private Option<String> forceCompact(Option<Map<String, String>> extraMetadata) throws IOException {
    Option<String> compactionInstantTimeOpt = scheduleCompaction(extraMetadata);
    compactionInstantTimeOpt.ifPresent(compactionInstantTime -> {
      try {
        // inline compaction should auto commit as the user is never given control
        compact(compactionInstantTime, true);
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    });
    return compactionInstantTimeOpt;
  }

  private void updateMetadataAndRollingStats(String actionType, HoodieCommitMetadata metadata,
      List<HoodieWriteStat> writeStats) {
    // TODO : make sure we cannot rollback / archive last commit file
    try {
      // Create a Hoodie table which encapsulated the commits and files visible
      HoodieTable table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
      // 0. All of the rolling stat management is only done by the DELTA commit for MOR and COMMIT for COW other wise
      // there may be race conditions
      HoodieRollingStatMetadata rollingStatMetadata = new HoodieRollingStatMetadata(actionType);
      // 1. Look up the previous compaction/commit and get the HoodieCommitMetadata from there.
      // 2. Now, first read the existing rolling stats and merge with the result of current metadata.

      // Need to do this on every commit (delta or commit) to support COW and MOR.

      for (HoodieWriteStat stat : writeStats) {
        String partitionPath = stat.getPartitionPath();
        // TODO: why is stat.getPartitionPath() null at times here.
        metadata.addWriteStat(partitionPath, stat);
        HoodieRollingStat hoodieRollingStat = new HoodieRollingStat(stat.getFileId(),
            stat.getNumWrites() - (stat.getNumUpdateWrites() - stat.getNumDeletes()), stat.getNumUpdateWrites(),
            stat.getNumDeletes(), stat.getTotalWriteBytes());
        rollingStatMetadata.addRollingStat(partitionPath, hoodieRollingStat);
      }
      // The last rolling stat should be present in the completed timeline
      Option<HoodieInstant> lastInstant =
          table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            table.getActiveTimeline().getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
        Option<String> lastRollingStat = Option
            .ofNullable(commitMetadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY));
        if (lastRollingStat.isPresent()) {
          rollingStatMetadata = rollingStatMetadata
              .merge(HoodieCommitMetadata.fromBytes(lastRollingStat.get().getBytes(), HoodieRollingStatMetadata.class));
        }
      }
      metadata.addMetadata(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY, rollingStatMetadata.toJsonString());
    } catch (IOException io) {
      throw new HoodieCommitException("Unable to save rolling stats");
    }
  }

  /**
   * Refers to different operation types.
   */
  enum OperationType {
    INSERT,
    INSERT_PREPPED,
    UPSERT,
    UPSERT_PREPPED,
    DELETE,
    BULK_INSERT,
    BULK_INSERT_PREPPED
  }
}
