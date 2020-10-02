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

package org.apache.hudi.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.ClientUtils;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.codahale.metrics.Timer;

import scala.Tuple2;

/**
 * Metadata implementation which saves partition and file listing within an internal MOR table
 * called Metadata Table. This table is created by listing files and partitions (first time) and kept in sync
 * using the instants on the main dataset.
 */
public class HoodieMetadataImpl extends HoodieMetadataCommon {
  private static final Logger LOG = LogManager.getLogger(HoodieMetadataImpl.class);

  // Metric names
  private static final String INITIALIZE_STR = "initialize";
  private static final String SYNC_STR = "sync";
  private static final String LOOKUP_PARTITIONS_STR = "lookup_partitions";
  private static final String LOOKUP_FILES_STR = "lookup_files";
  private static final String VALIDATE_PARTITIONS_STR = "validate_partitions";
  private static final String VALIDATE_FILES_STR = "validate_files";
  private static final String VALIDATE_ERRORS_STR = "validate_errors";
  private static final String SCAN_STR = "scan";

  // Stats names
  private static final String STAT_TOTAL_BASE_FILE_SIZE = "totalBaseFileSizeInBytes";
  private static final String STAT_TOTAL_LOG_FILE_SIZE = "totalLogFileSizeInBytes";
  private static final String STAT_COUNT_BASE_FILES = "baseFileCount";
  private static final String STAT_COUNT_LOG_FILES = "logFileCount";
  private static final String STAT_COUNT_PARTITION = "partitionCount";
  private static final String STAT_IN_SYNC = "isInSync";
  private static final String STAT_LAST_COMPACTION_TIMESTAMP = "lastCompactionTimestamp";

  private final JavaSparkContext jsc;
  private final Configuration hadoopConf;
  private final String datasetBasePath;
  private final String metadataBasePath;
  private final HoodieWriteConfig config;
  private final String tableName;
  private final Schema schema;
  private long maxMemorySizeInBytes = 1024 * 1024 * 1024; // TODO
  private int bufferSize = 10 * 1024 * 1024; // TODO
  private HoodieTableMetaClient metaClient;
  private HoodieMetrics metrics;
  private boolean validateLookups = false;
  private int numLookupsAllPartitions = 0;
  private int numLookupsFilesInPartition = 0;
  private int numScans = 0;
  private int numValidationErrors = 0;
  // In read only mode, no changes are made to the metadata table
  private boolean readOnly = true;

  // Readers for the base and log file which store the metadata
  private HoodieFileReader<GenericRecord> basefileReader;
  private HoodieMetadataMergedLogRecordScanner logRecordScanner;

  HoodieMetadataImpl(JavaSparkContext jsc, Configuration hadoopConf, String datasetBasePath, String metadataBasePath,
                         HoodieWriteConfig writeConfig, boolean readOnly) throws IOException {
    this.jsc = jsc;
    this.hadoopConf = hadoopConf;
    this.datasetBasePath = datasetBasePath;
    this.metadataBasePath = metadataBasePath;
    this.readOnly = readOnly;

    // Load the schema
    this.schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());

    if (readOnly) {
      this.config = HoodieWriteConfig.newBuilder().withPath(datasetBasePath).build();
      metaClient = new HoodieTableMetaClient(hadoopConf, metadataBasePath);
      this.metrics = new HoodieMetrics(this.config, this.config.getTableName());
      this.tableName = null;
    } else {
      this.tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;
      this.validateLookups = writeConfig.getFileListingMetadataVerify();

      this.config = createMetadataWriteConfig(writeConfig);
      this.metrics = new HoodieMetrics(this.config, this.config.getTableName());

      // Inline compaction and auto clean is required as we dont expose this table outside
      ValidationUtils.checkArgument(this.config.isAutoClean(), "Auto clean is required for Metadata Compaction config");
      ValidationUtils.checkArgument(this.config.isInlineCompaction(), "Inline compaction is required for Metadata Compaction config");
      // Metadata Table cannot have its metadata optimized
      ValidationUtils.checkArgument(this.config.shouldAutoCommit(), "Auto commit is required for Metadata Table");
      ValidationUtils.checkArgument(!this.config.useFileListingMetadata(), "File listing cannot be used for Metadata Table");

      initAndSyncMetadataTable();
    }
  }

  /**
   * Create a {@code HoodieWriteConfig} to use for the Metadata Table.
   *
   * @param writeConfig {@code HoodieWriteConfig} of the main dataset writer
   * @param schemaStr Metadata Table schema
   */
  private HoodieWriteConfig createMetadataWriteConfig(HoodieWriteConfig writeConfig) throws IOException {
    // Create the write config for the metadata table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withTimelineLayoutVersion(writeConfig.getTimelineLayoutVersion())
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
            .withConsistencyCheckEnabled(writeConfig.getConsistencyGuardConfig().isConsistencyCheckEnabled())
            .withInitialConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getInitialConsistencyCheckIntervalMs())
            .withMaxConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getMaxConsistencyCheckIntervalMs())
            .withMaxConsistencyChecks(writeConfig.getConsistencyGuardConfig().getMaxConsistencyChecks())
            .build())
        .withUseFileListingMetadata(false)
        .withFileListingMetadataVerify(false)
        .withAutoCommit(true)
        .withAvroSchemaValidate(true)
        .withEmbeddedTimelineServerEnabled(false)
        .withAssumeDatePartitioning(false)
        .withPath(metadataBasePath)
        .withSchema(HoodieMetadataRecord.getClassSchema().toString())
        .forTable(tableName)
        .withParallelism(1, 1).withDeleteParallelism(1).withRollbackParallelism(1).withFinalizeWriteParallelism(1)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexClass(HoodieMetadataIndex.class.getName()).build())
        .withCompactionConfig(writeConfig.getMetadataCompactionConfig());

    if (writeConfig.isMetricsOn()) {
      HoodieMetricsConfig.Builder metricsConfig = HoodieMetricsConfig.newBuilder()
          .withReporterType(writeConfig.getMetricsReporterType().toString())
          .on(true);
      switch (writeConfig.getMetricsReporterType()) {
        case GRAPHITE:
          metricsConfig.onGraphitePort(writeConfig.getGraphiteServerPort())
                       .toGraphiteHost(writeConfig.getGraphiteServerHost())
                       .usePrefix(writeConfig.getGraphiteMetricPrefix());
          break;
        case JMX:
          metricsConfig.onJmxPort(writeConfig.getJmxPort())
                       .toJmxHost(writeConfig.getJmxHost());
          break;
        case DATADOG:
          // TODO:
          break;
        case CONSOLE:
        case INMEMORY:
          break;
        default:
          throw new HoodieMetadataException("Unsupported Metrics Reporter type " + writeConfig.getMetricsReporterType());
      }

      builder.withMetricsConfig(metricsConfig.build());
    }

    return builder.build();
  }

  public HoodieWriteConfig getWriteConfig() {
    return config;
  }

  /**
   * Reload the metadata table by syncing it based on the commits on the dataset.
   *
   * This is only allowed
   */

  public void reload() throws IOException {
    initAndSyncMetadataTable();
  }

  /**
   * Initialize the metadata table if it does not exist. Update the metadata to bring it in sync with the file system.
   *
   * This can happen in two ways:
   * 1. If the metadata table did not exist, then file and partition listing is used
   * 2. If the metadata table exists, the instants from active timeline are read in order and changes applied
   *
   * The above logic has been chosen because it is faster to perform #1 at scale rather than read all the Instants
   * which are large in size (AVRO or JSON encoded and not compressed) and incur considerable IO for de-serialization
   * and decoding.
   */
  private void initAndSyncMetadataTable() throws IOException {
    ValidationUtils.checkState(!readOnly, "Metadata table cannot be initialized in readonly mode");

    final Timer.Context context = this.metrics.getMetadataCtx(INITIALIZE_STR);

    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf, datasetBasePath);
    FileSystem fs = FSUtils.getFs(metadataBasePath, hadoopConf);
    boolean exists = fs.exists(new Path(metadataBasePath, HoodieTableMetaClient.METAFOLDER_NAME));

    if (!exists) {
      // Initialize for the first time by listing partitions and files directly from the file system
      initFromFilesystem(datasetMetaClient);
    } else {
      metaClient = ClientUtils.createMetaClient(hadoopConf, config, true);
    }

    /*
    // TODO: We may not be able to sync in certain cases (instants archived etc)
    //if (!canSync(datasetMetaClient)) {
      // Need to recreate the table as sync has failed
      // TODO: delete the table
    //  initFromFilesystem(datasetMetaClient);
    //}
    */

    // This is always called even in case the table was created for the first time. This is because
    // initFromFilesystem() does file listing and hence may take a long time during which some new updates
    // may have occurred on the table. Hence, calling this always ensures that the metadata is brought in sync
    // with the active timeline.
    syncFromInstants(datasetMetaClient);

    // Publish some metrics
    if (context != null) {
      long durationInMs = metrics.getDurationInMs(context.stop());
      // Time to initilize and sync
      if (exists) {
        metrics.updateMetadataMetrics(INITIALIZE_STR, 0, 0);
        metrics.updateMetadataMetrics(SYNC_STR, durationInMs, 1);
      } else {
        metrics.updateMetadataMetrics(INITIALIZE_STR, durationInMs, 1);
        metrics.updateMetadataMetrics(SYNC_STR, 0, 0);
      }

      // Total size of the metadata and count of base/log files
      Map<String, String> stats = getStats(false);
      metrics.updateMetadataSizeMetrics(Long.valueOf(stats.get(STAT_TOTAL_BASE_FILE_SIZE)),
          Long.valueOf(stats.get(STAT_TOTAL_LOG_FILE_SIZE)), Integer.valueOf(stats.get(STAT_COUNT_BASE_FILES)),
          Integer.valueOf(stats.get(STAT_COUNT_LOG_FILES)));
    }
  }

  /**
   * Initialize the Metadata Table by listing files and partitions from the file system.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  private void initFromFilesystem(HoodieTableMetaClient datasetMetaClient) throws IOException {
    ValidationUtils.checkState(!readOnly, "Metadata table cannot be initialized in readonly mode");

    // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
    Option<HoodieInstant> latestInstant = datasetMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String createInstantTime = latestInstant.isPresent() ? latestInstant.get().getTimestamp() : SOLO_COMMIT_TIMESTAMP;

    LOG.info("Creating a new metadata table in " + metadataBasePath + " at instant " + createInstantTime);
    metaClient = HoodieTableMetaClient.initTableType(hadoopConf, metadataBasePath.toString(),
        HoodieTableType.MERGE_ON_READ, tableName, "archived", HoodieMetadataPayload.class.getName(),
        HoodieFileFormat.HFILE.toString());

    // List all partitions in the basePath of the containing dataset
    FileSystem fs = FSUtils.getFs(datasetBasePath, hadoopConf);
    List<String> partitions = FSUtils.getAllPartitionPaths(fs, datasetBasePath, false);
    LOG.info("Initializing metadata table by using file listings in " + partitions.size() + " partitions");

    // List all partitions in parallel and collect the files in them
    final String dbasePath = datasetBasePath;
    final SerializableConfiguration serializedConf = new SerializableConfiguration(hadoopConf);
    int parallelism =  Math.min(partitions.size(), 100) + 1; // +1 to ensure non zero
    JavaPairRDD<String, FileStatus[]> partitionFileListRDD = jsc.parallelize(partitions, parallelism)
        .mapToPair(partition -> {
          FileSystem fsys = FSUtils.getFs(dbasePath, serializedConf.get());
          FileStatus[] statuses = FSUtils.getAllDataFilesInPartition(fsys, new Path(dbasePath, partition));
          return new Tuple2<>(partition, statuses);
        });

    // Collect the list of partitions and file lists
    List<Tuple2<String, FileStatus[]>> partitionFileList = partitionFileListRDD.collect();

    // Create a HoodieCommitMetadata with writeStats for all discovered files
    int[] stats = {0};
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    partitionFileList.forEach(t -> {
      final String partition = t._1;
      try {
        if (!fs.exists(new Path(datasetBasePath, partition + Path.SEPARATOR + HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE))) {
          return;
        }
      } catch (IOException e) {
        throw new HoodieMetadataException("Failed to check partition " + partition, e);
      }

      // If the partition has no files then create a writeStat with no file path
      if (t._2.length == 0) {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPartitionPath(partition);
        metadata.addWriteStat(partition, writeStat);
      } else {
        Arrays.stream(t._2).forEach(status -> {
          String filename = status.getPath().getName();
          if (filename.equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE)) {
            return;
          }
          HoodieWriteStat writeStat = new HoodieWriteStat();
          writeStat.setPath(partition + Path.SEPARATOR + filename);
          writeStat.setPartitionPath(partition);
          writeStat.setTotalWriteBytes(status.getLen());
          metadata.addWriteStat(partition, writeStat);
        });
      }
      stats[0] += t._2.length;
    });

    LOG.info("Committing " + partitionFileList.size() + " partitions and " + stats[0] + " files to metadata");
    update(metadata, createInstantTime);
  }

  /**
   * Sync the Metadata Table from the instants created on the dataset.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  private void syncFromInstants(HoodieTableMetaClient datasetMetaClient) throws IOException {
    ValidationUtils.checkState(!readOnly, "Metadata table cannot be synced in readonly mode");

    List<HoodieInstant> instantsToSync = findInstantsToSync(datasetMetaClient);
    if (instantsToSync.isEmpty()) {
      return;
    }

    LOG.info("Syncing " + instantsToSync.size() + " instants to metadata table: " + instantsToSync);

    // Read each instant in order and sync it to metadata table
    final HoodieActiveTimeline timeline = datasetMetaClient.getActiveTimeline();
    for (HoodieInstant instant : instantsToSync) {
      LOG.info("Syncing instant " + instant + " to metadata table");

      switch (instant.getAction()) {
        case HoodieTimeline.CLEAN_ACTION: {
          // CLEAN is synced from the
          // - inflight instant which contains the HoodieCleanerPlan, or
          // - complete instant which contains the HoodieCleanMetadata
          try {
            HoodieInstant inflightCleanInstant = new HoodieInstant(true, instant.getAction(), instant.getTimestamp());
            ValidationUtils.checkArgument(inflightCleanInstant.isInflight());
            HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(datasetMetaClient, inflightCleanInstant);
            update(cleanerPlan, instant.getTimestamp());
          } catch (HoodieIOException e) {
            HoodieInstant cleanInstant = new HoodieInstant(false, instant.getAction(), instant.getTimestamp());
            ValidationUtils.checkArgument(cleanInstant.isCompleted());
            HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(datasetMetaClient, cleanInstant);
            update(cleanMetadata, instant.getTimestamp());
          }
          break;
        }
        case HoodieTimeline.DELTA_COMMIT_ACTION:
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.COMPACTION_ACTION: {
          ValidationUtils.checkArgument(instant.isCompleted());
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
              timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          update(commitMetadata, instant.getTimestamp());
          break;
        }
        case HoodieTimeline.ROLLBACK_ACTION: {
          ValidationUtils.checkArgument(instant.isCompleted());
          HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
              timeline.getInstantDetails(instant).get());
          update(rollbackMetadata, instant.getTimestamp());
          break;
        }
        case HoodieTimeline.RESTORE_ACTION: {
          ValidationUtils.checkArgument(instant.isCompleted());
          HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
              timeline.getInstantDetails(instant).get());
          update(restoreMetadata, instant.getTimestamp());
          break;
        }
        case HoodieTimeline.SAVEPOINT_ACTION: {
          ValidationUtils.checkArgument(instant.isCompleted());
          // Nothing to be done here
          break;
        }
        default: {
          throw new HoodieException("Unknown type of action " + instant.getAction());
        }
      }
    }
  }

  /**
   * Return an ordered list of instants which have not been synced to the Metadata Table.

   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  public List<HoodieInstant> findInstantsToSync(HoodieTableMetaClient datasetMetaClient) {
    HoodieActiveTimeline metaTimeline = metaClient.reloadActiveTimeline();

    // All instants since the last time metadata table was compacted are candidates for sync
    Option<String> compactionTimestamp = getLatestCompactionTimestamp();

    // If there has not been any compaction then the first delta commit instant should be the one at which
    // the metadata table was created. We should not sync any instants before that creation time.
    Option<HoodieInstant> oldestMetaInstant = Option.empty();
    if (!compactionTimestamp.isPresent()) {
      oldestMetaInstant = metaTimeline.getDeltaCommitTimeline().filterCompletedInstants().firstInstant();
      if (oldestMetaInstant.isPresent()) {
        // TODO: Ensure this is the instant at which we created the metadata table
      }
    }

    String metaSyncTimestamp = compactionTimestamp.isPresent() ? compactionTimestamp.get()
        : oldestMetaInstant.isPresent() ? oldestMetaInstant.get().getTimestamp() : SOLO_COMMIT_TIMESTAMP;

    // Metadata table is updated when an instant is completed except for the following:
    //  CLEAN: metadata table is updated during inflight. So for CLEAN we accept inflight actions.
    List<HoodieInstant> datasetInstants = datasetMetaClient.getActiveTimeline().getInstants()
        .filter(i -> i.isCompleted() || (i.getAction().equals(HoodieTimeline.CLEAN_ACTION) && i.isInflight()))
        .filter(i -> metaSyncTimestamp.isEmpty()
            || HoodieTimeline.compareTimestamps(i.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS,
                metaSyncTimestamp))
        .collect(Collectors.toList());

    // Each operation on dataset leads to a delta-commit on the metadata MOR table. So find only delta-commit
    // instants in metadata table which are after the last compaction.
    Map<String, HoodieInstant> metadataInstantMap = metaTimeline.getDeltaCommitTimeline().filterCompletedInstants()
        .findInstantsAfterOrEquals(metaSyncTimestamp, Integer.MAX_VALUE).getInstants()
        .collect(Collectors.toMap(HoodieInstant::getTimestamp, Function.identity()));

    List<HoodieInstant> instantsToSync = new LinkedList<>();
    datasetInstants.forEach(instant -> {
      if (metadataInstantMap.containsKey(instant.getTimestamp())) {
        // instant already synced to metadata table
        if (!instantsToSync.isEmpty()) {
          // TODO: async clean and async compaction are not yet handled. They have a timestamp which is in the past
          // (when the operation was scheduled) and even on completion they retain their old timestamp.
          LOG.warn("Found out-of-order already synced instant " + instant + ". Instants to sync=" + instantsToSync);
        }
      } else {
        instantsToSync.add(instant);
      }
    });
    return instantsToSync;
  }

  /**
   * Update from {@code HoodieCommitMetadata}.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime Timestamp at which the commit was performed
   */
  void update(HoodieCommitMetadata commitMetadata, String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    commitMetadata.getPartitionToWriteStats().forEach((partition, writeStats) -> {
      Map<String, Long> newFiles = new HashMap<>(writeStats.size());
      writeStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        if (pathWithPartition == null) {
          // Empty partition
          return;
        }
        String filename = pathWithPartition.substring(partition.length() + 1);
        ValidationUtils.checkState(!newFiles.containsKey(filename), "Duplicate files in HoodieCommitMetadata");
        newFiles.put(filename, hoodieWriteStat.getTotalWriteBytes());
      });

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.of(newFiles),
          Option.empty());
      records.add(record);
    });

    // New partitions created
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(
        new ArrayList<String>(commitMetadata.getPartitionToWriteStats().keySet()));
    records.add(record);

    LOG.info("Updating at " + instantTime + " from Commit/" + commitMetadata.getOperationType()
        + ". #partitions_updated=" + records.size());
    commit(records, instantTime, true);
  }

  /**
   * Update from {@code HoodieCleanerPlan}.
   *
   * @param cleanerPlan {@code HoodieCleanerPlan}
   * @param instantTime Timestamp at which the clean plan was generated
   */
  void update(HoodieCleanerPlan cleanerPlan, String instantTime) {
    // TODO: Cleaner update is called even before the operation has completed. Hence, this function may be called
    // multiple times and we dont need to update metadata again.
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    long cnt = timeline.filterCompletedInstants().getInstants().filter(i -> i.getTimestamp().equals(instantTime)).count();
    if (cnt == 1) {
      LOG.info("Ignoring update from cleaner plan for already completed instant " + instantTime);
      return;
    }

    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};
    cleanerPlan.getFilePathsToBeDeletedPerPartition().forEach((partition, deletedPathInfo) -> {
      fileDeleteCount[0] += deletedPathInfo.size();

      // Files deleted from a partition
      List<String> deletedFilenames = deletedPathInfo.stream().map(p -> new Path(p.getFilePath()).getName())
          .collect(Collectors.toList());
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(deletedFilenames));
      records.add(record);
    });

    LOG.info("Updating at " + instantTime + " from CleanerPlan. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0]);
    commit(records, instantTime, true);
  }

  /**
   * Update from {@code HoodieCleanMetadata}.
   *
   * @param cleanMetadata {@code HoodieCleanMetadata}
   * @param instantTime Timestamp at which the clean was completed
   */
  private void update(HoodieCleanMetadata cleanMetadata, String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};

    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getSuccessDeleteFiles();
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(new ArrayList<>(deletedFiles)));

      records.add(record);
      fileDeleteCount[0] += deletedFiles.size();
    });

    LOG.info("Updating at " + instantTime + " from Clean. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0]);
    commit(records, instantTime, true);
  }

  /**
   * Update from {@code HoodieRestoreMetadata}.
   *
   * @param restoreMetadata {@code HoodieRestoreMetadata}
   * @param instantTime Timestamp at which the restore was performed
   */
  void update(HoodieRestoreMetadata restoreMetadata, String instantTime) {
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
      rms.forEach(rm -> processRollbackMetadata(rm, partitionToDeletedFiles, partitionToAppendedFiles));
    });

    commitRollback(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Restore");
  }

  /**
   * Update from {@code HoodieRollbackMetadata}.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime Timestamp at which the rollback was performed
   */
  void update(HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    processRollbackMetadata(rollbackMetadata, partitionToDeletedFiles, partitionToAppendedFiles);
    commitRollback(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Rollback");
  }

  /**
   * Extracts information about the deleted and append files from the {@code HoodieRollbackMetadata}.
   *
   * During a rollback files may be deleted (COW, MOR) or rollback blocks be appended (MOR only) to files. This
   * function will extract this change file for each partition.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param partitionToDeletedFiles The {@code Map} to fill with files deleted per partition.
   * @param partitionToAppendedFiles The {@code Map} to fill with files appended per partition and their sizes.
   */
  private void processRollbackMetadata(HoodieRollbackMetadata rollbackMetadata,
                                       Map<String, List<String>> partitionToDeletedFiles,
                                       Map<String, Map<String, Long>> partitionToAppendedFiles) {
    rollbackMetadata.getPartitionMetadata().values().forEach(pm -> {
      final String partition = pm.getPartitionPath();

      if (!pm.getSuccessDeleteFiles().isEmpty()) {
        if (!partitionToDeletedFiles.containsKey(partition)) {
          partitionToDeletedFiles.put(partition, new ArrayList<>());
        }

        // Extract deleted file name from the absolute paths saved in getSuccessDeleteFiles()
        List<String> deletedFiles = pm.getSuccessDeleteFiles().stream().map(p -> new Path(p).getName())
            .collect(Collectors.toList());
        partitionToDeletedFiles.get(partition).addAll(deletedFiles);
      }

      if (!pm.getAppendFiles().isEmpty()) {
        if (!partitionToAppendedFiles.containsKey(partition)) {
          partitionToAppendedFiles.put(partition, new HashMap<>());
        }

        // Extract appended file name from the absolute paths saved in getAppendFiles()
        pm.getAppendFiles().forEach((path, size) -> {
          partitionToAppendedFiles.get(partition).merge(new Path(path).getName(), size, (oldSize, newSizeCopy) -> {
            return size + oldSize;
          });
        });
      }
    });
  }

  /**
   * Create file delete records and commit.
   *
   * @param partitionToDeletedFiles {@code Map} of partitions and the deleted files
   * @param instantTime Timestamp at which the deletes took place
   * @param operation Type of the operation which caused the files to be deleted
   */
  private void commitRollback(Map<String, List<String>> partitionToDeletedFiles,
                              Map<String, Map<String, Long>> partitionToAppendedFiles, String instantTime,
                              String operation) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileChangeCount = {0, 0}; // deletes, appends

    partitionToDeletedFiles.forEach((partition, deletedFiles) -> {
      // Rollbacks deletes instants from timeline. The instant being rolled-back may not have been synced to the
      // metadata table. Hence, the deleted filed need to be checked against the metadata.
      try {
        FileStatus[] existingStatuses = getAllFilesInPartition(new Path(datasetBasePath, partition));
        Set<String> currentFiles =
            Arrays.stream(existingStatuses).map(s -> s.getPath().getName()).collect(Collectors.toSet());

        int origCount = deletedFiles.size();
        deletedFiles.removeIf(f -> !currentFiles.contains(f));
        if (deletedFiles.size() != origCount) {
          LOG.warn("Some Files to be deleted as part of " + operation + " at " + instantTime + " were not found in the "
              + " metadata for partition " + partition
              + ". To delete = " + origCount + ", found=" + deletedFiles.size());
        }

        fileChangeCount[0] += deletedFiles.size();

        Option<Map<String, Long>> filesAdded = Option.empty();
        if (partitionToAppendedFiles.containsKey(partition)) {
          filesAdded = Option.of(partitionToAppendedFiles.remove(partition));
        }

        HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, filesAdded,
            Option.of(new ArrayList<>(deletedFiles)));
        records.add(record);
      } catch (IOException e) {
        throw new HoodieMetadataException("Failed to commit rollback deletes at instant " + instantTime, e);
      }
    });

    partitionToAppendedFiles.forEach((partition, appendedFileMap) -> {
      fileChangeCount[1] += appendedFileMap.size();

      // Validate that no appended file has been deleted
      ValidationUtils.checkState(
          !appendedFileMap.keySet().removeAll(partitionToDeletedFiles.getOrDefault(partition, Collections.emptyList())),
            "Rollback file cannot both be appended and deleted");

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.of(appendedFileMap),
          Option.empty());
      records.add(record);
    });

    LOG.info("Updating at " + instantTime + " from " + operation + ". #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileChangeCount[0] + ", #files_appended=" + fileChangeCount[1]);
    commit(prepRecords(records, METADATA_PARTITION_NAME), instantTime, true);
  }

  /**
   * Commit the {@code HoodieRecord}s to Metadata Table as a new delta-commit.
   *
   * @param records The records to commit
   * @param instantTime The timestamp of instant to create
   * @param performUpdate If True, records are upserted. If False, records are inserted
   */
  private synchronized void commit(List<HoodieRecord> records, String instantTime, boolean performUpdate) {
    ValidationUtils.checkState(!readOnly, "Metadata table cannot be committed in readonly mode");
    ValidationUtils.checkArgument(!records.isEmpty());

    // Close all readers
    if (basefileReader != null) {
      basefileReader.close();
      basefileReader = null;
    }
    logRecordScanner = null;

    try (HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config, true)) {
      writeClient.startCommitWithTime(instantTime);
      List<WriteStatus> statuses;
      if (performUpdate) {
        statuses = writeClient.upsert(jsc.parallelize(records), instantTime).collect();
      } else {
        statuses = writeClient.insert(jsc.parallelize(records), instantTime).collect();
      }
      statuses.forEach(writeStatus -> {
        if (writeStatus.hasErrors()) {
          throw new HoodieMetadataException("Failed to commit metadata table records at instant " + instantTime);
        }
      });
    }
  }

  /**
   * Returns a list of all partitions.
   */
  List<String> getAllPartitionPaths() throws IOException {
    final Timer.Context context = this.metrics.getMetadataCtx(LOOKUP_PARTITIONS_STR);

    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getMergedRecordByKey(HoodieMetadataImpl.RECORDKEY_PARTITION_LIST);
    List<String> partitions = Collections.emptyList();
    if (hoodieRecord.isPresent()) {
      if (!hoodieRecord.get().getData().getDeletions().isEmpty()) {
        throw new HoodieMetadataException("Metadata partition list record is inconsistent: "
            + hoodieRecord.get().getData());
      }

      partitions = hoodieRecord.get().getData().getFilenames();
    }

    ++numLookupsAllPartitions;

    long validateTime = 0;
    if (validateLookups) {
      // Validate the Metadata Table data by listing the partitions from the file system
      final Timer.Context contextValidate = this.metrics.getMetadataCtx(VALIDATE_PARTITIONS_STR);
      List<String> actualPartitions  = FSUtils.getAllPartitionPaths(metaClient.getFs(), datasetBasePath, false);

      Collections.sort(actualPartitions);
      Collections.sort(partitions);
      if (!actualPartitions.equals(partitions)) {
        LOG.error("Validation of metadata partition list failed. Lists do not match.");
        LOG.error("Partitions from metadata: " + Arrays.toString(partitions.toArray()));
        LOG.error("Partitions from file system: " + Arrays.toString(actualPartitions.toArray()));

        ++numValidationErrors;
        metrics.updateMetadataMetrics(VALIDATE_ERRORS_STR, 0, numValidationErrors);
      }

      // Return the direct listing as it should be correct
      partitions = actualPartitions;

      if (contextValidate != null) {
        validateTime = metrics.getDurationInMs(contextValidate.stop());
        metrics.updateMetadataMetrics(VALIDATE_PARTITIONS_STR, validateTime, numLookupsAllPartitions);
      }
    }

    if (context != null) {
      long durationInMs = metrics.getDurationInMs(context.stop()) - validateTime;
      metrics.updateMetadataMetrics(LOOKUP_PARTITIONS_STR, durationInMs, numLookupsAllPartitions);
    }

    return partitions;
  }

  /**
   * Return all the files from the partition.
   *
   * @param partitionPath The absolute path of the partition
   */
  FileStatus[] getAllFilesInPartition(Path partitionPath) throws IOException {
    final Timer.Context context = this.metrics.getMetadataCtx(LOOKUP_FILES_STR);

    String partitionName = FSUtils.getRelativePartitionPath(new Path(datasetBasePath), partitionPath);

    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getMergedRecordByKey(partitionName);
    FileStatus[] statuses = {};
    if (hoodieRecord.isPresent()) {
      if (!hoodieRecord.get().getData().getDeletions().isEmpty()) {
        throw new HoodieMetadataException("Metadata record for partition " + partitionName + " is inconsistent: "
              + hoodieRecord.get().getData());
      }
      statuses = hoodieRecord.get().getData().getFileStatuses(partitionPath);
    }

    ++numLookupsFilesInPartition;

    long validateTime = 0;
    if (validateLookups) {
      // Validate the Metadata Table data by listing the partitions from the file system
      final Timer.Context contextValidate = this.metrics.getMetadataCtx(VALIDATE_FILES_STR);
      validateTime = System.currentTimeMillis();
      // Compare the statuses from metadata to those from listing directly from the file system.
      // Ignore partition metadata file
      FileStatus[] directStatuses = metaClient.getFs().listStatus(partitionPath,
          p -> !p.getName().equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));
      List<String> directFilenames = Arrays.stream(directStatuses)
          .map(s -> s.getPath().getName()).collect(Collectors.toList());
      List<String> metadataFilenames = Arrays.stream(statuses)
          .map(s -> s.getPath().getName()).collect(Collectors.toList());

      Collections.sort(metadataFilenames);
      Collections.sort(directFilenames);
      if (!metadataFilenames.equals(directFilenames)) {
        LOG.error("Validation of metadata file listing for partition " + partitionName + " failed.");
        LOG.error("File list from metadata: " + Arrays.toString(metadataFilenames.toArray()));
        LOG.error("File list from direct listing: " + Arrays.toString(directFilenames.toArray()));

        ++numValidationErrors;
        metrics.updateMetadataMetrics(VALIDATE_ERRORS_STR, 0, numValidationErrors);
      }

      // Return the direct listing as it should be correct
      statuses = directStatuses;

      if (contextValidate != null) {
        validateTime = metrics.getDurationInMs(contextValidate.stop());
        metrics.updateMetadataMetrics(VALIDATE_FILES_STR, validateTime, numLookupsFilesInPartition);
      }
    }

    if (context != null) {
      long durationInMs = metrics.getDurationInMs(context.stop()) - validateTime;
      metrics.updateMetadataMetrics(LOOKUP_FILES_STR, durationInMs, numLookupsFilesInPartition);
    }

    return statuses;
  }

  /**
   * Open readers to the base and log files.
   */
  private synchronized void openBaseAndLogFiles() throws IOException {
    if (logRecordScanner != null) {
      // Already opened
      return;
    }

    final Timer.Context context = this.metrics.getMetadataCtx(SCAN_STR);

    // Metadata is in sync till the latest completed instant on the dataset
    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf, datasetBasePath);
    Option<HoodieInstant> datasetLatestInstant = datasetMetaClient.getActiveTimeline().filterCompletedInstants()
        .lastInstant();
    String latestInstantTime = datasetLatestInstant.isPresent() ? datasetLatestInstant.get().getTimestamp()
        : SOLO_COMMIT_TIMESTAMP;

    // Find the latest file slice
    HoodieTimeline timeline = metaClient.reloadActiveTimeline();
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
    List<FileSlice> latestSlices = fsView.getLatestFileSlices(METADATA_PARTITION_NAME).collect(Collectors.toList());
    ValidationUtils.checkArgument(latestSlices.size() == 1);

    // If the base file is present then create a reader
    Option<HoodieBaseFile> basefile = latestSlices.get(0).getBaseFile();
    if (basefile.isPresent()) {
      String basefilePath = basefile.get().getPath();
      basefileReader = HoodieFileReaderFactory.getFileReader(hadoopConf, new Path(basefilePath));

      LOG.info("Opened metadata base file from " + basefilePath + " at instant " + basefile.get().getCommitTime());
    }

    // Open the log record scanner using the log files from the latest file slice
    List<String> logFilePaths = latestSlices.get(0).getLogFiles().map(o -> o.getPath().toString())
        .collect(Collectors.toList());

    Option<HoodieInstant> lastInstant = timeline.filterCompletedInstants().lastInstant();
    String latestMetaInstantTimestamp = lastInstant.isPresent() ? lastInstant.get().getTimestamp()
        : SOLO_COMMIT_TIMESTAMP;
    if (!HoodieTimeline.compareTimestamps(latestInstantTime, HoodieTimeline.EQUALS, latestMetaInstantTimestamp)) {
      // TODO: This can be false positive if the metadata table had a compaction or clean
      LOG.warn("Metadata has more recent instant " + latestMetaInstantTimestamp + " than dataset " + latestInstantTime);
    }

    // TODO: The below code may open the metadata to include incomplete instants on the dataset
    logRecordScanner =
        new HoodieMetadataMergedLogRecordScanner(FSUtils.getFs(datasetBasePath, hadoopConf), metadataBasePath,
            logFilePaths, schema, latestMetaInstantTimestamp, maxMemorySizeInBytes, bufferSize,
            config.getSpillableMapBasePath(), null);

    LOG.info("Opened metadata log files from " + logFilePaths + " at instant " + latestInstantTime
        + "(dataset instant=" + latestInstantTime + ", metadata instant=" + latestMetaInstantTimestamp + ")");

    ++numScans;

    if (context != null) {
      long durationInMs = metrics.getDurationInMs(context.stop());
      metrics.updateMetadataMetrics(SCAN_STR, durationInMs, numScans);
    }
  }

  /**
   * Retrieve the merged {@code HoodieRecord} mapped to the given key.
   *
   * @param key The key of the record
   */
  private Option<HoodieRecord<HoodieMetadataPayload>> getMergedRecordByKey(String key) throws IOException {
    openBaseAndLogFiles();

    // Retrieve record from base file
    HoodieRecord<HoodieMetadataPayload> hoodieRecord = null;
    if (basefileReader != null) {
      Option<GenericRecord> baseRecord = basefileReader.getRecordByKey(key);
      if (baseRecord.isPresent()) {
        hoodieRecord = SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) baseRecord.get(),
            metaClient.getTableConfig().getPayloadClass());
      }
    }

    // Retrieve record from log file
    Option<HoodieRecord<HoodieMetadataPayload>> logHoodieRecord = logRecordScanner.getRecordByKey(key);
    if (logHoodieRecord.isPresent()) {
      if (hoodieRecord != null) {
        // Merge the payloads
        HoodieRecordPayload mergedPayload = hoodieRecord.getData().preCombine(logHoodieRecord.get().getData());
        hoodieRecord = new HoodieRecord(hoodieRecord.getKey(), mergedPayload);
      } else {
        hoodieRecord = logHoodieRecord.get();
      }
    }

    return Option.ofNullable(hoodieRecord);
  }

  /**
   * Return {@code True} if all Instants from the dataset have been synced with the Metadata Table.
   */
  boolean isInSync() {
    // There should not be any instants to sync
    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf, datasetBasePath);
    List<HoodieInstant> instantsToSync = findInstantsToSync(datasetMetaClient);
    return instantsToSync.isEmpty();
  }

  /**
   * Return the timestamp of the latest compaction instant.
   */
  Option<String> getLatestCompactionTimestamp() {
    HoodieTimeline timeline = metaClient.reloadActiveTimeline();
    Option<HoodieInstant> lastCompactionInstant = timeline.filterCompletedInstants()
        .filter(i -> i.getAction().equals(HoodieTimeline.COMMIT_ACTION)).lastInstant();

    if (lastCompactionInstant.isPresent()) {
      return Option.of(lastCompactionInstant.get().getTimestamp());
    } else {
      return Option.empty();
    }
  }

  public Map<String, String> getStats(boolean detailed) throws IOException {
    Map<String, String> stats = new HashMap<>();
    FileSystem fs = FSUtils.getFs(metadataBasePath, hadoopConf);

    // Total size of the metadata and count of base/log files
    long totalBaseFileSizeInBytes = 0;
    long totalLogFileSizeInBytes = 0;
    int baseFileCount = 0;
    int logFileCount = 0;
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
    List<FileSlice> latestSlices = fsView.getLatestFileSlices(METADATA_PARTITION_NAME).collect(Collectors.toList());

    for (FileSlice slice : latestSlices) {
      if (slice.getBaseFile().isPresent()) {
        totalBaseFileSizeInBytes += fs.getFileStatus(new Path(slice.getBaseFile().get().getPath())).getLen();
        ++baseFileCount;
      }
      Iterator<HoodieLogFile> it = slice.getLogFiles().iterator();
      while (it.hasNext()) {
        totalLogFileSizeInBytes += fs.getFileStatus(it.next().getPath()).getLen();
        ++logFileCount;
      }
    }

    stats.put(STAT_TOTAL_BASE_FILE_SIZE, String.valueOf(totalBaseFileSizeInBytes));
    stats.put(STAT_TOTAL_LOG_FILE_SIZE, String.valueOf(totalLogFileSizeInBytes));
    stats.put(STAT_COUNT_BASE_FILES, String.valueOf(baseFileCount));
    stats.put(STAT_COUNT_LOG_FILES, String.valueOf(logFileCount));

    if (detailed) {
      stats.put(STAT_COUNT_PARTITION, String.valueOf(getAllPartitionPaths().size()));
      stats.put(STAT_IN_SYNC, String.valueOf(isInSync()));
      stats.put(STAT_LAST_COMPACTION_TIMESTAMP, getLatestCompactionTimestamp().orElseGet(() -> "none"));
    }

    return stats;
  }
}
