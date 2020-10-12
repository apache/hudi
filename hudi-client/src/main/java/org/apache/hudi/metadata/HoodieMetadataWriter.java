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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Writer for Metadata Table.
 *
 * Partition and file listing are saved within an internal MOR table called Metadata Table. This table is created
 * by listing files and partitions (first time) and kept in sync using the instants on the main dataset.
 */
public class HoodieMetadataWriter extends HoodieMetadataReader implements Serializable {
  private static final Logger LOG = LogManager.getLogger(HoodieMetadataWriter.class);

  // Metric names
  public static final String INITIALIZE_STR = "initialize";
  public static final String SYNC_STR = "sync";

  private HoodieWriteConfig config;
  private String tableName;
  private static Map<String, HoodieMetadataWriter> instances = new HashMap<>();

  public static HoodieMetadataWriter instance(Configuration conf, HoodieWriteConfig writeConfig) {
    try {
      return new HoodieMetadataWriter(conf, writeConfig);
    } catch (IOException e) {
      throw new HoodieMetadataException("Could not initialize HoodieMetadataWriter", e);
    }
    /*
    return instances.computeIfAbsent(writeConfig.getBasePath(), k -> {
      try {
        return new HoodieMetadataWriter(conf, writeConfig);
      } catch (IOException e) {
        throw new HoodieMetadataException("Could not initialize HoodieMetadataWriter", e);
      }
    });
    */
  }

  HoodieMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig) throws IOException {
    super(hadoopConf, writeConfig.getBasePath(), writeConfig.getSpillableMapBasePath(),
        writeConfig.useFileListingMetadata(), writeConfig.getFileListingMetadataVerify());

    if (writeConfig.useFileListingMetadata()) {
      this.tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;
      this.config = createMetadataWriteConfig(writeConfig);
      enabled = true;

      // Inline compaction and auto clean is required as we dont expose this table outside
      ValidationUtils.checkArgument(this.config.isAutoClean(), "Auto clean is required for Metadata Compaction config");
      ValidationUtils.checkArgument(this.config.isInlineCompaction(), "Inline compaction is required for Metadata Compaction config");
      // Metadata Table cannot have its metadata optimized
      ValidationUtils.checkArgument(this.config.shouldAutoCommit(), "Auto commit is required for Metadata Table");
      ValidationUtils.checkArgument(!this.config.useFileListingMetadata(), "File listing cannot be used for Metadata Table");
    } else {
      enabled = false;
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
   */
  public void reload(JavaSparkContext jsc) throws IOException {
    if (enabled) {
      initialize(jsc);
    }
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
  public void initialize(JavaSparkContext jsc) {
    try {
      if (enabled) {
        initializeAndSync(jsc);
      }
    } catch (IOException e) {
      LOG.error("Failed to initialize metadata table. Metdata will be disabled.", e);
      enabled = false;
    }
  }

  private void initializeAndSync(JavaSparkContext jsc) throws IOException {
    long t1 = System.currentTimeMillis();

    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf.get(), datasetBasePath);
    FileSystem fs = FSUtils.getFs(metadataBasePath, hadoopConf.get());
    boolean exists = fs.exists(new Path(metadataBasePath, HoodieTableMetaClient.METAFOLDER_NAME));

    if (!exists) {
      // Initialize for the first time by listing partitions and files directly from the file system
      initFromFilesystem(jsc, datasetMetaClient);
    } else {
      metaClient = ClientUtils.createMetaClient(hadoopConf.get(), config, true);
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
    syncFromInstants(jsc, datasetMetaClient);

    // Publish some metrics
    long durationInMs = System.currentTimeMillis() - t1;
    // Time to initilize and sync
    if (exists) {
      updateMetrics(SYNC_STR, durationInMs);
    } else {
      updateMetrics(INITIALIZE_STR, durationInMs);
    }

    // Total size of the metadata and count of base/log files
    Map<String, String> stats = getStats(false);
    updateMetrics(Long.valueOf(stats.get(STAT_TOTAL_BASE_FILE_SIZE)),
        Long.valueOf(stats.get(STAT_TOTAL_LOG_FILE_SIZE)), Integer.valueOf(stats.get(STAT_COUNT_BASE_FILES)),
        Integer.valueOf(stats.get(STAT_COUNT_LOG_FILES)));
  }

  /**
   * Initialize the Metadata Table by listing files and partitions from the file system.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  private void initFromFilesystem(JavaSparkContext jsc, HoodieTableMetaClient datasetMetaClient) throws IOException {
    ValidationUtils.checkState(enabled, "Metadata table cannot be initialized as it is not enabled");

    // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
    Option<HoodieInstant> latestInstant = datasetMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String createInstantTime = latestInstant.isPresent() ? latestInstant.get().getTimestamp() : SOLO_COMMIT_TIMESTAMP;

    LOG.info("Creating a new metadata table in " + metadataBasePath + " at instant " + createInstantTime);
    metaClient = HoodieTableMetaClient.initTableType(hadoopConf.get(), metadataBasePath.toString(),
        HoodieTableType.MERGE_ON_READ, tableName, "archived", HoodieMetadataPayload.class.getName(),
        HoodieFileFormat.HFILE.toString());

    // List all partitions in the basePath of the containing dataset
    FileSystem fs = FSUtils.getFs(datasetBasePath, hadoopConf.get());
    List<String> partitions = FSUtils.getAllPartitionPaths(fs, datasetBasePath, false);
    LOG.info("Initializing metadata table by using file listings in " + partitions.size() + " partitions");

    // List all partitions in parallel and collect the files in them
    final String dbasePath = datasetBasePath;
    final SerializableConfiguration serializedConf = new SerializableConfiguration(hadoopConf);
    int parallelism =  Math.min(partitions.size(), jsc.defaultParallelism()) + 1; // +1 to prevent 0 parallelism
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
    update(jsc, metadata, createInstantTime);
  }

  /**
   * Sync the Metadata Table from the instants created on the dataset.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  private void syncFromInstants(JavaSparkContext jsc, HoodieTableMetaClient datasetMetaClient) throws IOException {
    ValidationUtils.checkState(enabled, "Metadata table cannot be synced as it is not enabled");

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
            update(jsc, cleanerPlan, instant.getTimestamp());
          } catch (HoodieIOException e) {
            HoodieInstant cleanInstant = new HoodieInstant(false, instant.getAction(), instant.getTimestamp());
            ValidationUtils.checkArgument(cleanInstant.isCompleted());
            HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(datasetMetaClient, cleanInstant);
            update(jsc, cleanMetadata, instant.getTimestamp());
          }
          break;
        }
        case HoodieTimeline.DELTA_COMMIT_ACTION:
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.COMPACTION_ACTION: {
          ValidationUtils.checkArgument(instant.isCompleted());
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
              timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          update(jsc, commitMetadata, instant.getTimestamp());
          break;
        }
        case HoodieTimeline.ROLLBACK_ACTION: {
          ValidationUtils.checkArgument(instant.isCompleted());
          HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
              timeline.getInstantDetails(instant).get());
          update(jsc, rollbackMetadata, instant.getTimestamp());
          break;
        }
        case HoodieTimeline.RESTORE_ACTION: {
          ValidationUtils.checkArgument(instant.isCompleted());
          HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
              timeline.getInstantDetails(instant).get());
          update(jsc, restoreMetadata, instant.getTimestamp());
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
   * Update from {@code HoodieCommitMetadata}.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime Timestamp at which the commit was performed
   */
  public void update(JavaSparkContext jsc, HoodieCommitMetadata commitMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    List<HoodieRecord> records = new LinkedList<>();
    List<String> allPartitions = new LinkedList<>();
    commitMetadata.getPartitionToWriteStats().forEach((partitionStatName, writeStats) -> {
      final String partition = partitionStatName.equals("") ? NON_PARTITIONED_NAME : partitionStatName;
      allPartitions.add(partition);

      Map<String, Long> newFiles = new HashMap<>(writeStats.size());
      writeStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        if (pathWithPartition == null) {
          // Empty partition
          return;
        }

        int offset = partition.equals(NON_PARTITIONED_NAME) ? 0 : partition.length() + 1;
        String filename = pathWithPartition.substring(offset);
        ValidationUtils.checkState(!newFiles.containsKey(filename), "Duplicate files in HoodieCommitMetadata");
        newFiles.put(filename, hoodieWriteStat.getTotalWriteBytes());
      });

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(
          partition, Option.of(newFiles), Option.empty());
      records.add(record);
    });

    // New partitions created
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(new ArrayList<String>(allPartitions));
    records.add(record);

    LOG.info("Updating at " + instantTime + " from Commit/" + commitMetadata.getOperationType()
        + ". #partitions_updated=" + records.size());
    commit(jsc, prepRecords(jsc, records, METADATA_PARTITION_NAME), instantTime);
  }

  /**
   * Update from {@code HoodieCleanerPlan}.
   *
   * @param cleanerPlan {@code HoodieCleanerPlan}
   * @param instantTime Timestamp at which the clean plan was generated
   */
  public void update(JavaSparkContext jsc, HoodieCleanerPlan cleanerPlan, String instantTime) {
    if (!enabled) {
      return;
    }

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
    commit(jsc, prepRecords(jsc, records, METADATA_PARTITION_NAME), instantTime);
  }

  /**
   * Update from {@code HoodieCleanMetadata}.
   *
   * @param cleanMetadata {@code HoodieCleanMetadata}
   * @param instantTime Timestamp at which the clean was completed
   */
  public void update(JavaSparkContext jsc, HoodieCleanMetadata cleanMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

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
    commit(jsc, prepRecords(jsc, records, METADATA_PARTITION_NAME), instantTime);
  }

  /**
   * Update from {@code HoodieRestoreMetadata}.
   *
   * @param restoreMetadata {@code HoodieRestoreMetadata}
   * @param instantTime Timestamp at which the restore was performed
   */
  public void update(JavaSparkContext jsc, HoodieRestoreMetadata restoreMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
      rms.forEach(rm -> processRollbackMetadata(rm, partitionToDeletedFiles, partitionToAppendedFiles));
    });
    commitRollback(jsc, partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Restore");
  }

  /**
   * Update from {@code HoodieRollbackMetadata}.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime Timestamp at which the rollback was performed
   */
  public void update(JavaSparkContext jsc, HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    processRollbackMetadata(rollbackMetadata, partitionToDeletedFiles, partitionToAppendedFiles);
    commitRollback(jsc, partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Rollback");
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
  private void commitRollback(JavaSparkContext jsc, Map<String, List<String>> partitionToDeletedFiles,
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
    commit(jsc, prepRecords(jsc, records, METADATA_PARTITION_NAME), instantTime);
  }

  /**
   * Commit the {@code HoodieRecord}s to Metadata Table as a new delta-commit.
   *
   * @param recordRDD The records to commit
   * @param instantTime The timestamp of instant to create
   */
  private synchronized void commit(JavaSparkContext jsc, JavaRDD<HoodieRecord> recordRDD, String instantTime) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be committed to as it is not enabled");

    closeReaders();

    try (HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config, true)) {
      writeClient.startCommitWithTime(instantTime);
      List<WriteStatus> statuses = writeClient.upsertPreppedRecords(recordRDD, instantTime).collect();
      statuses.forEach(writeStatus -> {
        if (writeStatus.hasErrors()) {
          throw new HoodieMetadataException("Failed to commit metadata table records at instant " + instantTime);
        }
      });
    }

    // Update total size of the metadata and count of base/log files
    Map<String, String> stats;
    try {
      stats = getStats(false);
      updateMetrics(Long.valueOf(stats.get(STAT_TOTAL_BASE_FILE_SIZE)),
          Long.valueOf(stats.get(STAT_TOTAL_LOG_FILE_SIZE)), Integer.valueOf(stats.get(STAT_COUNT_BASE_FILES)),
          Integer.valueOf(stats.get(STAT_COUNT_LOG_FILES)));
    } catch (IOException e) {
      LOG.error("Could not publish metadata size metrics", e);
    }
  }

  /**
   * Tag each record with the location.
   *
   * Since we only read the latest base file in a partition, we tag the records with the instant time of the latest
   * base file.
   */
  private JavaRDD<HoodieRecord> prepRecords(JavaSparkContext jsc, List<HoodieRecord> records, String partitionName) {
    HoodieTable table = HoodieTable.create(metaClient, config, hadoopConf.get());
    SliceView fsView = table.getSliceView();
    List<HoodieBaseFile> baseFiles = fsView.getLatestFileSlices(partitionName)
        .map(s -> s.getBaseFile())
        .filter(b -> b.isPresent())
        .map(b -> b.get())
        .collect(Collectors.toList());

    // All the metadata fits within a single base file
    if (partitionName.equals(METADATA_PARTITION_NAME)) {
      if (baseFiles.size() > 1) {
        throw new HoodieMetadataException("Multiple base files found in metadata partition");
      }
    }

    String fileId;
    String instantTime;
    if (!baseFiles.isEmpty()) {
      fileId = baseFiles.get(0).getFileId();
      instantTime = baseFiles.get(0).getCommitTime();
    } else {
      // If there is a log file then we can assume that it has the data
      List<HoodieLogFile> logFiles = fsView.getLatestFileSlices(HoodieMetadataWriter.METADATA_PARTITION_NAME)
          .map(s -> s.getLatestLogFile())
          .filter(b -> b.isPresent())
          .map(b -> b.get())
          .collect(Collectors.toList());
      if (logFiles.isEmpty()) {
        // No base and log files. All are new inserts
        return jsc.parallelize(records, 1);
      }

      fileId = logFiles.get(0).getFileId();
      instantTime = logFiles.get(0).getBaseCommitTime();
    }

    return jsc.parallelize(records, 1).map(r -> r.setCurrentLocation(new HoodieRecordLocation(instantTime, fileId)));
  }
}
