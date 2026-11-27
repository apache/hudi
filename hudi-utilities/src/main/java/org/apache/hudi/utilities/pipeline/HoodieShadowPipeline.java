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

package org.apache.hudi.utilities.pipeline;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfigHolder;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.sources.HoodieIncrSource;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.LAZY;
import static org.apache.hudi.common.model.WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL;
import static org.apache.hudi.hive.ddl.HiveSyncMode.HIVEQL;

/**
 * A tool that creates and incrementally maintains a "shadow" copy of a source Hudi (COW) table.
 *
 * <p>The destination table is bootstrapped by copying the source base files (with checksum
 * verification) and constructing a matching commit, after which it is kept in sync with the
 * source using {@link HoodieDeltaStreamer} backed by {@link HoodieIncrSource}. The run is guarded
 * by a Zookeeper-based lock so multiple invocations against the same destination cannot conflict.
 */
public class HoodieShadowPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieShadowPipeline.class);
  private SparkSession sparkSession;

  private SparkSession getOrCreateSparkSession() {
    if (sparkSession == null) {
      sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate();
    }
    return sparkSession;
  }

  private static void cleanDestination(SparkSession sparkSession, HoodieShadowPipelineConfig cfg,
                                       Configuration hadoopConf) {
    try {
      LOG.info("Cleaning destination path " + cfg.destPath);
      FileSystem fs = new Path(cfg.destPath).getFileSystem(hadoopConf);
      sparkSession.sql("DROP TABLE IF EXISTS " + cfg.hiveDatabase + "." + cfg.hiveTable);
      if (cfg.destTableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
        sparkSession.sql("DROP TABLE IF EXISTS " + cfg.hiveDatabase + "." + cfg.hiveTable + "_ro");
        sparkSession.sql("DROP TABLE IF EXISTS " + cfg.hiveDatabase + "." + cfg.hiveTable + "_rt");
      }
      fs.delete(new Path(cfg.destPath), true);
    } catch (Exception e) {
      LOG.error("Could not clean dataset with path " + cfg.destPath + " and table " + cfg.hiveDatabase + "." + cfg.hiveTable, e);
      throw new HoodieException("Could not clean dataset with path " + cfg.destPath + " and table " + cfg.hiveDatabase + "." + cfg.hiveTable, e);
    }
  }

  private static HoodieTableMetaClient initializeDataset(JavaSparkContext jssc, HoodieShadowPipelineConfig cfg,
                                                         HoodieTableMetaClient srcMetaClient,
                                                         TypedProperties props) throws Exception {
    // Create a new HUDI dataset
    LOG.info("Creating HUDI dataset at destination path " + cfg.destPath);
    HoodieTableMetaClient destMetaClient;
    HoodieTableMetaClient.TableBuilder tableBuilder;
    if (cfg.reuseHoodiePropertiesFileFromSrc) {
      Properties srcTableProperties = srcMetaClient.getTableConfig().getProps();
      Stream.of(HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(), HoodieTableConfig.NAME.key(),
              HoodieTableConfig.DATABASE_NAME.key(), HoodieTableConfig.VERSION.key())
          .forEach(srcTableProperties::remove);
      tableBuilder = HoodieTableMetaClient.newTableBuilder().fromMetaClient(srcMetaClient)
          .setDatabaseName(cfg.hiveDatabase)
          .setTableName(getTargetTableName(cfg))
          .fromProperties(srcTableProperties)
          .setTableType(HoodieTableType.valueOf(cfg.destTableType));
    } else {
      tableBuilder = HoodieTableMetaClient.newTableBuilder()
              .setDatabaseName(cfg.hiveDatabase)
              .setTableName(getTargetTableName(cfg))
              .setBaseFileFormat(srcMetaClient.getTableConfig().getBaseFileFormat().name())
              .setTableType(HoodieTableType.valueOf(cfg.destTableType))
              .setArchiveLogFolder(HoodieTableConfig.ARCHIVELOG_FOLDER.defaultValue())
              .setPartitionFields(cfg.partitionColumns)
              .setRecordKeyFields(cfg.recordKeyColumn)
              .setPopulateMetaFields(cfg.writeMetaFields)
              .setKeyGeneratorClassProp(cfg.keyGenerator)
              .setOrderingFields(cfg.sourceOrderingField);
      if (cfg.destTableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
        tableBuilder.setPayloadClassName(cfg.destPayloadClassName);
      }
    }
    destMetaClient = tableBuilder.initTable(HadoopFSUtils.getStorageConf(jssc.hadoopConfiguration()), cfg.destPath);

    // Find the partitions in source dataset
    HoodieSparkEngineContext sparkEngineContext = new HoodieSparkEngineContext(jssc);
    List<String> partitions = FSUtils.getAllPartitionPaths(sparkEngineContext, srcMetaClient, false);
    Collections.sort(partitions);

    // Apply partition filtering. As of now, only supports string's compareTo function for checking
    // if the partition fails between startPartition and endPartition.
    if (!cfg.startPartition.isEmpty()) {
      partitions = partitions.stream().filter(p -> p.compareTo(cfg.startPartition) >= 0).collect(Collectors.toList());
    }
    if (!cfg.endPartition.isEmpty()) {
      partitions = partitions.stream().filter(p -> p.compareTo(cfg.endPartition) <= 0).collect(Collectors.toList());
    }
    if (!cfg.selectedPartitions.isEmpty()) {
      partitions = partitions.stream().filter(p -> cfg.selectedPartitions.contains(p)).collect(Collectors.toList());
    }

    // Ignore . partitions that are not part of HUDI
    partitions = partitions.stream().filter(p -> !p.startsWith(".")).collect(Collectors.toList());

    HoodieWriteConfig srcConfig = HoodieWriteConfig
        .newBuilder()
        .withPath(cfg.srcPath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    final HoodieTable srcTable = HoodieSparkTable.create(srcConfig, sparkEngineContext, srcMetaClient);

    // Commit time will be the latest time on the source dataset
    final String srcCommitTime = StringUtils.isNullOrEmpty(cfg.sourceInstantTime)
        ? srcMetaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get().requestedTime()
        : cfg.sourceInstantTime;
    LOG.info(String.format("Source table's instant time is %s", srcCommitTime));

    // This step keeps the timestamps in latest format (msec granularity) even if the src dataset is of older version
    // with timestamps in second granularity. This is essential to support use-cases like RECORD_INDEX which do not work
    // correctly with mix order timestamps.
    final String destCommitTime = HoodieInstantTimeGenerator.formatDate(HoodieInstantTimeGenerator.parseDateFromInstantTime(srcCommitTime));

    // Create partition and collect list of latest file slices in them
    long[] totalSize = {0};
    LOG.info("Total partitions seen " + partitions.size());
    if (partitions.size() > 0) {
      LOG.info("Min partition value: " + partitions.get(0) + ", max partition value: " + partitions.get(partitions.size() - 1));
    }

    // Create partition paths and construct partitionToFilePairs in parallel.
    sparkEngineContext.setJobStatus(HoodieShadowPipeline.class.getSimpleName(), "Fetching all partition to file pairs");
    HoodieFileFormat fileFormat = HoodieFileFormat.valueOf(cfg.baseFileFormat);
    JavaRDD<Pair<String, String>> partitionFilePairs = jssc.parallelize(partitions, partitions.size())
        .flatMap(partitionStr -> {
          List<Pair<String, String>> partitionToFilePairs = new LinkedList<>();
          SyncableFileSystemView sliceView = (SyncableFileSystemView) srcTable.getSliceView();
          LOG.info("Creating partition " + partitionStr);
          final Option<HoodieFileFormat> partitionMetadataFileFormat;
          if (cfg.createPartitionMetafileWithoutSuffix) {
            partitionMetadataFileFormat = Option.empty();
          } else {
            partitionMetadataFileFormat = Option.of(fileFormat);
          }
          HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(
              destMetaClient.getStorage(), destCommitTime,
              new StoragePath(cfg.destPath), new StoragePath(cfg.destPath, partitionStr),
              partitionMetadataFileFormat);
          partitionMetadata.trySave();
          Stream<HoodieBaseFile> baseFileStream = cfg.bootstrapWithLatestBaseFiles
              ? sliceView.getLatestBaseFilesBeforeOrOn(partitionStr, srcCommitTime)
              : sliceView.getAllBaseFiles(partitionStr);
          Stream<HoodieBaseFile> filteredBaseFileStream = baseFileStream
              .filter(baseFile -> InstantComparison.compareTimestamps(baseFile.getCommitTime(),
                  InstantComparison.LESSER_THAN_OR_EQUALS, srcCommitTime));
          if (!StringUtils.isNullOrEmpty(cfg.maxFilesPerPartition)) {
            filteredBaseFileStream = filteredBaseFileStream
                .limit(Integer.parseInt(cfg.maxFilesPerPartition));
          }
          filteredBaseFileStream
              .forEach(b -> {
                partitionToFilePairs.add(Pair.of(partitionStr, b.getFileName()));
                totalSize[0] += b.getFileSize();
              });
          return partitionToFilePairs.iterator();
        });

    // Copy all files in parallel to the destination
    int partitionFilePairsCount = (int) partitionFilePairs.count();
    int copyParallelism = Math.min(partitionFilePairsCount, 100000);
    LOG.info(String.format("Copying %d files with total size %d", partitionFilePairsCount, totalSize[0]));
    HadoopStorageConfiguration serializableHadoopConf = new HadoopStorageConfiguration(jssc.hadoopConfiguration());
    sparkEngineContext.setJobStatus(HoodieShadowPipeline.class.getSimpleName(), "Copying all data files");
    JavaRDD<Pair<Boolean, Pair<String, String>>> statusesRdd = partitionFilePairs
        .repartition(copyParallelism)
        .map(partitionFilePair -> {
          final Path srcPath = new Path(cfg.srcPath + Path.SEPARATOR + partitionFilePair.getKey(), partitionFilePair.getValue());
          final Path destPath = new Path(cfg.destPath + Path.SEPARATOR + partitionFilePair.getKey(), partitionFilePair.getValue());
          int count = 0;
          while (count < 3) {
            try {
              count++;
              boolean checksumFlag = copyFileWithChecksum(serializableHadoopConf, srcPath, destPath);
              if (checksumFlag) {
                return Pair.of(true, Pair.of(partitionFilePair.getKey(), partitionFilePair.getValue()));
              }
            } catch (Exception e) {
              LOG.error("Exception in copying file " + Path.SEPARATOR + partitionFilePair.getKey()
                  + "/" +  partitionFilePair.getValue(), e);
            }
          }
          return Pair.of(false, Pair.of(partitionFilePair.getKey(), partitionFilePair.getValue()));
        }).filter(partitionFilePairStatus -> !partitionFilePairStatus.getKey());

    List<Pair<Boolean, Pair<String, String>>> statuses = statusesRdd.collect();
    LOG.info("Data file copy completed. No. of files failed to copy or failed in checksum " + statuses.size());

    // Print top 3 files that failed to copy.
    statuses = statuses.stream().limit(3).collect(Collectors.toList());
    List<String> top3FilesFailedToCopy = new ArrayList<>();
    for (Pair<Boolean, Pair<String, String>> statusPair: statuses) {
      LOG.info(String.format("Failed to fully copy partition %s and its file %s", statusPair.getKey(), statusPair.getValue()));
      top3FilesFailedToCopy.add(statusPair.getKey() + Path.SEPARATOR + statusPair.getValue());
    }
    if (statuses.size() > 0) {
      throw new HoodieException("Failed to copy total " + statuses.size() + "files. Top 3 files failed are " + top3FilesFailedToCopy);
    }

    String sourceCommitPattern = null;
    if (cfg.useSourceCommitDuringInitialization) {
      LOG.info("Copying the last commit instant directly from the source table");
      sourceCommitPattern = cfg.srcPath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME
          + Path.SEPARATOR + srcCommitTime + "*";
    } else if (cfg.useSourceTimelineDuringInitialization) {
      LOG.info("Copying the entire timeline directly from the source table");
      sourceCommitPattern = cfg.srcPath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME
          + Path.SEPARATOR + "2*";
    }

    FileSystem srcfs = HadoopFSUtils.getFs(cfg.srcPath, jssc.hadoopConfiguration());
    if (!StringUtils.isNullOrEmpty(sourceCommitPattern)) {
      LOG.info("Source commit pattern " + sourceCommitPattern);
      List<FileStatus> commitFileStatuses = Stream.of(srcfs.globStatus(new Path(sourceCommitPattern)))
              // Skip atomic-rename temp files (e.g. <ts>.commit.requested.tmp) — HoodieInstant's
              // state parser splits on '.' and throws IllegalArgumentException on "REQUESTED.TMP".
              .filter(fileStatus -> !fileStatus.getPath().getName().endsWith(".tmp"))
              .filter(fileStatus -> {
                // Extract timestamp from filename (e.g. "20230101120000.commit" -> "20230101120000")
                String fileName = fileStatus.getPath().getName();
                String timestamp = fileName.contains(".") ? fileName.substring(0, fileName.indexOf(".")) : fileName;
                return timestamp.compareTo(srcCommitTime) <= 0;
              }).collect(Collectors.toList());
      sparkEngineContext.setJobStatus(HoodieShadowPipeline.class.getSimpleName(), "Copying commit files");
      List<String> commitFilesFailedToCopy = sparkEngineContext.parallelize(commitFileStatuses)
          .repartition(64)
          .mapPartitions(commitFileStatusesList -> {
            List<String> failedToCopy = new ArrayList<>();
            commitFileStatusesList.forEachRemaining(fileStatus -> {
              Path srcPath = fileStatus.getPath();
              String instantFileName = fileStatus.getPath().getName();
              Path timelinePath = HadoopFSUtils.convertToHadoopPath(destMetaClient.getTimelinePath());
              Path destPath = new Path(timelinePath, instantFileName);
              int retries = 0;
              boolean checksumVerifiedCopyCreated = false;
              while (!checksumVerifiedCopyCreated && retries < 3) {
                try {
                  retries++;
                  checksumVerifiedCopyCreated = copyFileWithChecksum(serializableHadoopConf, srcPath, destPath);
                  ValidationUtils.checkState(checksumVerifiedCopyCreated, "Checksum failed while copying file " + srcPath);
                } catch (Exception e) {
                  LOG.error("Exception in copying commit file " + srcPath, e);
                }
              }
              if (!checksumVerifiedCopyCreated) {
                failedToCopy.add(srcPath.getName());
              }
            });
            return failedToCopy.iterator();
          }, true).collectAsList();
      LOG.info("Commit files failed to copy " + commitFilesFailedToCopy);
    } else {
      LOG.info("Creating a new commit instance at " + destCommitTime);
      // Commit metadata which has details of all the files added
      HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
      commitMetadata.setOperationType(WriteOperationType.INSERT);
      // Set the checkpoint key so that deltastreamer can start syncing from this commit onwards
      commitMetadata.addMetadata(HoodieStreamer.CHECKPOINT_KEY, srcCommitTime);
      // Set the schema from the source table so that schema resolution works on the dest table
      TableSchemaResolver srcSchemaResolver = new TableSchemaResolver(srcMetaClient);
      commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY,
          srcSchemaResolver.getTableSchema(false).toString());
      partitionFilePairs.collect().forEach(partitionFilePair -> {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPartitionPath(partitionFilePair.getKey());
        writeStat.setFileId(FSUtils.getFileId(partitionFilePair.getValue()));
        writeStat.setPrevCommit("");
        writeStat.setPath(partitionFilePair.getKey() + Path.SEPARATOR + partitionFilePair.getValue());
        commitMetadata.addWriteStat(partitionFilePair.getKey(), writeStat);
      });

      // Create a commit
      final HoodieInstant requestedInstant = destMetaClient.createNewInstant(
          HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, destCommitTime);
      destMetaClient.getActiveTimeline().createNewInstant(requestedInstant);
      destMetaClient.getActiveTimeline().transitionRequestedToInflight(requestedInstant, Option.empty());
      HoodieInstant inflightInstant = destMetaClient.createNewInstant(
          HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, destCommitTime);
      destMetaClient.getActiveTimeline().saveAsComplete(inflightInstant,
          Option.of(commitMetadata));
    }

    // add metadata checkpoint update
    if (cfg.enableDeltastreamerCheckpoint) {
      // Step 1: load commit metadata from last completed commit.
      HoodieInstant latestInstant = destMetaClient.reloadActiveTimeline().filterCompletedInstants().lastInstant().get();
      byte[] instantDetails = destMetaClient.getActiveTimeline().getInstantDetails(latestInstant).get();
      HoodieCommitMetadata commitMetadata = destMetaClient.getCommitMetadataSerDe().deserialize(latestInstant,
          new java.io.ByteArrayInputStream(instantDetails), () -> instantDetails.length == 0, HoodieCommitMetadata.class);
      // Step 2: Delete the completed instant.
      destMetaClient.getActiveTimeline().deleteInstantFileIfExists(latestInstant);
      // Step 3: Reload the metaclient.
      destMetaClient.reloadActiveTimeline();
      destMetaClient.reloadTableConfig();
      // Step 4: Update commitmetadata with deltastreamer checkpoint key.
      commitMetadata.addMetadata(HoodieStreamer.CHECKPOINT_KEY, destCommitTime);
      // Step 5: Create completed commit by using the modified commit metadata.
      HoodieInstant inflightInstant = destMetaClient.createNewInstant(
          HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, srcCommitTime);
      destMetaClient.getActiveTimeline().saveAsComplete(inflightInstant,
          Option.of(commitMetadata));
    }

    // Reload metaclient.
    destMetaClient.reloadActiveTimeline();
    destMetaClient.reloadTableConfig();

    // Bootstraps destination table.
    bootstrapMetadataTable(destMetaClient, sparkEngineContext, cfg, props);

    if (cfg.enableHiveSync) {
      runHiveSync(jssc, destMetaClient, cfg);
    }

    return destMetaClient;
  }

  private static boolean copyFileWithChecksum(HadoopStorageConfiguration serializableHadoopConf, Path srcPath, Path destPath) throws IOException {
    FileSystem srcFs = srcPath.getFileSystem(serializableHadoopConf.unwrap());
    FileSystem destFs = destPath.getFileSystem(serializableHadoopConf.unwrap());
    LOG.info("Copy initiating from {} to {}", srcPath, destPath);
    FileUtil.copy(srcFs, srcPath, destFs, destPath, false, serializableHadoopConf.unwrap());
    LOG.info("Copy complete from {} to {}", srcPath, destPath);
    // Validate checksum for the copied file.
    return checkCopiedFile(srcFs, srcPath, destFs, destPath);
  }

  private static void bootstrapMetadataTable(HoodieTableMetaClient metaClient, HoodieSparkEngineContext sparkEngineContext,
                                             HoodieShadowPipelineConfig cfg,
                                             TypedProperties userProperties) throws Exception {
    sparkEngineContext.setJobStatus(HoodieShadowPipeline.class.getSimpleName(), "Bootstrapping metadata table");
    Map<String, String> propertiesMap = new HashMap<>();
    userProperties.forEach((k, v) -> propertiesMap.put(k.toString(), v.toString()));
    propertiesMap.put("hoodie.datasource.write.table.type", cfg.destTableType);
    propertiesMap.put(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), OverwriteWithLatestAvroPayload.class.getName());
    propertiesMap.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), cfg.sourceOrderingField);
    propertiesMap.put(HoodieCleanConfig.AUTO_CLEAN.key(), "false");
    propertiesMap.put(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "false");
    if (cfg.allowDuplicatesInRecordIndex) {
      propertiesMap.put(HoodieStorageConfig.HFILE_WRITER_TO_ALLOW_DUPLICATES.key(), "true");
    }

    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    String schema = schemaResolver.getTableSchema(false).toString();
    HoodieWriteConfig writeConfig = DataSourceUtils.createHoodieConfig(schema, metaClient.getBasePath().toString(),
        metaClient.getTableConfig().getTableName(), propertiesMap);
    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(sparkEngineContext, writeConfig);
    // Create dummy inflight instant
    final String instantTime = writeClient.startCommit();
    writeClient.delete(sparkEngineContext.getJavaSparkContext().emptyRDD(), instantTime);

    // Rollback the dummy inflight commit
    writeClient = new SparkRDDWriteClient(sparkEngineContext, writeConfig);
    writeClient.rollback(instantTime);
  }

  /**
   * Check if the copied file is the same or not.
   */
  private static boolean checkCopiedFile(FileSystem srcFs, Path srcPath, FileSystem destFs, Path destPath) throws IOException {
    FileStatus srcStatus = srcFs.getFileStatus(srcPath);
    FileStatus destStatus = destFs.getFileStatus(destPath);
    LOG.info("Length of src file {} is {}", srcPath, srcStatus.getLen());
    LOG.info("Length of dest file {} is {}", destPath, destStatus.getLen());
    if (srcStatus.getLen() != destStatus.getLen()) {
      return false;
    }
    // Verify checksum between both the files
    FileChecksum srcChecksum = srcFs.getFileChecksum(srcPath);
    FileChecksum destChecksum = destFs.getFileChecksum(destPath);

    LOG.info("Checksum of src file {} is {}", srcPath, srcChecksum);
    LOG.info("Checksum of dest file {} is {}", destPath, destChecksum);

    // Handle the case where checksum is NONE for gs:// paths
    if (srcChecksum == null || destChecksum == null || "NONE".equals(srcChecksum.toString()) || "NONE".equals(destChecksum.toString())) {
      return true;
    }

    return srcChecksum.equals(destChecksum);
  }

  private static HoodieDeltaStreamer.Config createDeltaStreamerConfig(HoodieShadowPipelineConfig cfg,
                                                                      HoodieTableMetaClient srcMetaClient, HoodieTableMetaClient destMetaClient) throws IOException {
    HoodieDeltaStreamer.Config deltaConfig = new HoodieDeltaStreamer.Config();
    deltaConfig.targetBasePath = cfg.destPath;
    deltaConfig.targetTableName = getFullTargetTableName(cfg);
    deltaConfig.tableType = destMetaClient.getTableType().name();
    deltaConfig.baseFileFormat = srcMetaClient.getTableConfig().getBaseFileFormat().name();
    deltaConfig.sourceClassName = HoodieIncrSource.class.getName();
    deltaConfig.filterDupes = cfg.deduplicate;
    deltaConfig.sourceOrderingFields = cfg.sourceOrderingField;
    deltaConfig.enableMetaSync = cfg.enableHiveSync;
    deltaConfig.propsFilePath = "";
    deltaConfig.operation = cfg.operation;
    if (!StringUtils.isNullOrEmpty(cfg.schemaProviderClassName)) {
      deltaConfig.schemaProviderClassName = cfg.schemaProviderClassName;
    }

    return deltaConfig;
  }

  private static void validate(JavaSparkContext jsc, String srcPath, String destPath) {
    LOG.info("Validating destination datasets at " + destPath);
    validateData(jsc, srcPath, destPath);
  }

  private static String getFullTargetTableName(HoodieShadowPipelineConfig cfg) {
    return cfg.hiveDatabase + "." + getTargetTableName(cfg);
  }

  private static String getTargetTableName(HoodieShadowPipelineConfig cfg) {
    return StringUtils.isNullOrEmpty(cfg.hiveTable)
        ? cfg.datasetName + "_shadow"
        : cfg.hiveTable;
  }

  private static void validateData(JavaSparkContext jsc, String srcPath, String destPath) {
    // Validate the number of records in the source and destination datasets for the latest commit time
    HoodieTableMetaClient destMetaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).setBasePath(destPath).build();

    SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());
    HoodieActiveTimeline timeline = destMetaClient.getActiveTimeline();
    CommitMetadataSerDe commitMetadataSerDe = destMetaClient.getCommitMetadataSerDe();
    for (HoodieInstant commitInstant : timeline.getCommitTimeline().filterCompletedInstants().getReverseOrderedInstants().collect(Collectors.toList())) {
      try {
        byte[] details = timeline.getInstantDetails(commitInstant).get();
        HoodieCommitMetadata commitMetadata = commitMetadataSerDe.deserialize(commitInstant,
            new java.io.ByteArrayInputStream(details), () -> details.length == 0, HoodieCommitMetadata.class);
        final String commitTimestamp = commitInstant.requestedTime();
        if (StringUtils.isNullOrEmpty(commitMetadata.getMetadata(HoodieStreamer.CHECKPOINT_KEY))) {
          LOG.info("CHECKPOINT_KEY not found in commit metadata " + commitTimestamp);
          return;
        }
        final String checkpointKey = commitMetadata.getMetadata(HoodieStreamer.CHECKPOINT_KEY);
        LOG.info(String.format("Validating data in commit %s with checkpointKey %s", commitTimestamp, checkpointKey));

        // Incremental query finds records which match endInstantTime >=  _hoodie_commit_time > beginInstantTime.
        Date checkpointDate = HoodieInstantTimeGenerator.parseDateFromInstantTime(checkpointKey);
        String beginInstantTime = HoodieInstantTimeGenerator.formatDate(new Date(checkpointDate.getTime() - 1000));

        org.apache.spark.sql.Dataset<Row> srcDF = sqlContext.read().format("hudi")
            .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
            .option(DataSourceReadOptions.START_COMMIT().key(), beginInstantTime)
            .load(srcPath)
            .filter(String.format("%s = '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, checkpointKey));
        LOG.info("==== RECORD COUNT IN SRC FOR " + checkpointKey + " : " + srcDF.count());

        // Generate a timestamp lower than commitTimestamp to incrementally read records
        Date commitDate = HoodieInstantTimeGenerator.parseDateFromInstantTime(commitTimestamp);
        beginInstantTime = HoodieInstantTimeGenerator.formatDate(new Date(commitDate.getTime() - 1000));

        org.apache.spark.sql.Dataset<Row> destDF = sqlContext.read().format("hudi")
            .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
            .option(DataSourceReadOptions.START_COMMIT().key(), beginInstantTime)
            .load(destPath)
            .filter(String.format("%s = '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitTimestamp));
        LOG.info("==== RECORD COUNT IN DEST FOR " + commitTimestamp + " : " + destDF.count());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // Parse the command line arguments
    final HoodieShadowPipelineConfig cfg = new HoodieShadowPipelineConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    LOG.info("Provided configs are " + cfg);
    new HoodieShadowPipeline().run(cfg);
  }

  private void run(HoodieShadowPipelineConfig cfg) throws Exception {
    JavaSparkContext jssc = UtilHelpers.buildSparkContext(
            "HoodieShadowPipelineJob", "yarn", true, Collections.EMPTY_MAP);
    Configuration hadoopConf = jssc.hadoopConfiguration();

    HoodieTableMetaClient srcMetaClient;
    HoodieTableMetaClient destMetaClient;
    boolean datasetInitialized = false;

    // Check that the basePath exists
    try {
      srcMetaClient = HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf(hadoopConf)).setBasePath(cfg.srcPath)
          .setLoadActiveTimelineOnLoad(true)
          .build();
    } catch (TableNotFoundException e) {
      LOG.error("HUDI dataset not found at source path " + cfg.srcPath, e);
      throw e;
    }

    // Only COW support is implemented
    if (srcMetaClient.getTableType() != HoodieTableType.COPY_ON_WRITE) {
      throw new HoodieException("MOR tables are not supported yet.");
    }

    // Rest of the operations need to be under a lock to prevent conflicts
    try (LockManager lockManager = new LockManager(createWriteConfig(cfg), srcMetaClient.getStorage())) {
      LOG.info("Locking the destination table");
      lockManager.lock();

      if (cfg.deleteDestPath) {
        cleanDestination(getOrCreateSparkSession(), cfg, hadoopConf);
      }

      // Load the properties supplied by the user
      TypedProperties props;
      if (!StringUtils.isNullOrEmpty(cfg.propsFilePath)) {
        props = UtilHelpers.readConfig(jssc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.props).getProps();
      } else {
        props = UtilHelpers.getConfig(cfg.props).getProps();
      }

      // Create the destination dataset if not present
      try {
        destMetaClient = HoodieTableMetaClient.builder().setConf(HadoopFSUtils.getStorageConf(hadoopConf)).setBasePath(cfg.destPath).build();
        // At least one completed commit must be present if the dataset was created successfully
        if (destMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().countInstants() == 0) {
          throw new TableNotFoundException("");
        }
      } catch (TableNotFoundException e) {
        // Always clean before creating a new dataset to remove leftover files
        if (cfg.deleteDestPath) {
          cleanDestination(getOrCreateSparkSession(), cfg, hadoopConf);
        }
        destMetaClient = initializeDataset(jssc, cfg, srcMetaClient, props);
      }

      if (cfg.validateBefore) {
        // Since this is a validation before any operation, we need to manually upgrade the dataset if required
        upgradeIfRequired(jssc, destMetaClient, props);
        validate(jssc, cfg.srcPath, cfg.destPath);
      }

      if (!cfg.continuousMode) {
        LOG.info("Incremental run is disabled, so exiting the job without syncing with source table.");
        return;
      }

      // These properties are required so they override any user specified values
      props.put(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), cfg.srcPath);
      if (!StringUtils.isNullOrEmpty(cfg.recordKeyColumn)) {
        props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), cfg.recordKeyColumn);
      }
      props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), cfg.partitionColumns);
      props.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), cfg.keyGenerator);
      props.put(HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key(), cfg.instantsPerFetch);
      props.put(HoodieIncrSourceConfig.SOURCE_FILE_FORMAT.key(),
          srcMetaClient.getTableConfig().getBaseFileFormat().name().toLowerCase());

      // Set hive sync configs
      props.put(HoodieSyncConfig.META_SYNC_ENABLED.key(), cfg.enableHiveSync ? "true" : "false");
      props.put(HiveSyncConfigHolder.HIVE_SYNC_MODE.key(), HIVEQL.name());
      props.put(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), cfg.hiveDatabase);
      props.put(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), getTargetTableName(cfg));
      props.put(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), cfg.partitionColumns);
      props.put(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), cfg.destPath);
      props.put(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.key(), cfg.baseFileFormat);

      props.put(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), cfg.partitionValueExtractorClass);
      props.put(DataSourceWriteOptions.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE().key(), "true");

      if (cfg.allowDuplicatesInRecordIndex) {
        props.put(HoodieStorageConfig.HFILE_WRITER_TO_ALLOW_DUPLICATES.key(), "true");
      }

      // Metric config
      cfg.metricPrefix = cfg.metricPrefix.replace("{USER}", cfg.userid);

      props.put(HoodieMetricsConfig.TURN_METRICS_ON.key(), "true");
      props.put(HoodieMetricsConfig.EXECUTOR_METRICS_ENABLE.key(), "true");
      props.put(HoodieMetricsGraphiteConfig.GRAPHITE_METRIC_PREFIX_VALUE.key(), cfg.metricPrefix);
      props.put(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key(), MetricsReporterType.GRAPHITE.toString());

      if (datasetInitialized) {
        props.put(HoodieIncrSource.Config.READ_LATEST_INSTANT_ON_MISSING_CKPT, "true");
      }

      // Generate DeltaStreamer config
      HoodieDeltaStreamer.Config deltaConfig = createDeltaStreamerConfig(cfg, srcMetaClient, destMetaClient);

      // Perform the sync
      LOG.info("Starting shadow pipeline sync");
      String lastSyncedInstantTime = destMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant().get().requestedTime();
      while (true) {
        new HoodieDeltaStreamer(deltaConfig, jssc, Option.of(props)).sync();
        LOG.info("Completed deltastreamer sync call.");

        // Number of commits synced
        int numCommitsSynced = destMetaClient.reloadActiveTimeline().getCommitTimeline().filterCompletedInstants().findInstantsAfter(lastSyncedInstantTime, Integer.MAX_VALUE).countInstants();
        cfg.maxCommitsToSync -= numCommitsSynced;
        lastSyncedInstantTime = destMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant().get().requestedTime();

        if (!cfg.continuousMode || cfg.maxCommitsToSync <= 0) {
          break;
        } else {
          LOG.info(String.format("Sleeping for %d mins before next DeltaStreamer sync", cfg.sleepTimeBetweenRunsMins));
          Thread.sleep(cfg.sleepTimeBetweenRunsMins * 60 * 1000);
        }
      }

      if (cfg.validateAfter) {
        validate(jssc, cfg.srcPath, cfg.destPath);
      }
    }

    LOG.info("Stopping shadow pipeline sync");
  }

  private static void upgradeIfRequired(JavaSparkContext jsc, HoodieTableMetaClient destMetaClient, TypedProperties props) {
    if (destMetaClient.getTableConfig().getTableVersion().versionCode() < HoodieTableVersion.current().versionCode()) {
      LOG.info(String.format("Upgrading the dataset from version %d to the latest version %d",
          destMetaClient.getTableConfig().getTableVersion().versionCode(), HoodieTableVersion.current().versionCode()));

      HoodieWriteConfig updatedConfig = HoodieWriteConfig.newBuilder().withProps(props).withPath(destMetaClient.getBasePath().toString())
          .forTable(destMetaClient.getTableConfig().getTableName()).build();
      try {
        new UpgradeDowngrade(destMetaClient, updatedConfig, new HoodieSparkEngineContext(jsc), SparkUpgradeDowngradeHelper.getInstance())
            .run(HoodieTableVersion.current(), null);
        LOG.info(String.format("Table at \"%s\" upgraded / downgraded to version \"%s\".", destMetaClient.getBasePath(), HoodieTableVersion.current()));
      } catch (Exception e) {
        LOG.warn(String.format("Failed: Could not upgrade/downgrade table at \"%s\" to version \"%s\".", destMetaClient.getBasePath(), HoodieTableVersion.current()), e);
      }
    }
  }

  /**
   * When enableHiveSync is true, register dest dataset in HMS using HiveSyncTool.
   */
  public static void runHiveSync(JavaSparkContext jssc, HoodieTableMetaClient destMetaClient,
                                 HoodieShadowPipelineConfig config) throws IOException {
    LOG.info("Starting Hive sync for " + getFullTargetTableName(config));
    Properties props = new Properties();
    props.setProperty(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), config.destPath);
    props.setProperty(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.key(), config.baseFileFormat);
    props.setProperty(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), config.hiveDatabase);
    props.setProperty(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), getTargetTableName(config));
    props.setProperty(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), config.partitionColumns);
    props.setProperty(HiveSyncConfigHolder.HIVE_SYNC_MODE.key(), HIVEQL.name());
    props.put(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), config.partitionValueExtractorClass);
    if (HoodieTableType.MERGE_ON_READ.equals(destMetaClient.getTableType())) {
      props.put(DataSourceWriteOptions.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE().key(), "true");
    }
    try (HiveSyncTool syncTool = new HiveSyncTool(props, jssc.hadoopConfiguration())) {
      syncTool.syncHoodieTable();
    } catch (Exception e) {
      throw new HoodieException("Hive sync failed for " + getFullTargetTableName(config), e);
    }
    LOG.info("Dataset is registered in HMS: " + getFullTargetTableName(config));
  }

  private static HoodieWriteConfig createWriteConfig(HoodieShadowPipelineConfig cfg) {
    HoodieLockConfig.Builder lockConfigBuilder = HoodieLockConfig.newBuilder().withLockProvider(ZookeeperBasedLockProvider.class)
        .withClientNumRetries(10)
        .withClientRetryWaitTimeInMillis(60000L)
        .withLockWaitTimeInMillis(10 * 60000L)
        .withZkBasePath("/hudi/locks/hudi-shadow-pipelines")
        .withZkLockKey(getFullTargetTableName(cfg));
    if (!StringUtils.isNullOrEmpty(cfg.zookeeperUrl)) {
      lockConfigBuilder.withZkQuorum(cfg.zookeeperUrl);
    }

    return HoodieWriteConfig.newBuilder().forTable(getFullTargetTableName(cfg)).withPath(cfg.destPath)
        .withLockConfig(lockConfigBuilder.build())
        .withWriteConcurrencyMode(OPTIMISTIC_CONCURRENCY_CONTROL)
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(LAZY).build())
        .build();
  }
}
