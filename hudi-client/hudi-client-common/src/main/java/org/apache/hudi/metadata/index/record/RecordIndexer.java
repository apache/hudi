/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata.index.record;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.HoodieMergedReadHandle;
import org.apache.hudi.metadata.BaseFileRecordParsingUtils;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.config.HoodieCommonConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.MAX_MEMORY_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.readRecordKeysFromBaseFiles;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryResolveSchemaForTable;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;

public class RecordIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(RecordIndexer.class);
  // Average size of a record saved within the record index.
  // Record index has a fixed size schema. This has been calculated based on experiments with default settings
  // for block size (1MB), compression (GZ) and disabling the hudi metadata fields.
  public static final int RECORD_INDEX_AVERAGE_RECORD_SIZE = 48;
  private final EngineType engineType;
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final HoodieTable table;

  public RecordIndexer(EngineType engineType,
                       HoodieEngineContext engineContext,
                       HoodieWriteConfig dataTableWriteConfig,
                       HoodieTableMetaClient dataTableMetaClient,
                       HoodieTable table) {
    this.engineType = engineType;
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.table = table;
  }

  @Override
  public List<InitialIndexPartitionData> build(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    // TODO(yihua): could we unify the file listing across indexes?
    // Collect the list of latest base files present in each partition
    List<String> partitions = metadata.getAllPartitionPaths();
    fsView.get().loadAllPartitions();
    HoodieData<HoodieRecord> records = null;
    if (dataTableMetaClient.getTableConfig().getTableType() == HoodieTableType.COPY_ON_WRITE) {
      // for COW, we can only consider base files to initialize.
      final List<Pair<String, HoodieBaseFile>> partitionBaseFilePairs = new ArrayList<>();
      for (String partition : partitions) {
        partitionBaseFilePairs.addAll(fsView.get().getLatestBaseFiles(partition)
            .map(basefile -> Pair.of(partition, basefile)).collect(Collectors.toList()));
      }

      LOG.info("Initializing record index from {} base files in {} partitions",
          partitionBaseFilePairs.size(), partitions.size());

      // Collect record keys from the files in parallel
      records = readRecordKeysFromBaseFiles(
          engineContext,
          dataTableWriteConfig,
          partitionBaseFilePairs,
          false,
          dataTableWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism(),
          dataTableMetaClient.getBasePath(),
          engineContext.getStorageConf(),
          this.getClass().getSimpleName());
    } else {
      final List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
      String latestCommit = dataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant()
          .map(instant -> instant.requestedTime()).orElse(SOLO_COMMIT_TIMESTAMP);
      for (String partition : partitions) {
        fsView.get().getLatestMergedFileSlicesBeforeOrOn(partition, latestCommit)
            .forEach(fs -> partitionFileSlicePairs.add(Pair.of(partition, fs)));
      }

      LOG.info("Initializing record index from {} file slices in {} partitions",
          partitionFileSlicePairs.size(), partitions.size());
      records = readRecordKeysFromFileSliceSnapshot(
          engineContext,
          partitionFileSlicePairs,
          dataTableWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism(),
          this.getClass().getSimpleName(),
          dataTableMetaClient,
          dataTableWriteConfig,
          // TODO(yihua): is table instance needed here?
          table);
    }
    records.persist("MEMORY_AND_DISK_SER");
    final long recordCount = records.count();

    // Initialize the file groups
    final int numFileGroup = HoodieTableMetadataUtil.estimateFileGroupCount(
        RECORD_INDEX, recordCount, RECORD_INDEX_AVERAGE_RECORD_SIZE,
        dataTableWriteConfig.getRecordIndexMinFileGroupCount(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupCount(),
        dataTableWriteConfig.getRecordIndexGrowthFactor(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupSizeBytes());

    LOG.info("Initializing record index with {} mappings and {} file groups.", recordCount, numFileGroup);
    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, RECORD_INDEX.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionData> update(String instantTime,
                                         HoodieBackedTableMetadata tableMetadata,
                                         Lazy<HoodieTableFileSystemView> fsView,
                                         HoodieCommitMetadata commitMetadata) {
    HoodieData<HoodieRecord> recordsFromCommitMetadata = convertMetadataToRecordIndexRecords(
        engineContext, commitMetadata, dataTableWriteConfig.getMetadataConfig(),
        dataTableMetaClient, dataTableWriteConfig.getWritesFileIdEncoding(),
        instantTime, engineType);
    return Collections.singletonList(IndexPartitionData.of(
        RECORD_INDEX.getPartitionPath(),
        recordsFromCommitMetadata.union(
            getRecordIndexAdditionalUpserts(fsView, recordsFromCommitMetadata, commitMetadata))));
  }

  @Override
  public List<IndexPartitionData> clean(String instantTime, HoodieCleanMetadata cleanMetadata) {
    return Collections.emptyList();
  }

  /**
   * Fetch record locations from FileSlice snapshot.
   *
   * @param engineContext             context ot use.
   * @param partitionFileSlicePairs   list of pairs of partition and file slice.
   * @param recordIndexMaxParallelism parallelism to use.
   * @param activeModule              active module of interest.
   * @param metaClient                metaclient instance to use.
   * @param dataWriteConfig           write config to use.
   * @param hoodieTable               hoodie table instance of interest.
   * @return
   */
  private static HoodieData<HoodieRecord> readRecordKeysFromFileSliceSnapshot(HoodieEngineContext engineContext,
                                                                              List<Pair<String, FileSlice>> partitionFileSlicePairs,
                                                                              int recordIndexMaxParallelism,
                                                                              String activeModule,
                                                                              HoodieTableMetaClient metaClient,
                                                                              HoodieWriteConfig dataWriteConfig,
                                                                              HoodieTable hoodieTable) {
    if (partitionFileSlicePairs.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    Option<String> instantTime = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::requestedTime);

    engineContext.setJobStatus(activeModule,
        "Record Index: reading record keys from " + partitionFileSlicePairs.size() + " file slices");
    final int parallelism = Math.min(partitionFileSlicePairs.size(), recordIndexMaxParallelism);

    return engineContext.parallelize(partitionFileSlicePairs, parallelism).flatMap(partitionAndFileSlice -> {

      final String partition = partitionAndFileSlice.getKey();
      final FileSlice fileSlice = partitionAndFileSlice.getValue();
      final String fileId = fileSlice.getFileId();
      return new HoodieMergedReadHandle(dataWriteConfig, instantTime, hoodieTable,
          Pair.of(partition, fileSlice.getFileId()),
          Option.of(fileSlice)).getMergedRecords().stream()
          .map(record -> {
            HoodieRecord record1 = (HoodieRecord) record;
            return HoodieMetadataPayload.createRecordIndexUpdate(record1.getRecordKey(), partition, fileId,
                record1.getCurrentLocation().getInstantTime(), 0);
          }).iterator();
    });
  }

  @VisibleForTesting
  public static HoodieData<HoodieRecord> convertMetadataToRecordIndexRecords(HoodieEngineContext engineContext,
                                                                             HoodieCommitMetadata commitMetadata,
                                                                             HoodieMetadataConfig metadataConfig,
                                                                             HoodieTableMetaClient dataTableMetaClient,
                                                                             int writesFileIdEncoding,
                                                                             String instantTime,
                                                                             EngineType engineType) {
    List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());
    // Return early if there are no write stats, or if the operation is a compaction.
    if (allWriteStats.isEmpty() || commitMetadata.getOperationType() == WriteOperationType.COMPACT) {
      return engineContext.emptyHoodieData();
    }
    // RLI cannot support logs having inserts with current offering. So, lets validate that.
    if (allWriteStats.stream().anyMatch(writeStat -> {
      String fileName = FSUtils.getFileName(writeStat.getPath(), writeStat.getPartitionPath());
      return FSUtils.isLogFile(fileName) && writeStat.getNumInserts() > 0;
    })) {
      throw new HoodieIOException("RLI cannot support logs having inserts with current offering. Would recommend disabling Record Level Index");
    }

    try {
      Map<String, List<HoodieWriteStat>> writeStatsByFileId = allWriteStats.stream().collect(Collectors.groupingBy(HoodieWriteStat::getFileId));
      int parallelism = Math.max(Math.min(writeStatsByFileId.size(), metadataConfig.getRecordIndexMaxParallelism()), 1);
      String basePath = dataTableMetaClient.getBasePath().toString();
      HoodieFileFormat baseFileFormat = dataTableMetaClient.getTableConfig().getBaseFileFormat();
      StorageConfiguration storageConfiguration = dataTableMetaClient.getStorageConf();
      Option<Schema> writerSchemaOpt = tryResolveSchemaForTable(dataTableMetaClient);
      Option<Schema> finalWriterSchemaOpt = writerSchemaOpt;
      HoodieData<HoodieRecord> recordIndexRecords = engineContext.parallelize(new ArrayList<>(writeStatsByFileId.entrySet()), parallelism)
          .flatMap(writeStatsByFileIdEntry -> {
            String fileId = writeStatsByFileIdEntry.getKey();
            List<HoodieWriteStat> writeStats = writeStatsByFileIdEntry.getValue();
            // Partition the write stats into base file and log file write stats
            List<HoodieWriteStat> baseFileWriteStats = writeStats.stream()
                .filter(writeStat -> writeStat.getPath().endsWith(baseFileFormat.getFileExtension()))
                .collect(Collectors.toList());
            List<HoodieWriteStat> logFileWriteStats = writeStats.stream()
                .filter(writeStat -> FSUtils.isLogFile(new StoragePath(writeStats.get(0).getPath())))
                .collect(Collectors.toList());
            // Ensure that only one of base file or log file write stats exists
            checkState(baseFileWriteStats.isEmpty() || logFileWriteStats.isEmpty(),
                "A single fileId cannot have both base file and log file write stats in the same commit. FileId: " + fileId);
            // Process base file write stats
            if (!baseFileWriteStats.isEmpty()) {
              return baseFileWriteStats.stream()
                  .flatMap(writeStat -> {
                    HoodieStorage storage = HoodieStorageUtils.getStorage(new StoragePath(writeStat.getPath()), storageConfiguration);
                    return CollectionUtils.toStream(BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(basePath, writeStat, writesFileIdEncoding, instantTime, storage));
                  })
                  .iterator();
            }
            // Process log file write stats
            if (!logFileWriteStats.isEmpty()) {
              String partitionPath = logFileWriteStats.get(0).getPartitionPath();
              List<String> currentLogFilePaths = logFileWriteStats.stream()
                  .map(writeStat -> new StoragePath(dataTableMetaClient.getBasePath(), writeStat.getPath()).toString())
                  .collect(Collectors.toList());
              List<String> allLogFilePaths = logFileWriteStats.stream()
                  .flatMap(writeStat -> {
                    checkState(writeStat instanceof HoodieDeltaWriteStat, "Log file should be associated with a delta write stat");
                    List<String> currentLogFiles = ((HoodieDeltaWriteStat) writeStat).getLogFiles().stream()
                        .map(logFile -> new StoragePath(new StoragePath(dataTableMetaClient.getBasePath(), writeStat.getPartitionPath()), logFile).toString())
                        .collect(Collectors.toList());
                    return currentLogFiles.stream();
                  })
                  .collect(Collectors.toList());
              // Extract revived and deleted keys
              Pair<Set<String>, Set<String>> revivedAndDeletedKeys =
                  getRevivedAndDeletedKeysFromMergedLogs(dataTableMetaClient, instantTime, engineType, allLogFilePaths, finalWriterSchemaOpt, currentLogFilePaths);
              Set<String> revivedKeys = revivedAndDeletedKeys.getLeft();
              Set<String> deletedKeys = revivedAndDeletedKeys.getRight();
              // Process revived keys to create updates
              List<HoodieRecord> revivedRecords = revivedKeys.stream()
                  .map(recordKey -> HoodieMetadataPayload.createRecordIndexUpdate(recordKey, partitionPath, fileId, instantTime, writesFileIdEncoding))
                  .collect(Collectors.toList());
              // Process deleted keys to create deletes
              List<HoodieRecord> deletedRecords = deletedKeys.stream()
                  .map(HoodieMetadataPayload::createRecordIndexDelete)
                  .collect(Collectors.toList());
              // Combine all records into one list
              List<HoodieRecord> allRecords = new ArrayList<>();
              allRecords.addAll(revivedRecords);
              allRecords.addAll(deletedRecords);
              return allRecords.iterator();
            }
            LOG.warn("No base file or log file write stats found for fileId: {}", fileId);
            return Collections.emptyIterator();
          });

      // there are chances that same record key from data table has 2 entries (1 delete from older partition and 1 insert to newer partition)
      // lets do reduce by key to ignore the deleted entry.
      // first deduce parallelism to avoid too few tasks for large number of records.
      long totalWriteBytesForRLI = allWriteStats.stream().mapToLong(writeStat -> {
        // if there are no inserts or deletes, we can ignore this write stat for RLI
        if (writeStat.getNumInserts() == 0 && writeStat.getNumDeletes() == 0) {
          return 0;
        }
        return writeStat.getTotalWriteBytes();
      }).sum();
      // approximate task partition size of 100MB
      // (TODO: make this configurable)
      long targetPartitionSize = 100 * 1024 * 1024;
      parallelism = (int) Math.max(1, (totalWriteBytesForRLI + targetPartitionSize - 1) / targetPartitionSize);
      return reduceByKeys(recordIndexRecords, parallelism);
    } catch (Exception e) {
      throw new HoodieException("Failed to generate RLI records for metadata table", e);
    }
  }

  private HoodieData<HoodieRecord> getRecordIndexAdditionalUpserts(
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieData<HoodieRecord> updatesFromWriteStatuses,
      HoodieCommitMetadata commitMetadata) {
    WriteOperationType operationType = commitMetadata.getOperationType();
    if (operationType == WriteOperationType.INSERT_OVERWRITE) {
      // load existing records from replaced filegroups and left anti join overwriting records
      // return partition-level unmatched records (with newLocation being null) to be deleted from RLI
      return getRecordIndexReplacedRecords(fsView, (HoodieReplaceCommitMetadata) commitMetadata)
          .mapToPair(r -> Pair.of(r.getKey(), r))
          .leftOuterJoin(updatesFromWriteStatuses.mapToPair(r -> Pair.of(r.getKey(), r)))
          .values()
          .filter(p -> !p.getRight().isPresent())
          .map(Pair::getLeft);
    } else if (operationType == WriteOperationType.INSERT_OVERWRITE_TABLE) {
      // load existing records from replaced filegroups and left anti join overwriting records
      // return globally unmatched records (with newLocation being null) to be deleted from RLI
      return getRecordIndexReplacedRecords(fsView, (HoodieReplaceCommitMetadata) commitMetadata)
          .mapToPair(r -> Pair.of(r.getRecordKey(), r))
          .leftOuterJoin(updatesFromWriteStatuses.mapToPair(r -> Pair.of(r.getRecordKey(), r)))
          .values()
          .filter(p -> !p.getRight().isPresent())
          .map(Pair::getLeft);
    } else if (operationType == WriteOperationType.DELETE_PARTITION) {
      // all records from the target partition(s) to be deleted from RLI
      return getRecordIndexReplacedRecords(fsView, (HoodieReplaceCommitMetadata) commitMetadata);
    } else {
      return engineContext.emptyHoodieData();
    }
  }

  private HoodieData<HoodieRecord> getRecordIndexReplacedRecords(Lazy<HoodieTableFileSystemView> fsView,
                                                                 HoodieReplaceCommitMetadata replaceCommitMetadata) {
    HoodieTableFileSystemView view = fsView.get();
    List<Pair<String, HoodieBaseFile>> partitionBaseFilePairs = replaceCommitMetadata
        .getPartitionToReplaceFileIds()
        .keySet().stream()
        .flatMap(partition -> view.getLatestBaseFiles(partition).map(f -> Pair.of(partition, f)))
        .collect(Collectors.toList());
    return readRecordKeysFromBaseFiles(
        engineContext,
        dataTableWriteConfig,
        partitionBaseFilePairs,
        true,
        dataTableWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism(),
        dataTableMetaClient.getBasePath(),
        engineContext.getStorageConf(),
        this.getClass().getSimpleName());
  }

  /**
   * Get the revived and deleted keys from the merged log files. The logic is as below. Suppose:
   * <li>A = Set of keys that are valid (not deleted) in the previous log files merged</li>
   * <li>B = Set of keys that are valid in all log files including current log file merged</li>
   * <li>C = Set of keys that are deleted in the current log file</li>
   * <li>Then, D = Set of deleted keys = C - (B - A)</li>
   *
   * @param dataTableMetaClient  data table meta client
   * @param instantTime          timestamp of the commit
   * @param engineType           engine type (SPARK, FLINK, JAVA)
   * @param logFilePaths         list of log file paths including current and previous file slices
   * @param finalWriterSchemaOpt records schema
   * @param currentLogFilePaths  list of log file paths for the current instant
   * @return pair of revived and deleted keys
   */
  @VisibleForTesting
  public static Pair<Set<String>, Set<String>> getRevivedAndDeletedKeysFromMergedLogs(HoodieTableMetaClient dataTableMetaClient,
                                                                                      String instantTime,
                                                                                      EngineType engineType,
                                                                                      List<String> logFilePaths,
                                                                                      Option<Schema> finalWriterSchemaOpt,
                                                                                      List<String> currentLogFilePaths) {
    // Separate out the current log files
    List<String> logFilePathsWithoutCurrentLogFiles = logFilePaths.stream()
        .filter(logFilePath -> !currentLogFilePaths.contains(logFilePath))
        .collect(toList());
    if (logFilePathsWithoutCurrentLogFiles.isEmpty()) {
      // Only current log file is present, so we can directly get the deleted record keys from it and return the RLI records.
      Map<String, HoodieRecord> currentLogRecords =
          getLogRecords(currentLogFilePaths, dataTableMetaClient, finalWriterSchemaOpt, instantTime, engineType);
      Set<String> deletedKeys = currentLogRecords.entrySet().stream()
          .filter(entry -> isDeleteRecord(dataTableMetaClient, finalWriterSchemaOpt, entry.getValue()))
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());
      return Pair.of(Collections.emptySet(), deletedKeys);
    }
    return getRevivedAndDeletedKeys(dataTableMetaClient, instantTime, engineType, logFilePaths, finalWriterSchemaOpt, logFilePathsWithoutCurrentLogFiles);
  }

  private static Pair<Set<String>, Set<String>> getRevivedAndDeletedKeys(HoodieTableMetaClient dataTableMetaClient, String instantTime, EngineType engineType, List<String> logFilePaths,
                                                                         Option<Schema> finalWriterSchemaOpt, List<String> logFilePathsWithoutCurrentLogFiles) {
    // Fetch log records for all log files
    Map<String, HoodieRecord> allLogRecords =
        getLogRecords(logFilePaths, dataTableMetaClient, finalWriterSchemaOpt, instantTime, engineType);

    // Fetch log records for previous log files (excluding the current log files)
    Map<String, HoodieRecord> previousLogRecords =
        getLogRecords(logFilePathsWithoutCurrentLogFiles, dataTableMetaClient, finalWriterSchemaOpt, instantTime, engineType);

    // Partition valid (non-deleted) and deleted keys from previous log files in a single pass
    Map<Boolean, Set<String>> partitionedKeysForPreviousLogs = previousLogRecords.entrySet().stream()
        .collect(Collectors.partitioningBy(
            entry -> !isDeleteRecord(dataTableMetaClient, finalWriterSchemaOpt, entry.getValue()),
            Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
        ));
    Set<String> validKeysForPreviousLogs = partitionedKeysForPreviousLogs.get(true);
    Set<String> deletedKeysForPreviousLogs = partitionedKeysForPreviousLogs.get(false);

    // Partition valid (non-deleted) and deleted keys from all log files, including current, in a single pass
    Map<Boolean, Set<String>> partitionedKeysForAllLogs = allLogRecords.entrySet().stream()
        .collect(Collectors.partitioningBy(
            entry -> !isDeleteRecord(dataTableMetaClient, finalWriterSchemaOpt, entry.getValue()),
            Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
        ));
    Set<String> validKeysForAllLogs = partitionedKeysForAllLogs.get(true);
    Set<String> deletedKeysForAllLogs = partitionedKeysForAllLogs.get(false);

    return computeRevivedAndDeletedKeys(validKeysForPreviousLogs, deletedKeysForPreviousLogs, validKeysForAllLogs, deletedKeysForAllLogs);
  }

  private static boolean isDeleteRecord(HoodieTableMetaClient dataTableMetaClient, Option<Schema> finalWriterSchemaOpt, HoodieRecord record) {
    try {
      return record.isDelete(finalWriterSchemaOpt.get(), dataTableMetaClient.getTableConfig().getProps());
    } catch (IOException e) {
      throw new HoodieException("Failed to check if record is delete", e);
    }
  }

  private static Map<String, HoodieRecord> getLogRecords(List<String> logFilePaths,
                                                         HoodieTableMetaClient datasetMetaClient,
                                                         Option<Schema> writerSchemaOpt,
                                                         String latestCommitTimestamp,
                                                         EngineType engineType) {
    if (writerSchemaOpt.isPresent()) {
      final StorageConfiguration<?> storageConf = datasetMetaClient.getStorageConf();
      HoodieRecordMerger recordMerger = HoodieRecordUtils.createRecordMerger(
          datasetMetaClient.getBasePath().toString(),
          engineType,
          Collections.emptyList(),
          datasetMetaClient.getTableConfig().getRecordMergeStrategyId());

      // CRITICAL: Ensure allowInflightInstants is set to true while replacing the scanner with *LogRecordReader or HoodieFileGroupReader
      HoodieMergedLogRecordScanner mergedLogRecordScanner = HoodieMergedLogRecordScanner.newBuilder()
          .withStorage(datasetMetaClient.getStorage())
          .withBasePath(datasetMetaClient.getBasePath())
          .withLogFilePaths(logFilePaths)
          .withReaderSchema(writerSchemaOpt.get())
          .withLatestInstantTime(latestCommitTimestamp)
          .withReverseReader(false)
          .withMaxMemorySizeInBytes(storageConf.getLong(MAX_MEMORY_FOR_COMPACTION.key(), DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES))
          .withBufferSize(HoodieMetadataConfig.MAX_READER_BUFFER_SIZE_PROP.defaultValue())
          .withSpillableMapBasePath(FileIOUtils.getDefaultSpillableMapBasePath())
          .withOptimizedLogBlocksScan(storageConf.getBoolean("hoodie" + HoodieMetadataConfig.OPTIMIZED_LOG_BLOCKS_SCAN, false))
          .withDiskMapType(storageConf.getEnum(SPILLABLE_DISK_MAP_TYPE.key(), SPILLABLE_DISK_MAP_TYPE.defaultValue()))
          .withBitCaskDiskMapCompressionEnabled(storageConf.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
          .withRecordMerger(recordMerger)
          .withTableMetaClient(datasetMetaClient)
          .withAllowInflightInstants(true)
          .build();
      return mergedLogRecordScanner.getRecords();
    }
    return Collections.emptyMap();
  }

  @VisibleForTesting
  public static Pair<Set<String>, Set<String>> computeRevivedAndDeletedKeys(Set<String> validKeysForPreviousLogs,
                                                                            Set<String> deletedKeysForPreviousLogs,
                                                                            Set<String> validKeysForAllLogs,
                                                                            Set<String> deletedKeysForAllLogs) {
    // Compute revived keys: previously deleted but now valid
    Set<String> revivedKeys = new HashSet<>(deletedKeysForPreviousLogs);
    revivedKeys.retainAll(validKeysForAllLogs); // Intersection of previously deleted and now valid

    // Compute deleted keys: previously valid but now deleted
    Set<String> deletedKeys = new HashSet<>(validKeysForPreviousLogs);
    deletedKeys.retainAll(deletedKeysForAllLogs); // Intersection of previously valid and now deleted

    return Pair.of(revivedKeys, deletedKeys);
  }

  /**
   * There are chances that same record key from data table has 2 entries (1 delete from older partition and 1 insert to newer partition)
   * So, this method performs reduce by key to ignore the deleted entry.
   *
   * @param recordIndexRecords hoodie records after rli index lookup.
   * @param parallelism        parallelism to use.
   * @return
   */
  @VisibleForTesting
  public static HoodieData<HoodieRecord> reduceByKeys(HoodieData<HoodieRecord> recordIndexRecords, int parallelism) {
    return recordIndexRecords.mapToPair(
            (SerializablePairFunction<HoodieRecord, HoodieKey, HoodieRecord>) t -> Pair.of(t.getKey(), t))
        .reduceByKey((SerializableBiFunction<HoodieRecord, HoodieRecord, HoodieRecord>) (record1, record2) -> {
          boolean isRecord1Deleted = record1.getData() instanceof EmptyHoodieRecordPayload;
          boolean isRecord2Deleted = record2.getData() instanceof EmptyHoodieRecordPayload;
          if (isRecord1Deleted && !isRecord2Deleted) {
            return record2;
          } else if (!isRecord1Deleted && isRecord2Deleted) {
            return record1;
          } else if (isRecord1Deleted && isRecord2Deleted) {
            // let's delete just 1 of them
            return record1;
          } else {
            throw new HoodieIOException("Two HoodieRecord updates to RLI is seen for same record key " + record2.getRecordKey() + ", record 1 : "
                + record1.getData().toString() + ", record 2 : " + record2.getData().toString());
          }
        }, parallelism).values();
  }

  @VisibleForTesting
  public static Set<String> getRecordKeys(List<String> logFilePaths, HoodieTableMetaClient datasetMetaClient,
                                          Option<Schema> writerSchemaOpt, int maxBufferSize,
                                          String latestCommitTimestamp, boolean includeValidKeys,
                                          boolean includeDeletedKeys) throws IOException {
    if (writerSchemaOpt.isPresent()) {
      // read log file records without merging
      Set<String> allRecordKeys = new HashSet<>();
      HoodieUnMergedLogRecordScanner.Builder builder = HoodieUnMergedLogRecordScanner.newBuilder()
          .withStorage(datasetMetaClient.getStorage())
          .withBasePath(datasetMetaClient.getBasePath())
          .withLogFilePaths(logFilePaths)
          .withBufferSize(maxBufferSize)
          .withLatestInstantTime(latestCommitTimestamp)
          .withReaderSchema(writerSchemaOpt.get())
          .withTableMetaClient(datasetMetaClient);
      if (includeValidKeys) {
        builder.withLogRecordScannerCallback(record -> allRecordKeys.add(record.getRecordKey()));
      }
      if (includeDeletedKeys) {
        builder.withRecordDeletionCallback(deletedKey -> allRecordKeys.add(deletedKey.getRecordKey()));
      }
      HoodieUnMergedLogRecordScanner scanner = builder.build();
      scanner.scan();
      return allRecordKeys;
    }
    return Collections.emptySet();
  }
}
