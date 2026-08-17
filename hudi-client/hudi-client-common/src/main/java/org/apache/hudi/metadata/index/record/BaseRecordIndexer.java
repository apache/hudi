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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieAvroFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.BaseFileRecordParsingUtils;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.model.DataPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.schema.HoodieSchemaUtils.getRecordKeySchema;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.RECORD_INDEX_AVERAGE_RECORD_SIZE;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;

/**
 * Base implementation of {@link MetadataPartitionType#RECORD_INDEX} index.
 */
@Slf4j
public abstract class BaseRecordIndexer extends BaseIndexer {

  protected BaseRecordIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig, HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  protected DataPartitionAndRecords initializeRecordIndexPartition(
      List<FileSliceAndPartition> latestMergedPartitionFileSliceList,
      int recordIndexMaxParallelism) {
    return initializeRecordIndexPartition(null, latestMergedPartitionFileSliceList, recordIndexMaxParallelism);
  }

  protected DataPartitionAndRecords initializeRecordIndexPartition(
      String dataPartition,
      List<FileSliceAndPartition> latestMergedPartitionFileSliceList,
      int recordIndexMaxParallelism) {
    log.info("Initializing record index from {} file slices", latestMergedPartitionFileSliceList.size());
    HoodieData<HoodieRecord> records = readRecordKeysFromFileSliceSnapshot(
        engineContext,
        latestMergedPartitionFileSliceList,
        recordIndexMaxParallelism,
        this.getClass().getSimpleName(),
        dataTableMetaClient,
        dataTableWriteConfig);

    // Initialize the file groups
    final int fileGroupCount = estimateFileGroupCount(records);
    log.info("Initializing record index with {} file groups.", fileGroupCount);
    return new DataPartitionAndRecords(fileGroupCount, Option.ofNullable(dataPartition), records);
  }

  @Override
  public void postInitialization(HoodieTableMetaClient metadataMetaClient, HoodieData<HoodieRecord> records, int fileGroupCount, String relativePartitionPath) {
    super.postInitialization(metadataMetaClient, records, fileGroupCount, relativePartitionPath);
    // Validate record index after commit if validation is enabled
    if (dataTableWriteConfig.getMetadataConfig().isRecordIndexInitializationValidationEnabled()) {
      validateRecordIndex(records, fileGroupCount, metadataMetaClient);
    }
    records.unpersistWithDependencies();
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    return Collections.emptyList();
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(IndexUpdateContext context) {
    HoodieData<HoodieRecord> updatesFromWriteStatuses = convertMetadataToRecordIndexRecords(engineContext, context.commitMetadata(),
        dataTableWriteConfig.getMetadataConfig(), dataTableMetaClient, dataTableWriteConfig.getWritesFileIdEncoding(), context.instantTime());
    HoodieData<HoodieRecord> additionalUpdates = getRecordIndexAdditionalUpserts(updatesFromWriteStatuses, context.commitMetadata(), context.lazyFileSystemView());
    return Collections.singletonList(IndexPartitionAndRecords.of(RECORD_INDEX.getPartitionPath(), updatesFromWriteStatuses.union(additionalUpdates)));
  }

  /**
   * Validates the record index after bootstrap by comparing the expected record count with the actual
   * record count stored in the metadata table. The validation is performed in a distributed manner
   * using the engine context to count records from HFiles in parallel.
   *
   * @param recordIndexRecords the HoodieData containing the expected records
   * @param fileGroupCount the expected number of file groups
   * @param metadataMetaClient meta client for the metadata table
   */
  protected void validateRecordIndex(HoodieData<HoodieRecord> recordIndexRecords, int fileGroupCount, HoodieTableMetaClient metadataMetaClient) {
    String partitionName = MetadataPartitionType.RECORD_INDEX.getPartitionPath();
    HoodieTableFileSystemView fsView = HoodieTableMetadataUtil.getFileSystemViewForMetadataTable(metadataMetaClient);
    try {
      // Use merged file slices to handle cases with pending compactions
      List<FileSlice> fileSlices = HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, fsView, partitionName);

      // Filter to only file slices with base files and extract their storage paths
      List<StoragePath> baseFilePaths = fileSlices.stream()
          .filter(fileSlice -> fileSlice.getBaseFile().isPresent())
          .map(fileSlice -> fileSlice.getBaseFile().get().getStoragePath())
          .collect(Collectors.toList());

      // Count records in a distributed manner using the engine context
      long totalRecords = countRecordsInHFiles(baseFilePaths, metadataMetaClient);
      long expectedRecordCount = recordIndexRecords.count();

      ValidationUtils.checkArgument(totalRecords == expectedRecordCount, "Record Count Validation failed with "
          + totalRecords + " present in record index vs the expected " + expectedRecordCount);
      log.info("Record index initialized on {} shards (expected = {}) with {} records (expected = {})",
          fileSlices.size(), fileGroupCount, totalRecords, expectedRecordCount);
    } finally {
      fsView.close();
    }
  }

  /**
   * Counts the total number of records in HFiles in a distributed manner.
   *
   * @param baseFilePaths list of storage paths to HFiles
   * @param metadataMetaClient meta client for the metadata table
   * @return total number of records across all HFiles
   */
  private long countRecordsInHFiles(List<StoragePath> baseFilePaths, HoodieTableMetaClient metadataMetaClient) {
    if (baseFilePaths.isEmpty()) {
      return 0L;
    }

    int parallelism = Math.min(baseFilePaths.size(), dataTableWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism());
    StorageConfiguration<?> storageConf = metadataMetaClient.getStorageConf();
    HoodieFileFormat baseFileFormat = metadataMetaClient.getTableConfig().getBaseFileFormat();

    return engineContext.parallelize(baseFilePaths, parallelism)
        .mapPartitions(pathIterator -> {
          long count = 0L;
          while (pathIterator.hasNext()) {
            StoragePath path = pathIterator.next();
            try {
              HoodieStorage storage = HoodieStorageUtils.getStorage(path, storageConf);
              HoodieConfig readerConfig = new HoodieConfig();
              HoodieAvroFileReader reader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage)
                  .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
                  .getFileReader(readerConfig, path, baseFileFormat, Option.empty());
              try {
                count += reader.getTotalRecords();
              } finally {
                reader.close();
              }
            } catch (IOException e) {
              throw new HoodieIOException("Error reading total records from file " + path, e);
            }
          }
          return Collections.singletonList(count).iterator();
        }, true)
        .collectAsList()
        .stream()
        .mapToLong(Long::longValue)
        .sum();
  }

  /**
   * Fetch record locations from FileSlice snapshot.
   *
   * @param engineContext             context to use.
   * @param fileSlices                file slices with their data partitions.
   * @param recordIndexMaxParallelism parallelism to use.
   * @param activeModule              active module of interest.
   * @param metaClient                metaclient instance to use.
   * @param dataWriteConfig           write config to use.
   * @return metadata records for initializing record index entries.
   */
  protected static <T> HoodieData<HoodieRecord> readRecordKeysFromFileSliceSnapshot(HoodieEngineContext engineContext,
                                                                                  List<FileSliceAndPartition> fileSlices,
                                                                                  int recordIndexMaxParallelism,
                                                                                  String activeModule,
                                                                                  HoodieTableMetaClient metaClient,
                                                                                  HoodieWriteConfig dataWriteConfig) {
    if (fileSlices.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    Option<String> instantTime = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::requestedTime);
    if (!instantTime.isPresent()) {
      return engineContext.emptyHoodieData();
    }

    engineContext.setJobStatus(activeModule, "Record Index: reading record keys from " + fileSlices.size() + " file slices");
    final int parallelism = Math.min(fileSlices.size(), recordIndexMaxParallelism);
    ReaderContextFactory<T> readerContextFactory = engineContext.getReaderContextFactory(metaClient);
    return engineContext.parallelize(fileSlices, parallelism).flatMap(partitionAndFileSlice -> {
      final String partition = partitionAndFileSlice.getPartitionPath();
      final FileSlice fileSlice = partitionAndFileSlice.getFileSlice();
      final String fileId = fileSlice.getFileId();
      HoodieReaderContext<T> readerContext = readerContextFactory.getContext();
      HoodieSchema dataSchema = resolveDataSchemaForRLIBootstrap(metaClient, dataWriteConfig);
      HoodieSchema requestedSchema = metaClient.getTableConfig().populateMetaFields() ? getRecordKeySchema()
          : HoodieSchemaUtils.projectSchema(dataSchema, Arrays.asList(metaClient.getTableConfig().getRecordKeyFields().orElse(new String[0])));
      Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(dataWriteConfig.getInternalSchema());
      HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>builder()
          .withReaderContext(readerContext)
          .withHoodieTableMetaClient(metaClient)
          .withBaseFileOption(fileSlice.getBaseFile())
          .withLogFiles(fileSlice.getLogFiles())
          .withPartitionPath(fileSlice.getPartitionPath())
          .withLatestCommitTime(instantTime.get())
          .withDataSchema(dataSchema)
          .withRequestedSchema(requestedSchema)
          .withInternalSchemaOpt(internalSchemaOption)
          .withShouldUseRecordPosition(false)
          .withProps(metaClient.getTableConfig().getProps())
          .build();
      long baseFileInstantTimeMillis = HoodieMetadataPayload.parseRecordIndexInstantTime(fileSlice.getBaseInstantTime());
      return new CloseableMappingIterator<>(fileGroupReader.getClosableIterator(), record -> {
        String recordKey = readerContext.getRecordContext().getRecordKey(record, requestedSchema);
        return HoodieMetadataPayload.createRecordIndexUpdate(recordKey, partition, fileId,
            baseFileInstantTimeMillis, 0);
      });
    });
  }

  /**
   * Resolves the data schema (with metadata fields added) for use during record index bootstrap.
   * When the write config does not carry a schema (e.g. table-service operations such as clean),
   * falls back to resolving the schema from the table's commit history / data files.
   */
  static HoodieSchema resolveDataSchemaForRLIBootstrap(HoodieTableMetaClient metaClient, HoodieWriteConfig dataWriteConfig) {
    String writeSchemaStr = dataWriteConfig.getWriteSchema();
    HoodieSchema rawSchema;
    if (writeSchemaStr != null) {
      rawSchema = HoodieSchema.parse(writeSchemaStr);
    } else {
      try {
        rawSchema = new TableSchemaResolver(metaClient).getTableSchema(false);
      } catch (Exception e) {
        throw new HoodieException(
            String.format("Could not resolve schema for table %s for record index bootstrap", metaClient.getBasePath()), e);
      }
    }
    return HoodieSchemaCache.intern(HoodieSchemaUtils.addMetadataFields(rawSchema, dataWriteConfig.allowOperationMetadataField()));
  }

  protected int estimateFileGroupCount(HoodieData<HoodieRecord> records) {
    int minFileGroupCount;
    int maxFileGroupCount;
    if (dataTableWriteConfig.isRecordLevelIndexEnabled()) {
      minFileGroupCount = dataTableWriteConfig.getRecordLevelIndexMinFileGroupCount();
      maxFileGroupCount = dataTableWriteConfig.getRecordLevelIndexMaxFileGroupCount();
    } else {
      minFileGroupCount = dataTableWriteConfig.getGlobalRecordLevelIndexMinFileGroupCount();
      maxFileGroupCount = dataTableWriteConfig.getGlobalRecordLevelIndexMaxFileGroupCount();
    }
    Supplier<Long> recordCountSupplier = () -> {
      records.persist("MEMORY_AND_DISK_SER");
      long count = records.count();
      log.info("Initializing record index with {} mappings", count);
      return count;
    };
    return HoodieTableMetadataUtil.estimateFileGroupCount(
        MetadataPartitionType.RECORD_INDEX,
        recordCountSupplier,
        RECORD_INDEX_AVERAGE_RECORD_SIZE,
        minFileGroupCount,
        maxFileGroupCount,
        dataTableWriteConfig.getRecordIndexGrowthFactor(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupSizeBytes()
    );
  }

  @VisibleForTesting
  public static <T> HoodieData<HoodieRecord> convertMetadataToRecordIndexRecords(HoodieEngineContext engineContext,
                                                                                 HoodieCommitMetadata commitMetadata,
                                                                                 HoodieMetadataConfig metadataConfig,
                                                                                 HoodieTableMetaClient dataTableMetaClient,
                                                                                 int writesFileIdEncoding,
                                                                                 String instantTime) {
    List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());
    // Return early if there are no write stats, or if the operation is compaction or log compaction.
    if (allWriteStats.isEmpty()
        || commitMetadata.getOperationType() == WriteOperationType.COMPACT
        || commitMetadata.getOperationType() == WriteOperationType.LOG_COMPACT) {
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
      StorageConfiguration storageConfiguration = dataTableMetaClient.getStorageConf();
      Option<HoodieSchema> writerSchemaOpt = HoodieTableMetadataUtil.tryResolveSchemaForTable(dataTableMetaClient);
      Option<HoodieSchema> finalWriterSchemaOpt = writerSchemaOpt;
      ReaderContextFactory<T> readerContextFactory = engineContext.getReaderContextFactory(dataTableMetaClient);
      HoodieData<HoodieRecord> recordIndexRecords = engineContext.parallelize(new ArrayList<>(writeStatsByFileId.entrySet()), parallelism)
          .flatMap(writeStatsByFileIdEntry -> {
            String fileId = writeStatsByFileIdEntry.getKey();
            List<HoodieWriteStat> writeStats = writeStatsByFileIdEntry.getValue();
            // Partition the write stats into base file and log file write stats
            List<HoodieWriteStat> baseFileWriteStats = writeStats.stream()
                .filter(writeStat -> FSUtils.isBaseFile(new StoragePath(writeStat.getPath())))
                .collect(Collectors.toList());
            List<HoodieWriteStat> logFileWriteStats = writeStats.stream()
                .filter(writeStat -> FSUtils.isLogFile(new StoragePath(writeStat.getPath())))
                .collect(Collectors.toList());
            // Ensure that only one of base file or log file write stats exists
            checkState(baseFileWriteStats.isEmpty() || logFileWriteStats.isEmpty(),
                "A single fileId cannot have both base file and log file write stats in the same commit. FileId: " + fileId);
            // Process base file write stats
            if (!baseFileWriteStats.isEmpty()) {
              return baseFileWriteStats.stream()
                  .flatMap(writeStat -> {
                    HoodieStorage storage = HoodieStorageUtils.getStorage(new StoragePath(writeStat.getPath()), storageConfiguration);
                    return CollectionUtils.toStream(BaseFileRecordParsingUtils
                        .generateRLIMetadataHoodieRecordsForBaseFile(basePath, writeStat, writesFileIdEncoding, instantTime, storage, metadataConfig.isRecordLevelIndexEnabled()));
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
              Pair<Set<String>, Set<String>> revivedAndDeletedKeys = HoodieTableMetadataUtil.getRevivedAndDeletedKeysFromMergedLogs(dataTableMetaClient, instantTime,
                  allLogFilePaths, finalWriterSchemaOpt, currentLogFilePaths, partitionPath, readerContextFactory.getContext());
              Set<String> revivedKeys = revivedAndDeletedKeys.getLeft();
              Set<String> deletedKeys = revivedAndDeletedKeys.getRight();
              // Process revived keys to create updates
              long instantTimeMillis = HoodieMetadataPayload.parseRecordIndexInstantTime(instantTime);
              List<HoodieRecord> revivedRecords = revivedKeys.stream()
                  .map(recordKey -> HoodieMetadataPayload.createRecordIndexUpdate(recordKey, partitionPath, fileId, instantTimeMillis, writesFileIdEncoding))
                  .collect(Collectors.toList());
              // Process deleted keys to create deletes
              List<HoodieRecord> deletedRecords = deletedKeys.stream()
                  .map(key -> HoodieMetadataPayload.createRecordIndexDelete(key, partitionPath, metadataConfig.isRecordLevelIndexEnabled()))
                  .collect(Collectors.toList());
              // Combine all records into one list
              List<HoodieRecord> allRecords = new ArrayList<>();
              allRecords.addAll(revivedRecords);
              allRecords.addAll(deletedRecords);
              return allRecords.iterator();
            }
            log.warn("No base file or log file write stats found for fileId: {}", fileId);
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
      return HoodieTableMetadataUtil.reduceByKeys(recordIndexRecords, parallelism, metadataConfig.isRecordLevelIndexEnabled());
    } catch (Exception e) {
      throw new HoodieException("Failed to generate RLI records for metadata table", e);
    }
  }

  private HoodieData<HoodieRecord> getRecordIndexAdditionalUpserts(
      HoodieData<HoodieRecord> updatesFromWriteStatuses,
      HoodieCommitMetadata commitMetadata,
      Lazy<HoodieTableFileSystemView> fsView) {
    WriteOperationType operationType = commitMetadata.getOperationType();
    if (operationType == WriteOperationType.INSERT_OVERWRITE) {
      // load existing records from replaced filegroups and left anti join overwriting records
      // return partition-level unmatched records (with newLocation being null) to be deleted from RLI
      return getRecordIndexReplacedRecords((HoodieReplaceCommitMetadata) commitMetadata, fsView)
          .mapToPair(r -> Pair.of(r.getKey(), r))
          .leftOuterJoin(updatesFromWriteStatuses.mapToPair(r -> Pair.of(r.getKey(), r)))
          .values()
          .filter(p -> !p.getRight().isPresent())
          .map(Pair::getLeft);
    } else if (operationType == WriteOperationType.INSERT_OVERWRITE_TABLE) {
      // load existing records from replaced filegroups and left anti join overwriting records
      // return globally unmatched records (with newLocation being null) to be deleted from RLI
      return getRecordIndexReplacedRecords((HoodieReplaceCommitMetadata) commitMetadata, fsView)
          .mapToPair(r -> Pair.of(r.getRecordKey(), r))
          .leftOuterJoin(updatesFromWriteStatuses.mapToPair(r -> Pair.of(r.getRecordKey(), r)))
          .values()
          .filter(p -> !p.getRight().isPresent())
          .map(Pair::getLeft);
    } else if (operationType == WriteOperationType.DELETE_PARTITION) {
      // all records from the target partition(s) to be deleted from RLI
      return getRecordIndexReplacedRecords((HoodieReplaceCommitMetadata) commitMetadata, fsView);
    } else {
      return engineContext.emptyHoodieData();
    }
  }

  private HoodieData<HoodieRecord> getRecordIndexReplacedRecords(HoodieReplaceCommitMetadata replaceCommitMetadata, Lazy<HoodieTableFileSystemView> fsView) {
    List<Pair<String, HoodieBaseFile>> partitionBaseFilePairs = replaceCommitMetadata
        .getPartitionToReplaceFileIds()
        .keySet().stream()
        .flatMap(partition -> fsView.get().getLatestBaseFiles(partition).map(f -> Pair.of(partition, f)))
        .collect(Collectors.toList());
    return HoodieTableMetadataUtil.readRecordKeysFromBaseFiles(
        engineContext,
        dataTableWriteConfig,
        partitionBaseFilePairs,
        true,
        dataTableWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism(),
        dataTableMetaClient.getBasePath(),
        dataTableMetaClient.getStorageConf(),
        this.getClass().getSimpleName(),
        dataTableWriteConfig.isRecordLevelIndexEnabled());
  }
}
