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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieFileSliceReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.MAX_MEMORY_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createSecondaryIndexRecord;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.filePath;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionLatestFileSlicesIncludingInflight;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryResolveSchemaForTable;

/**
 * Utility methods for generating secondary index records during initialization and updates.
 */
public class SecondaryIndexRecordGenerationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SecondaryIndexRecordGenerationUtils.class);

  /**
   * Converts the write stats to secondary index records.
   *
   * @param allWriteStats   list of write stats
   * @param instantTime     instant time
   * @param indexDefinition secondary index definition
   * @param metadataConfig  metadata config
   * @param fsView          file system view as of instant time
   * @param dataMetaClient  data table meta client
   * @param engineContext   engine context
   * @param engineType      engine type (e.g. SPARK, FLINK or JAVA)
   * @return {@link HoodieData} of {@link HoodieRecord} to be updated in the metadata table for the given secondary index partition
   */
  @VisibleForTesting
  public static HoodieData<HoodieRecord> convertWriteStatsToSecondaryIndexRecords(List<HoodieWriteStat> allWriteStats,
                                                                                  String instantTime,
                                                                                  HoodieIndexDefinition indexDefinition,
                                                                                  HoodieMetadataConfig metadataConfig,
                                                                                  HoodieTableFileSystemView fsView,
                                                                                  HoodieTableMetaClient dataMetaClient,
                                                                                  HoodieEngineContext engineContext,
                                                                                  EngineType engineType) {
    // Secondary index cannot support logs having inserts with current offering. So, lets validate that.
    if (allWriteStats.stream().anyMatch(writeStat -> {
      String fileName = FSUtils.getFileName(writeStat.getPath(), writeStat.getPartitionPath());
      return FSUtils.isLogFile(fileName) && writeStat.getNumInserts() > 0;
    })) {
      throw new HoodieIOException("Secondary index cannot support logs having inserts with current offering. Please disable secondary index.");
    }

    Schema tableSchema;
    try {
      tableSchema = tryResolveSchemaForTable(dataMetaClient).get();
    } catch (Exception e) {
      throw new HoodieException("Failed to get latest schema for " + dataMetaClient.getBasePath(), e);
    }
    Map<String, List<HoodieWriteStat>> writeStatsByFileId = allWriteStats.stream().collect(Collectors.groupingBy(HoodieWriteStat::getFileId));
    int parallelism = Math.max(Math.min(writeStatsByFileId.size(), metadataConfig.getSecondaryIndexParallelism()), 1);

    return engineContext.parallelize(new ArrayList<>(writeStatsByFileId.entrySet()), parallelism).flatMap(writeStatsByFileIdEntry -> {
      String fileId = writeStatsByFileIdEntry.getKey();
      List<HoodieWriteStat> writeStats = writeStatsByFileIdEntry.getValue();
      String partition = writeStats.get(0).getPartitionPath();
      FileSlice previousFileSliceForFileId = fsView.getLatestFileSlice(partition, fileId).orElse(null);
      Map<String, String> recordKeyToSecondaryKeyForPreviousFileSlice;
      if (previousFileSliceForFileId == null) {
        // new file slice, so empty mapping for previous slice
        recordKeyToSecondaryKeyForPreviousFileSlice = Collections.emptyMap();
      } else {
        StoragePath previousBaseFile = previousFileSliceForFileId.getBaseFile().map(HoodieBaseFile::getStoragePath).orElse(null);
        List<String> logFiles =
            previousFileSliceForFileId.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).map(HoodieLogFile::getPath).map(StoragePath::toString).collect(Collectors.toList());
        recordKeyToSecondaryKeyForPreviousFileSlice =
            getRecordKeyToSecondaryKey(dataMetaClient, engineType, logFiles, tableSchema, partition, Option.ofNullable(previousBaseFile), indexDefinition, instantTime);
      }
      List<FileSlice> latestIncludingInflightFileSlices = getPartitionLatestFileSlicesIncludingInflight(dataMetaClient, Option.empty(), partition);
      FileSlice currentFileSliceForFileId = latestIncludingInflightFileSlices.stream().filter(fs -> fs.getFileId().equals(fileId)).findFirst()
          .orElseThrow(() -> new HoodieException("Could not find any file slice for fileId " + fileId));
      StoragePath currentBaseFile = currentFileSliceForFileId.getBaseFile().map(HoodieBaseFile::getStoragePath).orElse(null);
      List<String> logFilesIncludingInflight =
          currentFileSliceForFileId.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).map(HoodieLogFile::getPath).map(StoragePath::toString).collect(Collectors.toList());
      Map<String, String> recordKeyToSecondaryKeyForCurrentFileSlice =
          getRecordKeyToSecondaryKey(dataMetaClient, engineType, logFilesIncludingInflight, tableSchema, partition, Option.ofNullable(currentBaseFile), indexDefinition, instantTime);
      // Need to find what secondary index record should be deleted, and what should be inserted.
      // For each entry in recordKeyToSecondaryKeyForCurrentFileSlice, if it is not present in recordKeyToSecondaryKeyForPreviousFileSlice, then it should be inserted.
      // For each entry in recordKeyToSecondaryKeyForCurrentFileSlice, if it is present in recordKeyToSecondaryKeyForPreviousFileSlice, then it should be updated.
      // For each entry in recordKeyToSecondaryKeyForPreviousFileSlice, if it is not present in recordKeyToSecondaryKeyForCurrentFileSlice, then it should be deleted.
      List<HoodieRecord> records = new ArrayList<>();
      recordKeyToSecondaryKeyForCurrentFileSlice.forEach((recordKey, secondaryKey) -> {
        if (!recordKeyToSecondaryKeyForPreviousFileSlice.containsKey(recordKey)) {
          records.add(createSecondaryIndexRecord(recordKey, secondaryKey, indexDefinition.getIndexName(), false));
        } else {
          // delete previous entry and insert new value if secondaryKey is different
          if (!recordKeyToSecondaryKeyForPreviousFileSlice.get(recordKey).equals(secondaryKey)) {
            records.add(createSecondaryIndexRecord(recordKey, recordKeyToSecondaryKeyForPreviousFileSlice.get(recordKey), indexDefinition.getIndexName(), true));
            records.add(createSecondaryIndexRecord(recordKey, secondaryKey, indexDefinition.getIndexName(), false));
          }
        }
      });
      recordKeyToSecondaryKeyForPreviousFileSlice.forEach((recordKey, secondaryKey) -> {
        if (!recordKeyToSecondaryKeyForCurrentFileSlice.containsKey(recordKey)) {
          records.add(createSecondaryIndexRecord(recordKey, secondaryKey, indexDefinition.getIndexName(), true));
        }
      });
      return records.iterator();
    });
  }

  private static Map<String, String> getRecordKeyToSecondaryKey(HoodieTableMetaClient metaClient,
                                                                EngineType engineType, List<String> logFilePaths,
                                                                Schema tableSchema, String partition,
                                                                Option<StoragePath> dataFilePath,
                                                                HoodieIndexDefinition indexDefinition,
                                                                String instantTime) throws Exception {
    final String basePath = metaClient.getBasePath().toString();
    final StorageConfiguration<?> storageConf = metaClient.getStorageConf();

    HoodieRecordMerger recordMerger = HoodieRecordUtils.createRecordMerger(
        basePath,
        engineType,
        Collections.emptyList(),
        metaClient.getTableConfig().getRecordMergeStrategyId());

    HoodieMergedLogRecordScanner mergedLogRecordScanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(metaClient.getStorage())
        .withBasePath(metaClient.getBasePath())
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(tableSchema)
        .withLatestInstantTime(instantTime)
        .withReverseReader(false)
        .withMaxMemorySizeInBytes(storageConf.getLong(MAX_MEMORY_FOR_COMPACTION.key(), DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES))
        .withBufferSize(HoodieMetadataConfig.MAX_READER_BUFFER_SIZE_PROP.defaultValue())
        .withSpillableMapBasePath(FileIOUtils.getDefaultSpillableMapBasePath())
        .withPartition(partition)
        .withOptimizedLogBlocksScan(storageConf.getBoolean("hoodie" + HoodieMetadataConfig.OPTIMIZED_LOG_BLOCKS_SCAN, false))
        .withDiskMapType(storageConf.getEnum(SPILLABLE_DISK_MAP_TYPE.key(), SPILLABLE_DISK_MAP_TYPE.defaultValue()))
        .withBitCaskDiskMapCompressionEnabled(storageConf.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
        .withRecordMerger(recordMerger)
        .withTableMetaClient(metaClient)
        .build();

    Option<HoodieFileReader> baseFileReader = Option.empty();
    if (dataFilePath.isPresent()) {
      baseFileReader = Option.of(HoodieIOFactory.getIOFactory(metaClient.getStorage()).getReaderFactory(recordMerger.getRecordType()).getFileReader(getReaderConfigs(storageConf), dataFilePath.get()));
    }
    HoodieFileSliceReader fileSliceReader = new HoodieFileSliceReader(baseFileReader, mergedLogRecordScanner, tableSchema, metaClient.getTableConfig().getPreCombineField(), recordMerger,
        metaClient.getTableConfig().getProps(), Option.empty(), Option.empty());
    // Collect the records from the iterator in a map by record key to secondary key
    Map<String, String> recordKeyToSecondaryKey = new HashMap<>();
    while (fileSliceReader.hasNext()) {
      HoodieRecord record = (HoodieRecord) fileSliceReader.next();
      String secondaryKey = getSecondaryKey(record, tableSchema, indexDefinition);
      if (secondaryKey != null) {
        // no delete records here
        recordKeyToSecondaryKey.put(record.getRecordKey(tableSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD), secondaryKey);
      }
    }
    return recordKeyToSecondaryKey;
  }

  private static String getSecondaryKey(HoodieRecord record, Schema tableSchema, HoodieIndexDefinition indexDefinition) {
    try {
      if (record.toIndexedRecord(tableSchema, CollectionUtils.emptyProps()).isPresent()) {
        GenericRecord genericRecord = (GenericRecord) (record.toIndexedRecord(tableSchema, CollectionUtils.emptyProps()).get()).getData();
        String secondaryKeyFields = String.join(".", indexDefinition.getSourceFields());
        return HoodieAvroUtils.getNestedFieldValAsString(genericRecord, secondaryKeyFields, true, false);
      }
    } catch (IOException e) {
      LOG.debug("Failed to fetch secondary key for record key " + record.getKey().toString());
    }
    return null;
  }

  public static HoodieData<HoodieRecord> readSecondaryKeysFromFileSlices(HoodieEngineContext engineContext,
                                                                         List<Pair<String, FileSlice>> partitionFileSlicePairs,
                                                                         int secondaryIndexMaxParallelism,
                                                                         String activeModule, HoodieTableMetaClient metaClient, EngineType engineType,
                                                                         HoodieIndexDefinition indexDefinition) {
    if (partitionFileSlicePairs.isEmpty()) {
      return engineContext.emptyHoodieData();
    }
    final int parallelism = Math.min(partitionFileSlicePairs.size(), secondaryIndexMaxParallelism);
    final StoragePath basePath = metaClient.getBasePath();
    Schema tableSchema;
    try {
      tableSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    } catch (Exception e) {
      throw new HoodieException("Failed to get latest schema for " + metaClient.getBasePath(), e);
    }

    engineContext.setJobStatus(activeModule, "Secondary Index: reading secondary keys from " + partitionFileSlicePairs.size() + " file slices");
    return engineContext.parallelize(partitionFileSlicePairs, parallelism).flatMap(partitionAndBaseFile -> {
      final String partition = partitionAndBaseFile.getKey();
      final FileSlice fileSlice = partitionAndBaseFile.getValue();
      List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).map(l -> l.getPath().toString()).collect(Collectors.toList());
      Option<StoragePath> dataFilePath = Option.ofNullable(fileSlice.getBaseFile().map(baseFile -> filePath(basePath, partition, baseFile.getFileName())).orElseGet(null));
      Schema readerSchema;
      if (dataFilePath.isPresent()) {
        readerSchema = HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getFileFormatUtils(metaClient.getTableConfig().getBaseFileFormat())
            .readAvroSchema(metaClient.getStorage(), dataFilePath.get());
      } else {
        readerSchema = tableSchema;
      }
      return createSecondaryIndexGenerator(metaClient, engineType, logFilePaths, readerSchema, partition, dataFilePath, indexDefinition,
          metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::requestedTime).orElse(""));
    });
  }

  private static ClosableIterator<HoodieRecord> createSecondaryIndexGenerator(HoodieTableMetaClient metaClient,
                                                                              EngineType engineType, List<String> logFilePaths,
                                                                              Schema tableSchema, String partition,
                                                                              Option<StoragePath> dataFilePath,
                                                                              HoodieIndexDefinition indexDefinition,
                                                                              String instantTime) throws Exception {
    final String basePath = metaClient.getBasePath().toString();
    final StorageConfiguration<?> storageConf = metaClient.getStorageConf();

    HoodieRecordMerger recordMerger = HoodieRecordUtils.createRecordMerger(
        basePath,
        engineType,
        Collections.emptyList(),
        metaClient.getTableConfig().getRecordMergeStrategyId());

    HoodieMergedLogRecordScanner mergedLogRecordScanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(metaClient.getStorage())
        .withBasePath(metaClient.getBasePath())
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(tableSchema)
        .withLatestInstantTime(instantTime)
        .withReverseReader(false)
        .withMaxMemorySizeInBytes(storageConf.getLong(MAX_MEMORY_FOR_COMPACTION.key(), DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES))
        .withBufferSize(HoodieMetadataConfig.MAX_READER_BUFFER_SIZE_PROP.defaultValue())
        .withSpillableMapBasePath(FileIOUtils.getDefaultSpillableMapBasePath())
        .withPartition(partition)
        .withOptimizedLogBlocksScan(storageConf.getBoolean("hoodie" + HoodieMetadataConfig.OPTIMIZED_LOG_BLOCKS_SCAN, false))
        .withDiskMapType(storageConf.getEnum(SPILLABLE_DISK_MAP_TYPE.key(), SPILLABLE_DISK_MAP_TYPE.defaultValue()))
        .withBitCaskDiskMapCompressionEnabled(storageConf.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
        .withRecordMerger(recordMerger)
        .withTableMetaClient(metaClient)
        .build();

    Option<HoodieFileReader> baseFileReader = Option.empty();
    if (dataFilePath.isPresent()) {
      baseFileReader = Option.of(HoodieIOFactory.getIOFactory(metaClient.getStorage()).getReaderFactory(recordMerger.getRecordType()).getFileReader(getReaderConfigs(storageConf), dataFilePath.get()));
    }
    HoodieFileSliceReader fileSliceReader = new HoodieFileSliceReader(baseFileReader, mergedLogRecordScanner, tableSchema, metaClient.getTableConfig().getPreCombineField(), recordMerger,
        metaClient.getTableConfig().getProps(),
        Option.empty(), Option.empty());
    ClosableIterator<HoodieRecord> fileSliceIterator = ClosableIterator.wrap(fileSliceReader);
    return new ClosableIterator<HoodieRecord>() {
      private HoodieRecord nextValidRecord;

      @Override
      public void close() {
        fileSliceIterator.close();
      }

      @Override
      public boolean hasNext() {
        // As part of hasNext() we try to find the valid non-delete record that has a secondary key.
        if (nextValidRecord != null) {
          return true;
        }

        // Secondary key is null when there is a delete record, and we only have the record key.
        // This can happen when the record is deleted in the log file.
        // In this case, we need not index the record because for the given record key,
        // we have already prepared the delete record before reaching this point.
        // NOTE: Delete record should not happen when initializing the secondary index i.e. when called from readSecondaryKeysFromFileSlices,
        // because from that call, we get the merged records as of some committed instant. So, delete records must have been filtered out.
        // Loop to find the next valid record or exhaust the iterator.
        while (fileSliceIterator.hasNext()) {
          HoodieRecord record = fileSliceIterator.next();
          String secondaryKey = getSecondaryKey(record);
          if (secondaryKey != null) {
            nextValidRecord = createSecondaryIndexRecord(
                record.getRecordKey(tableSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD),
                secondaryKey,
                indexDefinition.getIndexName(),
                false
            );
            return true;
          }
        }

        // If no valid records are found
        return false;
      }

      @Override
      public HoodieRecord next() {
        if (!hasNext()) {
          throw new NoSuchElementException("No more valid records available.");
        }
        HoodieRecord result = nextValidRecord;
        nextValidRecord = null;  // Reset for the next call
        return result;
      }

      private String getSecondaryKey(HoodieRecord record) {
        try {
          if (record.toIndexedRecord(tableSchema, CollectionUtils.emptyProps()).isPresent()) {
            GenericRecord genericRecord = (GenericRecord) (record.toIndexedRecord(tableSchema, CollectionUtils.emptyProps()).get()).getData();
            String secondaryKeyFields = String.join(".", indexDefinition.getSourceFields());
            return HoodieAvroUtils.getNestedFieldValAsString(genericRecord, secondaryKeyFields, true, false);
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed to fetch records: " + e);
        }
        return null;
      }
    };
  }
}
