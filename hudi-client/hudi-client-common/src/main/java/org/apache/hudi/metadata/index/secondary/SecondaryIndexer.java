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

package org.apache.hudi.metadata.index.secondary;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.MAX_MEMORY_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createSecondaryIndexRecord;
import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getIndexDefinition;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.filePath;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getIndexPartitionsToInit;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.isNewSecondaryIndexDefinitionRequired;
import static org.apache.hudi.metadata.index.record.RecordIndexer.RECORD_INDEX_AVERAGE_RECORD_SIZE;

/**
 * Implementation of {@link SECONDARY_INDEX} index
 */
public class SecondaryIndexer implements Indexer {
  private static final Logger LOG = LoggerFactory.getLogger(SecondaryIndexer.class);
  private final HoodieEngineContext engineContext;
  private final EngineType engineType;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final Lazy<Set<String>> secondaryIndexPartitionsToInit;

  public SecondaryIndexer(HoodieEngineContext engineContext,
                          EngineType engineType,
                          HoodieWriteConfig dataTableWriteConfig,
                          HoodieTableMetaClient dataTableMetaClient) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.engineType = engineType;
    this.secondaryIndexPartitionsToInit = Lazy.lazily(() -> {
      HoodieMetadataConfig metadataConfig = dataTableWriteConfig.getMetadataConfig();
      return getIndexPartitionsToInit(
          SECONDARY_INDEX,
          metadataConfig,
          dataTableMetaClient,
          () -> isNewSecondaryIndexDefinitionRequired(metadataConfig, dataTableMetaClient),
          metadataConfig::getSecondaryIndexColumn,
          metadataConfig::getSecondaryIndexName,
          PARTITION_NAME_SECONDARY_INDEX_PREFIX,
          PARTITION_NAME_SECONDARY_INDEX
      );
    });
  }

  @Override
  public List<InitialIndexPartitionData> initialize(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    if (secondaryIndexPartitionsToInit.get().size() != 1) {
      if (secondaryIndexPartitionsToInit.get().size() > 1) {
        LOG.warn("Skipping secondary index initialization as only one secondary index "
                + "bootstrap at a time is supported for now. Provided: {}",
            secondaryIndexPartitionsToInit.get());
      }
      return Collections.emptyList();
    }
    String indexName = secondaryIndexPartitionsToInit.get().iterator().next();

    HoodieIndexDefinition indexDefinition = getIndexDefinition(dataTableMetaClient, indexName);
    ValidationUtils.checkState(indexDefinition != null, "Secondary Index definition is not present for index " + indexName);
    List<Pair<String, FileSlice>> partitionFileSlicePairs = Indexer.getPartitionFileSlicePairs(
        dataTableMetaClient, metadata, fsView.get());

    int parallelism = Math.min(partitionFileSlicePairs.size(),
        dataTableWriteConfig.getMetadataConfig().getSecondaryIndexParallelism());
    HoodieData<HoodieRecord> records = readSecondaryKeysFromFileSlices(
        engineContext,
        partitionFileSlicePairs,
        parallelism,
        this.getClass().getSimpleName(),
        dataTableMetaClient,
        engineType,
        indexDefinition);

    // Initialize the file groups - using the same estimation logic as that of record index
    final int numFileGroup = HoodieTableMetadataUtil.estimateFileGroupCount(
        RECORD_INDEX, records.count(), RECORD_INDEX_AVERAGE_RECORD_SIZE,
        dataTableWriteConfig.getRecordIndexMinFileGroupCount(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupCount(),
        dataTableWriteConfig.getRecordIndexGrowthFactor(),
        dataTableWriteConfig.getRecordIndexMaxFileGroupSizeBytes());

    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, secondaryIndexPartitionsToInit.get().iterator().next(), records));
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
