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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.index.model.DataPartitionAndRecords;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.schema.HoodieSchemaUtils.getRecordKeySchema;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.RECORD_INDEX_AVERAGE_RECORD_SIZE;

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
    records.unpersist();
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
          .filter(fs -> fs.getBaseFile().isPresent())
          .map(fs -> fs.getBaseFile().get().getStoragePath())
          .collect(Collectors.toList());

      // Count records in a distributed manner using the engine context
      long totalRecords = countRecordsInHFiles(baseFilePaths, metadataMetaClient);
      long expectedRecordCount = recordIndexRecords.count();

      ValidationUtils.checkArgument(totalRecords == expectedRecordCount, "Record Count Validation failed with "
          + totalRecords + " present in record index vs the expected " + expectedRecordCount);
      log.info(String.format("Record index initialized on %d shards (expected = %d) with %d records (expected = %d)",
          fileSlices.size(), fileGroupCount, totalRecords, expectedRecordCount));
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
   * @param partitionFileSlicePairs   list of pairs of partition and file slice.
   * @param recordIndexMaxParallelism parallelism to use.
   * @param activeModule              active module of interest.
   * @param metaClient                metaclient instance to use.
   * @param dataWriteConfig           write config to use.
   * @return metadata records for initializing record index entries.
   */
  protected <T> HoodieData<HoodieRecord> readRecordKeysFromFileSliceSnapshot(
      HoodieEngineContext engineContext,
      List<FileSliceAndPartition> partitionFileSlicePairs,
      int recordIndexMaxParallelism,
      String activeModule,
      HoodieTableMetaClient metaClient,
      HoodieWriteConfig dataWriteConfig) {
    if (partitionFileSlicePairs.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    Option<String> instantTime = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::requestedTime);
    if (!instantTime.isPresent()) {
      return engineContext.emptyHoodieData();
    }

    engineContext.setJobStatus(activeModule, "Record Index: reading record keys from " + partitionFileSlicePairs.size() + " file slices");
    final int parallelism = Math.min(partitionFileSlicePairs.size(), recordIndexMaxParallelism);
    ReaderContextFactory<T> readerContextFactory = engineContext.getReaderContextFactory(metaClient);
    return engineContext.parallelize(partitionFileSlicePairs, parallelism).flatMap(partitionAndFileSlice -> {
      final String partition = partitionAndFileSlice.partitionPath();
      final FileSlice fileSlice = partitionAndFileSlice.fileSlice();
      final String fileId = fileSlice.getFileId();
      HoodieReaderContext<T> readerContext = readerContextFactory.getContext();
      HoodieSchema dataSchema = HoodieSchemaCache.intern(HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(dataWriteConfig.getWriteSchema()), dataWriteConfig.allowOperationMetadataField()));
      HoodieSchema requestedSchema = metaClient.getTableConfig().populateMetaFields() ? getRecordKeySchema()
          : HoodieSchemaUtils.projectSchema(dataSchema, Arrays.asList(metaClient.getTableConfig().getRecordKeyFields().orElse(new String[0])));
      Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(dataWriteConfig.getInternalSchema());
      HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>newBuilder()
          .withReaderContext(readerContext)
          .withHoodieTableMetaClient(metaClient)
          .withFileSlice(fileSlice)
          .withLatestCommitTime(instantTime.get())
          .withDataSchema(dataSchema)
          .withRequestedSchema(requestedSchema)
          .withInternalSchema(internalSchemaOption)
          .withShouldUseRecordPosition(false)
          .withProps(metaClient.getTableConfig().getProps())
          .build();
      String baseFileInstantTime = fileSlice.getBaseInstantTime();
      return new CloseableMappingIterator<>(fileGroupReader.getClosableIterator(), record -> {
        String recordKey = readerContext.getRecordContext().getRecordKey(record, requestedSchema);
        return HoodieMetadataPayload.createRecordIndexUpdate(recordKey, partition, fileId,
            baseFileInstantTime, 0);
      });
    });
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
}
