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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createSecondaryIndexRecord;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.filePath;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryResolveSchemaForTable;

/**
 * Utility methods for generating secondary index records during initialization and updates.
 */
public class SecondaryIndexRecordGenerationUtils {

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
   * @param writeConfig     hoodie write config.
   * @return {@link HoodieData} of {@link HoodieRecord} to be updated in the metadata table for the given secondary index partition
   */
  @VisibleForTesting
  public static <T> HoodieData<HoodieRecord> convertWriteStatsToSecondaryIndexRecords(List<HoodieWriteStat> allWriteStats,
                                                                                      String instantTime,
                                                                                      HoodieIndexDefinition indexDefinition,
                                                                                      HoodieMetadataConfig metadataConfig,
                                                                                      HoodieTableFileSystemView fsView,
                                                                                      HoodieTableMetaClient dataMetaClient,
                                                                                      HoodieEngineContext engineContext,
                                                                                      HoodieWriteConfig writeConfig
                                                                                      ) {
    TypedProperties props = writeConfig.getProps();
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

    ReaderContextFactory<T> readerContextFactory = engineContext.getReaderContextFactory(dataMetaClient);
    return engineContext.parallelize(new ArrayList<>(writeStatsByFileId.entrySet()), parallelism).flatMap(writeStatsByFileIdEntry -> {
      String fileId = writeStatsByFileIdEntry.getKey();
      List<HoodieWriteStat> writeStats = writeStatsByFileIdEntry.getValue();
      String partition = writeStats.get(0).getPartitionPath();
      StoragePath basePath = dataMetaClient.getBasePath();

      // validate that for a given fileId, either we have 1 parquet file or N log files.
      AtomicInteger totalParquetFiles = new AtomicInteger();
      AtomicInteger totalLogFiles = new AtomicInteger();
      writeStats.stream().forEach(writeStat -> {
        if (FSUtils.isLogFile(new StoragePath(basePath, writeStat.getPath()))) {
          totalLogFiles.getAndIncrement();
        } else {
          totalParquetFiles.getAndIncrement();
        }
      });

      ValidationUtils.checkArgument(!(totalParquetFiles.get() > 0 && totalLogFiles.get() > 0), "Only either of base file or log files are expected for a given file group. "
          + "Partition " + partition + ", fileId " + fileId);
      if (totalParquetFiles.get() > 0) {
        // we should expect only 1 parquet file
        ValidationUtils.checkArgument(writeStats.size() == 1, "Only one new parquet file expected per file group per commit");
      }
      // Instantiate Remote table FSV
      TableFileSystemView.SliceView sliceView = getSliceView(writeConfig,  dataMetaClient);
      Option<FileSlice> fileSliceOption = sliceView.getLatestMergedFileSliceBeforeOrOn(partition, instantTime, fileId);
      Map<String, String> recordKeyToSecondaryKeyForPreviousFileSlice;
      Map<String, String> recordKeyToSecondaryKeyForCurrentFileSlice;
      if (fileSliceOption.isPresent()) { // if previous file slice is present.
        recordKeyToSecondaryKeyForPreviousFileSlice =
            getRecordKeyToSecondaryKey(dataMetaClient, readerContextFactory.getContext(), fileSliceOption.get(), tableSchema, indexDefinition, instantTime, props, false);
        // branch out based on whether new parquet file is added or log files are added.
        if (totalParquetFiles.get() > 0) { // new base file/file slice is created in current commit.
          FileSlice currentFileSliceForFileId = new FileSlice(partition, instantTime, fileId);
          HoodieWriteStat stat = writeStats.get(0);
          StoragePathInfo baseFilePathInfo = new StoragePathInfo(new StoragePath(basePath, stat.getPath()), stat.getFileSizeInBytes(), false, (short) 0, 0, 0);
          currentFileSliceForFileId.setBaseFile(new HoodieBaseFile(baseFilePathInfo));
          recordKeyToSecondaryKeyForCurrentFileSlice =
              getRecordKeyToSecondaryKey(dataMetaClient, readerContextFactory.getContext(), currentFileSliceForFileId, tableSchema, indexDefinition, instantTime, props, true);
        } else { // log files are added in current commit
          // add new log files to existing latest file slice and compute the secondary index to primary key mapping.
          FileSlice latestFileSlice = fileSliceOption.get();
          writeStats.stream().forEach(writeStat -> {
            StoragePathInfo logFile = new StoragePathInfo(new StoragePath(basePath, writeStat.getPath()), writeStat.getFileSizeInBytes(), false, (short) 0, 0, 0);
            latestFileSlice.addLogFile(new HoodieLogFile(logFile));
          });
          recordKeyToSecondaryKeyForCurrentFileSlice =
              getRecordKeyToSecondaryKey(dataMetaClient, readerContextFactory.getContext(), latestFileSlice, tableSchema, indexDefinition, instantTime, props, true);
        }
      } else { // new file group
        recordKeyToSecondaryKeyForPreviousFileSlice = Collections.emptyMap(); // previous slice is empty.
        FileSlice currentFileSliceForFileId = new FileSlice(partition, instantTime, fileId);
        HoodieWriteStat stat = writeStats.get(0);
        StoragePathInfo baseFilePathInfo = new StoragePathInfo(new StoragePath(basePath, stat.getPath()), stat.getFileSizeInBytes(), false, (short) 0, 0, 0);
        currentFileSliceForFileId.setBaseFile(new HoodieBaseFile(baseFilePathInfo));
        recordKeyToSecondaryKeyForCurrentFileSlice =
            getRecordKeyToSecondaryKey(dataMetaClient, readerContextFactory.getContext(), currentFileSliceForFileId, tableSchema, indexDefinition, instantTime, props, true);
      }   // Need to find what secondary index record should be deleted, and what should be inserted.
      // For each entry in recordKeyToSecondaryKeyForCurrentFileSlice, if it is not present in recordKeyToSecondaryKeyForPreviousFileSlice, then it should be inserted.
      // For each entry in recordKeyToSecondaryKeyForCurrentFileSlice, if it is present in recordKeyToSecondaryKeyForPreviousFileSlice, then it should be updated.
      // For each entry in recordKeyToSecondaryKeyForPreviousFileSlice, if it is not present in recordKeyToSecondaryKeyForCurrentFileSlice, then it should be deleted.
      List<HoodieRecord> records = new ArrayList<>();
      recordKeyToSecondaryKeyForCurrentFileSlice.forEach((recordKey, secondaryKey) -> {
        if (!recordKeyToSecondaryKeyForPreviousFileSlice.containsKey(recordKey)) {
          records.add(createSecondaryIndexRecord(recordKey, secondaryKey, indexDefinition.getIndexName(), false));
        } else {
          // delete previous entry and insert new value if secondaryKey is different
          String previousSecondaryKey = recordKeyToSecondaryKeyForPreviousFileSlice.get(recordKey);
          if (!Objects.equals(previousSecondaryKey, secondaryKey)) {
            records.add(createSecondaryIndexRecord(recordKey, previousSecondaryKey, indexDefinition.getIndexName(), true));
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

  private static TableFileSystemView.SliceView getSliceView(HoodieWriteConfig config, HoodieTableMetaClient dataMetaClient) {
    HoodieEngineContext context = new HoodieLocalEngineContext(dataMetaClient.getStorageConf());
    FileSystemViewManager viewManager = FileSystemViewManager.createViewManager(context, config.getMetadataConfig(), config.getViewStorageConfig(),
        config.getCommonConfig(), unused -> getMetadataTable(config, dataMetaClient, context));
    return viewManager.getFileSystemView(dataMetaClient);
  }

  private static HoodieTableMetadata getMetadataTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient, HoodieEngineContext context) {
    return metaClient.getTableFormat().getMetadataFactory().create(context, metaClient.getStorage(), config.getMetadataConfig(), config.getBasePath());
  }

  public static <T> Map<String, String> getRecordKeyToSecondaryKey(HoodieTableMetaClient metaClient,
                                                                    HoodieReaderContext<T> readerContext,
                                                                    FileSlice fileSlice,
                                                                    Schema tableSchema,
                                                                    HoodieIndexDefinition indexDefinition,
                                                                    String instantTime,
                                                                    TypedProperties props,
                                                                    boolean allowInflightInstants) throws IOException {
    Map<String, String> recordKeyToSecondaryKey = new HashMap<>();
    try (ClosableIterator<Pair<String, String>> recordKeyAndSecondaryIndexValueIter =
             createSecondaryIndexRecordGenerator(readerContext, metaClient, fileSlice, tableSchema, indexDefinition, instantTime, props, allowInflightInstants)) {
      while (recordKeyAndSecondaryIndexValueIter.hasNext()) {
        Pair<String, String> recordKeyAndSecondaryIndexValue = recordKeyAndSecondaryIndexValueIter.next();
        recordKeyToSecondaryKey.put(recordKeyAndSecondaryIndexValue.getKey(), recordKeyAndSecondaryIndexValue.getValue());
      }
    }
    return recordKeyToSecondaryKey;
  }

  public static <T> HoodieData<HoodieRecord> readSecondaryKeysFromFileSlices(HoodieEngineContext engineContext,
                                                                             List<Pair<String, FileSlice>> partitionFileSlicePairs,
                                                                             int secondaryIndexMaxParallelism,
                                                                             String activeModule, HoodieTableMetaClient metaClient,
                                                                             HoodieIndexDefinition indexDefinition,
                                                                             TypedProperties props) {
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
    ReaderContextFactory<T> readerContextFactory = engineContext.getReaderContextFactory(metaClient);
    engineContext.setJobStatus(activeModule, "Secondary Index: reading secondary keys from " + partitionFileSlicePairs.size() + " file slices");
    HoodieFileFormat baseFileFormat = metaClient.getTableConfig().getBaseFileFormat();
    return engineContext.parallelize(partitionFileSlicePairs, parallelism).flatMap(partitionAndBaseFile -> {
      final String partition = partitionAndBaseFile.getKey();
      final FileSlice fileSlice = partitionAndBaseFile.getValue();
      Option<StoragePath> dataFilePath = Option.ofNullable(fileSlice.getBaseFile().map(baseFile -> filePath(basePath, partition, baseFile.getFileName())).orElseGet(null));
      Schema readerSchema;
      if (dataFilePath.isPresent()) {
        readerSchema = HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getFileFormatUtils(baseFileFormat)
            .readAvroSchema(metaClient.getStorage(), dataFilePath.get());
      } else {
        readerSchema = tableSchema;
      }
      ClosableIterator<Pair<String, String>> secondaryIndexGenerator = createSecondaryIndexRecordGenerator(readerContextFactory.getContext(), metaClient, fileSlice, readerSchema, indexDefinition,
          metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::requestedTime).orElse(""), props, false);
      return new CloseableMappingIterator<>(secondaryIndexGenerator, pair -> createSecondaryIndexRecord(pair.getKey(), pair.getValue(), indexDefinition.getIndexName(), false));
    });
  }

  /**
   * Constructs an iterator with a pair of the record key and the secondary index value for each record in the file slice.
   */
  private static <T> ClosableIterator<Pair<String, String>> createSecondaryIndexRecordGenerator(HoodieReaderContext<T> readerContext,
                                                                                                HoodieTableMetaClient metaClient,
                                                                                                FileSlice fileSlice,
                                                                                                Schema tableSchema,
                                                                                                HoodieIndexDefinition indexDefinition,
                                                                                                String instantTime,
                                                                                                TypedProperties props,
                                                                                                boolean allowInflightInstants) throws IOException {
    String secondaryKeyField = indexDefinition.getSourceFieldsKey();
    Schema requestedSchema = getRequestedSchemaForSecondaryIndex(metaClient, tableSchema, secondaryKeyField);
    HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>newBuilder()
        .withReaderContext(readerContext)
        .withFileSlice(fileSlice)
        .withHoodieTableMetaClient(metaClient)
        .withProps(props)
        .withLatestCommitTime(instantTime)
        .withDataSchema(tableSchema)
        .withRequestedSchema(requestedSchema)
        .withAllowInflightInstants(allowInflightInstants)
        .build();

    return new ClosableIterator<Pair<String, String>>() {
      private final ClosableIterator<T> recordIterator = fileGroupReader.getClosableIterator();
      private Pair<String, String> nextValidRecord;

      @Override
      public void close() {
        recordIterator.close();
      }

      @Override
      public boolean hasNext() {
        // As part of hasNext() we try to find the valid non-delete record that has a secondary key.
        if (nextValidRecord != null) {
          return true;
        }

        while (recordIterator.hasNext()) {
          T record = recordIterator.next();
          Object secondaryKey = readerContext.getRecordContext().getValue(record, requestedSchema, secondaryKeyField);
            nextValidRecord = Pair.of(
                readerContext.getRecordContext().getRecordKey(record, requestedSchema),
                secondaryKey == null ? null : secondaryKey.toString()
            );
          return true;
        }

        // If no valid records are found
        return false;
      }

      @Override
      public Pair<String, String> next() {
        if (!hasNext()) {
          throw new NoSuchElementException("No more valid records available.");
        }
        Pair<String, String> result = nextValidRecord;
        nextValidRecord = null;  // Reset for the next call
        return result;
      }
    };
  }

  private static Schema getRequestedSchemaForSecondaryIndex(HoodieTableMetaClient metaClient, Schema tableSchema, String secondaryKeyField) {
    String[] recordKeyFields;
    if (tableSchema.getField(RECORD_KEY_METADATA_FIELD) != null) {
      recordKeyFields = new String[] {RECORD_KEY_METADATA_FIELD};
    } else {
      recordKeyFields = metaClient.getTableConfig().getRecordKeyFields().orElse(new String[0]);
    }
    String[] projectionFields = Arrays.copyOf(recordKeyFields, recordKeyFields.length + 1);
    projectionFields[recordKeyFields.length] = secondaryKeyField;
    return HoodieAvroUtils.projectSchema(tableSchema, Arrays.asList(projectionFields));
  }
}
