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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieMetadataPayload.createSecondaryIndexRecord;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.filePath;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionLatestFileSlicesIncludingInflight;
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
   * @param props           the writer properties
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
                                                                                      TypedProperties props) {
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
      FileSlice previousFileSliceForFileId = fsView.getLatestFileSlice(partition, fileId).orElse(null);
      Map<String, String> recordKeyToSecondaryKeyForPreviousFileSlice;
      if (previousFileSliceForFileId == null) {
        // new file slice, so empty mapping for previous slice
        recordKeyToSecondaryKeyForPreviousFileSlice = Collections.emptyMap();
      } else {
        recordKeyToSecondaryKeyForPreviousFileSlice =
            getRecordKeyToSecondaryKey(dataMetaClient, readerContextFactory.getContext(), previousFileSliceForFileId, tableSchema, indexDefinition, instantTime, props, false);
      }
      List<FileSlice> latestIncludingInflightFileSlices = getPartitionLatestFileSlicesIncludingInflight(dataMetaClient, Option.empty(), partition);
      FileSlice currentFileSliceForFileId = latestIncludingInflightFileSlices.stream().filter(fs -> fs.getFileId().equals(fileId)).findFirst()
          .orElseThrow(() -> new HoodieException("Could not find any file slice for fileId " + fileId));
      Map<String, String> recordKeyToSecondaryKeyForCurrentFileSlice =
          getRecordKeyToSecondaryKey(dataMetaClient, readerContextFactory.getContext(), currentFileSliceForFileId, tableSchema, indexDefinition, instantTime, props, true);
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
          String previousSecondaryKey = recordKeyToSecondaryKeyForPreviousFileSlice.get(recordKey);
          if (!previousSecondaryKey.equals(secondaryKey)) {
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

  private static <T> Map<String, String> getRecordKeyToSecondaryKey(HoodieTableMetaClient metaClient,
                                                                    HoodieReaderContext<T> readerContext,
                                                                    FileSlice fileSlice,
                                                                    Schema tableSchema,
                                                                    HoodieIndexDefinition indexDefinition,
                                                                    String instantTime,
                                                                    TypedProperties props,
                                                                    boolean allowInflightInstants) throws Exception {
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
    return engineContext.parallelize(partitionFileSlicePairs, parallelism).flatMap(partitionAndBaseFile -> {
      final String partition = partitionAndBaseFile.getKey();
      final FileSlice fileSlice = partitionAndBaseFile.getValue();
      Option<StoragePath> dataFilePath = Option.ofNullable(fileSlice.getBaseFile().map(baseFile -> filePath(basePath, partition, baseFile.getFileName())).orElseGet(null));
      Schema readerSchema;
      if (dataFilePath.isPresent()) {
        readerSchema = HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getFileFormatUtils(metaClient.getTableConfig().getBaseFileFormat())
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
                                                                                                boolean allowInflightInstants) throws Exception {
    String secondaryKeyField = String.join(".", indexDefinition.getSourceFields());
    HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>newBuilder()
        .withReaderContext(readerContext)
        .withFileSlice(fileSlice)
        .withHoodieTableMetaClient(metaClient)
        .withProps(props)
        .withLatestCommitTime(instantTime)
        .withDataSchema(tableSchema)
        .withRequestedSchema(tableSchema)
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

        // Secondary key is null when there is a delete record, and we only have the record key.
        // This can happen when the record is deleted in the log file.
        // In this case, we need not index the record because for the given record key,
        // we have already prepared the delete record before reaching this point.
        // NOTE: Delete record should not happen when initializing the secondary index i.e. when called from readSecondaryKeysFromFileSlices,
        // because from that call, we get the merged records as of some committed instant. So, delete records must have been filtered out.
        // Loop to find the next valid record or exhaust the iterator.
        while (recordIterator.hasNext()) {
          T record = recordIterator.next();
          Object secondaryKey = readerContext.getValue(record, tableSchema, secondaryKeyField);
          if (secondaryKey != null) {
            nextValidRecord = Pair.of(
                readerContext.getRecordKey(record, tableSchema),
                secondaryKey.toString()
            );
            return true;
          }
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
}
