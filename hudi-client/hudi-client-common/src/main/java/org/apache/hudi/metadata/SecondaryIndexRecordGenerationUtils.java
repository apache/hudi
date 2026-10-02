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
import org.apache.hudi.common.data.HoodieBroadcast;
import org.apache.hudi.common.data.HoodieBroadcastScope;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createSecondaryIndexRecord;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryResolveSchemaForTable;

/**
 * Utility methods for generating secondary index records during initialization and updates.
 */
@Slf4j
public class SecondaryIndexRecordGenerationUtils {

  /**
   * Converts the write stats to secondary index records.
   *
   * @param allWriteStats   list of write stats
   * @param instantTime     instant time
   * @param indexDefinition secondary index definition
   * @param metadataConfig  metadata config
   * @param dataMetaClient  data table meta client
   * @param engineContext   engine context
   * @param writeConfig     hoodie write config.
   * @param commitMetadata  metadata of the commit the write stats belong to. Provides the schema when the table has no
   *                        completed commit yet, and the replaced file groups for a replace commit.
   * @return {@link HoodieData} of {@link HoodieRecord} to be updated in the metadata table for the given secondary index partition
   */
  @VisibleForTesting
  public static <T> HoodieData<HoodieRecord> convertWriteStatsToSecondaryIndexRecords(List<HoodieWriteStat> allWriteStats,
                                                                                      String instantTime,
                                                                                      HoodieIndexDefinition indexDefinition,
                                                                                      HoodieMetadataConfig metadataConfig,
                                                                                      HoodieTableMetaClient dataMetaClient,
                                                                                      HoodieEngineContext engineContext,
                                                                                      HoodieWriteConfig writeConfig,
                                                                                      HoodieCommitMetadata commitMetadata) {
    return convertWriteStatsToSecondaryIndexRecords(allWriteStats, instantTime, indexDefinition, metadataConfig, dataMetaClient, engineContext,
        writeConfig, commitMetadata, Lazy.lazily(() -> getPreviousFileSlices(allWriteStats, instantTime, dataMetaClient, writeConfig, commitMetadata)),
        new HoodieBroadcastScope(engineContext));
  }

  /**
   * Converts the write stats to secondary index records, with the previous file slices of the file groups that the
   * commit writes or replaces looked up by {@link #getPreviousFileSlices}. The file slices do not depend on the index,
   * so the secondary indexes of a table share one lookup per commit. The broadcasts are made in the given scope.
   */
  public static <T> HoodieData<HoodieRecord> convertWriteStatsToSecondaryIndexRecords(List<HoodieWriteStat> allWriteStats,
                                                                                      String instantTime,
                                                                                      HoodieIndexDefinition indexDefinition,
                                                                                      HoodieMetadataConfig metadataConfig,
                                                                                      HoodieTableMetaClient dataMetaClient,
                                                                                      HoodieEngineContext engineContext,
                                                                                      HoodieWriteConfig writeConfig,
                                                                                      HoodieCommitMetadata commitMetadata,
                                                                                      Lazy<Map<HoodieFileGroupId, FileSlice>> previousFileSlices,
                                                                                      HoodieBroadcastScope broadcastScope) {
    // Secondary index cannot support logs having inserts with current offering. So, lets validate that.
    if (allWriteStats.stream().anyMatch(writeStat -> {
      String fileName = FSUtils.getFileName(writeStat.getPath(), writeStat.getPartitionPath());
      return FSUtils.isLogFile(fileName) && writeStat.getNumInserts() > 0;
    })) {
      throw new HoodieIOException("Secondary index cannot support logs having inserts with current offering. Please disable secondary index.");
    }

    HoodieSchema tableSchema = resolveTableSchema(dataMetaClient, commitMetadata);
    Map<String, List<HoodieWriteStat>> writeStatsByFileId = allWriteStats.stream().collect(Collectors.groupingBy(HoodieWriteStat::getFileId));
    int parallelism = Math.max(Math.min(writeStatsByFileId.size(), metadataConfig.getSecondaryIndexParallelism()), 1);

    List<HoodieFileGroupId> replacedFileGroups = getReplacedFileGroupsNotWritten(commitMetadata, dataMetaClient, writeStatsByFileId.keySet());
    Map<HoodieFileGroupId, FileSlice> latestFileSlices = previousFileSlices.get();

    List<Pair<List<HoodieWriteStat>, Option<FileSlice>>> fileGroupWrites = writeStatsByFileId.entrySet().stream()
        .map(entry -> Pair.of(entry.getValue(), Option.ofNullable(latestFileSlices.get(
            new HoodieFileGroupId(entry.getValue().get(0).getPartitionPath(), entry.getKey())))))
        .collect(Collectors.toList());
    TypedProperties props = writeConfig.getProps();
    ReaderContextFactory<T> readerContextFactory = engineContext.getReaderContextFactory(dataMetaClient);
    HoodieBroadcast<HoodieTableMetaClient> metaClientBroadcast = broadcastScope.broadcast(dataMetaClient);
    HoodieData<HoodieRecord> secondaryIndexRecords = engineContext.parallelize(fileGroupWrites, parallelism).flatMap(fileGroupWrite ->
        convertFileGroupWriteToSecondaryIndexRecords(fileGroupWrite.getKey(), fileGroupWrite.getValue(), metaClientBroadcast.value(),
            readerContextFactory.getContext(), tableSchema, indexDefinition, instantTime, props));

    if (!replacedFileGroups.isEmpty()) {
      secondaryIndexRecords = secondaryIndexRecords.union(convertReplacedFileGroupsToSecondaryIndexRecords(
          replacedFileGroups, latestFileSlices, instantTime, indexDefinition, metadataConfig, metaClientBroadcast, readerContextFactory,
          engineContext, props, tableSchema));
    }

    // Deduplicate secondary index records by grouping by the secondary index key
    // (secondaryKey$recordKey). This handles the case where a record moves from one file group to
    // another (partition path update), which generates both a delete (from old fileId) and an
    // insert (to new fileId). Similar to how Record Level Index handles partition path update,
    // we prefer non-deleted records.
    return HoodieTableMetadataUtil.reduceByKeys(secondaryIndexRecords, parallelism, false);
  }

  /**
   * Resolves the schema of the table from its completed commits. A table without any completed commit, e.g. one
   * that registers files written outside Hudi for the first time, only has the schema of the current commit, which
   * Hudi stores as an empty string when the commit carries no schema.
   */
  private static HoodieSchema resolveTableSchema(HoodieTableMetaClient dataMetaClient, HoodieCommitMetadata commitMetadata) {
    Option<HoodieSchema> tableSchema;
    try {
      tableSchema = tryResolveSchemaForTable(dataMetaClient);
    } catch (Exception e) {
      throw new HoodieException("Failed to get latest schema for " + dataMetaClient.getBasePath(), e);
    }
    if (tableSchema.isPresent()) {
      return tableSchema.get();
    }
    String commitSchema = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
    if (StringUtils.isNullOrEmpty(commitSchema)) {
      throw new HoodieException("Table " + dataMetaClient.getBasePath() + " has no completed commit to resolve its schema from and the commit "
          + "metadata carries no " + HoodieCommitMetadata.SCHEMA_KEY);
    }
    return HoodieSchema.parse(commitSchema);
  }

  /**
   * Converts the writes of one file group in the commit to secondary index records.
   *
   * @param writeStats        write stats of the file group
   * @param previousFileSlice latest merged file slice of the file group before the commit, if any
   */
  private static <T> Iterator<HoodieRecord> convertFileGroupWriteToSecondaryIndexRecords(List<HoodieWriteStat> writeStats,
                                                                                        Option<FileSlice> previousFileSlice,
                                                                                        HoodieTableMetaClient dataMetaClient,
                                                                                        HoodieReaderContext<T> readerContext,
                                                                                        HoodieSchema tableSchema,
                                                                                        HoodieIndexDefinition indexDefinition,
                                                                                        String instantTime,
                                                                                        TypedProperties props) throws IOException {
    String fileId = writeStats.get(0).getFileId();
    String partition = writeStats.get(0).getPartitionPath();
    StoragePath basePath = dataMetaClient.getBasePath();

    // validate that for a given fileId, either we have 1 parquet file or N log files.
    AtomicInteger totalParquetFiles = new AtomicInteger();
    AtomicInteger totalLogFiles = new AtomicInteger();
    writeStats.forEach(writeStat -> {
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
    Map<String, String> recordKeyToSecondaryKeyForPreviousFileSlice;
    Map<String, String> recordKeyToSecondaryKeyForCurrentFileSlice;
    if (previousFileSlice.isPresent()) { // if previous file slice is present.
      recordKeyToSecondaryKeyForPreviousFileSlice =
          getRecordKeyToSecondaryKey(dataMetaClient, readerContext, previousFileSlice.get(), tableSchema, indexDefinition, instantTime, props, false);
      // branch out based on whether new parquet file is added or log files are added.
      if (totalParquetFiles.get() > 0) { // new base file/file slice is created in current commit.
        FileSlice currentFileSliceForFileId = new FileSlice(partition, instantTime, fileId);
        HoodieWriteStat stat = writeStats.get(0);
        StoragePathInfo baseFilePathInfo = new StoragePathInfo(new StoragePath(basePath, stat.getPath()), stat.getFileSizeInBytes(), false, (short) 0, 0, 0);
        currentFileSliceForFileId.setBaseFile(new HoodieBaseFile(baseFilePathInfo));
        recordKeyToSecondaryKeyForCurrentFileSlice =
            getRecordKeyToSecondaryKey(dataMetaClient, readerContext, currentFileSliceForFileId, tableSchema, indexDefinition, instantTime, props, true);
      } else { // log files are added in current commit
        // add new log files to a copy of the latest file slice and compute the secondary index to primary key mapping.
        FileSlice latestFileSlice = new FileSlice(previousFileSlice.get());
        writeStats.forEach(writeStat -> {
          StoragePathInfo logFile = new StoragePathInfo(new StoragePath(basePath, writeStat.getPath()), writeStat.getFileSizeInBytes(), false, (short) 0, 0, 0);
          latestFileSlice.addLogFile(new HoodieLogFile(logFile));
        });
        recordKeyToSecondaryKeyForCurrentFileSlice =
            getRecordKeyToSecondaryKey(dataMetaClient, readerContext, latestFileSlice, tableSchema, indexDefinition, instantTime, props, true);
      }
    } else { // new file group
      recordKeyToSecondaryKeyForPreviousFileSlice = Collections.emptyMap(); // previous slice is empty.
      FileSlice currentFileSliceForFileId = new FileSlice(partition, instantTime, fileId);
      HoodieWriteStat stat = writeStats.get(0);
      StoragePathInfo baseFilePathInfo = new StoragePathInfo(new StoragePath(basePath, stat.getPath()), stat.getFileSizeInBytes(), false, (short) 0, 0, 0);
      currentFileSliceForFileId.setBaseFile(new HoodieBaseFile(baseFilePathInfo));
      recordKeyToSecondaryKeyForCurrentFileSlice =
          getRecordKeyToSecondaryKey(dataMetaClient, readerContext, currentFileSliceForFileId, tableSchema, indexDefinition, instantTime, props, true);
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
  }

  /**
   * Looks up the latest merged file slice before or on the instant of the commit of each file group that the commit
   * writes, or replaces without writing to it again. File groups without such a file slice are absent from the result.
   */
  public static Map<HoodieFileGroupId, FileSlice> getPreviousFileSlices(List<HoodieWriteStat> allWriteStats,
                                                                        String instantTime,
                                                                        HoodieTableMetaClient dataMetaClient,
                                                                        HoodieWriteConfig writeConfig,
                                                                        HoodieCommitMetadata commitMetadata) {
    Map<String, String> partitionByWrittenFileId = new HashMap<>();
    allWriteStats.forEach(writeStat -> partitionByWrittenFileId.putIfAbsent(writeStat.getFileId(), writeStat.getPartitionPath()));
    List<HoodieFileGroupId> fileGroupIds = partitionByWrittenFileId.entrySet().stream()
        .map(entry -> new HoodieFileGroupId(entry.getValue(), entry.getKey()))
        .collect(Collectors.toCollection(ArrayList::new));
    fileGroupIds.addAll(getReplacedFileGroupsNotWritten(commitMetadata, dataMetaClient, partitionByWrittenFileId.keySet()));
    return getLatestMergedFileSlices(writeConfig, dataMetaClient, instantTime, fileGroupIds);
  }

  /**
   * Returns the file groups that a replace commit without a known operation type replaces on a table without record
   * keys, and does not write to again. Such a commit registers files written outside Hudi and drops the replaced file
   * groups, e.g. files superseded by newer files, without rewriting their records under the same key. A file group
   * that the same commit writes again is skipped, because its rewritten records reach the index through the write stats.
   */
  private static List<HoodieFileGroupId> getReplacedFileGroupsNotWritten(HoodieCommitMetadata commitMetadata, HoodieTableMetaClient dataMetaClient,
                                                                         Set<String> writtenFileIds) {
    if (!(commitMetadata instanceof HoodieReplaceCommitMetadata) || !WriteOperationType.isUnknown(commitMetadata.getOperationType())
        || dataMetaClient.getTableConfig().hasRecordKey()) {
      return Collections.emptyList();
    }
    return ((HoodieReplaceCommitMetadata) commitMetadata).getPartitionToReplaceFileIds().entrySet().stream()
        .flatMap(partitionAndFileIds -> partitionAndFileIds.getValue().stream()
            .filter(fileId -> !writtenFileIds.contains(fileId))
            .map(fileId -> new HoodieFileGroupId(partitionAndFileIds.getKey(), fileId)))
        .collect(Collectors.toList());
  }

  /**
   * Generates delete records for every record of the replaced file groups that are not written again by the commit.
   * A keyed table that takes this path can delete and insert the same key in one commit when a record moves
   * between file groups; {@link HoodieTableMetadataUtil#reduceByKeys} then prefers the non-deleted record.
   * A table without record keys never writes a file group it replaces, because the record index rejects such a commit.
   */
  private static <T> HoodieData<HoodieRecord> convertReplacedFileGroupsToSecondaryIndexRecords(List<HoodieFileGroupId> replacedFileGroups,
                                                                                               Map<HoodieFileGroupId, FileSlice> latestFileSlices,
                                                                                               String instantTime,
                                                                                               HoodieIndexDefinition indexDefinition,
                                                                                               HoodieMetadataConfig metadataConfig,
                                                                                               HoodieBroadcast<HoodieTableMetaClient> metaClientBroadcast,
                                                                                               ReaderContextFactory<T> readerContextFactory,
                                                                                               HoodieEngineContext engineContext,
                                                                                               TypedProperties props,
                                                                                               HoodieSchema tableSchema) {
    List<FileSlice> replacedFileSlices = new ArrayList<>();
    for (HoodieFileGroupId replacedFileGroup : replacedFileGroups) {
      FileSlice replacedFileSlice = latestFileSlices.get(replacedFileGroup);
      if (replacedFileSlice == null) {
        log.warn("Replaced file group {} in partition {} has no file slice to read the secondary keys to delete from",
            replacedFileGroup.getFileId(), replacedFileGroup.getPartitionPath());
      } else {
        replacedFileSlices.add(replacedFileSlice);
      }
    }
    if (replacedFileSlices.isEmpty()) {
      return engineContext.emptyHoodieData();
    }
    int parallelism = Math.max(Math.min(replacedFileSlices.size(), metadataConfig.getSecondaryIndexParallelism()), 1);
    return engineContext.parallelize(replacedFileSlices, parallelism).flatMap(replacedFileSlice -> {
      ClosableIterator<Pair<String, String>> recordKeyAndSecondaryKeyIterator = createSecondaryIndexRecordGenerator(
          readerContextFactory.getContext(), metaClientBroadcast.value(), replacedFileSlice, tableSchema, indexDefinition, instantTime, props, false);
      return new CloseableMappingIterator<>(recordKeyAndSecondaryKeyIterator,
          pair -> createSecondaryIndexRecord(pair.getKey(), pair.getValue(), indexDefinition.getIndexName(), true));
    });
  }

  /**
   * Looks up the latest merged file slice before or on the given instant of each file group, on one file system view
   * that is closed afterwards. File groups without such a file slice are absent from the result.
   */
  private static Map<HoodieFileGroupId, FileSlice> getLatestMergedFileSlices(HoodieWriteConfig config,
                                                                             HoodieTableMetaClient dataMetaClient,
                                                                             String instantTime,
                                                                             List<HoodieFileGroupId> fileGroupIds) {
    if (fileGroupIds.isEmpty()) {
      return Collections.emptyMap();
    }
    HoodieEngineContext context = new HoodieLocalEngineContext(dataMetaClient.getStorageConf());
    FileSystemViewManager viewManager = FileSystemViewManager.createViewManager(context, config.getMetadataConfig(), config.getViewStorageConfig(),
        config.getCommonConfig(), unused -> getMetadataTable(config, dataMetaClient, context));
    try {
      SyncableFileSystemView view = viewManager.getFileSystemView(dataMetaClient);
      Map<HoodieFileGroupId, FileSlice> latestFileSlices = new HashMap<>();
      FileSystemViewStorageType storageType = config.getViewStorageConfig().getStorageType();
      if (storageType == FileSystemViewStorageType.REMOTE_FIRST || storageType == FileSystemViewStorageType.REMOTE_ONLY) {
        // the timeline server answers the lookup of one file group without sending the other file groups of its partition
        fileGroupIds.forEach(fileGroupId -> {
          Option<FileSlice> fileSlice = view.getLatestMergedFileSliceBeforeOrOn(fileGroupId.getPartitionPath(), instantTime, fileGroupId.getFileId());
          if (fileSlice.isPresent()) {
            latestFileSlices.put(fileGroupId, fileSlice.get());
          }
        });
      } else {
        Map<String, Set<String>> fileIdsByPartition = fileGroupIds.stream().collect(Collectors.groupingBy(
            HoodieFileGroupId::getPartitionPath, Collectors.mapping(HoodieFileGroupId::getFileId, Collectors.toSet())));
        view.loadPartitions(new ArrayList<>(fileIdsByPartition.keySet()));
        fileIdsByPartition.forEach((partition, fileIds) -> view.getLatestMergedFileSlicesBeforeOrOn(partition, instantTime)
            .filter(fileSlice -> fileIds.contains(fileSlice.getFileId()))
            .forEach(fileSlice -> latestFileSlices.put(new HoodieFileGroupId(partition, fileSlice.getFileId()), fileSlice)));
      }
      return latestFileSlices;
    } finally {
      viewManager.close();
    }
  }

  private static HoodieTableMetadata getMetadataTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient, HoodieEngineContext context) {
    return metaClient.getTableFormat().getMetadataFactory().create(context, metaClient.getStorage(), config.getMetadataConfig(), config.getBasePath());
  }

  public static <T> Map<String, String> getRecordKeyToSecondaryKey(HoodieTableMetaClient metaClient,
                                                                    HoodieReaderContext<T> readerContext,
                                                                    FileSlice fileSlice,
                                                                    HoodieSchema tableSchema,
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
                                                                             List<FileSliceAndPartition> fileSlices,
                                                                             int secondaryIndexMaxParallelism,
                                                                             String activeModule, HoodieTableMetaClient metaClient,
                                                                             HoodieIndexDefinition indexDefinition,
                                                                             TypedProperties props) {
    if (fileSlices.isEmpty()) {
      return engineContext.emptyHoodieData();
    }
    final int parallelism = Math.min(fileSlices.size(), secondaryIndexMaxParallelism);
    ReaderContextFactory<T> readerContextFactory = engineContext.getReaderContextFactory(metaClient);
    engineContext.setJobStatus(activeModule, "Secondary Index: reading secondary keys from " + fileSlices.size() + " file slices");
    HoodieFileFormat baseFileFormat = metaClient.getTableConfig().getBaseFileFormat();
    // a file slice without a base file has only log files, whose schema is the table schema. It is resolved once here
    // rather than in every task, because resolving it loads the timeline
    Option<HoodieSchema> tableSchema = fileSlices.stream().anyMatch(slice -> !slice.getFileSlice().getBaseFile().isPresent())
        ? Option.of(resolveTableSchema(metaClient))
        : Option.empty();
    String latestCompletedInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::requestedTime).orElse("");
    HoodieBroadcast<HoodieTableMetaClient> metaClientBroadcast = engineContext.broadcast(metaClient);
    return engineContext.parallelize(fileSlices, parallelism).flatMap(partitionAndBaseFile -> {
      final FileSlice fileSlice = partitionAndBaseFile.getFileSlice();
      final HoodieTableMetaClient taskMetaClient = metaClientBroadcast.value();
      // the storage path keeps the directory prefix of a file written outside Hudi, which its file name alone loses
      Option<StoragePath> dataFilePath = fileSlice.getBaseFile().map(HoodieBaseFile::getStoragePath);
      HoodieSchema readerSchema = dataFilePath.isPresent()
          ? HoodieIOFactory.getIOFactory(taskMetaClient.getStorage()).getFileFormatUtils(baseFileFormat).readSchema(taskMetaClient.getStorage(), dataFilePath.get())
          : tableSchema.get();
      ClosableIterator<Pair<String, String>> secondaryIndexGenerator = createSecondaryIndexRecordGenerator(
          readerContextFactory.getContext(), taskMetaClient, fileSlice, readerSchema, indexDefinition, latestCompletedInstant, props, false);
      return new CloseableMappingIterator<>(secondaryIndexGenerator, pair -> createSecondaryIndexRecord(pair.getKey(), pair.getValue(), indexDefinition.getIndexName(), false));
    });
  }

  private static HoodieSchema resolveTableSchema(HoodieTableMetaClient metaClient) {
    try {
      return new TableSchemaResolver(metaClient).getTableSchema();
    } catch (Exception e) {
      throw new HoodieException("Failed to get latest schema for " + metaClient.getBasePath(), e);
    }
  }

  /**
   * Constructs an iterator with a pair of the record key and the secondary index value for each record in the file slice.
   */
  private static <T> ClosableIterator<Pair<String, String>> createSecondaryIndexRecordGenerator(HoodieReaderContext<T> readerContext,
                                                                                                HoodieTableMetaClient metaClient,
                                                                                                FileSlice fileSlice,
                                                                                                HoodieSchema tableSchema,
                                                                                                HoodieIndexDefinition indexDefinition,
                                                                                                String instantTime,
                                                                                                TypedProperties props,
                                                                                                boolean allowInflightInstants) throws IOException {
    String secondaryKeyField = indexDefinition.getSourceFieldsKey();
    HoodieSchema requestedSchema = getRequestedSchemaForSecondaryIndex(metaClient, tableSchema, secondaryKeyField);
    // a table without record keys, e.g. one that registers files written outside Hudi, keys every row by the path of its
    // base file relative to the table and the row position, the same key the record index generates from the base file.
    // The positions of a merged file slice would not line up with the base file, so such a slice must not have log files.
    boolean generateRecordKeys = !metaClient.getTableConfig().hasRecordKey();
    if (generateRecordKeys) {
      ValidationUtils.checkState(fileSlice.getBaseFile().isPresent() && !fileSlice.hasLogFiles(),
          "File group " + fileSlice.getFileId() + " in partition " + fileSlice.getPartitionPath() + " needs a base file and no log files "
              + "to key its rows by position, because the table has no record key");
    }
    Option<String> relativeFilePath = generateRecordKeys
        ? Option.of(FSUtils.getRelativePartitionPath(metaClient.getBasePath(), fileSlice.getBaseFile().get().getStoragePath()))
        : Option.empty();
    HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>builder()
        .withReaderContext(readerContext)
        .withBaseFileOption(fileSlice.getBaseFile())
        .withLogFiles(fileSlice.getLogFiles())
        .withPartitionPath(fileSlice.getPartitionPath())
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
      private long rowPosition = 0;

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
          nextValidRecord = Pair.of(getRecordKey(record), secondaryKey == null ? null : secondaryKey.toString());
          rowPosition++;
          return true;
        }

        // If no valid records are found
        return false;
      }

      private String getRecordKey(T record) {
        if (relativeFilePath.isPresent()) {
          return ExternalFilePathUtil.generateRecordKeyForRow(relativeFilePath.get(), rowPosition);
        }
        return readerContext.getRecordContext().getRecordKey(record, requestedSchema);
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

  private static HoodieSchema getRequestedSchemaForSecondaryIndex(HoodieTableMetaClient metaClient, HoodieSchema tableSchema, String secondaryKeyField) {
    String[] recordKeyFields;
    if (tableSchema.getField(RECORD_KEY_METADATA_FIELD).isPresent()) {
      recordKeyFields = new String[] {RECORD_KEY_METADATA_FIELD};
    } else {
      recordKeyFields = metaClient.getTableConfig().getRecordKeyFields().orElse(new String[0]);
    }
    String[] projectionFields = Arrays.copyOf(recordKeyFields, recordKeyFields.length + 1);
    projectionFields[recordKeyFields.length] = secondaryKeyField;
    return HoodieSchemaUtils.projectSchema(tableSchema, Arrays.asList(projectionFields));
  }
}
