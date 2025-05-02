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

package org.apache.hudi.metadata.index.partitionstats;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.ExpressionIndexRecordGenerator;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.fs.FSUtils.getFileNameFromPath;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getPartitionFileSlicePairs;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileSystemViewForMetadataTable;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryResolveSchemaForTable;
import static org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS;
import static org.apache.hudi.metadata.index.columnstats.ColumnStatsIndexer.getColumnsToIndex;
import static org.apache.hudi.metadata.index.columnstats.ColumnStatsIndexer.readColumnRangeMetadataFrom;

public class PartitionStatsIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsIndexer.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final ExpressionIndexRecordGenerator indexHelper;

  public PartitionStatsIndexer(HoodieEngineContext engineContext,
                               HoodieWriteConfig dataTableWriteConfig,
                               HoodieTableMetaClient dataTableMetaClient,
                               ExpressionIndexRecordGenerator indexHelper) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.indexHelper = indexHelper;
  }

  @Override
  public List<InitialIndexPartitionData> build(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {
    // For PARTITION_STATS, COLUMN_STATS should also be enabled
    if (!dataTableWriteConfig.isMetadataColumnStatsIndexEnabled()) {
      LOG.warn("Skipping partition stats initialization as column stats index is not enabled. Please enable {}",
          HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
      return Collections.emptyList();
    }
    HoodieData<HoodieRecord> records =
        convertFilesToPartitionStatsRecords(engineContext,
            getPartitionFileSlicePairs(dataTableMetaClient, metadata, fsView.get()),
            dataTableWriteConfig.getMetadataConfig(),
            dataTableMetaClient, Option.empty(), Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
    final int numFileGroup = dataTableWriteConfig.getMetadataConfig().getPartitionStatsIndexFileGroupCount();
    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, PARTITION_STATS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionData> update(String instantTime,
                                         HoodieBackedTableMetadata tableMetadata,
                                         Lazy<HoodieTableFileSystemView> lazyFileSystemView, HoodieCommitMetadata commitMetadata) {
    checkState(MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(dataTableMetaClient),
        "Column stats partition must be enabled to generate partition stats. Please enable: " + HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
    // Generate Hoodie Pair data of partition name and list of column range metadata for all the files in that partition
    return convertMetadataToPartitionStatRecords(
        commitMetadata, engineContext,
        dataTableMetaClient, tableMetadata, dataTableWriteConfig.getMetadataConfig(),
        Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()),
        commitMetadata.getOperationType().equals(WriteOperationType.DELETE_PARTITION));
  }

  @Override
  public List<IndexPartitionData> clean(String instantTime, HoodieCleanMetadata cleanMetadata) {
    // TODO(yihua): fix the API to return option so that empty data does not do update in partition
    return Collections.emptyList();
  }

  public static HoodieData<HoodieRecord> convertFilesToPartitionStatsRecords(HoodieEngineContext engineContext,
                                                                             List<Pair<String, FileSlice>> partitionInfoList,
                                                                             HoodieMetadataConfig metadataConfig,
                                                                             HoodieTableMetaClient dataTableMetaClient,
                                                                             Option<Schema> writerSchemaOpt,
                                                                             Option<HoodieRecord.HoodieRecordType> recordTypeOpt) {
    if (partitionInfoList.isEmpty()) {
      return engineContext.emptyHoodieData();
    }
    Lazy<Option<Schema>> lazyWriterSchemaOpt = writerSchemaOpt.isPresent() ? Lazy.eagerly(writerSchemaOpt) : Lazy.lazily(() -> tryResolveSchemaForTable(dataTableMetaClient));
    final Map<String, Schema> columnsToIndexSchemaMap = getColumnsToIndex(dataTableMetaClient.getTableConfig(), metadataConfig, lazyWriterSchemaOpt,
        dataTableMetaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().empty(), recordTypeOpt);
    if (columnsToIndexSchemaMap.isEmpty()) {
      LOG.warn("No columns to index for partition stats index");
      return engineContext.emptyHoodieData();
    }
    LOG.debug("Indexing following columns for partition stats index: {}", columnsToIndexSchemaMap);

    // Group by partition path and collect file names (BaseFile and LogFiles)
    List<Pair<String, Set<String>>> partitionToFileNames = partitionInfoList.stream()
        .collect(Collectors.groupingBy(Pair::getLeft,
            Collectors.mapping(pair -> extractFileNames(pair.getRight()), Collectors.toList())))
        .entrySet().stream()
        .map(entry -> Pair.of(entry.getKey(),
            entry.getValue().stream().flatMap(Set::stream).collect(Collectors.toSet())))
        .collect(Collectors.toList());

    // Create records for MDT
    int parallelism = Math.max(Math.min(partitionToFileNames.size(), metadataConfig.getPartitionStatsIndexParallelism()), 1);
    return engineContext.parallelize(partitionToFileNames, parallelism).flatMap(partitionInfo -> {
      final String partitionPath = partitionInfo.getKey();
      // Step 1: Collect Column Metadata for Each File
      List<List<HoodieColumnRangeMetadata<Comparable>>> fileColumnMetadata = partitionInfo.getValue().stream()
          .map(fileName -> getFileStatsRangeMetadata(partitionPath, fileName, dataTableMetaClient, new ArrayList<>(columnsToIndexSchemaMap.keySet()), false,
              metadataConfig.getMaxReaderBufferSize()))
          .collect(Collectors.toList());

      return collectAndProcessColumnMetadata(fileColumnMetadata, partitionPath, true, columnsToIndexSchemaMap).iterator();
    });
  }

  public static List<IndexPartitionData> convertMetadataToPartitionStatRecords(HoodieCommitMetadata commitMetadata, HoodieEngineContext engineContext, HoodieTableMetaClient dataMetaClient,
                                                                               HoodieTableMetadata tableMetadata, HoodieMetadataConfig metadataConfig,
                                                                               Option<HoodieRecord.HoodieRecordType> recordTypeOpt, boolean isDeletePartition) {
    try {
      Option<Schema> writerSchema =
          Option.ofNullable(commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY))
              .flatMap(writerSchemaStr ->
                  isNullOrEmpty(writerSchemaStr)
                      ? Option.empty()
                      : Option.of(new Schema.Parser().parse(writerSchemaStr)));
      HoodieTableConfig tableConfig = dataMetaClient.getTableConfig();
      Option<Schema> tableSchema = writerSchema.map(schema -> tableConfig.populateMetaFields() ? addMetadataFields(schema) : schema);
      if (tableSchema.isEmpty()) {
        return Collections.emptyList();
      }
      Lazy<Option<Schema>> writerSchemaOpt = Lazy.eagerly(tableSchema);
      Map<String, Schema> columnsToIndexSchemaMap = getColumnsToIndex(dataMetaClient.getTableConfig(), metadataConfig, writerSchemaOpt, false, recordTypeOpt);
      if (columnsToIndexSchemaMap.isEmpty()) {
        return Collections.emptyList();
      }

      // if this is DELETE_PARTITION, then create delete metadata payload for all columns for partition_stats
      if (isDeletePartition) {
        HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
        Map<String, List<String>> partitionToReplaceFileIds = replaceCommitMetadata.getPartitionToReplaceFileIds();
        List<String> partitionsToDelete = new ArrayList<>(partitionToReplaceFileIds.keySet());
        if (partitionToReplaceFileIds.isEmpty()) {
          return Collections.emptyList();
        }
        return Collections.singletonList(IndexPartitionData.of(
            PARTITION_STATS.getPartitionPath(),
            engineContext.parallelize(partitionsToDelete, partitionsToDelete.size()).flatMap(partition -> {
              Stream<HoodieRecord> columnRangeMetadata = columnsToIndexSchemaMap.keySet().stream()
                  .flatMap(column -> HoodieMetadataPayload.createPartitionStatsRecords(
                      partition,
                      Collections.singletonList(HoodieColumnRangeMetadata.stub("", column)),
                      true, true, Option.empty()));
              return columnRangeMetadata.iterator();
            })));
      }

      // In this function we fetch column range metadata for all new files part of commit metadata along with all the other files
      // of the affected partitions. The column range metadata is grouped by partition name to generate HoodiePairData of partition name
      // and list of column range metadata for that partition files. This pair data is then used to generate partition stat records.
      List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
          .flatMap(Collection::stream).collect(Collectors.toList());
      if (allWriteStats.isEmpty()) {
        return Collections.emptyList();
      }

      List<String> colsToIndex = new ArrayList<>(columnsToIndexSchemaMap.keySet());
      LOG.debug("Indexing following columns for partition stats index: {}", columnsToIndexSchemaMap.keySet());
      // Group by partitionPath and then gather write stats lists,
      // where each inner list contains HoodieWriteStat objects that have the same partitionPath.
      List<List<HoodieWriteStat>> partitionedWriteStats = new ArrayList<>(allWriteStats.stream()
          .collect(Collectors.groupingBy(HoodieWriteStat::getPartitionPath))
          .values());

      int parallelism = Math.max(Math.min(partitionedWriteStats.size(), metadataConfig.getPartitionStatsIndexParallelism()), 1);
      boolean shouldScanColStatsForTightBound = isShouldScanColStatsForTightBound(dataMetaClient);

      HoodiePairData<String, List<HoodieColumnRangeMetadata<Comparable>>> columnRangeMetadata = engineContext.parallelize(partitionedWriteStats, parallelism).mapToPair(partitionedWriteStat -> {
        final String partitionName = partitionedWriteStat.get(0).getPartitionPath();
        // Step 1: Collect Column Metadata for Each File part of current commit metadata
        List<HoodieColumnRangeMetadata<Comparable>> fileColumnMetadata = partitionedWriteStat.stream()
            .flatMap(writeStat -> translateWriteStatToFileStats(writeStat, dataMetaClient, colsToIndex, tableSchema).stream()).collect(toList());

        if (shouldScanColStatsForTightBound) {
          checkState(tableMetadata != null, "tableMetadata should not be null when scanning metadata table");
          // Collect Column Metadata for Each File part of active file system view of latest snapshot
          // Get all file names, including log files, in a set from the file slices
          Set<String> fileNames = getPartitionLatestFileSlicesIncludingInflight(dataMetaClient, Option.empty(), partitionName).stream()
              .flatMap(fileSlice -> Stream.concat(
                  Stream.of(fileSlice.getBaseFile().map(HoodieBaseFile::getFileName).orElse(null)),
                  fileSlice.getLogFiles().map(HoodieLogFile::getFileName)))
              .filter(Objects::nonNull)
              .collect(Collectors.toSet());
          // Fetch metadata table COLUMN_STATS partition records for above files
          List<HoodieColumnRangeMetadata<Comparable>> partitionColumnMetadata = tableMetadata
              .getRecordsByKeyPrefixes(generateKeyPrefixes(colsToIndex, partitionName), MetadataPartitionType.COLUMN_STATS.getPartitionPath(), false)
              // schema and properties are ignored in getInsertValue, so simply pass as null
              .map(record -> ((HoodieMetadataPayload) record.getData()).getColumnStatMetadata())
              .filter(Option::isPresent)
              .map(colStatsOpt -> colStatsOpt.get())
              .filter(stats -> fileNames.contains(stats.getFileName()))
              .map(HoodieColumnRangeMetadata::fromColumnStats).collectAsList();
          if (!partitionColumnMetadata.isEmpty()) {
            // incase of shouldScanColStatsForTightBound = true, we compute stats for the partition of interest for all files from getLatestFileSlice() excluding current commit here
            // already fileColumnMetadata contains stats for files from the current infliht commit. so, we are adding both together and sending it to collectAndProcessColumnMetadata
            fileColumnMetadata.addAll(partitionColumnMetadata);
          }
        }

        return Pair.of(partitionName, fileColumnMetadata);
      });

      return Collections.singletonList(IndexPartitionData.of(
          PARTITION_STATS.getPartitionPath(),
          convertMetadataToPartitionStatsRecords(
              columnRangeMetadata, dataMetaClient, columnsToIndexSchemaMap)));
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  private static HoodieData<HoodieRecord> convertMetadataToPartitionStatsRecords(HoodiePairData<String, List<HoodieColumnRangeMetadata<Comparable>>> columnRangeMetadataPartitionPair,
                                                                                 HoodieTableMetaClient dataMetaClient,
                                                                                 Map<String, Schema> colsToIndexSchemaMap
  ) {
    try {
      return columnRangeMetadataPartitionPair
          .flatMapValues(List::iterator)
          .groupByKey()
          .map(pair -> {
            final String partitionName = pair.getLeft();
            return collectAndProcessColumnMetadata(pair.getRight(), partitionName, isShouldScanColStatsForTightBound(dataMetaClient), Option.empty(), colsToIndexSchemaMap);
          })
          .flatMap(recordStream -> recordStream.iterator());
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  private static Stream<HoodieRecord> collectAndProcessColumnMetadata(
      List<List<HoodieColumnRangeMetadata<Comparable>>> fileColumnMetadata,
      String partitionPath, boolean isTightBound,
      Map<String, Schema> colsToIndexSchemaMap
  ) {
    return collectAndProcessColumnMetadata(partitionPath, isTightBound, Option.empty(), fileColumnMetadata.stream().flatMap(List::stream), colsToIndexSchemaMap);
  }

  private static Stream<HoodieRecord> collectAndProcessColumnMetadata(Iterable<HoodieColumnRangeMetadata<Comparable>> fileColumnMetadataIterable, String partitionPath,
                                                                      boolean isTightBound, Option<String> indexPartitionOpt,
                                                                      Map<String, Schema> colsToIndexSchemaMap
  ) {

    List<HoodieColumnRangeMetadata<Comparable>> fileColumnMetadata = new ArrayList<>();
    fileColumnMetadataIterable.forEach(fileColumnMetadata::add);
    // Group by Column Name
    return collectAndProcessColumnMetadata(partitionPath, isTightBound, indexPartitionOpt, fileColumnMetadata.stream(), colsToIndexSchemaMap);
  }

  private static Stream<HoodieRecord> collectAndProcessColumnMetadata(String partitionPath, boolean isTightBound, Option<String> indexPartitionOpt,
                                                                      Stream<HoodieColumnRangeMetadata<Comparable>> fileColumnMetadata,
                                                                      Map<String, Schema> colsToIndexSchemaMap
  ) {
    // Group by Column Name
    Map<String, List<HoodieColumnRangeMetadata<Comparable>>> columnMetadataMap =
        fileColumnMetadata.collect(Collectors.groupingBy(HoodieColumnRangeMetadata::getColumnName, Collectors.toList()));

    // Aggregate Column Ranges
    Stream<HoodieColumnRangeMetadata<Comparable>> partitionStatsRangeMetadata = columnMetadataMap.entrySet().stream()
        .map(entry -> FileFormatUtils.getColumnRangeInPartition(partitionPath, entry.getValue(), colsToIndexSchemaMap));

    // Create Partition Stats Records
    return HoodieMetadataPayload.createPartitionStatsRecords(partitionPath, partitionStatsRangeMetadata.collect(Collectors.toList()), false, isTightBound, indexPartitionOpt);
  }

  private static Set<String> extractFileNames(FileSlice fileSlice) {
    Set<String> fileNames = new HashSet<>();
    Option<HoodieBaseFile> baseFile = fileSlice.getBaseFile();
    baseFile.ifPresent(hoodieBaseFile -> fileNames.add(hoodieBaseFile.getFileName()));
    fileSlice.getLogFiles().forEach(hoodieLogFile -> fileNames.add(hoodieLogFile.getFileName()));
    return fileNames;
  }

  private static List<HoodieColumnRangeMetadata<Comparable>> getFileStatsRangeMetadata(String partitionPath,
                                                                                       String fileName,
                                                                                       HoodieTableMetaClient datasetMetaClient,
                                                                                       List<String> columnsToIndex,
                                                                                       boolean isDeleted,
                                                                                       int maxBufferSize) {
    if (isDeleted) {
      return columnsToIndex.stream()
          .map(entry -> HoodieColumnRangeMetadata.stub(fileName, entry))
          .collect(Collectors.toList());
    }
    return readColumnRangeMetadataFrom(partitionPath, fileName, datasetMetaClient, columnsToIndex, maxBufferSize);
  }

  public static boolean isShouldScanColStatsForTightBound(HoodieTableMetaClient dataMetaClient) {
    return MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(dataMetaClient);
  }

  private static List<HoodieColumnRangeMetadata<Comparable>> translateWriteStatToFileStats(HoodieWriteStat writeStat,
                                                                                           HoodieTableMetaClient datasetMetaClient,
                                                                                           List<String> columnsToIndex,
                                                                                           Option<Schema> writerSchemaOpt) {
    if (writeStat instanceof HoodieDeltaWriteStat && ((HoodieDeltaWriteStat) writeStat).getColumnStats().isPresent()) {
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMap = ((HoodieDeltaWriteStat) writeStat).getColumnStats().get();
      return columnRangeMap.values().stream().collect(Collectors.toList());
    }

    String filePath = writeStat.getPath();
    return getFileStatsRangeMetadata(writeStat.getPartitionPath(), getFileNameFromPath(filePath), datasetMetaClient, columnsToIndex, false, -1);
  }

  /**
   * Get the latest file slices for a given partition including the inflight ones.
   *
   * @param metaClient     - instance of {@link HoodieTableMetaClient}
   * @param fileSystemView - hoodie table file system view, which will be fetched from meta client if not already present
   * @param partition      - name of the partition whose file groups are to be loaded
   * @return
   */
  public static List<FileSlice> getPartitionLatestFileSlicesIncludingInflight(HoodieTableMetaClient metaClient,
                                                                              Option<HoodieTableFileSystemView> fileSystemView,
                                                                              String partition) {
    HoodieTableFileSystemView fsView = null;
    try {
      fsView = fileSystemView.orElseGet(() -> getFileSystemViewForMetadataTable(metaClient));
      Stream<FileSlice> fileSliceStream = fsView.getLatestFileSlicesIncludingInflight(partition);
      return fileSliceStream
          .sorted(Comparator.comparing(FileSlice::getFileId))
          .collect(Collectors.toList());
    } finally {
      if (!fileSystemView.isPresent() && fsView != null) {
        fsView.close();
      }
    }
  }

  /**
   * Generate key prefixes for each combination of column name in {@param columnsToIndex} and {@param partitionName}.
   */
  public static List<String> generateKeyPrefixes(List<String> columnsToIndex, String partitionName) {
    List<String> keyPrefixes = new ArrayList<>();
    PartitionIndexID partitionIndexId = new PartitionIndexID(getColumnStatsIndexPartitionIdentifier(partitionName));
    for (String columnName : columnsToIndex) {
      ColumnIndexID columnIndexID = new ColumnIndexID(columnName);
      String keyPrefix = columnIndexID.asBase64EncodedString()
          .concat(partitionIndexId.asBase64EncodedString());
      keyPrefixes.add(keyPrefix);
    }

    return keyPrefixes;
  }
}
