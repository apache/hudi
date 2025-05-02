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

package org.apache.hudi.metadata.index.columnstats;

import org.apache.hudi.avro.ConvertingGenericData;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.BooleanWrapper;
import org.apache.hudi.avro.model.DateWrapper;
import org.apache.hudi.avro.model.DoubleWrapper;
import org.apache.hudi.avro.model.FloatWrapper;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.IntWrapper;
import org.apache.hudi.avro.model.LongWrapper;
import org.apache.hudi.avro.model.StringWrapper;
import org.apache.hudi.avro.model.TimeMicrosWrapper;
import org.apache.hudi.avro.model.TimestampMicrosWrapper;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.Indexer;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.fs.FSUtils.getFileNameFromPath;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.index.HoodieIndexUtils.register;
import static org.apache.hudi.metadata.HoodieTableMetadata.EMPTY_PARTITION_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.coerceToComparable;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.fetchPartitionFileInfoTriplets;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryResolveSchemaForTable;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;

public class ColumnStatsIndexer implements Indexer {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnStatsIndexer.class);
  private static final Set<Class<?>> COLUMN_STATS_RECORD_SUPPORTED_TYPES = new HashSet<>(Arrays.asList(
      IntWrapper.class, BooleanWrapper.class, DateWrapper.class,
      DoubleWrapper.class, FloatWrapper.class, LongWrapper.class,
      StringWrapper.class, TimeMicrosWrapper.class, TimestampMicrosWrapper.class));
  // The maximum allowed precision and scale as per the payload schema. See DecimalWrapper in HoodieMetadata.avsc:
  // https://github.com/apache/hudi/blob/45dedd819e56e521148bde51a3dfa4e472ea70cd/hudi-common/src/main/avro/HoodieMetadata.avsc#L247
  private static final int DECIMAL_MAX_PRECISION = 30;
  private static final int DECIMAL_MAX_SCALE = 15;
  @VisibleForTesting
  static final String[] META_COLS_TO_ALWAYS_INDEX = {COMMIT_TIME_METADATA_FIELD, RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD};
  @VisibleForTesting
  public static final Set<String> META_COL_SET_TO_INDEX = new HashSet<>(Arrays.asList(META_COLS_TO_ALWAYS_INDEX));
  @VisibleForTesting
  static final Map<String, Schema> META_COLS_TO_ALWAYS_INDEX_SCHEMA_MAP = new TreeMap() {
    {
      put(COMMIT_TIME_METADATA_FIELD, Schema.create(Schema.Type.STRING));
      put(RECORD_KEY_METADATA_FIELD, Schema.create(Schema.Type.STRING));
      put(PARTITION_PATH_METADATA_FIELD, Schema.create(Schema.Type.STRING));
    }
  };

  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final Lazy<List<String>> columnsToIndex;

  public ColumnStatsIndexer(HoodieEngineContext engineContext,
                            HoodieWriteConfig dataTableWriteConfig,
                            HoodieTableMetaClient dataTableMetaClient) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.columnsToIndex = Lazy.lazily(() ->
        new ArrayList<>(getColumnsToIndex(dataTableMetaClient.getTableConfig(),
            dataTableWriteConfig.getMetadataConfig(),
            Lazy.lazily(() -> tryResolveSchemaForTable(dataTableMetaClient)),
            true,
            Option.of(dataTableWriteConfig.getRecordMerger().getRecordType())).keySet()));
  }

  @Override
  public List<InitialIndexPartitionData> build(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {

    final int numFileGroup = dataTableWriteConfig.getMetadataConfig().getColumnStatsIndexFileGroupCount();
    // TODO(yihua): Revisit to see to return -1
    if (partitionToFilesMap.isEmpty()) {
      return Collections.singletonList(InitialIndexPartitionData.of(
          numFileGroup, COLUMN_STATS.getPartitionPath(), engineContext.emptyHoodieData()));
    }

    if (columnsToIndex.get().isEmpty()) {
      // this can only happen if meta fields are disabled and cols to index is not explicitly overridden.
      return Collections.singletonList(InitialIndexPartitionData.of(
          numFileGroup, COLUMN_STATS.getPartitionPath(), engineContext.emptyHoodieData()));
    }

    LOG.info("Indexing {} columns for column stats index", columnsToIndex.get().size());

    // during initialization, we need stats for base and log files.
    HoodieData<HoodieRecord> records = convertFilesToColumnStatsRecords(
        engineContext, Collections.emptyMap(), partitionToFilesMap, dataTableMetaClient,
        dataTableWriteConfig.getMetadataConfig(),
        dataTableWriteConfig.getColumnStatsIndexParallelism(),
        dataTableWriteConfig.getMetadataConfig().getMaxReaderBufferSize(),
        columnsToIndex.get());

    return Collections.singletonList(InitialIndexPartitionData.of(
        numFileGroup, COLUMN_STATS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionData> update(String instantTime,
                                         HoodieBackedTableMetadata tableMetadata,
                                         Lazy<HoodieTableFileSystemView> lazyFileSystemView, HoodieCommitMetadata commitMetadata) {
    return convertMetadataToColumnStatsRecords(
        commitMetadata, engineContext,
        dataTableMetaClient, dataTableWriteConfig.getMetadataConfig(),
        Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
  }

  @Override
  public List<IndexPartitionData> clean(String instantTime, HoodieCleanMetadata cleanMetadata) {
    Option<HoodieData<HoodieRecord>> recordOption = convertMetadataToColumnStatsRecords(cleanMetadata, engineContext,
        dataTableMetaClient, dataTableWriteConfig.getMetadataConfig(),
        Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
    return recordOption.isPresent()
        ? Collections.singletonList(IndexPartitionData.of(COLUMN_STATS.getPartitionPath(), recordOption.get()))
        : Collections.emptyList();
  }

  @Override
  public void updateTableConfig() {
    // TODO(yihua): though this engine-independent, only Spark has implemented before this PR.
    //  Revisit to make sure if that's intentional
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withIndexFunction(PARTITION_NAME_COLUMN_STATS)
        .withSourceFields(columnsToIndex.get())
        .withIndexOptions(Collections.EMPTY_MAP)
        .build();
    LOG.info("Registering Or Updating the index {}", PARTITION_NAME_COLUMN_STATS);
    register(dataTableMetaClient, indexDefinition);
  }

  /**
   * Convert added and deleted action metadata to column stats index records.
   */
  public static HoodieData<HoodieRecord> convertFilesToColumnStatsRecords(HoodieEngineContext engineContext,
                                                                          Map<String, List<String>> partitionToDeletedFiles,
                                                                          Map<String, Map<String, Long>> partitionToAppendedFiles,
                                                                          HoodieTableMetaClient dataMetaClient,
                                                                          HoodieMetadataConfig metadataConfig,
                                                                          int columnStatsIndexParallelism,
                                                                          int maxReaderBufferSize,
                                                                          List<String> columnsToIndex) {
    if ((partitionToAppendedFiles.isEmpty() && partitionToDeletedFiles.isEmpty())) {
      return engineContext.emptyHoodieData();
    }
    LOG.info("Indexing {} columns for column stats index", columnsToIndex.size());

    // Create the tuple (partition, filename, isDeleted) to handle both deletes and appends
    final List<Tuple3<String, String, Boolean>> partitionFileFlagTupleList = fetchPartitionFileInfoTriplets(partitionToDeletedFiles, partitionToAppendedFiles);

    // Create records MDT
    int parallelism = Math.max(Math.min(partitionFileFlagTupleList.size(), columnStatsIndexParallelism), 1);
    return engineContext.parallelize(partitionFileFlagTupleList, parallelism).flatMap(partitionFileFlagTuple -> {
      final String partitionPath = partitionFileFlagTuple.f0;
      final String filename = partitionFileFlagTuple.f1;
      final boolean isDeleted = partitionFileFlagTuple.f2;
      return getColumnStatsRecords(partitionPath, filename, dataMetaClient, columnsToIndex, isDeleted, maxReaderBufferSize).iterator();
    });
  }

  public static List<IndexPartitionData> convertMetadataToColumnStatsRecords(HoodieCommitMetadata commitMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             HoodieTableMetaClient dataMetaClient,
                                                                             HoodieMetadataConfig metadataConfig,
                                                                             Option<HoodieRecord.HoodieRecordType> recordTypeOpt) {
    List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());

    if (allWriteStats.isEmpty()) {
      return Collections.emptyList();
    }

    try {
      Map<String, Schema> columnsToIndexSchemaMap = getColumnsToIndex(commitMetadata, dataMetaClient, metadataConfig, recordTypeOpt);
      if (columnsToIndexSchemaMap.isEmpty()) {
        // In case there are no columns to index, bail
        return Collections.emptyList();
      }
      List<String> columnsToIndex = new ArrayList<>(columnsToIndexSchemaMap.keySet());
      int parallelism = Math.max(Math.min(allWriteStats.size(), metadataConfig.getColumnStatsIndexParallelism()), 1);
      return Collections.singletonList(IndexPartitionData.of(
          COLUMN_STATS.getPartitionPath(),
          engineContext.parallelize(allWriteStats, parallelism)
              .flatMap(writeStat ->
                  translateWriteStatToColumnStats(writeStat, dataMetaClient, columnsToIndex).iterator())));
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  /**
   * Convert clean metadata to column stats index records.
   *
   * @param cleanMetadata  - Clean action metadata
   * @param engineContext  - Engine context
   * @param dataMetaClient - HoodieTableMetaClient for data
   * @param metadataConfig - HoodieMetadataConfig
   * @return List of column stats index records for the clean metadata
   */
  public static Option<HoodieData<HoodieRecord>> convertMetadataToColumnStatsRecords(
      HoodieCleanMetadata cleanMetadata,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient dataMetaClient,
      HoodieMetadataConfig metadataConfig,
      Option<HoodieRecord.HoodieRecordType> recordTypeOpt) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> deleteFileList.add(Pair.of(partition, entry)));
    });
    if (deleteFileList.isEmpty()) {
      return Option.empty();
    }

    List<String> columnsToIndex = new ArrayList<>(getColumnsToIndex(dataMetaClient.getTableConfig(), metadataConfig,
        Lazy.lazily(() -> tryResolveSchemaForTable(dataMetaClient)), false, recordTypeOpt).keySet());

    if (columnsToIndex.isEmpty()) {
      // In case there are no columns to index, bail
      LOG.warn("No columns to index for column stats index.");
      return Option.empty();
    }

    int parallelism = Math.max(Math.min(deleteFileList.size(), metadataConfig.getColumnStatsIndexParallelism()), 1);
    return Option.of(engineContext.parallelize(deleteFileList, parallelism)
        .flatMap(deleteFileInfoPair -> {
          String partitionPath = deleteFileInfoPair.getLeft();
          String fileName = deleteFileInfoPair.getRight();
          return getColumnStatsRecords(partitionPath, fileName, dataMetaClient, columnsToIndex, true).iterator();
        }));
  }

  @VisibleForTesting
  public static Map<String, Schema> getColumnsToIndex(HoodieTableConfig tableConfig,
                                                      HoodieMetadataConfig metadataConfig,
                                                      Lazy<Option<Schema>> tableSchemaLazyOpt,
                                                      Option<HoodieRecord.HoodieRecordType> recordType) {
    return getColumnsToIndex(tableConfig, metadataConfig, tableSchemaLazyOpt, false, recordType);
  }

  @VisibleForTesting
  public static Map<String, Schema> getColumnsToIndex(HoodieTableConfig tableConfig,
                                                      HoodieMetadataConfig metadataConfig,
                                                      Lazy<Option<Schema>> tableSchemaLazyOpt,
                                                      boolean isTableInitializing) {
    return getColumnsToIndex(tableConfig, metadataConfig, tableSchemaLazyOpt, isTableInitializing, Option.empty());
  }

  @VisibleForTesting
  public static Map<String, Schema> getColumnsToIndex(HoodieTableConfig tableConfig,
                                                      HoodieMetadataConfig metadataConfig,
                                                      Lazy<Option<Schema>> tableSchemaLazyOpt,
                                                      boolean isTableInitializing,
                                                      Option<HoodieRecord.HoodieRecordType> recordType) {
    Map<String, Schema> columnsToIndexWithoutRequiredMetas = getColumnsToIndexWithoutRequiredMetaFields(metadataConfig, tableSchemaLazyOpt, isTableInitializing, recordType);
    if (!tableConfig.populateMetaFields()) {
      return columnsToIndexWithoutRequiredMetas;
    }

    Map<String, Schema> colsToIndexSchemaMap = new LinkedHashMap<>();
    colsToIndexSchemaMap.putAll(META_COLS_TO_ALWAYS_INDEX_SCHEMA_MAP);
    colsToIndexSchemaMap.putAll(columnsToIndexWithoutRequiredMetas);
    return colsToIndexSchemaMap;
  }

  public static Map<String, Schema> getColumnsToIndex(HoodieCommitMetadata commitMetadata, HoodieTableMetaClient dataMetaClient,
                                                      HoodieMetadataConfig metadataConfig, Option<HoodieRecord.HoodieRecordType> recordTypeOpt) {
    Option<Schema> writerSchema =
        Option.ofNullable(commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY))
            .flatMap(writerSchemaStr ->
                isNullOrEmpty(writerSchemaStr)
                    ? Option.empty()
                    : Option.of(new Schema.Parser().parse(writerSchemaStr)));

    HoodieTableConfig tableConfig = dataMetaClient.getTableConfig();

    // NOTE: Writer schema added to commit metadata will not contain Hudi's metadata fields
    Option<Schema> tableSchema = writerSchema.isEmpty()
        ? tableConfig.getTableCreateSchema() // the write schema does not set up correctly
        : writerSchema.map(schema -> tableConfig.populateMetaFields() ? addMetadataFields(schema) : schema);

    return getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(tableSchema), false, recordTypeOpt);
  }

  /**
   * Get list of columns that should be indexed for col stats or partition stats
   * We always index META_COLS_TO_ALWAYS_INDEX If metadataConfig.getColumnsEnabledForColumnStatsIndex()
   * is empty, we will use metadataConfig.maxColumnsToIndexForColStats() and index the first n columns in the table in addition to the
   * required meta cols
   *
   * @param metadataConfig      metadata config
   * @param tableSchemaLazyOpt  lazy option of the table schema
   * @param isTableInitializing true if table is being initialized.
   * @param recordType          Option of record type. Used to determine which types are valid to index
   * @return list of columns that should be indexed
   */
  private static Map<String, Schema> getColumnsToIndexWithoutRequiredMetaFields(HoodieMetadataConfig metadataConfig,
                                                                                Lazy<Option<Schema>> tableSchemaLazyOpt,
                                                                                boolean isTableInitializing,
                                                                                Option<HoodieRecord.HoodieRecordType> recordType) {
    List<String> columnsToIndex = metadataConfig.getColumnsEnabledForColumnStatsIndex();
    if (!columnsToIndex.isEmpty()) {
      // if explicitly overridden
      if (isTableInitializing) {
        Map<String, Schema> toReturn = new LinkedHashMap<>();
        columnsToIndex.forEach(colName -> toReturn.put(colName, null));
        return toReturn;
      }
      ValidationUtils.checkArgument(tableSchemaLazyOpt.get().isPresent(), "Table schema not found for the table while computing col stats");
      // filter for eligible fields
      Option<Schema> tableSchema = tableSchemaLazyOpt.get();
      Map<String, Schema> colsToIndexSchemaMap = new LinkedHashMap<>();
      columnsToIndex.stream().filter(fieldName -> !META_COL_SET_TO_INDEX.contains(fieldName))
          .map(colName -> Pair.of(colName, HoodieAvroUtils.getSchemaForField(tableSchema.get(), colName).getRight().schema()))
          .filter(fieldNameSchemaPair -> isColumnTypeSupported(fieldNameSchemaPair.getValue(), recordType))
          .forEach(entry -> colsToIndexSchemaMap.put(entry.getKey(), entry.getValue()));
      return colsToIndexSchemaMap;
    }
    // if not overridden
    if (tableSchemaLazyOpt.get().isPresent()) {
      Map<String, Schema> colsToIndexSchemaMap = new LinkedHashMap<>();
      tableSchemaLazyOpt.get().map(schema -> getFirstNSupportedFields(schema, metadataConfig.maxColumnsToIndexForColStats(), recordType)).orElse(Stream.empty())
          .forEach(entry -> colsToIndexSchemaMap.put(entry.getKey(), entry.getValue()));
      return colsToIndexSchemaMap;
    } else {
      // initialize col stats index config with empty list of cols
      return Collections.emptyMap();
    }
  }

  private static Stream<HoodieRecord> translateWriteStatToColumnStats(HoodieWriteStat writeStat,
                                                                      HoodieTableMetaClient datasetMetaClient,
                                                                      List<String> columnsToIndex) {
    if (writeStat instanceof HoodieDeltaWriteStat && ((HoodieDeltaWriteStat) writeStat).getColumnStats().isPresent()) {
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMap = ((HoodieDeltaWriteStat) writeStat).getColumnStats().get();
      Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = columnRangeMap.values();
      return HoodieMetadataPayload.createColumnStatsRecords(writeStat.getPartitionPath(), columnRangeMetadataList, false);
    }

    String filePath = writeStat.getPath();
    return getColumnStatsRecords(writeStat.getPartitionPath(), getFileNameFromPath(filePath), datasetMetaClient, columnsToIndex, false);
  }

  private static Stream<HoodieRecord> getColumnStatsRecords(String partitionPath,
                                                            String fileName,
                                                            HoodieTableMetaClient datasetMetaClient,
                                                            List<String> columnsToIndex,
                                                            boolean isDeleted) {
    return getColumnStatsRecords(partitionPath, fileName, datasetMetaClient, columnsToIndex, isDeleted, -1);
  }

  public static Stream<HoodieRecord> getColumnStatsRecords(String partitionPath,
                                                           String fileName,
                                                           HoodieTableMetaClient datasetMetaClient,
                                                           List<String> columnsToIndex,
                                                           boolean isDeleted,
                                                           int maxBufferSize) {

    if (isDeleted) {
      List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = columnsToIndex.stream()
          .map(entry -> HoodieColumnRangeMetadata.stub(fileName, entry))
          .collect(Collectors.toList());

      return HoodieMetadataPayload.createColumnStatsRecords(partitionPath, columnRangeMetadataList, true);
    }
    List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata =
        readColumnRangeMetadataFrom(partitionPath, fileName, datasetMetaClient, columnsToIndex, maxBufferSize);

    return HoodieMetadataPayload.createColumnStatsRecords(partitionPath, columnRangeMetadata, false);
  }

  public static List<HoodieColumnRangeMetadata<Comparable>> readColumnRangeMetadataFrom(String partitionPath,
                                                                                         String fileName,
                                                                                         HoodieTableMetaClient datasetMetaClient,
                                                                                         List<String> columnsToIndex,
                                                                                         int maxBufferSize) {
    String partitionPathFileName = (partitionPath.equals(EMPTY_PARTITION_NAME) || partitionPath.equals(NON_PARTITIONED_NAME)) ? fileName
        : partitionPath + "/" + fileName;
    try {
      StoragePath fullFilePath = new StoragePath(datasetMetaClient.getBasePath(), partitionPathFileName);
      if (partitionPathFileName.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        return HoodieIOFactory.getIOFactory(datasetMetaClient.getStorage())
            .getFileFormatUtils(HoodieFileFormat.PARQUET)
            .readColumnStatsFromMetadata(datasetMetaClient.getStorage(), fullFilePath, columnsToIndex);
      } else if (FSUtils.isLogFile(fileName)) {
        Option<Schema> writerSchemaOpt = tryResolveSchemaForTable(datasetMetaClient);
        LOG.warn("Reading log file: {}, to build column range metadata.", partitionPathFileName);
        return getLogFileColumnRangeMetadata(fullFilePath.toString(), datasetMetaClient, columnsToIndex, writerSchemaOpt, maxBufferSize);
      }
      LOG.warn("Column range index not supported for: {}", partitionPathFileName);
      return Collections.emptyList();
    } catch (Exception e) {
      // NOTE: In case reading column range metadata from individual file failed,
      //       we simply fall back, in lieu of failing the whole task
      LOG.error("Failed to fetch column range metadata for: {}", partitionPathFileName);
      return Collections.emptyList();
    }
  }

  /**
   * Read column range metadata from log file.
   */
  @VisibleForTesting
  public static List<HoodieColumnRangeMetadata<Comparable>> getLogFileColumnRangeMetadata(String filePath, HoodieTableMetaClient datasetMetaClient,
                                                                                          List<String> columnsToIndex, Option<Schema> writerSchemaOpt,
                                                                                          int maxBufferSize) throws IOException {
    if (writerSchemaOpt.isPresent()) {
      List<Pair<String, Schema.Field>> fieldsToIndex = columnsToIndex.stream().map(fieldName -> HoodieAvroUtils.getSchemaForField(writerSchemaOpt.get(), fieldName))
          .collect(Collectors.toList());
      // read log file records without merging
      List<HoodieRecord> records = new ArrayList<>();
      HoodieUnMergedLogRecordScanner scanner = HoodieUnMergedLogRecordScanner.newBuilder()
          .withStorage(datasetMetaClient.getStorage())
          .withBasePath(datasetMetaClient.getBasePath())
          .withLogFilePaths(Collections.singletonList(filePath))
          .withBufferSize(maxBufferSize)
          .withLatestInstantTime(datasetMetaClient.getActiveTimeline().getCommitsTimeline().lastInstant().get().requestedTime())
          .withReaderSchema(writerSchemaOpt.get())
          .withTableMetaClient(datasetMetaClient)
          .withLogRecordScannerCallback(records::add)
          .build();
      scanner.scan();
      if (records.isEmpty()) {
        return Collections.emptyList();
      }
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataMap =
          collectColumnRangeMetadata(records, fieldsToIndex, getFileNameFromPath(filePath), writerSchemaOpt.get(), datasetMetaClient.getStorage().getConf());
      return new ArrayList<>(columnRangeMetadataMap.values());
    }
    return Collections.emptyList();
  }

  /**
   * Collects {@link HoodieColumnRangeMetadata} for the provided collection of records, pretending
   * as if provided records have been persisted w/in given {@code filePath}
   *
   * @param records      target records to compute column range metadata for
   * @param targetFields columns (fields) to be collected
   * @param filePath     file path value required for {@link HoodieColumnRangeMetadata}
   * @return map of {@link HoodieColumnRangeMetadata} for each of the provided target fields for
   * the collection of provided records
   */
  public static Map<String, HoodieColumnRangeMetadata<Comparable>> collectColumnRangeMetadata(
      List<HoodieRecord> records,
      List<Pair<String, Schema.Field>> targetFields,
      String filePath,
      Schema recordSchema,
      StorageConfiguration<?> storageConfig) {
    // Helper class to calculate column stats
    class ColumnStats {
      Object minValue;
      Object maxValue;
      long nullCount;
      long valueCount;
    }

    HashMap<String, ColumnStats> allColumnStats = new HashMap<>();

    final Properties properties = new Properties();
    properties.setProperty(HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(),
        storageConfig.getString(HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    // Collect stats for all columns by iterating through records while accounting
    // corresponding stats
    records.forEach((record) -> {
      // For each column (field) we have to index update corresponding column stats
      // with the values from this record
      targetFields.forEach(fieldNameFieldPair -> {
        String fieldName = fieldNameFieldPair.getKey();
        Schema fieldSchema = resolveNullableSchema(fieldNameFieldPair.getValue().schema());
        ColumnStats colStats = allColumnStats.computeIfAbsent(fieldName, ignored -> new ColumnStats());
        Object fieldValue;
        if (record.getRecordType() == HoodieRecord.HoodieRecordType.AVRO) {
          fieldValue = HoodieAvroUtils.getRecordColumnValues(record, new String[] {fieldName}, recordSchema, false)[0];
          if (fieldSchema.getType() == Schema.Type.INT && fieldSchema.getLogicalType() != null && fieldSchema.getLogicalType() == LogicalTypes.date()) {
            fieldValue = java.sql.Date.valueOf(fieldValue.toString());
          }

        } else if (record.getRecordType() == HoodieRecord.HoodieRecordType.SPARK) {
          fieldValue = record.getColumnValues(recordSchema, new String[] {fieldName}, false)[0];
          if (fieldSchema.getType() == Schema.Type.INT && fieldSchema.getLogicalType() != null && fieldSchema.getLogicalType() == LogicalTypes.date()) {
            fieldValue = java.sql.Date.valueOf(LocalDate.ofEpochDay((Integer) fieldValue).toString());
          }
        } else if (record.getRecordType() == HoodieRecord.HoodieRecordType.FLINK) {
          fieldValue = record.getColumnValueAsJava(recordSchema, fieldName, properties);
        } else {
          throw new HoodieException(String.format("Unknown record type: %s", record.getRecordType()));
        }

        colStats.valueCount++;
        if (fieldValue != null && isColumnTypeSupported(fieldSchema, Option.of(record.getRecordType()))) {
          // Set the min value of the field
          if (colStats.minValue == null
              || ConvertingGenericData.INSTANCE.compare(fieldValue, colStats.minValue, fieldSchema) < 0) {
            colStats.minValue = fieldValue;
          }
          // Set the max value of the field
          if (colStats.maxValue == null || ConvertingGenericData.INSTANCE.compare(fieldValue, colStats.maxValue, fieldSchema) > 0) {
            colStats.maxValue = fieldValue;
          }
        } else {
          colStats.nullCount++;
        }
      });
    });

    Stream<HoodieColumnRangeMetadata<Comparable>> hoodieColumnRangeMetadataStream =
        targetFields.stream().map(fieldNameFieldPair -> {
          String fieldName = fieldNameFieldPair.getKey();
          Schema fieldSchema = fieldNameFieldPair.getValue().schema();
          ColumnStats colStats = allColumnStats.get(fieldName);
          HoodieColumnRangeMetadata<Comparable> hcrm = HoodieColumnRangeMetadata.<Comparable>create(
              filePath,
              fieldName,
              colStats == null ? null : coerceToComparable(fieldSchema, colStats.minValue),
              colStats == null ? null : coerceToComparable(fieldSchema, colStats.maxValue),
              colStats == null ? 0L : colStats.nullCount,
              colStats == null ? 0L : colStats.valueCount,
              // NOTE: Size and compressed size statistics are set to 0 to make sure we're not
              //       mixing up those provided by Parquet with the ones from other encodings,
              //       since those are not directly comparable
              0L,
              0L
          );
          return hcrm;
        });
    return hoodieColumnRangeMetadataStream.collect(
        Collectors.toMap(HoodieColumnRangeMetadata::getColumnName, Function.identity()));
  }

  public static Option<String> getColumnStatsValueAsString(Object statsValue) {
    if (statsValue == null) {
      LOG.info("Invalid column stats value: {}", statsValue);
      return Option.empty();
    }
    Class<?> statsValueClass = statsValue.getClass();
    if (COLUMN_STATS_RECORD_SUPPORTED_TYPES.contains(statsValueClass)) {
      return Option.of(String.valueOf(((IndexedRecord) statsValue).get(0)));
    } else {
      throw new HoodieNotSupportedException("Unsupported type: " + statsValueClass.getSimpleName());
    }
  }

  public static boolean isColumnTypeSupported(Schema schema, Option<HoodieRecord.HoodieRecordType> recordType) {
    Schema schemaToCheck = resolveNullableSchema(schema);
    // Check for precision and scale if the schema has a logical decimal type.
    LogicalType logicalType = schemaToCheck.getLogicalType();
    if (logicalType != null && logicalType instanceof LogicalTypes.Decimal) {
      LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
      if (decimalType.getPrecision() + (DECIMAL_MAX_SCALE - decimalType.getScale()) > DECIMAL_MAX_PRECISION || decimalType.getScale() > DECIMAL_MAX_SCALE) {
        return false;
      }
    }

    // if record type is set and if its AVRO, MAP, ARRAY, RECORD and ENUM types are unsupported.
    if (recordType.isPresent() && recordType.get() == HoodieRecord.HoodieRecordType.AVRO) {
      return (schemaToCheck.getType() != Schema.Type.RECORD && schemaToCheck.getType() != Schema.Type.ARRAY && schemaToCheck.getType() != Schema.Type.MAP
          && schemaToCheck.getType() != Schema.Type.ENUM);
    }
    // if record Type is not set or if recordType is SPARK then we cannot support AVRO, MAP, ARRAY, RECORD, ENUM and FIXED and BYTES type as well.
    // HUDI-8585 will add support for BYTES and FIXED
    return schemaToCheck.getType() != Schema.Type.RECORD && schemaToCheck.getType() != Schema.Type.ARRAY && schemaToCheck.getType() != Schema.Type.MAP
        && schemaToCheck.getType() != Schema.Type.ENUM && schemaToCheck.getType() != Schema.Type.BYTES && schemaToCheck.getType() != Schema.Type.FIXED;
  }

  private static Stream<Pair<String, Schema>> getFirstNSupportedFields(Schema tableSchema, int n, Option<HoodieRecord.HoodieRecordType> recordType) {
    return getFirstNFields(tableSchema.getFields().stream()
        .filter(field -> isColumnTypeSupported(field.schema(), recordType)).map(field -> Pair.of(field.name(), field.schema())), n);
  }

  private static Stream<Pair<String, Schema>> getFirstNFields(Stream<Pair<String, Schema>> fieldSchemaPairStream, int n) {
    return fieldSchemaPairStream.filter(fieldSchemaPair -> !HOODIE_META_COLUMNS_WITH_OPERATION.contains(fieldSchemaPair.getKey())).limit(n);
  }

}
