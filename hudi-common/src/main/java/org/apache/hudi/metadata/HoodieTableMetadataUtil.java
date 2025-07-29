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

import org.apache.hudi.avro.ConvertingGenericData;
import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.BooleanWrapper;
import org.apache.hudi.avro.model.DateWrapper;
import org.apache.hudi.avro.model.DoubleWrapper;
import org.apache.hudi.avro.model.FloatWrapper;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataFileInfo;
import org.apache.hudi.avro.model.HoodieRecordIndexInfo;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.IntWrapper;
import org.apache.hudi.avro.model.LongWrapper;
import org.apache.hudi.avro.model.StringWrapper;
import org.apache.hudi.avro.model.TimeMicrosWrapper;
import org.apache.hudi.avro.model.TimestampMicrosWrapper;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.buffer.KeyBasedFileGroupRecordBuffer;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.util.Lazy;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.avro.HoodieAvroUtils.projectSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.unwrapAvroValueWrapper;
import static org.apache.hudi.avro.HoodieAvroUtils.wrapValueIntoAvro;
import static org.apache.hudi.common.config.HoodieCommonConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.MAX_MEMORY_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieReaderConfig.REALTIME_SKIP_MERGE;
import static org.apache.hudi.common.fs.FSUtils.getFileNameFromPath;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.EXPRESSION_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.IDENTITY_TRANSFORM;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_IS_TIGHT_BOUND;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_MISSING_FILEINDEX_FALLBACK;
import static org.apache.hudi.metadata.HoodieTableMetadata.EMPTY_PARTITION_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.MetadataPartitionType.fromPartitionPath;
import static org.apache.hudi.metadata.MetadataPartitionType.isNewExpressionIndexDefinitionRequired;
import static org.apache.hudi.metadata.MetadataPartitionType.isNewSecondaryIndexDefinitionRequired;

/**
 * A utility to convert timeline information to metadata table records.
 */
public class HoodieTableMetadataUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableMetadataUtil.class);

  public static final String PARTITION_NAME_FILES = "files";
  public static final String PARTITION_NAME_PARTITION_STATS = "partition_stats";
  public static final String PARTITION_NAME_COLUMN_STATS = "column_stats";
  public static final String PARTITION_NAME_BLOOM_FILTERS = "bloom_filters";
  public static final String PARTITION_NAME_RECORD_INDEX = "record_index";
  public static final String PARTITION_NAME_EXPRESSION_INDEX = "expr_index";
  public static final String PARTITION_NAME_EXPRESSION_INDEX_PREFIX = "expr_index_";
  public static final String PARTITION_NAME_SECONDARY_INDEX = "secondary_index";
  public static final String PARTITION_NAME_SECONDARY_INDEX_PREFIX = "secondary_index_";

  private static final Set<Schema.Type> SUPPORTED_TYPES_PARTITION_STATS = new HashSet<>(Arrays.asList(
      Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING, Schema.Type.BOOLEAN, Schema.Type.NULL, Schema.Type.BYTES));
  public static final Set<String> SUPPORTED_META_FIELDS_PARTITION_STATS = new HashSet<>(Arrays.asList(
      HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName(),
      HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.getFieldName(),
      HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD.getFieldName()));

  // The maximum allowed precision and scale as per the payload schema. See DecimalWrapper in HoodieMetadata.avsc:
  // https://github.com/apache/hudi/blob/45dedd819e56e521148bde51a3dfa4e472ea70cd/hudi-common/src/main/avro/HoodieMetadata.avsc#L247
  private static final int DECIMAL_MAX_PRECISION = 30;
  private static final int DECIMAL_MAX_SCALE = 15;

  private HoodieTableMetadataUtil() {
  }

  public static final Set<Class<?>> COLUMN_STATS_RECORD_SUPPORTED_TYPES = new HashSet<>(Arrays.asList(
      IntWrapper.class, BooleanWrapper.class, DateWrapper.class,
      DoubleWrapper.class, FloatWrapper.class, LongWrapper.class,
      StringWrapper.class, TimeMicrosWrapper.class, TimestampMicrosWrapper.class));

  /**
   * Returns whether the files partition of metadata table is ready for read.
   *
   * @param metaClient {@link HoodieTableMetaClient} instance.
   * @return true if the files partition of metadata table is ready for read,
   * based on the table config; false otherwise.
   */
  public static boolean isFilesPartitionAvailable(HoodieTableMetaClient metaClient) {
    return metaClient.getTableConfig().getMetadataPartitions()
        .contains(HoodieTableMetadataUtil.PARTITION_NAME_FILES);
  }

  /**
   * Collects {@link HoodieColumnRangeMetadata} for the provided collection of records, pretending
   * as if provided records have been persisted w/in given {@code filePath}
   *
   * @param records target records to compute column range metadata for
   * @param targetFields columns (fields) to be collected
   * @param filePath file path value required for {@link HoodieColumnRangeMetadata}
   *
   * @return map of {@link HoodieColumnRangeMetadata} for each of the provided target fields for
   *         the collection of provided records
   */
  public static Map<String, HoodieColumnRangeMetadata<Comparable>> collectColumnRangeMetadata(
      Iterator<HoodieRecord> records,
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
    records.forEachRemaining((record) -> {
      // For each column (field) we have to index update corresponding column stats
      // with the values from this record
      targetFields.forEach(fieldNameFieldPair -> {
        String fieldName = fieldNameFieldPair.getKey();
        Schema fieldSchema = resolveNullableSchema(fieldNameFieldPair.getValue().schema());
        ColumnStats colStats = allColumnStats.computeIfAbsent(fieldName, ignored -> new ColumnStats());
        Object fieldValue;
        if (record.getRecordType() == HoodieRecordType.AVRO) {
          fieldValue = HoodieAvroUtils.getRecordColumnValues(record, new String[]{fieldName}, recordSchema, false)[0];
          if (fieldValue != null && fieldSchema.getType() == Schema.Type.INT && fieldSchema.getLogicalType() != null && fieldSchema.getLogicalType() == LogicalTypes.date()) {
            fieldValue = java.sql.Date.valueOf(fieldValue.toString());
          }

        } else if (record.getRecordType() == HoodieRecordType.SPARK) {
          fieldValue = record.getColumnValues(recordSchema, new String[]{fieldName}, false)[0];
          if (fieldValue != null && fieldSchema.getType() == Schema.Type.INT && fieldSchema.getLogicalType() != null && fieldSchema.getLogicalType() == LogicalTypes.date()) {
            fieldValue = java.sql.Date.valueOf(LocalDate.ofEpochDay((Integer) fieldValue).toString());
          }
        } else if (record.getRecordType() == HoodieRecordType.FLINK) {
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

  /**
   * Delete the metadata table for the dataset. This will be invoked during upgrade/downgrade operation during which
   * no other
   * process should be running.
   *
   * @param basePath base path of the dataset
   * @param context  instance of {@link HoodieEngineContext}.
   */
  public static void deleteMetadataTable(String basePath, HoodieEngineContext context) {
    HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath).setConf(context.getStorageConf().newInstance()).build();
    deleteMetadataTable(dataMetaClient, context, false);
  }

  /**
   * Deletes the metadata partition from the file system.
   *
   * @param basePath      - base path of the dataset
   * @param context       - instance of {@link HoodieEngineContext}
   * @param partitionPath - Partition path of the partition to delete
   */
  public static void deleteMetadataPartition(StoragePath basePath, HoodieEngineContext context, String partitionPath) {
    HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath).setConf(context.getStorageConf().newInstance()).build();
    deleteMetadataTablePartition(dataMetaClient, context, partitionPath, false);
  }

  /**
   * Check if the given metadata partition exists.
   *
   * @param basePath base path of the dataset
   * @param context  instance of {@link HoodieEngineContext}.
   */
  public static boolean metadataPartitionExists(String basePath, HoodieEngineContext context, String partitionPath) {
    final String metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    HoodieStorage storage = HoodieStorageUtils.getStorage(metadataTablePath, context.getStorageConf());
    try {
      return storage.exists(new StoragePath(metadataTablePath, partitionPath));
    } catch (Exception e) {
      throw new HoodieIOException(String.format("Failed to check metadata partition %s exists.", partitionPath));
    }
  }

  public static boolean metadataPartitionExists(StoragePath basePath, HoodieEngineContext context, String partitionPath) {
    return metadataPartitionExists(basePath.toString(), context, partitionPath);
  }

  /**
   * Convert commit action to metadata records for the enabled partition types.
   *
   * @param context               - Engine context to use
   * @param hoodieConfig          - Hudi configs
   * @param commitMetadata        - Commit action metadata
   * @param instantTime           - Action instant time
   * @param dataMetaClient        - HoodieTableMetaClient for data
   * @param tableMetadata
   * @param metadataConfig        - HoodieMetadataConfig
   * @param enabledPartitionTypes - Set of enabled MDT partitions to update
   * @param bloomFilterType       - Type of generated bloom filter records
   * @param bloomIndexParallelism - Parallelism for bloom filter record generation
   * @param enableOptimizeLogBlocksScan - flag used to enable scanInternalV2 for log blocks in data table
   * @return Map of partition to metadata records for the commit action
   */
  public static Map<String, HoodieData<HoodieRecord>> convertMetadataToRecords(HoodieEngineContext context, HoodieConfig hoodieConfig, HoodieCommitMetadata commitMetadata,
                                                                               String instantTime, HoodieTableMetaClient dataMetaClient, HoodieTableMetadata tableMetadata,
                                                                               HoodieMetadataConfig metadataConfig, Set<String> enabledPartitionTypes, String bloomFilterType,
                                                                               int bloomIndexParallelism, int writesFileIdEncoding, EngineType engineType,
                                                                               Option<HoodieRecordType> recordTypeOpt, boolean enableOptimizeLogBlocksScan) {
    final Map<String, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = context.parallelize(
        convertMetadataToFilesPartitionRecords(commitMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES.getPartitionPath(), filesPartitionRecordsRDD);

    if (enabledPartitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath())) {
      final HoodieData<HoodieRecord> metadataBloomFilterRecords = convertMetadataToBloomFilterRecords(
          context, hoodieConfig, commitMetadata, instantTime, dataMetaClient, bloomFilterType, bloomIndexParallelism);
      partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath(), metadataBloomFilterRecords);
    }

    if (enabledPartitionTypes.contains(MetadataPartitionType.COLUMN_STATS.getPartitionPath())) {
      final HoodieData<HoodieRecord> metadataColumnStatsRDD = convertMetadataToColumnStatsRecords(commitMetadata, context,
          dataMetaClient, metadataConfig, recordTypeOpt);
      partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), metadataColumnStatsRDD);
    }
    if (enabledPartitionTypes.contains(MetadataPartitionType.PARTITION_STATS.getPartitionPath())) {
      checkState(MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(dataMetaClient),
          "Column stats partition must be enabled to generate partition stats. Please enable: " + HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
      // Generate Hoodie Pair data of partition name and list of column range metadata for all the files in that partition
      boolean isDeletePartition = commitMetadata.getOperationType().equals(WriteOperationType.DELETE_PARTITION);
      final HoodieData<HoodieRecord> partitionStatsRDD = convertMetadataToPartitionStatRecords(commitMetadata, context,
          dataMetaClient, tableMetadata, metadataConfig, recordTypeOpt, isDeletePartition);
      partitionToRecordsMap.put(MetadataPartitionType.PARTITION_STATS.getPartitionPath(), partitionStatsRDD);
    }
    if (enabledPartitionTypes.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath())) {
      partitionToRecordsMap.put(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), convertMetadataToRecordIndexRecords(context, commitMetadata, metadataConfig,
          dataMetaClient, writesFileIdEncoding, instantTime, engineType, enableOptimizeLogBlocksScan));
    }
    return partitionToRecordsMap;
  }

  /**
   * Finds all new files/partitions created as part of commit and creates metadata table records for them.
   *
   * @param commitMetadata - Commit action metadata
   * @param instantTime    - Commit action instant time
   * @return List of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToFilesPartitionRecords(HoodieCommitMetadata commitMetadata,
                                                                          String instantTime) {
    List<HoodieRecord> records = new ArrayList<>(commitMetadata.getPartitionToWriteStats().size());

    // Add record bearing added partitions list
    List<String> partitionsAdded = getPartitionsAdded(commitMetadata);

    records.add(HoodieMetadataPayload.createPartitionListRecord(partitionsAdded));

    // Update files listing records for each individual partition
    HoodieAccumulator newFileCount = HoodieAtomicLongAccumulator.create();
    List<HoodieRecord<HoodieMetadataPayload>> updatedPartitionFilesRecords =
        commitMetadata.getPartitionToWriteStats().entrySet()
            .stream()
            .map(entry -> {
              String partitionStatName = entry.getKey();
              List<HoodieWriteStat> writeStats = entry.getValue();

              HashMap<String, Long> updatedFilesToSizesMapping =
                  writeStats.stream().reduce(new HashMap<>(writeStats.size()),
                      (map, stat) -> {
                        String pathWithPartition = stat.getPath();
                        if (pathWithPartition == null) {
                          // Empty partition
                          LOG.warn("Unable to find path in write stat to update metadata table {}", stat);
                          return map;
                        }

                        String fileName = FSUtils.getFileName(pathWithPartition, partitionStatName);

                        // Since write-stats are coming in no particular order, if the same
                        // file have previously been appended to w/in the txn, we simply pick max
                        // of the sizes as reported after every write, since file-sizes are
                        // monotonically increasing (ie file-size never goes down, unless deleted)
                        map.merge(fileName, stat.getFileSizeInBytes(), Math::max);

                        Map<String, Long> cdcPathAndSizes = stat.getCdcStats();
                        if (cdcPathAndSizes != null && !cdcPathAndSizes.isEmpty()) {
                          cdcPathAndSizes.forEach((key, value) -> map.put(FSUtils.getFileName(key, partitionStatName), value));
                        }
                        return map;
                      },
                      CollectionUtils::combine);

              newFileCount.add(updatedFilesToSizesMapping.size());
              return HoodieMetadataPayload.createPartitionFilesRecord(partitionStatName, updatedFilesToSizesMapping,
                  Collections.emptyList());
            })
            .collect(Collectors.toList());

    records.addAll(updatedPartitionFilesRecords);

    LOG.info("Updating at {} from Commit/{}. #partitions_updated={}, #files_added={}", instantTime, commitMetadata.getOperationType(),
        records.size(), newFileCount.value());

    return records;
  }

  private static List<String> getPartitionsAdded(HoodieCommitMetadata commitMetadata) {
    return commitMetadata.getPartitionToWriteStats().keySet().stream()
        // We need to make sure we properly handle case of non-partitioned tables
        .map(HoodieTableMetadataUtil::getPartitionIdentifierForFilesPartition)
        .collect(Collectors.toList());
  }

  /**
   * Returns all the incremental write partition paths as a set with the given commits metadata.
   *
   * @param metadataList The commits metadata
   * @return the partition path set
   */
  public static Set<String> getWritePartitionPaths(List<HoodieCommitMetadata> metadataList) {
    return metadataList.stream()
        .map(HoodieCommitMetadata::getWritePartitionPaths)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  /**
   * Convert commit action metadata to bloom filter records.
   *
   * @param context                 - Engine context to use
   * @param hoodieConfig            - Hudi configs
   * @param commitMetadata          - Commit action metadata
   * @param instantTime             - Action instant time
   * @param dataMetaClient          - HoodieTableMetaClient for data
   * @param bloomFilterType         - Type of generated bloom filter records
   * @param bloomIndexParallelism   - Parallelism for bloom filter record generation
   * @return HoodieData of metadata table records
   */
  public static HoodieData<HoodieRecord> convertMetadataToBloomFilterRecords(HoodieEngineContext context,
                                                                             HoodieConfig hoodieConfig,
                                                                             HoodieCommitMetadata commitMetadata,
                                                                             String instantTime,
                                                                             HoodieTableMetaClient dataMetaClient,
                                                                             String bloomFilterType,
                                                                             int bloomIndexParallelism) {
    final List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());
    if (allWriteStats.isEmpty()) {
      return context.emptyHoodieData();
    }

    final int parallelism = Math.max(Math.min(allWriteStats.size(), bloomIndexParallelism), 1);
    HoodieData<HoodieWriteStat> allWriteStatsRDD = context.parallelize(allWriteStats, parallelism);
    return allWriteStatsRDD.flatMap(hoodieWriteStat -> {
      final String partition = hoodieWriteStat.getPartitionPath();

      // For bloom filter index, delta writes do not change the base file bloom filter entries
      if (hoodieWriteStat instanceof HoodieDeltaWriteStat) {
        return Collections.emptyListIterator();
      }

      String pathWithPartition = hoodieWriteStat.getPath();
      if (pathWithPartition == null) {
        // Empty partition
        LOG.error("Failed to find path in write stat to update metadata table {}", hoodieWriteStat);
        return Collections.emptyListIterator();
      }

      String fileName = FSUtils.getFileName(pathWithPartition, partition);
      if (!FSUtils.isBaseFile(new StoragePath(fileName))) {
        return Collections.emptyListIterator();
      }

      final StoragePath writeFilePath = new StoragePath(dataMetaClient.getBasePath(), pathWithPartition);
      try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(dataMetaClient.getStorage())
          .getReaderFactory(HoodieRecordType.AVRO).getFileReader(hoodieConfig, writeFilePath)) {
        try {
          final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
          if (fileBloomFilter == null) {
            LOG.error("Failed to read bloom filter for {}", writeFilePath);
            return Collections.emptyListIterator();
          }
          ByteBuffer bloomByteBuffer = ByteBuffer.wrap(getUTF8Bytes(fileBloomFilter.serializeToString()));
          HoodieRecord record = HoodieMetadataPayload.createBloomFilterMetadataRecord(
              partition, fileName, instantTime, bloomFilterType, bloomByteBuffer, false);
          return Collections.singletonList(record).iterator();
        } catch (Exception e) {
          LOG.error("Failed to read bloom filter for {}", writeFilePath);
          return Collections.emptyListIterator();
        }
      } catch (IOException e) {
        LOG.error("Failed to get bloom filter for file: {}, write stat: {}", writeFilePath, hoodieWriteStat);
      }
      return Collections.emptyListIterator();
    });
  }

  /**
   * Convert the clean action to metadata records.
   */
  public static Map<String, HoodieData<HoodieRecord>> convertMetadataToRecords(HoodieEngineContext engineContext,
                                                                               HoodieCleanMetadata cleanMetadata,
                                                                               String instantTime,
                                                                               HoodieTableMetaClient dataMetaClient,
                                                                               HoodieMetadataConfig metadataConfig,
                                                                               List<MetadataPartitionType> enabledPartitionTypes,
                                                                               int bloomIndexParallelism,
                                                                               Option<HoodieRecordType> recordTypeOpt) {
    final Map<String, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = engineContext.parallelize(
        convertMetadataToFilesPartitionRecords(cleanMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES.getPartitionPath(), filesPartitionRecordsRDD);
    if (enabledPartitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS)) {
      final HoodieData<HoodieRecord> metadataBloomFilterRecordsRDD =
          convertMetadataToBloomFilterRecords(cleanMetadata, engineContext, instantTime, bloomIndexParallelism);
      partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath(), metadataBloomFilterRecordsRDD);
    }

    if (enabledPartitionTypes.contains(MetadataPartitionType.COLUMN_STATS)) {
      final HoodieData<HoodieRecord> metadataColumnStatsRDD =
          convertMetadataToColumnStatsRecords(cleanMetadata, engineContext,
              dataMetaClient, metadataConfig, recordTypeOpt);
      partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), metadataColumnStatsRDD);
    }
    if (enabledPartitionTypes.contains(MetadataPartitionType.EXPRESSION_INDEX)) {
      convertMetadataToExpressionIndexRecords(engineContext, cleanMetadata, instantTime, dataMetaClient, metadataConfig, bloomIndexParallelism, partitionToRecordsMap,
          recordTypeOpt);
    }

    return partitionToRecordsMap;
  }

  private static void convertMetadataToExpressionIndexRecords(HoodieEngineContext engineContext, HoodieCleanMetadata cleanMetadata,
                                                              String instantTime, HoodieTableMetaClient dataMetaClient,
                                                              HoodieMetadataConfig metadataConfig, int bloomIndexParallelism,
                                                              Map<String, HoodieData<HoodieRecord>> partitionToRecordsMap,
                                                              Option<HoodieRecordType> recordTypeOpt) {
    Option<HoodieIndexMetadata> indexMetadata = dataMetaClient.getIndexMetadata();
    if (indexMetadata.isPresent()) {
      HoodieIndexMetadata metadata = indexMetadata.get();
      Map<String, HoodieIndexDefinition> indexDefinitions = metadata.getIndexDefinitions();
      if (indexDefinitions.isEmpty()) {
        throw new HoodieMetadataException("Expression index metadata not found");
      }
      // iterate over each index definition and check:
      // if it is a expression index using column_stats, then follow the same approach as column_stats
      // if it is a expression index using bloom_filters, then follow the same approach as bloom_filters
      // else throw an exception
      for (Map.Entry<String, HoodieIndexDefinition> entry : indexDefinitions.entrySet()) {
        String indexName = entry.getKey();
        HoodieIndexDefinition indexDefinition = entry.getValue();
        if (MetadataPartitionType.EXPRESSION_INDEX.equals(fromPartitionPath(indexDefinition.getIndexName()))) {
          if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
            partitionToRecordsMap.put(indexName, convertMetadataToBloomFilterRecords(cleanMetadata, engineContext, instantTime, bloomIndexParallelism));
          } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
            HoodieMetadataConfig modifiedMetadataConfig = HoodieMetadataConfig.newBuilder()
                .withProperties(metadataConfig.getProps())
                .withColumnStatsIndexForColumns(String.join(",", indexDefinition.getSourceFields()))
                .build();
            partitionToRecordsMap.put(indexName,
                convertMetadataToColumnStatsRecords(cleanMetadata, engineContext, dataMetaClient, modifiedMetadataConfig, recordTypeOpt));
          } else {
            throw new HoodieMetadataException("Unsupported expression index type");
          }
        }
      }
    } else {
      throw new HoodieMetadataException("Expression index metadata not found");
    }
  }

  /**
   * Finds all files that were deleted as part of a clean and creates metadata table records for them.
   *
   * @param cleanMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToFilesPartitionRecords(HoodieCleanMetadata cleanMetadata,
                                                                          String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};
    List<String> deletedPartitions = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partitionName, partitionMetadata) -> {
      boolean isPartitionDeleted = partitionMetadata.getIsPartitionDeleted();
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      records.add(HoodieMetadataPayload.createPartitionFilesRecord(partitionName, Collections.emptyMap(),
          deletedFiles, isPartitionDeleted));
      fileDeleteCount[0] += deletedFiles.size();
      if (isPartitionDeleted) {
        deletedPartitions.add(partitionName);
      }
    });

    if (!deletedPartitions.isEmpty()) {
      // if there are partitions to be deleted, add them to delete list
      records.add(HoodieMetadataPayload.createPartitionListRecord(deletedPartitions, true));
    }
    LOG.info("Updating at {} from Clean. #partitions_updated={}, #files_deleted={}, #partitions_deleted={}",
            instantTime, records.size(), fileDeleteCount[0], deletedPartitions.size());
    return records;
  }

  public static Map<String, HoodieData<HoodieRecord>> convertMissingPartitionRecords(HoodieEngineContext engineContext,
                                                                                     List<String> deletedPartitions, Map<String, Map<String, Long>> filesAdded,
                                                                                     Map<String, List<String>> filesDeleted, String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};
    int[] filesAddedCount = {0};

    filesAdded.forEach((partition, filesToAdd) -> {
      filesAddedCount[0] += filesToAdd.size();
      List<String> filesToDelete = filesDeleted.getOrDefault(partition, Collections.emptyList());
      fileDeleteCount[0] += filesToDelete.size();
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, filesToAdd, filesToDelete);
      records.add(record);
    });

    // there could be partitions which only has missing deleted files.
    filesDeleted.forEach((partition, filesToDelete) -> {
      if (!filesAdded.containsKey(partition)) {
        fileDeleteCount[0] += filesToDelete.size();
        HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Collections.emptyMap(), filesToDelete);
        records.add(record);
      }
    });

    if (!deletedPartitions.isEmpty()) {
      // if there are partitions to be deleted, add them to delete list
      records.add(HoodieMetadataPayload.createPartitionListRecord(deletedPartitions, true));
    }

    LOG.info("Re-adding missing records at {} during Restore. #partitions_updated={}, #files_added={}, #files_deleted={}, #partitions_deleted={}",
            instantTime, records.size(), filesAddedCount[0], fileDeleteCount[0], deletedPartitions.size());
    return Collections.singletonMap(MetadataPartitionType.FILES.getPartitionPath(), engineContext.parallelize(records, 1));
  }

  /**
   * Convert clean metadata to bloom filter index records.
   *
   * @param cleanMetadata           - Clean action metadata
   * @param engineContext           - Engine context
   * @param instantTime             - Clean action instant time
   * @param bloomIndexParallelism   - Parallelism for bloom filter record generation
   * @return List of bloom filter index records for the clean metadata
   */
  public static HoodieData<HoodieRecord> convertMetadataToBloomFilterRecords(HoodieCleanMetadata cleanMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             String instantTime,
                                                                             int bloomIndexParallelism) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> {
        final StoragePath deletedFilePath = new StoragePath(entry);
        if (FSUtils.isBaseFile(deletedFilePath)) {
          deleteFileList.add(Pair.of(partition, deletedFilePath.getName()));
        }
      });
    });

    final int parallelism = Math.max(Math.min(deleteFileList.size(), bloomIndexParallelism), 1);
    HoodieData<Pair<String, String>> deleteFileListRDD = engineContext.parallelize(deleteFileList, parallelism);
    return deleteFileListRDD.map(deleteFileInfoPair -> HoodieMetadataPayload.createBloomFilterMetadataRecord(
        deleteFileInfoPair.getLeft(), deleteFileInfoPair.getRight(), instantTime, StringUtils.EMPTY_STRING,
        ByteBuffer.allocate(0), true));
  }

  /**
   * Convert clean metadata to column stats index records.
   *
   * @param cleanMetadata                    - Clean action metadata
   * @param engineContext                    - Engine context
   * @param dataMetaClient                   - HoodieTableMetaClient for data
   * @param metadataConfig                   - HoodieMetadataConfig
   * @return List of column stats index records for the clean metadata
   */
  public static HoodieData<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCleanMetadata cleanMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             HoodieTableMetaClient dataMetaClient,
                                                                             HoodieMetadataConfig metadataConfig,
                                                                             Option<HoodieRecordType> recordTypeOpt) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> deleteFileList.add(Pair.of(partition, entry)));
    });
    if (deleteFileList.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    List<String> columnsToIndex = new ArrayList<>(getColumnsToIndex(dataMetaClient.getTableConfig(), metadataConfig,
        Lazy.lazily(() -> tryResolveSchemaForTable(dataMetaClient)), false, recordTypeOpt).keySet());

    if (columnsToIndex.isEmpty()) {
      // In case there are no columns to index, bail
      LOG.warn("No columns to index for column stats index.");
      return engineContext.emptyHoodieData();
    }

    int parallelism = Math.max(Math.min(deleteFileList.size(), metadataConfig.getColumnStatsIndexParallelism()), 1);
    return engineContext.parallelize(deleteFileList, parallelism)
        .flatMap(deleteFileInfoPair -> {
          String partitionPath = deleteFileInfoPair.getLeft();
          String fileName = deleteFileInfoPair.getRight();
          return getColumnStatsRecords(partitionPath, fileName, dataMetaClient, columnsToIndex, true).iterator();
        });
  }

  @VisibleForTesting
  public static <T> HoodieData<HoodieRecord> convertMetadataToRecordIndexRecords(HoodieEngineContext engineContext,
                                                                                 HoodieCommitMetadata commitMetadata,
                                                                                 HoodieMetadataConfig metadataConfig,
                                                                                 HoodieTableMetaClient dataTableMetaClient,
                                                                                 int writesFileIdEncoding,
                                                                                 String instantTime,
                                                                                 EngineType engineType,
                                                                                 boolean enableOptimizeLogBlocksScan) {
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
      ReaderContextFactory<T> readerContextFactory = engineContext.getReaderContextFactory(dataTableMetaClient);
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
              Pair<Set<String>, Set<String>> revivedAndDeletedKeys = getRevivedAndDeletedKeysFromMergedLogs(dataTableMetaClient, instantTime, allLogFilePaths, finalWriterSchemaOpt,
                  currentLogFilePaths, partitionPath, readerContextFactory.getContext(), enableOptimizeLogBlocksScan);
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

  /**
   * Get the revived and deleted keys from the merged log files. The logic is as below. Suppose:
   * <li>A = Set of keys that are valid (not deleted) in the previous log files merged</li>
   * <li>B = Set of keys that are valid in all log files including current log file merged</li>
   * <li>C = Set of keys that are deleted in the current log file</li>
   * <li>Then, D = Set of deleted keys = C - (B - A)</li>
   *
   * @param dataTableMetaClient  data table meta client
   * @param instantTime          timestamp of the commit
   * @param partitionPath        partition path of the log files
   * @param readerContext        the reader context for the engine
   * @param logFilePaths         list of log file paths including current and previous file slices
   * @param finalWriterSchemaOpt records schema
   * @param currentLogFilePaths  list of log file paths for the current instant
   * @param enableOptimizedLogBlocksScan - flag used to enable scanInternalV2 for log blocks in data table
   * @return pair of revived and deleted keys
   */
  @VisibleForTesting
  public static <T> Pair<Set<String>, Set<String>> getRevivedAndDeletedKeysFromMergedLogs(HoodieTableMetaClient dataTableMetaClient,
                                                                                          String instantTime,
                                                                                          List<String> logFilePaths,
                                                                                          Option<Schema> finalWriterSchemaOpt,
                                                                                          List<String> currentLogFilePaths,
                                                                                          String partitionPath,
                                                                                          HoodieReaderContext<T> readerContext,
                                                                                          boolean enableOptimizedLogBlocksScan) {
    // Separate out the current log files
    List<String> logFilePathsWithoutCurrentLogFiles = logFilePaths.stream()
        .filter(logFilePath -> !currentLogFilePaths.contains(logFilePath))
        .collect(toList());
    if (logFilePathsWithoutCurrentLogFiles.isEmpty()) {
      // Only current log file is present, so we can directly get the deleted record keys from it and return the RLI records.
      try (ClosableIterator<BufferedRecord<T>> currentLogRecords =
               getLogRecords(currentLogFilePaths, dataTableMetaClient, finalWriterSchemaOpt, instantTime, partitionPath, readerContext, enableOptimizedLogBlocksScan)) {
        Set<String> deletedKeys = new HashSet<>();
        currentLogRecords.forEachRemaining(record -> {
          if (record.isDelete()) {
            deletedKeys.add(record.getRecordKey());
          }
        });
        return Pair.of(Collections.emptySet(), deletedKeys);
      }
    }
    return getRevivedAndDeletedKeys(dataTableMetaClient, instantTime, partitionPath, readerContext,
            logFilePaths, finalWriterSchemaOpt, logFilePathsWithoutCurrentLogFiles, enableOptimizedLogBlocksScan);
  }

  private static <T> Pair<Set<String>, Set<String>> getRevivedAndDeletedKeys(HoodieTableMetaClient dataTableMetaClient,
                                                                             String instantTime,
                                                                             String partitionPath,
                                                                             HoodieReaderContext<T> readerContext,
                                                                             List<String> logFilePaths,
                                                                             Option<Schema> finalWriterSchemaOpt,
                                                                             List<String> logFilePathsWithoutCurrentLogFiles,
                                                                             boolean enableOptimizedLogBlocksScan) {
    // Partition valid (non-deleted) and deleted keys from all log files, including current, in a single pass
    Set<String> validKeysForAllLogs = new HashSet<>();
    Set<String> deletedKeysForAllLogs = new HashSet<>();
    // Fetch log records for all log files
    try (ClosableIterator<BufferedRecord<T>> allLogRecords =
             getLogRecords(logFilePaths, dataTableMetaClient, finalWriterSchemaOpt, instantTime, partitionPath, readerContext, enableOptimizedLogBlocksScan)) {
      allLogRecords.forEachRemaining(record -> {
        if (record.isDelete()) {
          deletedKeysForAllLogs.add(record.getRecordKey());
        } else {
          validKeysForAllLogs.add(record.getRecordKey());
        }
      });
    }

    // Partition valid (non-deleted) and deleted keys from previous log files in a single pass
    Set<String> validKeysForPreviousLogs = new HashSet<>();
    Set<String> deletedKeysForPreviousLogs = new HashSet<>();
    // Fetch log records for previous log files (excluding the current log files)
    try (ClosableIterator<BufferedRecord<T>> previousLogRecords =
             getLogRecords(logFilePathsWithoutCurrentLogFiles, dataTableMetaClient, finalWriterSchemaOpt, instantTime, partitionPath, readerContext, enableOptimizedLogBlocksScan)) {
      previousLogRecords.forEachRemaining(record -> {
        if (record.isDelete()) {
          deletedKeysForPreviousLogs.add(record.getRecordKey());
        } else {
          validKeysForPreviousLogs.add(record.getRecordKey());
        }
      });
    }

    return computeRevivedAndDeletedKeys(validKeysForPreviousLogs, deletedKeysForPreviousLogs, validKeysForAllLogs, deletedKeysForAllLogs);
  }

  private static <T> ClosableIterator<BufferedRecord<T>> getLogRecords(List<String> logFilePaths,
                                                                       HoodieTableMetaClient datasetMetaClient,
                                                                       Option<Schema> writerSchemaOpt,
                                                                       String latestCommitTimestamp,
                                                                       String partitionPath,
                                                                       HoodieReaderContext<T> readerContext,
                                                                       boolean enableOptimizedLogBlocksScan) {
    if (writerSchemaOpt.isPresent() && !logFilePaths.isEmpty()) {
      List<HoodieLogFile> logFiles = logFilePaths.stream().map(HoodieLogFile::new).collect(Collectors.toList());
      FileSlice fileSlice = new FileSlice(partitionPath, logFiles.get(0).getFileId(), logFiles.get(0).getDeltaCommitTime());
      logFiles.forEach(fileSlice::addLogFile);
      final StorageConfiguration<?> storageConf = datasetMetaClient.getStorageConf();
      TypedProperties properties = getFileGroupReaderPropertiesFromStorageConf(storageConf);
      readerContext.setLatestCommitTime(latestCommitTimestamp);
      readerContext.setHasBootstrapBaseFile(false);
      readerContext.setHasLogFiles(true);
      HoodieTableConfig tableConfig = datasetMetaClient.getTableConfig();
      readerContext.initRecordMerger(properties);
      readerContext.setSchemaHandler(new FileGroupReaderSchemaHandler<>(readerContext, writerSchemaOpt.get(), writerSchemaOpt.get(), Option.empty(), tableConfig, properties));
      HoodieReadStats readStats = new HoodieReadStats();
      KeyBasedFileGroupRecordBuffer<T> recordBuffer = new KeyBasedFileGroupRecordBuffer<>(readerContext, datasetMetaClient,
          readerContext.getMergeMode(), PartialUpdateMode.NONE, properties, tableConfig.getPreCombineFields(),
          UpdateProcessor.create(readStats, readerContext, true, Option.empty()));

      // CRITICAL: Ensure allowInflightInstants is set to true
      try (HoodieMergedLogRecordReader<T> mergedLogRecordReader = HoodieMergedLogRecordReader.<T>newBuilder()
          .withStorage(datasetMetaClient.getStorage())
          .withHoodieReaderContext(readerContext)
          .withLogFiles(logFilePaths.stream().map(HoodieLogFile::new).collect(toList()))
          .withReverseReader(false)
          .withBufferSize(HoodieMetadataConfig.MAX_READER_BUFFER_SIZE_PROP.defaultValue())
          .withPartition(partitionPath)
          .withAllowInflightInstants(true)
          .withMetaClient(datasetMetaClient)
          .withAllowInflightInstants(true)
          .withRecordBuffer(recordBuffer)
          .withOptimizedLogBlocksScan(enableOptimizedLogBlocksScan)
          .build()) {
        // initializes the record buffer with the log records
        return recordBuffer.getLogRecordIterator();
      }
    }
    return ClosableIterator.wrap(Collections.emptyIterator());
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
   * @param recordIndexRecords hoodie records after rli index lookup.
   * @param parallelism parallelism to use.
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

  /**
   * Convert rollback action metadata to metadata table records.
   * <p>
   * We only need to handle FILES partition here as HUDI rollbacks on MOR table may end up adding a new log file. All other partitions
   * are handled by actual rollback of the deltacommit which added records to those partitions.
   */
  public static Map<String, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext engineContext, HoodieTableMetaClient dataTableMetaClient, HoodieRollbackMetadata rollbackMetadata, String instantTime) {

    List<HoodieRecord> filesPartitionRecords = HoodieTableMetadataUtil.convertMetadataToRollbackRecords(rollbackMetadata, instantTime, dataTableMetaClient);
    final HoodieData<HoodieRecord> rollbackRecordsRDD = filesPartitionRecords.isEmpty() ? engineContext.emptyHoodieData()
        : engineContext.parallelize(filesPartitionRecords, filesPartitionRecords.size());

    return Collections.singletonMap(MetadataPartitionType.FILES.getPartitionPath(), rollbackRecordsRDD);
  }

  /**
   * Convert rollback action metadata to files partition records.
   * Consider only new log files added.
   */
  private static List<HoodieRecord> convertMetadataToRollbackRecords(HoodieRollbackMetadata rollbackMetadata,
                                                                     String instantTime,
                                                                     HoodieTableMetaClient dataTableMetaClient) {
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    processRollbackMetadata(rollbackMetadata, partitionToAppendedFiles);
    return convertFilesToFilesPartitionRecords(Collections.emptyMap(), partitionToAppendedFiles, instantTime, "Rollback");
  }

  /**
   * Extracts information about the deleted and append files from the {@code HoodieRollbackMetadata}.
   * <p>
   * During a rollback files may be deleted (COW, MOR) or rollback blocks be appended (MOR only) to files. This
   * function will extract this change file for each partition.
   *
   * @param rollbackMetadata         {@code HoodieRollbackMetadata}
   * @param partitionToAppendedFiles The {@code Map} to fill with files appended per partition and their sizes.
   */
  private static void processRollbackMetadata(HoodieRollbackMetadata rollbackMetadata,
                                              Map<String, Map<String, Long>> partitionToAppendedFiles) {
    rollbackMetadata.getPartitionMetadata().values().forEach(pm -> {
      // Has this rollback produced new files?
      boolean hasRollbackLogFiles = pm.getRollbackLogFiles() != null && !pm.getRollbackLogFiles().isEmpty();
      final String partition = pm.getPartitionPath();
      final String partitionId = getPartitionIdentifierForFilesPartition(partition);

      BiFunction<Long, Long, Long> fileMergeFn = (oldSize, newSizeCopy) -> {
        // if a file exists in both written log files and rollback log files, we want to pick the one that is higher
        // as rollback file could have been updated after written log files are computed.
        return oldSize > newSizeCopy ? oldSize : newSizeCopy;
      };

      if (hasRollbackLogFiles) {
        if (!partitionToAppendedFiles.containsKey(partitionId)) {
          partitionToAppendedFiles.put(partitionId, new HashMap<>());
        }

        // Extract appended file name from the absolute paths saved in getAppendFiles()
        pm.getRollbackLogFiles().forEach((path, size) -> {
          String fileName = new StoragePath(path).getName();
          partitionToAppendedFiles.get(partitionId).merge(fileName, size, fileMergeFn);
        });

        // Extract original log files from failed commit
        pm.getLogFilesFromFailedCommit().forEach((path, size) -> {
          String fileName = new StoragePath(path).getName();
          partitionToAppendedFiles.get(partitionId).merge(fileName, size, fileMergeFn);
        });
      }
    });
  }

  /**
   * Convert rollback action metadata to files partition records.
   */
  protected static List<HoodieRecord> convertFilesToFilesPartitionRecords(Map<String, List<String>> partitionToDeletedFiles,
                                                                          Map<String, Map<String, Long>> partitionToAppendedFiles,
                                                                          String instantTime, String operation) {
    List<HoodieRecord> records = new ArrayList<>(partitionToDeletedFiles.size() + partitionToAppendedFiles.size());
    int[] fileChangeCount = {0, 0}; // deletes, appends

    partitionToDeletedFiles.forEach((partitionName, deletedFiles) -> {
      fileChangeCount[0] += deletedFiles.size();

      Map<String, Long> filesAdded = Collections.emptyMap();
      if (partitionToAppendedFiles.containsKey(partitionName)) {
        filesAdded = partitionToAppendedFiles.remove(partitionName);
      }

      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partitionName, filesAdded,
          deletedFiles);
      records.add(record);
    });

    partitionToAppendedFiles.forEach((partitionName, appendedFileMap) -> {
      final String partition = getPartitionIdentifierForFilesPartition(partitionName);
      fileChangeCount[1] += appendedFileMap.size();

      // Validate that no appended file has been deleted
      checkState(
          !appendedFileMap.keySet().removeAll(partitionToDeletedFiles.getOrDefault(partition, Collections.emptyList())),
          "Rollback file cannot both be appended and deleted");

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, appendedFileMap,
          Collections.emptyList());
      records.add(record);
    });

    LOG.info("Found at {} from {}. #partitions_updated={}, #files_deleted={}, #files_appended={}",
            instantTime, operation, records.size(), fileChangeCount[0], fileChangeCount[1]);

    return records;
  }

  public static String getColumnStatsIndexPartitionIdentifier(String partitionName) {
    return getPartitionIdentifier(partitionName);
  }

  public static String getBloomFilterIndexPartitionIdentifier(String partitionName) {
    return getPartitionIdentifier(partitionName);
  }

  public static String getPartitionIdentifierForFilesPartition(String relativePartitionPath) {
    return getPartitionIdentifier(relativePartitionPath);
  }

  /**
   * Returns partition name for the given path.
   */
  public static String getPartitionIdentifier(@Nonnull String relativePartitionPath) {
    return EMPTY_PARTITION_NAME.equals(relativePartitionPath) ? NON_PARTITIONED_NAME : relativePartitionPath;
  }

  /**
   * Convert added and deleted files metadata to bloom filter index records.
   */
  public static HoodieData<HoodieRecord> convertFilesToBloomFilterRecords(HoodieEngineContext engineContext,
                                                                          Map<String, List<String>> partitionToDeletedFiles,
                                                                          Map<String, Map<String, Long>> partitionToAppendedFiles,
                                                                          String instantTime,
                                                                          HoodieTableMetaClient dataMetaClient,
                                                                          int bloomIndexParallelism,
                                                                          String bloomFilterType) {
    // Create the tuple (partition, filename, isDeleted) to handle both deletes and appends
    final List<Tuple3<String, String, Boolean>> partitionFileFlagTupleList = fetchPartitionFileInfoTriplets(partitionToDeletedFiles, partitionToAppendedFiles);

    // Create records MDT
    int parallelism = Math.max(Math.min(partitionFileFlagTupleList.size(), bloomIndexParallelism), 1);
    return engineContext.parallelize(partitionFileFlagTupleList, parallelism).flatMap(partitionFileFlagTuple -> {
      final String partitionName = partitionFileFlagTuple.f0;
      final String filename = partitionFileFlagTuple.f1;
      final boolean isDeleted = partitionFileFlagTuple.f2;
      if (!FSUtils.isBaseFile(new StoragePath(filename))) {
        LOG.warn("Ignoring file {} as it is not a base file", filename);
        return Stream.<HoodieRecord>empty().iterator();
      }

      // Read the bloom filter from the base file if the file is being added
      ByteBuffer bloomFilterBuffer = ByteBuffer.allocate(0);
      if (!isDeleted) {
        final String pathWithPartition = partitionName + "/" + filename;
        final StoragePath addedFilePath = new StoragePath(dataMetaClient.getBasePath(), pathWithPartition);
        bloomFilterBuffer = readBloomFilter(dataMetaClient.getStorage(), addedFilePath);

        // If reading the bloom filter failed then do not add a record for this file
        if (bloomFilterBuffer == null) {
          LOG.error("Failed to read bloom filter from {}", addedFilePath);
          return Stream.<HoodieRecord>empty().iterator();
        }
      }

      return Stream.<HoodieRecord>of(HoodieMetadataPayload.createBloomFilterMetadataRecord(
              partitionName, filename, instantTime, bloomFilterType, bloomFilterBuffer, partitionFileFlagTuple.f2))
          .iterator();
    });
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

  private static ByteBuffer readBloomFilter(HoodieStorage storage, StoragePath filePath) throws IOException {
    HoodieConfig hoodieConfig = getReaderConfigs(storage.getConf());
    try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(hoodieConfig, filePath)) {
      final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
      if (fileBloomFilter == null) {
        return null;
      }
      return ByteBuffer.wrap(getUTF8Bytes(fileBloomFilter.serializeToString()));
    }
  }

  private static List<Tuple3<String, String, Boolean>> fetchPartitionFileInfoTriplets(
      Map<String, List<String>> partitionToDeletedFiles,
      Map<String, Map<String, Long>> partitionToAppendedFiles) {
    // Total number of files which are added or deleted
    final int totalFiles = partitionToDeletedFiles.values().stream().mapToInt(List::size).sum()
        + partitionToAppendedFiles.values().stream().mapToInt(Map::size).sum();
    final List<Tuple3<String, String, Boolean>> partitionFileFlagTupleList = new ArrayList<>(totalFiles);
    partitionToDeletedFiles.entrySet().stream()
        .flatMap(entry -> entry.getValue().stream().map(deletedFile -> Tuple3.of(entry.getKey(), deletedFile, true)))
        .collect(Collectors.toCollection(() -> partitionFileFlagTupleList));
    partitionToAppendedFiles.entrySet().stream()
        .flatMap(
            entry -> entry.getValue().keySet().stream().map(addedFile -> Tuple3.of(entry.getKey(), addedFile, false)))
        .collect(Collectors.toCollection(() -> partitionFileFlagTupleList));
    return partitionFileFlagTupleList;
  }

  /**
   * <p>
   * This method should be used when processing a batch of records that are guaranteed to have the same key format
   * (either all containing the secondary index separator or none containing it).
   * <p>
   * For secondary index partitions (version >= 2), if the record keys contain the secondary index separator,
   * the unescaped secondary key portion is used for hashing. Otherwise, the full record key is used.
   *
   * @param needsSecondaryKeyExtraction Whether to extract secondary key from composite keys (should be determined by caller)
   *
   * @return function that maps secondary keys to file group indices.
   */
  public static SerializableBiFunction<String, Integer, Integer> getSecondaryKeyToFileGroupMappingFunction(boolean needsSecondaryKeyExtraction) {
    if (needsSecondaryKeyExtraction) {
      return (recordKey, numFileGroups) -> {
        String secondaryKey = SecondaryIndexKeyUtils.getUnescapedSecondaryKeyPrefixFromSecondaryIndexKey(recordKey);
        return mapRecordKeyToFileGroupIndex(secondaryKey, numFileGroups);
      };
    }
    return HoodieTableMetadataUtil::mapRecordKeyToFileGroupIndex;
  }

  // change to configurable larger group
  public static int mapRecordKeyToFileGroupIndex(String recordKey, int numFileGroups) {
    int h = 0;
    for (int i = 0; i < recordKey.length(); ++i) {
      h = 31 * h + recordKey.charAt(i);
    }

    return Math.abs(Math.abs(h) % numFileGroups);
  }

  /**
   * Get the latest file slices for a Metadata Table partition. If the file slice is
   * because of pending compaction instant, then merge the file slice with the one
   * just before the compaction instant time. The list of file slices returned is
   * sorted in the correct order of file group name.
   *
   * @param metaClient Instance of {@link HoodieTableMetaClient}.
   * @param fsView     Metadata table filesystem view.
   * @param partition  The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getPartitionLatestMergedFileSlices(
      HoodieTableMetaClient metaClient, HoodieTableFileSystemView fsView, String partition) {
    LOG.info("Loading latest merged file slices for metadata table partition {}", partition);
    return getPartitionFileSlices(metaClient, Option.of(fsView), partition, true);
  }

  /**
   * Get the latest file slices for a Metadata Table partition. The list of file slices
   * returned is sorted in the correct order of file group name.
   *
   * @param metaClient - Instance of {@link HoodieTableMetaClient}.
   * @param fsView     - Metadata table filesystem view
   * @param partition  - The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getPartitionLatestFileSlices(HoodieTableMetaClient metaClient,
                                                             Option<HoodieTableFileSystemView> fsView, String partition) {
    LOG.info("Loading latest file slices for metadata table partition {}", partition);
    return getPartitionFileSlices(metaClient, fsView, partition, false);
  }

  /**
   * Get metadata table file system view.
   *
   * @param metaClient - Metadata table meta client
   * @return Filesystem view for the metadata table
   */
  public static HoodieTableFileSystemView getFileSystemViewForMetadataTable(HoodieTableMetaClient metaClient) {
    // If there are no commits on the metadata table then the table's
    // default FileSystemView will not return any file slices even
    // though we may have initialized them.
    HoodieTimeline timeline = metaClient.getActiveTimeline();
    TimelineFactory factory = metaClient.getTableFormat().getTimelineFactory();
    if (timeline.empty()) {
      final HoodieInstant instant = metaClient.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION,
          HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
      timeline = factory.createDefaultTimeline(Stream.of(instant), metaClient.getActiveTimeline());
    }
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    return HoodieTableFileSystemView.fileListingBasedFileSystemView(engineContext, metaClient, timeline);
  }

  /**
   * Get the latest file slices for a given partition.
   *
   * @param metaClient      - Instance of {@link HoodieTableMetaClient}.
   * @param partition       - The name of the partition whose file groups are to be loaded.
   * @param mergeFileSlices - When enabled, will merge the latest file slices with the last known
   *                        completed instant. This is useful for readers when there are pending
   *                        compactions. MergeFileSlices when disabled, will return the latest file
   *                        slices without any merging, and this is needed for the writers.
   * @return List of latest file slices for all file groups in a given partition.
   */
  private static List<FileSlice> getPartitionFileSlices(HoodieTableMetaClient metaClient,
                                                        Option<HoodieTableFileSystemView> fileSystemView,
                                                        String partition,
                                                        boolean mergeFileSlices) {
    HoodieTableFileSystemView fsView = null;
    try {
      fsView = fileSystemView.orElseGet(() -> getFileSystemViewForMetadataTable(metaClient));
      Stream<FileSlice> fileSliceStream;
      if (mergeFileSlices) {
        if (metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().isPresent()) {
          fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(
              // including pending compaction instant as the last instant so that the finished delta commits
              // that start earlier than the compaction can be queried.
              partition, metaClient.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant().get().requestedTime());
        } else {
          return Collections.emptyList();
        }
      } else {
        fileSliceStream = fsView.getLatestFileSlices(partition);
      }
      return fileSliceStream.sorted(Comparator.comparing(FileSlice::getFileId)).collect(Collectors.toList());
    } finally {
      if (!fileSystemView.isPresent()) {
        fsView.close();
      }
    }
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

  public static HoodieData<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCommitMetadata commitMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             HoodieTableMetaClient dataMetaClient,
                                                                             HoodieMetadataConfig metadataConfig,
                                                                             Option<HoodieRecordType> recordTypeOpt) {
    List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());

    if (allWriteStats.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    try {
      Map<String, Schema> columnsToIndexSchemaMap = getColumnsToIndex(commitMetadata, dataMetaClient, metadataConfig, recordTypeOpt);
      if (columnsToIndexSchemaMap.isEmpty()) {
        // In case there are no columns to index, bail
        return engineContext.emptyHoodieData();
      }
      List<String> columnsToIndex = new ArrayList<>(columnsToIndexSchemaMap.keySet());
      int parallelism = Math.max(Math.min(allWriteStats.size(), metadataConfig.getColumnStatsIndexParallelism()), 1);
      return engineContext.parallelize(allWriteStats, parallelism)
          .flatMap(writeStat ->
              translateWriteStatToColumnStats(writeStat, dataMetaClient, columnsToIndex).iterator());
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  public static Map<String, Schema> getColumnsToIndex(HoodieCommitMetadata commitMetadata, HoodieTableMetaClient dataMetaClient,
                                               HoodieMetadataConfig metadataConfig, Option<HoodieRecordType> recordTypeOpt) {
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

  @VisibleForTesting
  static final String[] META_COLS_TO_ALWAYS_INDEX = {COMMIT_TIME_METADATA_FIELD, RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD};
  @VisibleForTesting
  public static final Set<String> META_COL_SET_TO_INDEX = new HashSet<>(Arrays.asList(META_COLS_TO_ALWAYS_INDEX));
  @VisibleForTesting
  static final Map<String, Schema> META_COLS_TO_ALWAYS_INDEX_SCHEMA_MAP = new TreeMap() {{
      put(COMMIT_TIME_METADATA_FIELD, Schema.create(Schema.Type.STRING));
      put(RECORD_KEY_METADATA_FIELD, Schema.create(Schema.Type.STRING));
      put(PARTITION_PATH_METADATA_FIELD, Schema.create(Schema.Type.STRING));
    }};

  @VisibleForTesting
  public static Map<String, Schema> getColumnsToIndex(HoodieTableConfig tableConfig,
                                               HoodieMetadataConfig metadataConfig,
                                               Lazy<Option<Schema>> tableSchemaLazyOpt,
                                               Option<HoodieRecordType> recordType) {
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
                                               Option<HoodieRecordType> recordType) {
    Map<String, Schema> columnsToIndexWithoutRequiredMetas = getColumnsToIndexWithoutRequiredMetaFields(metadataConfig, tableSchemaLazyOpt, isTableInitializing, recordType);
    if (!tableConfig.populateMetaFields()) {
      return columnsToIndexWithoutRequiredMetas;
    }

    Map<String, Schema> colsToIndexSchemaMap = new LinkedHashMap<>();
    colsToIndexSchemaMap.putAll(META_COLS_TO_ALWAYS_INDEX_SCHEMA_MAP);
    colsToIndexSchemaMap.putAll(columnsToIndexWithoutRequiredMetas);
    return colsToIndexSchemaMap;
  }

  /**
   * Get list of columns that should be indexed for col stats or partition stats
   * We always index META_COLS_TO_ALWAYS_INDEX If metadataConfig.getColumnsEnabledForColumnStatsIndex()
   * is empty, we will use metadataConfig.maxColumnsToIndexForColStats() and index the first n columns in the table in addition to the
   * required meta cols
   *
   * @param metadataConfig       metadata config
   * @param tableSchemaLazyOpt   lazy option of the table schema
   * @param isTableInitializing true if table is being initialized.
   * @param recordType           Option of record type. Used to determine which types are valid to index
   * @return list of columns that should be indexed
   */
  private static Map<String, Schema> getColumnsToIndexWithoutRequiredMetaFields(HoodieMetadataConfig metadataConfig,
                                                                           Lazy<Option<Schema>> tableSchemaLazyOpt,
                                                                           boolean isTableInitializing,
                                                                           Option<HoodieRecordType> recordType) {
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

  private static Stream<Pair<String, Schema>> getFirstNSupportedFields(Schema tableSchema, int n, Option<HoodieRecordType> recordType) {
    return getFirstNFields(tableSchema.getFields().stream()
        .filter(field -> isColumnTypeSupported(field.schema(), recordType)).map(field -> Pair.of(field.name(), field.schema())), n);
  }

  private static Stream<Pair<String, Schema>> getFirstNFields(Stream<Pair<String, Schema>> fieldSchemaPairStream, int n) {
    return fieldSchemaPairStream.filter(fieldSchemaPair -> !HOODIE_META_COLUMNS_WITH_OPERATION.contains(fieldSchemaPair.getKey())).limit(n);
  }

  private static Stream<HoodieRecord> translateWriteStatToColumnStats(HoodieWriteStat writeStat,
                                                                      HoodieTableMetaClient datasetMetaClient,
                                                                      List<String> columnsToIndex) {
    if (writeStat.getColumnStats().isPresent()) {
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMap = writeStat.getColumnStats().get();
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

  private static Stream<HoodieRecord> getColumnStatsRecords(String partitionPath,
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

  private static List<HoodieColumnRangeMetadata<Comparable>> readColumnRangeMetadataFrom(String partitionPath,
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
        return getLogFileColumnRangeMetadata(fullFilePath.toString(), partitionPath, datasetMetaClient, columnsToIndex, writerSchemaOpt, maxBufferSize);
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
  public static List<HoodieColumnRangeMetadata<Comparable>> getLogFileColumnRangeMetadata(String filePath, String partitionPath,
                                                                                          HoodieTableMetaClient datasetMetaClient,
                                                                                          List<String> columnsToIndex, Option<Schema> writerSchemaOpt,
                                                                                          int maxBufferSize) throws IOException {
    if (writerSchemaOpt.isPresent()) {
      List<Pair<String, Schema.Field>> fieldsToIndex = columnsToIndex.stream().map(fieldName -> HoodieAvroUtils.getSchemaForField(writerSchemaOpt.get(), fieldName))
          .collect(Collectors.toList());
      // read log files without merging for lower overhead, log files may contain multiple records for the same key resulting in a wider range of values than the merged result
      HoodieLogFile logFile = new HoodieLogFile(filePath);
      FileSlice fileSlice = new FileSlice(partitionPath, logFile.getDeltaCommitTime(), logFile.getFileId());
      fileSlice.addLogFile(logFile);
      TypedProperties properties = new TypedProperties();
      properties.setProperty(MAX_MEMORY_FOR_MERGE.key(), Long.toString(maxBufferSize));
      properties.setProperty(HoodieReaderConfig.MERGE_TYPE.key(), REALTIME_SKIP_MERGE);
      // Currently only avro is fully supported for extracting column ranges (see HUDI-8585)
      HoodieReaderContext readerContext = new HoodieAvroReaderContext(datasetMetaClient.getStorageConf(), datasetMetaClient.getTableConfig(), Option.empty(), Option.empty());
      HoodieFileGroupReader fileGroupReader = HoodieFileGroupReader.newBuilder()
          .withReaderContext(readerContext)
          .withHoodieTableMetaClient(datasetMetaClient)
          .withLogFiles(Stream.of(logFile))
          .withPartitionPath(partitionPath)
          .withBaseFileOption(Option.empty())
          .withDataSchema(writerSchemaOpt.get())
          .withRequestedSchema(writerSchemaOpt.get())
          .withLatestCommitTime(datasetMetaClient.getActiveTimeline().getCommitsTimeline().lastInstant().get().requestedTime())
          .withProps(properties)
          .build();
      try (ClosableIterator<HoodieRecord> recordIterator = (ClosableIterator<HoodieRecord>) fileGroupReader.getClosableHoodieRecordIterator()) {
        if (!recordIterator.hasNext()) {
          return Collections.emptyList();
        }
        Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataMap =
            collectColumnRangeMetadata(recordIterator, fieldsToIndex, getFileNameFromPath(filePath), writerSchemaOpt.get(), datasetMetaClient.getStorage().getConf());
        return new ArrayList<>(columnRangeMetadataMap.values());
      }
    }
    return Collections.emptyList();
  }

  /**
   * Does an upcast for {@link BigDecimal} instance to align it with scale/precision expected by
   * the {@link LogicalTypes.Decimal} Avro logical type
   */
  public static BigDecimal tryUpcastDecimal(BigDecimal value, final LogicalTypes.Decimal decimal) {
    final int scale = decimal.getScale();
    final int valueScale = value.scale();

    boolean scaleAdjusted = false;
    if (valueScale != scale) {
      try {
        value = value.setScale(scale, RoundingMode.UNNECESSARY);
        scaleAdjusted = true;
      } catch (ArithmeticException aex) {
        throw new AvroTypeException(
            "Cannot encode decimal with scale " + valueScale + " as scale " + scale + " without rounding");
      }
    }

    int precision = decimal.getPrecision();
    int valuePrecision = value.precision();
    if (valuePrecision > precision) {
      if (scaleAdjusted) {
        throw new AvroTypeException("Cannot encode decimal with precision " + valuePrecision + " as max precision "
            + precision + ". This is after safely adjusting scale from " + valueScale + " to required " + scale);
      } else {
        throw new AvroTypeException(
            "Cannot encode decimal with precision " + valuePrecision + " as max precision " + precision);
      }
    }

    return value;
  }

  public static Option<Schema> tryResolveSchemaForTable(HoodieTableMetaClient dataTableMetaClient) {
    if (dataTableMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
      return Option.empty();
    }
    try {
      TableSchemaResolver schemaResolver = new TableSchemaResolver(dataTableMetaClient);
      return Option.of(schemaResolver.getTableAvroSchema());
    } catch (Exception e) {
      throw new HoodieException("Failed to get latest columns for " + dataTableMetaClient.getBasePath(), e);
    }
  }

  /**
   * Given a schema, coerces provided value to instance of {@link Comparable<?>} such that
   * it could subsequently be used in column stats
   *
   * NOTE: This method has to stay compatible with the semantic of
   *      {@link FileFormatUtils#readColumnStatsFromMetadata} as they are used in tandem
   */
  public static Comparable<?> coerceToComparable(Schema schema, Object val) {
    if (val == null) {
      return null;
    }

    switch (schema.getType()) {
      case UNION:
        // TODO we need to handle unions in general case as well
        return coerceToComparable(resolveNullableSchema(schema), val);

      case FIXED:
      case BYTES:
        if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
          return (Comparable<?>) val;
        }
        return (ByteBuffer) val;


      case INT:
        if (schema.getLogicalType() == LogicalTypes.date()
            || schema.getLogicalType() == LogicalTypes.timeMillis()) {
          // NOTE: This type will be either {@code java.sql.Date} or {org.joda.LocalDate}
          //       depending on the Avro version. Hence, we simply cast it to {@code Comparable<?>}
          return (Comparable<?>) val;
        }
        return castToInteger(val);

      case LONG:
        if (schema.getLogicalType() == LogicalTypes.timeMicros()
            || schema.getLogicalType() == LogicalTypes.timestampMicros()
            || schema.getLogicalType() == LogicalTypes.timestampMillis()) {
          // NOTE: This type will be either {@code java.sql.Date} or {org.joda.LocalDate}
          //       depending on the Avro version. Hence, we simply cast it to {@code Comparable<?>}
          return (Comparable<?>) val;
        }
        return castToLong(val);

      case STRING:
        // unpack the avro Utf8 if possible
        return val.toString();
      case FLOAT:
        return castToFloat(val);
      case DOUBLE:
        return castToDouble((val));
      case BOOLEAN:
        return (Comparable<?>) val;

      // TODO add support for those types
      case ENUM:
      case MAP:
      case NULL:
      case RECORD:
      case ARRAY:
        return null;

      default:
        throw new IllegalStateException("Unexpected type: " + schema.getType());
    }
  }

  private static Integer castToInteger(Object val) {
    if (val == null) {
      return null;
    }
    if (val instanceof Integer) {
      return (Integer) val;
    } else if (val instanceof Long) {
      return ((Long) val).intValue();
    } else if (val instanceof Float) {
      return ((Float)val).intValue();
    } else if (val instanceof Double) {
      return ((Double)val).intValue();
    } else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1 : 0;
    }  else {
      // best effort casting
      return Integer.parseInt(val.toString());
    }
  }

  private static Long castToLong(Object val) {
    if (val == null) {
      return null;
    }
    if (val instanceof Integer) {
      return ((Integer) val).longValue();
    } else if (val instanceof Long) {
      return ((Long) val);
    } else if (val instanceof Float) {
      return ((Float)val).longValue();
    } else if (val instanceof Double) {
      return ((Double)val).longValue();
    } else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1L : 0L;
    }  else {
      // best effort casting
      return Long.parseLong(val.toString());
    }
  }

  private static Float castToFloat(Object val) {
    if (val == null) {
      return null;
    }
    if (val instanceof Integer) {
      return ((Integer) val).floatValue();
    } else if (val instanceof Long) {
      return ((Long) val).floatValue();
    } else if (val instanceof Float) {
      return ((Float)val).floatValue();
    } else if (val instanceof Double) {
      return ((Double)val).floatValue();
    } else if (val instanceof Boolean) {
      return (Boolean) val ? 1.0f : 0.0f;
    }  else {
      // best effort casting
      return Float.parseFloat(val.toString());
    }
  }

  private static Double castToDouble(Object val) {
    if (val == null) {
      return null;
    }
    if (val instanceof Integer) {
      return ((Integer) val).doubleValue();
    } else if (val instanceof Long) {
      return ((Long) val).doubleValue();
    } else if (val instanceof Float) {
      return ((Float)val).doubleValue();
    } else if (val instanceof Double) {
      return ((Double)val).doubleValue();
    } else if (val instanceof Boolean) {
      return (Boolean) val ? 1.0d : 0.0d;
    }  else {
      // best effort casting
      return Double.parseDouble(val.toString());
    }
  }

  public static boolean isColumnTypeSupported(Schema schema, Option<HoodieRecordType> recordType) {
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
    if (recordType.isPresent() && recordType.get() == HoodieRecordType.AVRO) {
      return (schemaToCheck.getType() != Schema.Type.RECORD && schemaToCheck.getType() != Schema.Type.ARRAY && schemaToCheck.getType() != Schema.Type.MAP
          && schemaToCheck.getType() != Schema.Type.ENUM);
    }
    // if record Type is not set or if recordType is SPARK then we cannot support AVRO, MAP, ARRAY, RECORD, ENUM and FIXED and BYTES type as well.
    // HUDI-8585 will add support for BYTES and FIXED
    return schemaToCheck.getType() != Schema.Type.RECORD && schemaToCheck.getType() != Schema.Type.ARRAY && schemaToCheck.getType() != Schema.Type.MAP
        && schemaToCheck.getType() != Schema.Type.ENUM && schemaToCheck.getType() != Schema.Type.BYTES && schemaToCheck.getType() != Schema.Type.FIXED;
  }

  public static Set<String> getInflightMetadataPartitions(HoodieTableConfig tableConfig) {
    return new HashSet<>(tableConfig.getMetadataPartitionsInflight());
  }

  public static Set<String> getInflightAndCompletedMetadataPartitions(HoodieTableConfig tableConfig) {
    Set<String> inflightAndCompletedPartitions = getInflightMetadataPartitions(tableConfig);
    inflightAndCompletedPartitions.addAll(tableConfig.getMetadataPartitions());
    return inflightAndCompletedPartitions;
  }

  public static Set<String> getValidInstantTimestamps(HoodieTableMetaClient dataMetaClient,
                                                      HoodieTableMetaClient metadataMetaClient) {
    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    HoodieActiveTimeline datasetTimeline = dataMetaClient.getActiveTimeline();
    Set<String> datasetPendingInstants = datasetTimeline.filterInflightsAndRequested().getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toSet());
    Set<String> validInstantTimestamps = datasetTimeline.filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toSet());

    // We should also add completed indexing delta commits in the metadata table, as they do not
    // have corresponding completed instant in the data table
    validInstantTimestamps.addAll(
        metadataMetaClient.getActiveTimeline()
            .filter(instant -> instant.isCompleted() && isValidInstant(datasetPendingInstants, instant))
            .getInstantsAsStream()
            .map(HoodieInstant::requestedTime)
            .collect(Collectors.toList()));

    // For any rollbacks and restores, we cannot neglect the instants that they are rolling back.
    // The rollback instant should be more recent than the start of the timeline for it to have rolled back any
    // instant which we have a log block for.
    final String earliestInstantTime = validInstantTimestamps.isEmpty() ? SOLO_COMMIT_TIMESTAMP : Collections.min(validInstantTimestamps);
    datasetTimeline.getRollbackAndRestoreTimeline().filterCompletedInstants().getInstantsAsStream()
            .filter(instant -> compareTimestamps(instant.requestedTime(), GREATER_THAN, earliestInstantTime))
            .forEach(instant -> validInstantTimestamps.addAll(getRollbackedCommits(instant, datasetTimeline, dataMetaClient.getInstantGenerator())));

    // add restore and rollback instants from MDT.
    metadataMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline().filterCompletedInstants()
        .filter(instant -> instant.getAction().equals(HoodieTimeline.RESTORE_ACTION) || instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION))
        .getInstants().forEach(instant -> validInstantTimestamps.add(instant.requestedTime()));

    metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants()
        .filter(instant ->  instant.requestedTime().startsWith(SOLO_COMMIT_TIMESTAMP))
        .getInstants().forEach(instant -> validInstantTimestamps.add(instant.requestedTime()));
    return validInstantTimestamps;
  }

  /**
   * Checks if the Instant is a delta commit and has a valid suffix for operations on MDT.
   *
   * @param datasetPendingInstants The dataset pending instants
   * @param instant {@code HoodieInstant} to check.
   * @return {@code true} if the instant is valid.
   */
  private static boolean isValidInstant(Set<String> datasetPendingInstants, HoodieInstant instant) {
    // only includes a deltacommit,
    // filter out any MDT instant that has pending corespondent dataset instant,
    // this comes from a case that one instant fails to commit after MDT had been committed.
    return instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION) && !datasetPendingInstants.contains(instant.requestedTime());
  }

  /**
   * Checks if a delta commit in metadata table is written by async indexer.
   * <p>
   * TODO(HUDI-5733): This should be cleaned up once the proper fix of rollbacks in the
   *  metadata table is landed.
   *
   * @param dataIndexTimeline The instant timeline comprised with index commits from data table.
   * @param instant The metadata table instant to check.
   * @return {@code true} if from async indexer; {@code false} otherwise.
   */
  public static boolean isIndexingCommit(
      HoodieTimeline dataIndexTimeline,
      String instant) {
    // A data table index commit was written as a delta commit on metadata table, use the data table
    // timeline for auxiliary check.

    // If this is a MDT, the pending delta commit on active timeline must also be active on the DT
    // based on the fact that the MDT is committed before the DT.
    return dataIndexTimeline.containsInstant(instant);
  }

  /**
   * Returns a list of commits which were rolled back as part of a Rollback or Restore operation.
   *
   * @param instant  The Rollback operation to read
   * @param timeline instant of timeline from dataset.
   */
  private static List<String> getRollbackedCommits(HoodieInstant instant, HoodieActiveTimeline timeline, InstantGenerator factory) {
    try {
      List<String> commitsToRollback;
      if (instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)) {
        try {
          HoodieRollbackMetadata rollbackMetadata = timeline.readRollbackMetadata(instant);
          commitsToRollback = rollbackMetadata.getCommitsRollback();
        } catch (IOException e) {
          // if file is empty, fetch the commits to rollback from rollback.requested file
          HoodieRollbackPlan rollbackPlan =
              timeline.readRollbackPlan(
                  factory.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION,
                      instant.requestedTime()));
          commitsToRollback = Collections.singletonList(rollbackPlan.getInstantToRollback().getCommitTime());
          LOG.warn("Had to fetch rollback info from requested instant since completed file is empty {}", instant);
        }
        return commitsToRollback;
      }

      List<String> rollbackedCommits = new LinkedList<>();
      if (instant.getAction().equals(HoodieTimeline.RESTORE_ACTION)) {
        // Restore is made up of several rollbacks
        HoodieRestoreMetadata restoreMetadata = timeline.readRestoreMetadata(instant);
        restoreMetadata.getHoodieRestoreMetadata().values()
            .forEach(rms -> rms.forEach(rm -> rollbackedCommits.addAll(rm.getCommitsRollback())));
      }
      return rollbackedCommits;
    } catch (IOException e) {
      throw new HoodieMetadataException("Error retrieving rollback commits for instant " + instant, e);
    }
  }

  /**
   * Delete the metadata table for the dataset and backup if required.
   *
   * @param dataMetaClient {@code HoodieTableMetaClient} of the dataset for which metadata table is to be deleted
   * @param context        instance of {@link HoodieEngineContext}.
   * @param backup         Whether metadata table should be backed up before deletion. If true, the table is backed up to the
   *                       directory with name metadata_<current_timestamp>.
   * @return The backup directory if backup was requested
   */
  public static String deleteMetadataTable(HoodieTableMetaClient dataMetaClient, HoodieEngineContext context, boolean backup) {
    final StoragePath metadataTablePath =
        HoodieTableMetadata.getMetadataTableBasePath(dataMetaClient.getBasePath());
    HoodieStorage storage = dataMetaClient.getStorage();
    dataMetaClient.getTableConfig().clearMetadataPartitions(dataMetaClient);
    try {
      if (!storage.exists(metadataTablePath)) {
        return null;
      }
    } catch (FileNotFoundException e) {
      // Ignoring exception as metadata table already does not exist
      return null;
    } catch (IOException e) {
      throw new HoodieMetadataException("Failed to check metadata table existence", e);
    }

    if (backup) {
      final StoragePath metadataBackupPath = new StoragePath(metadataTablePath.getParent(), ".metadata_" + HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
      LOG.info("Backing up metadata directory to {} before deletion", metadataBackupPath);
      try {
        if (storage.rename(metadataTablePath, metadataBackupPath)) {
          return metadataBackupPath.toString();
        }
      } catch (Exception e) {
        // If rename fails, we will ignore the backup and still delete the MDT
        LOG.error("Failed to backup metadata table using rename", e);
      }
    }

    LOG.info("Deleting metadata table from {}", metadataTablePath);
    try {
      storage.deleteDirectory(metadataTablePath);
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to delete metadata table from path " + metadataTablePath, e);
    }

    return null;
  }

  /**
   * Delete a partition within the metadata table.
   * <p>
   * This can be used to delete a partition so that it can be re-bootstrapped.
   *
   * @param dataMetaClient {@code HoodieTableMetaClient} of the dataset for which metadata table is to be deleted
   * @param context        instance of {@code HoodieEngineContext}.
   * @param backup         Whether metadata table should be backed up before deletion. If true, the table is backed up to the
   *                       directory with name metadata_<current_timestamp>.
   * @param partitionPath  The partition to delete
   * @return The backup directory if backup was requested, null otherwise
   */
  public static String deleteMetadataTablePartition(HoodieTableMetaClient dataMetaClient, HoodieEngineContext context,
                                                    String partitionPath, boolean backup) {
    if (partitionPath.equals(MetadataPartitionType.FILES.getPartitionPath())) {
      return deleteMetadataTable(dataMetaClient, context, backup);
    }

    final StoragePath metadataTablePartitionPath = new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(dataMetaClient.getBasePath()), partitionPath);
    HoodieStorage storage = dataMetaClient.getStorage();
    dataMetaClient.getTableConfig().setMetadataPartitionState(dataMetaClient, partitionPath, false);
    try {
      if (!storage.exists(metadataTablePartitionPath)) {
        return null;
      }
    } catch (FileNotFoundException e) {
      // Ignoring exception as metadata table already does not exist
      LOG.debug("Metadata table partition {} not found at path {}", partitionPath, metadataTablePartitionPath);
      return null;
    } catch (Exception e) {
      throw new HoodieMetadataException(String.format("Failed to check existence of MDT partition %s at path %s: ", partitionPath, metadataTablePartitionPath), e);
    }

    if (backup) {
      final StoragePath metadataPartitionBackupPath = new StoragePath(metadataTablePartitionPath.getParent().getParent(),
          String.format(".metadata_%s_%s", partitionPath, HoodieInstantTimeGenerator.getCurrentInstantTimeStr()));
      LOG.info("Backing up MDT partition {} to {} before deletion", partitionPath, metadataPartitionBackupPath);
      try {
        if (storage.rename(metadataTablePartitionPath, metadataPartitionBackupPath)) {
          return metadataPartitionBackupPath.toString();
        }
      } catch (Exception e) {
        // If rename fails, we will try to delete the table instead
        LOG.error(String.format("Failed to backup MDT partition %s using rename", partitionPath), e);
      }
    } else {
      LOG.info("Deleting metadata table partition from {}", metadataTablePartitionPath);
      try {
        storage.deleteDirectory(metadataTablePartitionPath);
      } catch (Exception e) {
        throw new HoodieMetadataException("Failed to delete metadata table partition from path " + metadataTablePartitionPath, e);
      }
    }

    return null;
  }

  /**
   * Return the complete fileID for a file group within a MDT partition.
   * <p>
   * MDT fileGroups have the format <fileIDPrefix>-<index>. The fileIDPrefix is hardcoded for each MDT partition and index is an integer.
   *
   * @param partitionType The type of the MDT partition
   * @param index         Index of the file group within the partition
   * @return The fileID
   */
  public static String getFileIDForFileGroup(MetadataPartitionType partitionType, int index, String partitionName) {
    if (MetadataPartitionType.EXPRESSION_INDEX.equals(partitionType) || MetadataPartitionType.SECONDARY_INDEX.equals(partitionType)) {
      return String.format("%s%04d-%d", partitionName.replaceAll("_", "-").concat("-"), index, 0);
    }
    return String.format("%s%04d-%d", partitionType.getFileIdPrefix(), index, 0);
  }

  /**
   * Extract the index from the fileID of a file group in the MDT partition. See {@code getFileIDForFileGroup} for the format of the fileID.
   *
   * @param fileId fileID of a file group.
   * @return The index of file group
   */
  public static int getFileGroupIndexFromFileId(String fileId) {
    final int endIndex = getFileIdLengthWithoutFileIndex(fileId);
    final int fromIndex = fileId.lastIndexOf("-", endIndex - 1);
    return Integer.parseInt(fileId.substring(fromIndex + 1, endIndex));
  }

  /**
   * Extract the fileID prefix from the fileID of a file group in the MDT partition. See {@code getFileIDForFileGroup} for the format of the fileID.
   *
   * @param fileId fileID of a file group.
   * @return The fileID without the file index
   */
  public static String getFileGroupPrefix(String fileId) {
    return fileId.substring(0, getFileIdLengthWithoutFileIndex(fileId));
  }

  /**
   * Returns the length of the fileID ignoring the fileIndex suffix
   * <p>
   * 0.10 version MDT code added -0 (0th fileIndex) to the fileID. This was removed later.
   * <p>
   * Examples:
   * 0.11+ version: fileID: files-0000     returns 10
   * 0.10 version:   fileID: files-0000-0  returns 10
   *
   * @param fileId The fileID
   * @return The length of the fileID ignoring the fileIndex suffix
   */
  private static int getFileIdLengthWithoutFileIndex(String fileId) {
    return fileId.endsWith("-0") ? fileId.length() - 2 : fileId.length();
  }

  /**
   * Estimates the file group count to use for a MDT partition.
   *
   * @param partitionType         Type of the partition for which the file group count is to be estimated.
   * @param recordCount           The number of records expected to be written.
   * @param averageRecordSize     Average size of each record to be written.
   * @param minFileGroupCount     Minimum number of file groups to use.
   * @param maxFileGroupCount     Maximum number of file groups to use.
   * @param growthFactor          By what factor are the records (recordCount) expected to grow?
   * @param maxFileGroupSizeBytes Maximum size of the file group.
   * @return The estimated number of file groups.
   */
  public static int estimateFileGroupCount(MetadataPartitionType partitionType, long recordCount, int averageRecordSize, int minFileGroupCount,
                                           int maxFileGroupCount, float growthFactor, int maxFileGroupSizeBytes) {
    int fileGroupCount;

    // If a fixed number of file groups are desired
    if ((minFileGroupCount == maxFileGroupCount) && (minFileGroupCount != 0)) {
      fileGroupCount = minFileGroupCount;
    } else {
      // Number of records to estimate for
      final long expectedNumRecords = (long) Math.ceil((float) recordCount * growthFactor);
      // Maximum records that should be written to each file group so that it does not go over the size limit required
      final long maxRecordsPerFileGroup = maxFileGroupSizeBytes / Math.max(averageRecordSize, 1L);
      final long estimatedFileGroupCount = expectedNumRecords / maxRecordsPerFileGroup;

      if (estimatedFileGroupCount >= maxFileGroupCount) {
        fileGroupCount = maxFileGroupCount;
      } else if (estimatedFileGroupCount <= minFileGroupCount) {
        fileGroupCount = minFileGroupCount;
      } else {
        fileGroupCount = Math.max(1, (int) estimatedFileGroupCount);
      }
    }

    LOG.info("Estimated file group count for MDT partition {} is {} "
            + "[recordCount={}, avgRecordSize={}, minFileGroupCount={}, maxFileGroupCount={}, growthFactor={}, "
            + "maxFileGroupSizeBytes={}]", partitionType.name(), fileGroupCount, recordCount, averageRecordSize, minFileGroupCount,
        maxFileGroupCount, growthFactor, maxFileGroupSizeBytes);
    return fileGroupCount;
  }

  /**
   * Returns true if any enabled metadata partition in the given hoodie table requires WriteStatus to track the written records.
   *
   * @param config     MDT config
   * @param metaClient {@code HoodieTableMetaClient} of the data table
   * @return true if WriteStatus should track the written records else false.
   */
  public static boolean getMetadataPartitionsNeedingWriteStatusTracking(HoodieMetadataConfig config, HoodieTableMetaClient metaClient) {
    // Does any enabled partition need to track the written records
    if (MetadataPartitionType.getMetadataPartitionsNeedingWriteStatusTracking().stream().anyMatch(p -> metaClient.getTableConfig().isMetadataPartitionAvailable(p))) {
      return true;
    }

    // Does any inflight partitions need to track the written records
    Set<String> metadataPartitionsInflight = metaClient.getTableConfig().getMetadataPartitionsInflight();
    if (MetadataPartitionType.getMetadataPartitionsNeedingWriteStatusTracking().stream().anyMatch(p -> metadataPartitionsInflight.contains(p.getPartitionPath()))) {
      return true;
    }

    // Does any enabled partition being enabled need to track the written records
    return config.isRecordIndexEnabled();
  }

  /**
   * Gets the location from record index content.
   *
   * @param recordIndexInfo {@link HoodieRecordIndexInfo} instance.
   * @return {@link HoodieRecordGlobalLocation} containing the location.
   */
  public static HoodieRecordGlobalLocation getLocationFromRecordIndexInfo(HoodieRecordIndexInfo recordIndexInfo) {
    return getLocationFromRecordIndexInfo(
        recordIndexInfo.getPartitionName(), recordIndexInfo.getFileIdEncoding(),
        recordIndexInfo.getFileIdHighBits(), recordIndexInfo.getFileIdLowBits(),
        recordIndexInfo.getFileIndex(), recordIndexInfo.getFileId(),
        recordIndexInfo.getInstantTime());
  }

  /**
   * Gets the location from record index content.
   * Note that, a UUID based fileId is stored as 3 pieces in record index (fileIdHighBits,
   * fileIdLowBits and fileIndex). FileID format is {UUID}-{fileIndex}.
   * The arguments are consistent with what {@link HoodieRecordIndexInfo} contains.
   *
   * @param partition      The partition name the record belongs to.
   * @param fileIdEncoding FileId encoding. Possible values are 0 and 1. O represents UUID based
   *                       fileID, and 1 represents raw string format of the fileId.
   * @param fileIdHighBits High 64 bits if the fileId is based on UUID format.
   * @param fileIdLowBits  Low 64 bits if the fileId is based on UUID format.
   * @param fileIndex      Index representing file index which is used to re-construct UUID based fileID.
   * @param originalFileId FileId of the location where record belongs to.
   *                       When the encoding is 1, fileID is stored in raw string format.
   * @param instantTime    Epoch time in millisecond representing the commit time at which record was added.
   * @return {@link HoodieRecordGlobalLocation} containing the location.
   */
  public static HoodieRecordGlobalLocation getLocationFromRecordIndexInfo(
      String partition, int fileIdEncoding, long fileIdHighBits, long fileIdLowBits,
      int fileIndex, String originalFileId, Long instantTime) {
    String fileId = null;
    if (fileIdEncoding == 0) {
      // encoding 0 refers to UUID based fileID
      final UUID uuid = new UUID(fileIdHighBits, fileIdLowBits);
      fileId = uuid.toString();
      if (fileIndex != RECORD_INDEX_MISSING_FILEINDEX_FALLBACK) {
        fileId += "-" + fileIndex;
      }
    } else {
      // encoding 1 refers to no encoding. fileID as is.
      fileId = originalFileId;
    }

    final java.util.Date instantDate = new java.util.Date(instantTime);
    return new HoodieRecordGlobalLocation(partition, HoodieInstantTimeGenerator.formatDate(instantDate), fileId);
  }

  /**
   * Reads the record keys from the base files and returns a {@link HoodieData} of {@link HoodieRecord} to be updated in the metadata table.
   * Use {@link #readRecordKeysFromFileSlices} instead.
   */
  @Deprecated
  public static HoodieData<HoodieRecord> readRecordKeysFromBaseFiles(HoodieEngineContext engineContext,
                                                                     HoodieConfig config,
                                                                     List<Pair<String, HoodieBaseFile>> partitionBaseFilePairs,
                                                                     boolean forDelete,
                                                                     int recordIndexMaxParallelism,
                                                                     StoragePath basePath,
                                                                     StorageConfiguration<?> configuration,
                                                                     String activeModule) {
    if (partitionBaseFilePairs.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    engineContext.setJobStatus(activeModule, "Record Index: reading record keys from " + partitionBaseFilePairs.size() + " base files");
    final int parallelism = Math.min(partitionBaseFilePairs.size(), recordIndexMaxParallelism);
    return engineContext.parallelize(partitionBaseFilePairs, parallelism).flatMap(partitionAndBaseFile -> {
      final String partition = partitionAndBaseFile.getKey();
      final HoodieBaseFile baseFile = partitionAndBaseFile.getValue();
      final String filename = baseFile.getFileName();
      StoragePath dataFilePath = filePath(basePath, partition, filename);

      final String fileId = baseFile.getFileId();
      final String instantTime = baseFile.getCommitTime();
      HoodieFileReader reader = HoodieIOFactory.getIOFactory(HoodieStorageUtils.getStorage(basePath, configuration))
          .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
          .getFileReader(config, dataFilePath);
      return getHoodieRecordIterator(reader.getRecordKeyIterator(), forDelete, partition, fileId, instantTime);
    });
  }

  /**
   * Reads the record keys from the given file slices and returns a {@link HoodieData} of {@link HoodieRecord} to be updated in the metadata table.
   * If file slice does not have any base file, then iterates over the log files to get the record keys.
   */
  public static <T> HoodieData<HoodieRecord> readRecordKeysFromFileSlices(HoodieEngineContext engineContext,
                                                                          List<Pair<String, FileSlice>> partitionFileSlicePairs,
                                                                          int recordIndexMaxParallelism,
                                                                          String activeModule,
                                                                          HoodieTableMetaClient metaClient) {
    if (partitionFileSlicePairs.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    engineContext.setJobStatus(activeModule, "Record Index: reading record keys from " + partitionFileSlicePairs.size() + " file slices");
    final int parallelism = Math.min(partitionFileSlicePairs.size(), recordIndexMaxParallelism);
    final StoragePath basePath = metaClient.getBasePath();
    final StorageConfiguration<?> storageConf = metaClient.getStorageConf();
    final Schema tableSchema;
    try {
      tableSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    } catch (Exception e) {
      throw new HoodieException("Unable to resolve table schema for table", e);
    }
    ReaderContextFactory<T> readerContextFactory = engineContext.getReaderContextFactory(metaClient);
    String latestCommitTime = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::requestedTime).orElse("");
    return engineContext.parallelize(partitionFileSlicePairs, parallelism).flatMap(partitionAndBaseFile -> {
      final String partition = partitionAndBaseFile.getKey();
      final FileSlice fileSlice = partitionAndBaseFile.getValue();
      if (!fileSlice.getBaseFile().isPresent()) {
        HoodieFileGroupReader fileGroupReader = HoodieFileGroupReader.<T>newBuilder()
            .withReaderContext(readerContextFactory.getContext())
            .withHoodieTableMetaClient(metaClient)
            .withFileSlice(fileSlice)
            .withDataSchema(tableSchema)
            .withRequestedSchema(HoodieAvroUtils.getRecordKeySchema())
            .withLatestCommitTime(latestCommitTime)
            .withProps(getFileGroupReaderPropertiesFromStorageConf(storageConf))
            .build();

        ClosableIterator<String> recordKeyIterator = fileGroupReader.getClosableKeyIterator();
        return getHoodieRecordIterator(recordKeyIterator, false, partition, fileSlice.getFileId(), fileSlice.getBaseInstantTime());
      }
      final HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
      final String filename = baseFile.getFileName();
      StoragePath dataFilePath = filePath(basePath, partition, filename);

      final String fileId = baseFile.getFileId();
      final String instantTime = baseFile.getCommitTime();
      HoodieConfig hoodieConfig = getReaderConfigs(storageConf);
      HoodieFileReader reader = HoodieIOFactory.getIOFactory(metaClient.getStorage())
          .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
          .getFileReader(hoodieConfig, dataFilePath);
      return getHoodieRecordIterator(reader.getRecordKeyIterator(), false, partition, fileId, instantTime);
    });
  }

  public static Schema getProjectedSchemaForExpressionIndex(HoodieIndexDefinition indexDefinition, HoodieTableMetaClient metaClient, Schema tableSchema) {
    List<String> partitionFields = metaClient.getTableConfig().getPartitionFields()
        .map(Arrays::asList)
        .orElse(Collections.emptyList());
    List<String> sourceFields = indexDefinition.getSourceFields();
    List<String> mergedFields = new ArrayList<>(partitionFields.size() + sourceFields.size());
    mergedFields.addAll(partitionFields);
    mergedFields.addAll(sourceFields);
    return addMetadataFields(projectSchema(tableSchema, mergedFields));
  }

  public static StoragePath filePath(StoragePath basePath, String partition, String filename) {
    if (partition.isEmpty()) {
      return new StoragePath(basePath, filename);
    } else {
      return new StoragePath(basePath, partition + StoragePath.SEPARATOR + filename);
    }
  }

  private static ClosableIterator<HoodieRecord> getHoodieRecordIterator(ClosableIterator<String> recordKeyIterator,
                                                                        boolean forDelete,
                                                                        String partition,
                                                                        String fileId,
                                                                        String instantTime
  ) {
    return new ClosableIterator<HoodieRecord>() {
      @Override
      public void close() {
        recordKeyIterator.close();
      }

      @Override
      public boolean hasNext() {
        return recordKeyIterator.hasNext();
      }

      @Override
      public HoodieRecord next() {
        return forDelete
            ? HoodieMetadataPayload.createRecordIndexDelete(recordKeyIterator.next())
            : HoodieMetadataPayload.createRecordIndexUpdate(recordKeyIterator.next(), partition, fileId, instantTime, 0);
      }
    };
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

  public static HoodieData<HoodieRecord> collectAndProcessExprIndexPartitionStatRecords(HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> fileColumnMetadata,
                                                                                        boolean isTightBound, Option<String> indexPartitionOpt) {
    // Step 1: Group by partition name
    HoodiePairData<String, Iterable<HoodieColumnRangeMetadata<Comparable>>> columnMetadataMap = fileColumnMetadata.groupByKey();
    // Step 2: Aggregate Column Ranges
    return columnMetadataMap.map(entry -> {
      String partitionName = entry.getKey();
      Iterable<HoodieColumnRangeMetadata<Comparable>> iterable = entry.getValue();
      final HoodieColumnRangeMetadata<Comparable>[] finalMetadata = new HoodieColumnRangeMetadata[] {null};
      iterable.forEach(e -> {
        HoodieColumnRangeMetadata<Comparable> rangeMetadata = HoodieColumnRangeMetadata.create(
            partitionName, e.getColumnName(), e.getMinValue(), e.getMaxValue(),
            e.getNullCount(), e.getValueCount(), e.getTotalSize(), e.getTotalUncompressedSize());
        finalMetadata[0] = HoodieColumnRangeMetadata.merge(finalMetadata[0], rangeMetadata);
      });
      return HoodieMetadataPayload.createPartitionStatsRecords(partitionName, Collections.singletonList(finalMetadata[0]), false, isTightBound, indexPartitionOpt)
          .collect(Collectors.toList());
    }).flatMap(List::iterator);
  }

  public static HoodieData<HoodieRecord> convertFilesToPartitionStatsRecords(HoodieEngineContext engineContext,
                                                                             List<Pair<String, FileSlice>> partitionInfoList,
                                                                             HoodieMetadataConfig metadataConfig,
                                                                             HoodieTableMetaClient dataTableMetaClient,
                                                                             Lazy<Option<Schema>> lazyWriterSchemaOpt,
                                                                             Option<HoodieRecordType> recordTypeOpt) {
    if (partitionInfoList.isEmpty()) {
      return engineContext.emptyHoodieData();
    }
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

  public static HoodieData<HoodieRecord> convertMetadataToPartitionStatRecords(HoodieCommitMetadata commitMetadata, HoodieEngineContext engineContext, HoodieTableMetaClient dataMetaClient,
                                                                               HoodieTableMetadata tableMetadata, HoodieMetadataConfig metadataConfig,
                                                                               Option<HoodieRecordType> recordTypeOpt, boolean isDeletePartition) {
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
        return engineContext.emptyHoodieData();
      }
      Lazy<Option<Schema>> writerSchemaOpt = Lazy.eagerly(tableSchema);
      Map<String, Schema> columnsToIndexSchemaMap = getColumnsToIndex(dataMetaClient.getTableConfig(), metadataConfig, writerSchemaOpt, false, recordTypeOpt);
      if (columnsToIndexSchemaMap.isEmpty()) {
        return engineContext.emptyHoodieData();
      }

      // if this is DELETE_PARTITION, then create delete metadata payload for all columns for partition_stats
      if (isDeletePartition) {
        HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
        Map<String, List<String>> partitionToReplaceFileIds = replaceCommitMetadata.getPartitionToReplaceFileIds();
        List<String> partitionsToDelete = new ArrayList<>(partitionToReplaceFileIds.keySet());
        if (partitionToReplaceFileIds.isEmpty()) {
          return engineContext.emptyHoodieData();
        }
        return engineContext.parallelize(partitionsToDelete, partitionsToDelete.size()).flatMap(partition -> {
          Stream<HoodieRecord> columnRangeMetadata = columnsToIndexSchemaMap.keySet().stream()
              .flatMap(column -> HoodieMetadataPayload.createPartitionStatsRecords(
                  partition,
                  Collections.singletonList(HoodieColumnRangeMetadata.stub("", column)),
                  true, true, Option.empty()));
          return columnRangeMetadata.iterator();
        });
      }

      // In this function we fetch column range metadata for all new files part of commit metadata along with all the other files
      // of the affected partitions. The column range metadata is grouped by partition name to generate HoodiePairData of partition name
      // and list of column range metadata for that partition files. This pair data is then used to generate partition stat records.
      List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
          .flatMap(Collection::stream).collect(Collectors.toList());
      if (allWriteStats.isEmpty()) {
        return engineContext.emptyHoodieData();
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
              .getRecordsByKeyPrefixes(
                  HoodieListData.lazy(generateColumnStatsKeys(colsToIndex, partitionName)), 
                  MetadataPartitionType.COLUMN_STATS.getPartitionPath(), false)
              // schema and properties are ignored in getInsertValue, so simply pass as null
              .map(record -> ((HoodieMetadataPayload)record.getData()).getColumnStatMetadata())
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

      return convertMetadataToPartitionStatsRecords(columnRangeMetadata, dataMetaClient, columnsToIndexSchemaMap);
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  public static boolean isShouldScanColStatsForTightBound(HoodieTableMetaClient dataMetaClient) {
    return MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(dataMetaClient);
  }

  public static HoodieIndexDefinition getHoodieIndexDefinition(String indexName, HoodieTableMetaClient metaClient) {
    return metaClient.getIndexForMetadataPartition(indexName)
        .orElseThrow(() -> new HoodieIndexException("Expression Index definition is not present for index: " + indexName));
  }

  public static Option<HoodieIndexVersion> getIndexVersionOption(String metadataPartitionPath, HoodieTableMetaClient metaClient) {
    Option<HoodieIndexMetadata> indexMetadata = metaClient.getIndexMetadata();
    if (indexMetadata.isEmpty()) {
      return Option.empty();
    }
    Map<String, HoodieIndexDefinition> indexDefs = indexMetadata.get().getIndexDefinitions();
    if (!indexDefs.containsKey(metadataPartitionPath)) {
      return Option.empty();
    }
    return Option.of(indexDefs.get(metadataPartitionPath).getVersion());
  }

  public static HoodieIndexVersion existingIndexVersionOrDefault(String metadataPartitionPath, HoodieTableMetaClient dataMetaClient) {
    return getIndexVersionOption(metadataPartitionPath, dataMetaClient).orElseGet(
        () -> HoodieIndexVersion.getCurrentVersion(dataMetaClient.getTableConfig().getTableVersion(), metadataPartitionPath));
  }

  /**
   * Generate key prefixes for each combination of column name in {@param columnsToIndex} and {@param partitionName}.
   * @deprecated Use {@link #generateColumnStatsKeys} instead
   */
  @Deprecated
  public static List<String> generateKeyPrefixes(List<String> columnsToIndex, String partitionName) {
    List<ColumnStatsIndexPrefixRawKey> rawKeys = generateColumnStatsKeys(columnsToIndex, partitionName);
    return rawKeys.stream()
        .map(key -> key.encode())
        .collect(Collectors.toList());
  }

  /**
   * Generate column stats index keys for each combination of column name in {@param columnsToIndex} and {@param partitionName}.
   */
  public static List<ColumnStatsIndexPrefixRawKey> generateColumnStatsKeys(List<String> columnsToIndex, String partitionName) {
    List<ColumnStatsIndexPrefixRawKey> keys = new ArrayList<>();
    for (String columnName : columnsToIndex) {
      keys.add(new ColumnStatsIndexPrefixRawKey(columnName, partitionName));
    }
    return keys;
  }

  private static List<HoodieColumnRangeMetadata<Comparable>> translateWriteStatToFileStats(HoodieWriteStat writeStat,
                                                                                           HoodieTableMetaClient datasetMetaClient,
                                                                                           List<String> columnsToIndex,
                                                                                           Option<Schema> writerSchemaOpt) {
    if (writeStat instanceof HoodieDeltaWriteStat && ((HoodieDeltaWriteStat) writeStat).getColumnStats().isPresent()) {
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMap = ((HoodieDeltaWriteStat) writeStat).getColumnStats().get();
      return new ArrayList<>(columnRangeMap.values());
    }

    String filePath = writeStat.getPath();
    return getFileStatsRangeMetadata(writeStat.getPartitionPath(), getFileNameFromPath(filePath), datasetMetaClient, columnsToIndex, false, -1);
  }

  public static String getPartitionStatsIndexKey(String partitionPath, String columnName) {
    final PartitionIndexID partitionIndexID = new PartitionIndexID(getColumnStatsIndexPartitionIdentifier(partitionPath));
    final ColumnIndexID columnIndexID = new ColumnIndexID(columnName);
    return columnIndexID.asBase64EncodedString().concat(partitionIndexID.asBase64EncodedString());
  }

  public static String getPartitionStatsIndexKey(String partitionPathPrefix, String partitionPath, String columnName) {
    final PartitionIndexID partitionPrefixID = new PartitionIndexID(getColumnStatsIndexPartitionIdentifier(partitionPathPrefix));
    final PartitionIndexID partitionIndexID = new PartitionIndexID(getColumnStatsIndexPartitionIdentifier(partitionPath));
    final ColumnIndexID columnIndexID = new ColumnIndexID(columnName);
    String partitionID = partitionPrefixID.asBase64EncodedString().concat(partitionIndexID.asBase64EncodedString());
    return columnIndexID.asBase64EncodedString().concat(partitionID);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static HoodieMetadataColumnStats mergeColumnStatsRecords(HoodieMetadataColumnStats prevColumnStats,
                                                                  HoodieMetadataColumnStats newColumnStats) {
    checkArgument(Objects.equals(prevColumnStats.getColumnName(), newColumnStats.getColumnName()));
    // We're handling 2 cases in here
    //  - New record is a tombstone: in this case it simply overwrites previous state
    //  - Previous record is a tombstone: in that case new proper record would also
    //    be simply overwriting previous state
    if (newColumnStats.getIsDeleted() || prevColumnStats.getIsDeleted()) {
      return newColumnStats;
    }

    // If new column stats is tight bound, then discard the previous column stats
    if (newColumnStats.getIsTightBound()) {
      return newColumnStats;
    }

    Comparable minValue =
        (Comparable) Stream.of(
                (Comparable) unwrapAvroValueWrapper(prevColumnStats.getMinValue()),
                (Comparable) unwrapAvroValueWrapper(newColumnStats.getMinValue()))
            .filter(Objects::nonNull)
            .min(Comparator.naturalOrder())
            .orElse(null);

    Comparable maxValue =
        (Comparable) Stream.of(
                (Comparable) unwrapAvroValueWrapper(prevColumnStats.getMaxValue()),
                (Comparable) unwrapAvroValueWrapper(newColumnStats.getMaxValue()))
            .filter(Objects::nonNull)
            .max(Comparator.naturalOrder())
            .orElse(null);

    HoodieMetadataColumnStats.Builder columnStatsBuilder = HoodieMetadataColumnStats.newBuilder(HoodieMetadataPayload.METADATA_COLUMN_STATS_BUILDER_STUB.get())
        .setFileName(newColumnStats.getFileName())
        .setColumnName(newColumnStats.getColumnName())
        .setMinValue(wrapValueIntoAvro(minValue))
        .setMaxValue(wrapValueIntoAvro(maxValue))
        .setValueCount(prevColumnStats.getValueCount() + newColumnStats.getValueCount())
        .setNullCount(prevColumnStats.getNullCount() + newColumnStats.getNullCount())
        .setTotalSize(prevColumnStats.getTotalSize() + newColumnStats.getTotalSize())
        .setTotalUncompressedSize(prevColumnStats.getTotalUncompressedSize() + newColumnStats.getTotalUncompressedSize())
        .setIsDeleted(newColumnStats.getIsDeleted());
    if (newColumnStats.hasField(COLUMN_STATS_FIELD_IS_TIGHT_BOUND)) {
      columnStatsBuilder.setIsTightBound(newColumnStats.getIsTightBound());
    }
    return columnStatsBuilder.build();
  }

  public static Map<String, HoodieMetadataFileInfo> combineFileSystemMetadata(HoodieMetadataPayload older, HoodieMetadataPayload newer) {
    Map<String, HoodieMetadataFileInfo> combinedFileInfo = new HashMap<>();
    // First, add all files listed in the previous record
    if (older.filesystemMetadata != null) {
      combinedFileInfo.putAll(older.filesystemMetadata);
    }

    // Second, merge in the files listed in the new record
    if (newer.filesystemMetadata != null) {
      validatePayload(newer.type, newer.filesystemMetadata);

      newer.filesystemMetadata.forEach((key, fileInfo) -> {
        combinedFileInfo.merge(key, fileInfo,
            // Combine previous record w/ the new one, new records taking precedence over
            // the old one
            //
            // NOTE: That if previous listing contains the file that is being deleted by the tombstone
            //       record (`IsDeleted` = true) in the new one, we simply delete the file from the resulting
            //       listing as well as drop the tombstone itself.
            //       However, if file is not present in the previous record we have to persist tombstone
            //       record in the listing to make sure we carry forward information that this file
            //       was deleted. This special case could occur since the merging flow is 2-stage:
            //          - First we merge records from all of the delta log-files
            //          - Then we merge records from base-files with the delta ones (coming as a result
            //          of the previous step)
            (oldFileInfo, newFileInfo) -> {
              // NOTE: We can't assume that MT update records will be ordered the same way as actual
              //       FS operations (since they are not atomic), therefore MT record merging should be a
              //       _commutative_ & _associative_ operation (ie one that would work even in case records
              //       will get re-ordered), which is
              //          - Possible for file-sizes (since file-sizes will ever grow, we can simply
              //          take max of the old and new records)
              //          - Not possible for is-deleted flags*
              //
              //       *However, we're assuming that the case of concurrent write and deletion of the same
              //       file is _impossible_ -- it would only be possible with concurrent upsert and
              //       rollback operation (affecting the same log-file), which is implausible, b/c either
              //       of the following have to be true:
              //          - We're appending to failed log-file (then the other writer is trying to
              //          rollback it concurrently, before it's own write)
              //          - Rollback (of completed instant) is running concurrently with append (meaning
              //          that restore is running concurrently with a write, which is also nut supported
              //          currently)
              if (newFileInfo.getIsDeleted()) {
                if (oldFileInfo.getIsDeleted()) {
                  LOG.warn("A file is repeatedly deleted in the files partition of the metadata table: {}", key);
                  return newFileInfo;
                }
                return null;
              }
              return new HoodieMetadataFileInfo(
                  Math.max(newFileInfo.getSize(), oldFileInfo.getSize()), false);
            });
      });
    }
    return combinedFileInfo;
  }

  private static void validatePayload(int type, Map<String, HoodieMetadataFileInfo> filesystemMetadata) {
    if (type == MetadataPartitionType.FILES.getRecordType()) {
      filesystemMetadata.forEach((fileName, fileInfo) -> checkState(fileInfo.getIsDeleted() || fileInfo.getSize() > 0, "Existing files should have size > 0"));
    }
  }

  public static Set<String> getExpressionIndexPartitionsToInit(MetadataPartitionType partitionType, HoodieMetadataConfig metadataConfig, HoodieTableMetaClient dataMetaClient) {
    return getIndexPartitionsToInit(
        partitionType,
        metadataConfig,
        dataMetaClient,
        () -> isNewExpressionIndexDefinitionRequired(metadataConfig, dataMetaClient),
        metadataConfig::getExpressionIndexColumn,
        metadataConfig::getExpressionIndexName,
        PARTITION_NAME_EXPRESSION_INDEX_PREFIX,
        metadataConfig.getExpressionIndexType()
    );
  }

  public static Set<String> getSecondaryIndexPartitionsToInit(MetadataPartitionType partitionType, HoodieMetadataConfig metadataConfig, HoodieTableMetaClient dataMetaClient) {
    return getIndexPartitionsToInit(
        partitionType,
        metadataConfig,
        dataMetaClient,
        () -> isNewSecondaryIndexDefinitionRequired(metadataConfig, dataMetaClient),
        metadataConfig::getSecondaryIndexColumn,
        metadataConfig::getSecondaryIndexName,
        PARTITION_NAME_SECONDARY_INDEX_PREFIX,
        PARTITION_NAME_SECONDARY_INDEX
    );
  }

  /**
   * Fetches uninitialized index partitions for the given partition type.
   * If no such partitions are found and a new index definition is required,
   * this method adds the new index definition before re-fetching the partitions.
   * This ensures that all required index partitions are initialized.
   *
   * @param partitionType       The type of metadata partition (e.g., expression index, secondary index).
   * @param metadataConfig      Configuration object containing metadata-related properties.
   * @param dataMetaClient      Metadata client for interacting with the table's metadata.
   * @param isNewIndexRequired  A supplier to determine whether a new index definition is required.
   * @param getIndexedColumn    A supplier to fetch the column to be indexed.
   * @param getIndexName        A supplier to fetch the user-defined index name.
   * @param partitionNamePrefix A prefix to ensure index names follow naming conventions.
   * @param indexType           The type of index being initialized (e.g., expression index, secondary index).
   * @return A set of index partitions that require initialization, or an empty set if none are required.
   */
  private static Set<String> getIndexPartitionsToInit(MetadataPartitionType partitionType,
                                                      HoodieMetadataConfig metadataConfig,
                                                      HoodieTableMetaClient dataMetaClient,
                                                      Supplier<Boolean> isNewIndexRequired,
                                                      Supplier<String> getIndexedColumn,
                                                      Supplier<String> getIndexName,
                                                      String partitionNamePrefix,
                                                      String indexType) {
    // Fetch existing uninitialized partitions for which index definition already exists
    Set<String> indexPartitionsToInit = getIndexPartitionsToInitBasedOnIndexDefinition(partitionType, dataMetaClient);

    // If no index partition found, check if new index definition need to be added based on metadata write configs
    if (indexPartitionsToInit.isEmpty() && isNewIndexRequired.get()) {
      String indexedColumn = getIndexedColumn.get();
      String indexName = getSecondaryOrExpressionIndexName(getIndexName, partitionNamePrefix, indexedColumn);

      // Build and register the new index definition
      HoodieIndexDefinition.Builder indexDefinitionBuilder = HoodieIndexDefinition.newBuilder()
          .withIndexName(indexName)
          .withIndexType(indexType)
          .withVersion(existingIndexVersionOrDefault(indexName, dataMetaClient))
          .withSourceFields(Collections.singletonList(indexedColumn));
      if (partitionNamePrefix.equals(PARTITION_NAME_EXPRESSION_INDEX_PREFIX)) {
        indexDefinitionBuilder.withIndexOptions(metadataConfig.getExpressionIndexOptions());
        indexDefinitionBuilder.withIndexFunction(metadataConfig.getExpressionIndexOptions().getOrDefault(EXPRESSION_OPTION, IDENTITY_TRANSFORM));
      }

      dataMetaClient.buildIndexDefinition(indexDefinitionBuilder.build());

      // Re-fetch the partitions after adding the new definition
      indexPartitionsToInit = getIndexPartitionsToInitBasedOnIndexDefinition(partitionType, dataMetaClient);
    }

    return indexPartitionsToInit;
  }

  public static String getSecondaryOrExpressionIndexName(Supplier<String> getConfiguredIndexName, String partitionNamePrefix, String indexedColumn) {
    String indexName = getConfiguredIndexName.get();

    // Use a default index name if the indexed column is specified but index name is not
    if (StringUtils.isNullOrEmpty(indexName) && StringUtils.nonEmpty(indexedColumn)) {
      indexName = partitionNamePrefix + indexedColumn;
    }

    // Ensure the index name has the appropriate prefix
    if (StringUtils.nonEmpty(indexName) && !indexName.startsWith(partitionNamePrefix)) {
      indexName = partitionNamePrefix + indexName;
    }
    return indexName;
  }

  private static Set<String> getIndexPartitionsToInitBasedOnIndexDefinition(MetadataPartitionType partitionType, HoodieTableMetaClient dataMetaClient) {
    if (dataMetaClient.getIndexMetadata().isEmpty()) {
      return Collections.emptySet();
    }

    Set<String> indexPartitions = dataMetaClient.getIndexMetadata().get().getIndexDefinitions().values().stream()
        .map(HoodieIndexDefinition::getIndexName)
        .filter(indexName -> indexName.startsWith(partitionType.getPartitionPath()))
        .collect(Collectors.toSet());
    Set<String> completedMetadataPartitions = dataMetaClient.getTableConfig().getMetadataPartitions();
    indexPartitions.removeAll(completedMetadataPartitions);
    return indexPartitions;
  }

  /**
   * A class which represents a directory and the files and directories inside it.
   * <p>
   * A {@code PartitionFileInfo} object saves the name of the partition and various properties requires of each file
   * required for initializing the metadata table. Saving limited properties reduces the total memory footprint when
   * a very large number of files are present in the dataset being initialized.
   */
  public static class DirectoryInfo implements Serializable {
    // Relative path of the directory (relative to the base directory)
    private final String relativePath;
    // Map of filenames within this partition to their respective sizes
    private final HashMap<String, Long> filenameToSizeMap;
    // List of directories within this partition
    private final List<StoragePath> subDirectories = new ArrayList<>();
    // Is this a hoodie partition
    private boolean isHoodiePartition = false;

    public DirectoryInfo(String relativePath, List<StoragePathInfo> pathInfos, String maxInstantTime, Set<String> pendingDataInstants) {
      this(relativePath, pathInfos, maxInstantTime, pendingDataInstants, true);
    }

    /**
     * When files are directly fetched from Metadata table we do not need to validate HoodiePartitions.
     */
    public DirectoryInfo(String relativePath, List<StoragePathInfo> pathInfos, String maxInstantTime, Set<String> pendingDataInstants,
                         boolean validateHoodiePartitions) {
      this.relativePath = relativePath;

      // Pre-allocate with the maximum length possible
      filenameToSizeMap = new HashMap<>(pathInfos.size());

      // Presence of partition meta file implies this is a HUDI partition
      // if input files are directly fetched from MDT, it may not contain the HoodiePartitionMetadata file. So, we can ignore the validation for isHoodiePartition.
      isHoodiePartition = !validateHoodiePartitions || pathInfos.stream().anyMatch(status -> status.getPath().getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX));
      for (StoragePathInfo pathInfo : pathInfos) {
        // Do not attempt to search for more subdirectories inside directories that are partitions
        if (!isHoodiePartition && pathInfo.isDirectory()) {
          // Ignore .hoodie directory as there cannot be any partitions inside it
          if (!pathInfo.getPath().getName().equals(HoodieTableMetaClient.METAFOLDER_NAME)) {
            this.subDirectories.add(pathInfo.getPath());
          }
        } else if (isHoodiePartition && FSUtils.isDataFile(pathInfo.getPath())) {
          // Regular HUDI data file (base file or log file)
          String dataFileCommitTime = FSUtils.getCommitTime(pathInfo.getPath().getName());
          // Limit the file listings to files which were created by successful commits before the maxInstant time.
          if (!pendingDataInstants.contains(dataFileCommitTime) && compareTimestamps(dataFileCommitTime, LESSER_THAN_OR_EQUALS, maxInstantTime)) {
            filenameToSizeMap.put(pathInfo.getPath().getName(), pathInfo.getLength());
          }
        }
      }
    }

    String getRelativePath() {
      return relativePath;
    }

    int getTotalFiles() {
      return filenameToSizeMap.size();
    }

    boolean isHoodiePartition() {
      return isHoodiePartition;
    }

    List<StoragePath> getSubDirectories() {
      return subDirectories;
    }

    // Returns a map of filenames mapped to their lengths
    Map<String, Long> getFileNameToSizeMap() {
      return filenameToSizeMap;
    }
  }

  private static TypedProperties getFileGroupReaderPropertiesFromStorageConf(StorageConfiguration<?> storageConf) {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(MAX_MEMORY_FOR_MERGE.key(),
        Long.toString(storageConf.getLong(MAX_MEMORY_FOR_COMPACTION.key(), DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES)));
    properties.setProperty(SPILLABLE_DISK_MAP_TYPE.key(),
        storageConf.getEnum(SPILLABLE_DISK_MAP_TYPE.key(), SPILLABLE_DISK_MAP_TYPE.defaultValue()).toString());
    properties.setProperty(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        Boolean.toString(storageConf.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue())));
    return properties;
  }
}
