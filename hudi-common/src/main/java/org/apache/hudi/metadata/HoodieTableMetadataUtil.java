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
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieFileSliceReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFactory;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
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
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldSchemaFromWriteSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.getSchemaForFields;
import static org.apache.hudi.avro.HoodieAvroUtils.unwrapAvroValueWrapper;
import static org.apache.hudi.avro.HoodieAvroUtils.wrapValueIntoAvro;
import static org.apache.hudi.common.config.HoodieCommonConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.MAX_MEMORY_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieReaderConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.compareTimestamps;
import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_MISSING_FILEINDEX_FALLBACK;
import static org.apache.hudi.metadata.HoodieTableMetadata.EMPTY_PARTITION_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;

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
  public static final String PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX = "func_index_";
  public static final String PARTITION_NAME_SECONDARY_INDEX = "secondary_index";
  public static final String PARTITION_NAME_SECONDARY_INDEX_PREFIX = "secondary_index_";

  private HoodieTableMetadataUtil() {
  }

  public static final Set<Class<?>> COLUMN_STATS_RECORD_SUPPORTED_TYPES = new HashSet<>(Arrays.asList(
      IntWrapper.class, BooleanWrapper.class, DateWrapper.class,
      DoubleWrapper.class, FloatWrapper.class, LongWrapper.class,
      StringWrapper.class, TimeMicrosWrapper.class, TimestampMicrosWrapper.class));

  // we have max of 4 partitions (FILES, COL_STATS, BLOOM, RLI)
  private static final List<String> VALID_PARTITION_INITIALIZATION_TIME_SUFFIXES = Arrays.asList("010", "011", "012", "013");

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
      List<HoodieRecord> records, List<Schema.Field> targetFields, String filePath, Schema recordSchema) {
    // Helper class to calculate column stats
    class ColumnStats {
      Object minValue;
      Object maxValue;
      long nullCount;
      long valueCount;
    }

    HashMap<String, ColumnStats> allColumnStats = new HashMap<>();

    // Collect stats for all columns by iterating through records while accounting
    // corresponding stats
    records.forEach((record) -> {
      // For each column (field) we have to index update corresponding column stats
      // with the values from this record
      targetFields.forEach(field -> {
        ColumnStats colStats = allColumnStats.computeIfAbsent(field.name(), ignored -> new ColumnStats());

        Schema fieldSchema = getNestedFieldSchemaFromWriteSchema(recordSchema, field.name());
        Object fieldValue;
        if (record.getRecordType() == HoodieRecordType.AVRO) {
          fieldValue = HoodieAvroUtils.getRecordColumnValues(record, new String[]{field.name()}, recordSchema, false)[0];
        } else if (record.getRecordType() == HoodieRecordType.SPARK) {
          fieldValue = record.getColumnValues(recordSchema, new String[]{field.name()}, false)[0];
        } else {
          throw new HoodieException(String.format("Unknown record type: %s", record.getRecordType()));
        }

        colStats.valueCount++;
        if (fieldValue != null && canCompare(fieldSchema, record.getRecordType())) {
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
        targetFields.stream().map(field -> {
          ColumnStats colStats = allColumnStats.get(field.name());
          HoodieColumnRangeMetadata<Comparable> hcrm = HoodieColumnRangeMetadata.<Comparable>create(
              filePath,
              field.name(),
              colStats == null ? null : coerceToComparable(field.schema(), colStats.minValue),
              colStats == null ? null : coerceToComparable(field.schema(), colStats.maxValue),
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

  /**
   * Converts instance of {@link HoodieMetadataColumnStats} to {@link HoodieColumnRangeMetadata}
   */
  public static HoodieColumnRangeMetadata<Comparable> convertColumnStatsRecordToColumnRangeMetadata(HoodieMetadataColumnStats columnStats) {
    return HoodieColumnRangeMetadata.<Comparable>create(
        columnStats.getFileName(),
        columnStats.getColumnName(),
        unwrapAvroValueWrapper(columnStats.getMinValue()),
        unwrapAvroValueWrapper(columnStats.getMaxValue()),
        columnStats.getNullCount(),
        columnStats.getValueCount(),
        columnStats.getTotalSize(),
        columnStats.getTotalUncompressedSize());
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
   * @param context                          - Engine context to use
   * @param hoodieConfig                     - Hudi configs
   * @param commitMetadata                   - Commit action metadata
   * @param instantTime                      - Action instant time
   * @param dataMetaClient                   - HoodieTableMetaClient for data
   * @param enabledPartitionTypes            - List of enabled MDT partitions
   * @param bloomFilterType                  - Type of generated bloom filter records
   * @param bloomIndexParallelism            - Parallelism for bloom filter record generation
   * @param isColumnStatsIndexEnabled        - Is column stats index enabled
   * @param columnStatsIndexParallelism      - Parallelism for column stats index records generation
   * @param targetColumnsForColumnStatsIndex - List of columns for column stats index
   * @return Map of partition to metadata records for the commit action
   */
  public static Map<String, HoodieData<HoodieRecord>> convertMetadataToRecords(HoodieEngineContext context,
                                                                               HoodieConfig hoodieConfig,
                                                                               HoodieCommitMetadata commitMetadata,
                                                                               String instantTime,
                                                                               HoodieTableMetaClient dataMetaClient,
                                                                               List<MetadataPartitionType> enabledPartitionTypes,
                                                                               String bloomFilterType,
                                                                               int bloomIndexParallelism,
                                                                               boolean isColumnStatsIndexEnabled,
                                                                               int columnStatsIndexParallelism,
                                                                               List<String> targetColumnsForColumnStatsIndex,
                                                                               HoodieMetadataConfig metadataConfig) {
    final Map<String, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = context.parallelize(
        convertMetadataToFilesPartitionRecords(commitMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES.getPartitionPath(), filesPartitionRecordsRDD);

    if (enabledPartitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS)) {
      final HoodieData<HoodieRecord> metadataBloomFilterRecords = convertMetadataToBloomFilterRecords(
          context, hoodieConfig, commitMetadata, instantTime, dataMetaClient, bloomFilterType, bloomIndexParallelism);
      partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath(), metadataBloomFilterRecords);
    }

    if (enabledPartitionTypes.contains(MetadataPartitionType.COLUMN_STATS)) {
      final HoodieData<HoodieRecord> metadataColumnStatsRDD = convertMetadataToColumnStatsRecords(commitMetadata, context,
          dataMetaClient, isColumnStatsIndexEnabled, columnStatsIndexParallelism, targetColumnsForColumnStatsIndex);
      partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), metadataColumnStatsRDD);
    }
    if (enabledPartitionTypes.contains(MetadataPartitionType.PARTITION_STATS)) {
      final HoodieData<HoodieRecord> partitionStatsRDD = convertMetadataToPartitionStatsRecords(commitMetadata, context, dataMetaClient, metadataConfig);
      partitionToRecordsMap.put(MetadataPartitionType.PARTITION_STATS.getPartitionPath(), partitionStatsRDD);
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
                                                                               List<MetadataPartitionType> enabledPartitionTypes,
                                                                               int bloomIndexParallelism,
                                                                               boolean isColumnStatsIndexEnabled,
                                                                               int columnStatsIndexParallelism,
                                                                               List<String> targetColumnsForColumnStatsIndex) {
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
              dataMetaClient, isColumnStatsIndexEnabled, columnStatsIndexParallelism, targetColumnsForColumnStatsIndex);
      partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), metadataColumnStatsRDD);
    }

    return partitionToRecordsMap;
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
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partitionName, Collections.emptyMap(),
          deletedFiles);
      records.add(record);
      fileDeleteCount[0] += deletedFiles.size();
      boolean isPartitionDeleted = partitionMetadata.getIsPartitionDeleted();
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
   * @param isColumnStatsIndexEnabled        - Is column stats index enabled
   * @param columnStatsIndexParallelism      - Parallelism for column stats index records generation
   * @param targetColumnsForColumnStatsIndex - List of columns for column stats index
   * @return List of column stats index records for the clean metadata
   */
  public static HoodieData<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCleanMetadata cleanMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             HoodieTableMetaClient dataMetaClient,
                                                                             boolean isColumnStatsIndexEnabled,
                                                                             int columnStatsIndexParallelism,
                                                                             List<String> targetColumnsForColumnStatsIndex) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> deleteFileList.add(Pair.of(partition, entry)));
    });

    List<String> columnsToIndex =
        getColumnsToIndex(isColumnStatsIndexEnabled, targetColumnsForColumnStatsIndex,
            Lazy.lazily(() -> tryResolveSchemaForTable(dataMetaClient)));

    if (columnsToIndex.isEmpty()) {
      // In case there are no columns to index, bail
      LOG.warn("No columns to index for column stats index.");
      return engineContext.emptyHoodieData();
    }

    int parallelism = Math.max(Math.min(deleteFileList.size(), columnStatsIndexParallelism), 1);
    return engineContext.parallelize(deleteFileList, parallelism)
        .flatMap(deleteFileInfoPair -> {
          String partitionPath = deleteFileInfoPair.getLeft();
          String filePath = deleteFileInfoPair.getRight();

          if (filePath.endsWith(HoodieFileFormat.PARQUET.getFileExtension()) || ExternalFilePathUtil.isExternallyCreatedFile(filePath)) {
            return getColumnStatsRecords(partitionPath, filePath, dataMetaClient, columnsToIndex, true).iterator();
          }
          return Collections.emptyListIterator();
        });
  }

  private static void reAddLogFilesFromRollbackPlan(HoodieTableMetaClient dataTableMetaClient, String instantTime,
                                                    Map<String, Map<String, Long>> partitionToFilesMap) {
    InstantFactory factory = dataTableMetaClient.getTimelineLayout().getInstantFactory();
    HoodieInstant rollbackInstant = factory.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, instantTime);
    HoodieInstant requested = factory.getRollbackRequestedInstant(rollbackInstant);
    try {
      HoodieRollbackPlan rollbackPlan = TimelineMetadataUtils.deserializeAvroMetadata(
          dataTableMetaClient.getActiveTimeline().readRollbackInfoAsBytes(requested).get(), HoodieRollbackPlan.class);

      rollbackPlan.getRollbackRequests().forEach(rollbackRequest -> {
        final String partitionId = getPartitionIdentifierForFilesPartition(rollbackRequest.getPartitionPath());
        partitionToFilesMap.computeIfAbsent(partitionId, s -> new HashMap<>());
        // fetch only log files that are expected to be RB'd in DT as part of this rollback. these log files will not be deleted, but rendered
        // invalid once rollback is complete.
        if (!rollbackRequest.getLogBlocksToBeDeleted().isEmpty()) {
          Map<String, Long> logFiles = new HashMap<>();
          rollbackRequest.getLogBlocksToBeDeleted().forEach((k,v) -> {
            String fileName = k.substring(k.lastIndexOf("/") + 1);
            // rollback plan may not have size for log files to be rolled back. but while merging w/ original commits, the size will get adjusted.
            logFiles.put(fileName, 1L);
          });
          partitionToFilesMap.get(partitionId).putAll(logFiles);
        }
      });
    } catch (IOException e) {
      throw new HoodieMetadataException("Parsing rollback plan for " + rollbackInstant + " failed ");
    }
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
    reAddLogFilesFromRollbackPlan(dataTableMetaClient, instantTime, partitionToAppendedFiles);
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
  private static String getPartitionIdentifier(@Nonnull String relativePartitionPath) {
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
                                                                          boolean isColumnStatsIndexEnabled,
                                                                          int columnStatsIndexParallelism,
                                                                          List<String> targetColumnsForColumnStatsIndex) {
    // Find the columns to index
    final List<String> columnsToIndex =
        getColumnsToIndex(isColumnStatsIndexEnabled, targetColumnsForColumnStatsIndex,
            Lazy.lazily(() -> tryResolveSchemaForTable(dataMetaClient)));
    if (columnsToIndex.isEmpty()) {
      // In case there are no columns to index, bail
      LOG.warn("No columns to index for column stats index.");
      return engineContext.emptyHoodieData();
    }

    LOG.info("Indexing {} columns for column stats index", columnsToIndex.size());

    // Create the tuple (partition, filename, isDeleted) to handle both deletes and appends
    final List<Tuple3<String, String, Boolean>> partitionFileFlagTupleList = fetchPartitionFileInfoTriplets(partitionToDeletedFiles, partitionToAppendedFiles);

    // Create records MDT
    int parallelism = Math.max(Math.min(partitionFileFlagTupleList.size(), columnStatsIndexParallelism), 1);
    return engineContext.parallelize(partitionFileFlagTupleList, parallelism).flatMap(partitionFileFlagTuple -> {
      final String partitionName = partitionFileFlagTuple.f0;
      final String filename = partitionFileFlagTuple.f1;
      final boolean isDeleted = partitionFileFlagTuple.f2;
      if (!FSUtils.isBaseFile(new StoragePath(filename)) || !filename.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        LOG.warn("Ignoring file {} as it is not a PARQUET file", filename);
        return Stream.<HoodieRecord>empty().iterator();
      }

      final String filePathWithPartition = partitionName + "/" + filename;
      return getColumnStatsRecords(partitionName, filePathWithPartition, dataMetaClient, columnsToIndex, isDeleted).iterator();
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
   * Map a record key to a file group in partition of interest.
   * <p>
   * Note: For hashing, the algorithm is same as String.hashCode() but is being defined here as hashCode()
   * implementation is not guaranteed by the JVM to be consistent across JVM versions and implementations.
   *
   * @param recordKey record key for which the file group index is looked up for.
   * @return An integer hash of the given string
   */
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
  public static HoodieTableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient) {
    // If there are no commits on the metadata table then the table's
    // default FileSystemView will not return any file slices even
    // though we may have initialized them.
    HoodieTimeline timeline = metaClient.getActiveTimeline();
    TimelineFactory factory = metaClient.getTimelineLayout().getTimelineFactory();
    if (timeline.empty()) {
      final HoodieInstant instant = metaClient.getTimelineLayout().getInstantFactory().createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION,
          metaClient.createNewInstantTime(false));
      timeline = factory.createDefaultTimeline(Stream.of(instant), metaClient.getActiveTimeline()::getInstantDetails);
    }
    return new HoodieTableFileSystemView(metaClient, timeline);
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
      fsView = fileSystemView.orElseGet(() -> getFileSystemView(metaClient));
      Stream<FileSlice> fileSliceStream;
      if (mergeFileSlices) {
        if (metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().isPresent()) {
          fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(
              // including pending compaction instant as the last instant so that the finished delta commits
              // that start earlier than the compaction can be queried.
              partition, metaClient.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant().get().getRequestTime());
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
      fsView = fileSystemView.orElseGet(() -> getFileSystemView(metaClient));
      Stream<FileSlice> fileSliceStream = fsView.getLatestFileSlicesIncludingInflight(partition);
      return fileSliceStream
          .sorted(Comparator.comparing(FileSlice::getFileId))
          .collect(Collectors.toList());
    } finally {
      if (!fileSystemView.isPresent()) {
        fsView.close();
      }
    }
  }

  public static HoodieData<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCommitMetadata commitMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             HoodieTableMetaClient dataMetaClient,
                                                                             boolean isColumnStatsIndexEnabled,
                                                                             int columnStatsIndexParallelism,
                                                                             List<String> targetColumnsForColumnStatsIndex) {
    List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());

    if (allWriteStats.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    try {
      Option<Schema> writerSchema =
          Option.ofNullable(commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY))
              .flatMap(writerSchemaStr ->
                  isNullOrEmpty(writerSchemaStr)
                      ? Option.empty()
                      : Option.of(new Schema.Parser().parse(writerSchemaStr)));

      HoodieTableConfig tableConfig = dataMetaClient.getTableConfig();

      // NOTE: Writer schema added to commit metadata will not contain Hudi's metadata fields
      Option<Schema> tableSchema = writerSchema.map(schema ->
          tableConfig.populateMetaFields() ? addMetadataFields(schema) : schema);

      List<String> columnsToIndex = getColumnsToIndex(isColumnStatsIndexEnabled, targetColumnsForColumnStatsIndex,
          Lazy.eagerly(tableSchema));

      if (columnsToIndex.isEmpty()) {
        // In case there are no columns to index, bail
        return engineContext.emptyHoodieData();
      }

      int parallelism = Math.max(Math.min(allWriteStats.size(), columnStatsIndexParallelism), 1);
      return engineContext.parallelize(allWriteStats, parallelism)
          .flatMap(writeStat ->
              translateWriteStatToColumnStats(writeStat, dataMetaClient, columnsToIndex).iterator());
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  /**
   * Get the list of columns for the table for column stats indexing
   */
  private static List<String> getColumnsToIndex(boolean isColumnStatsIndexEnabled,
                                                List<String> targetColumnsForColumnStatsIndex,
                                                Lazy<Option<Schema>> lazyWriterSchemaOpt) {
    checkState(isColumnStatsIndexEnabled);

    if (!targetColumnsForColumnStatsIndex.isEmpty()) {
      return targetColumnsForColumnStatsIndex;
    }

    Option<Schema> writerSchemaOpt = lazyWriterSchemaOpt.get();
    return writerSchemaOpt
        .map(writerSchema ->
            writerSchema.getFields().stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
  }

  private static Stream<HoodieRecord> translateWriteStatToColumnStats(HoodieWriteStat writeStat,
                                                                      HoodieTableMetaClient datasetMetaClient,
                                                                      List<String> columnsToIndex) {
    if (writeStat instanceof HoodieDeltaWriteStat && ((HoodieDeltaWriteStat) writeStat).getColumnStats().isPresent()) {
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMap = ((HoodieDeltaWriteStat) writeStat).getColumnStats().get();
      Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = columnRangeMap.values();
      return HoodieMetadataPayload.createColumnStatsRecords(writeStat.getPartitionPath(), columnRangeMetadataList, false);
    }

    return getColumnStatsRecords(writeStat.getPartitionPath(), writeStat.getPath(), datasetMetaClient, columnsToIndex, false);
  }

  private static Stream<HoodieRecord> getColumnStatsRecords(String partitionPath,
                                                            String filePath,
                                                            HoodieTableMetaClient datasetMetaClient,
                                                            List<String> columnsToIndex,
                                                            boolean isDeleted) {
    String filePartitionPath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
    String fileName = FSUtils.getFileName(filePath, partitionPath);

    if (isDeleted) {
      // TODO we should delete records instead of stubbing them
      List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = columnsToIndex.stream()
          .map(entry -> HoodieColumnRangeMetadata.stub(fileName, entry))
          .collect(Collectors.toList());

      return HoodieMetadataPayload.createColumnStatsRecords(partitionPath, columnRangeMetadataList, true);
    }

    List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata =
        readColumnRangeMetadataFrom(filePartitionPath, datasetMetaClient, columnsToIndex);

    return HoodieMetadataPayload.createColumnStatsRecords(partitionPath, columnRangeMetadata, false);
  }

  private static List<HoodieColumnRangeMetadata<Comparable>> readColumnRangeMetadataFrom(String filePath,
                                                                                         HoodieTableMetaClient datasetMetaClient,
                                                                                         List<String> columnsToIndex) {
    try {
      if (filePath.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        StoragePath fullFilePath = new StoragePath(datasetMetaClient.getBasePath(), filePath);
        return HoodieIOFactory.getIOFactory(datasetMetaClient.getStorage())
            .getFileFormatUtils(HoodieFileFormat.PARQUET)
            .readColumnStatsFromMetadata(datasetMetaClient.getStorage(), fullFilePath, columnsToIndex);
      }

      LOG.warn("Column range index not supported for: {}", filePath);
      return Collections.emptyList();
    } catch (Exception e) {
      // NOTE: In case reading column range metadata from individual file failed,
      //       we simply fall back, in lieu of failing the whole task
      LOG.error("Failed to fetch column range metadata for: {}", filePath);
      return Collections.emptyList();
    }
  }

  /**
   * Does an upcast for {@link BigDecimal} instance to align it with scale/precision expected by
   * the {@link org.apache.avro.LogicalTypes.Decimal} Avro logical type
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

  private static Option<Schema> tryResolveSchemaForTable(HoodieTableMetaClient dataTableMetaClient) {
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
  private static Comparable<?> coerceToComparable(Schema schema, Object val) {
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
        return (Integer) val;

      case LONG:
        if (schema.getLogicalType() == LogicalTypes.timeMicros()
            || schema.getLogicalType() == LogicalTypes.timestampMicros()
            || schema.getLogicalType() == LogicalTypes.timestampMillis()) {
          // NOTE: This type will be either {@code java.sql.Date} or {org.joda.LocalDate}
          //       depending on the Avro version. Hence, we simply cast it to {@code Comparable<?>}
          return (Comparable<?>) val;
        }
        return (Long) val;

      case STRING:
        // unpack the avro Utf8 if possible
        return val.toString();
      case FLOAT:
      case DOUBLE:
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

  private static boolean canCompare(Schema schema, HoodieRecordType recordType) {
    // if recordType is SPARK then we cannot compare RECORD and ARRAY types in addition to MAP type
    if (recordType == HoodieRecordType.SPARK) {
      return schema.getType() != Schema.Type.RECORD && schema.getType() != Schema.Type.ARRAY && schema.getType() != Schema.Type.MAP;
    }
    return schema.getType() != Schema.Type.MAP;
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
    Set<String> datasetPendingInstants = datasetTimeline.filterInflightsAndRequested().getInstantsAsStream().map(HoodieInstant::getRequestTime).collect(Collectors.toSet());
    Set<String> validInstantTimestamps = datasetTimeline.filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::getRequestTime).collect(Collectors.toSet());

    // We should also add completed indexing delta commits in the metadata table, as they do not
    // have corresponding completed instant in the data table
    validInstantTimestamps.addAll(
        metadataMetaClient.getActiveTimeline()
            .filter(instant -> instant.isCompleted() && isValidInstant(datasetPendingInstants, instant))
            .getInstantsAsStream()
            .map(HoodieInstant::getRequestTime)
            .collect(Collectors.toList()));

    // For any rollbacks and restores, we cannot neglect the instants that they are rolling back.
    // The rollback instant should be more recent than the start of the timeline for it to have rolled back any
    // instant which we have a log block for.
    final String earliestInstantTime = validInstantTimestamps.isEmpty() ? SOLO_COMMIT_TIMESTAMP : Collections.min(validInstantTimestamps);
    datasetTimeline.getRollbackAndRestoreTimeline().filterCompletedInstants().getInstantsAsStream()
            .filter(instant -> compareTimestamps(instant.getRequestTime(), GREATER_THAN, earliestInstantTime))
            .forEach(instant -> validInstantTimestamps.addAll(getRollbackedCommits(instant, datasetTimeline, dataMetaClient.getTimelineLayout().getInstantFactory())));

    // add restore and rollback instants from MDT.
    metadataMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline().filterCompletedInstants()
        .filter(instant -> instant.getAction().equals(HoodieTimeline.RESTORE_ACTION) || instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION))
        .getInstants().forEach(instant -> validInstantTimestamps.add(instant.getRequestTime()));

    metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants()
        .filter(instant ->  instant.getRequestTime().startsWith(SOLO_COMMIT_TIMESTAMP))
        .getInstants().forEach(instant -> validInstantTimestamps.add(instant.getRequestTime()));
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
    return instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION) && !datasetPendingInstants.contains(instant.getRequestTime());
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
  private static List<String> getRollbackedCommits(HoodieInstant instant, HoodieActiveTimeline timeline, InstantFactory factory) {
    try {
      List<String> commitsToRollback;
      if (instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)) {
        try {
          HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
              timeline.getInstantDetails(instant).get());
          commitsToRollback = rollbackMetadata.getCommitsRollback();
        } catch (IOException e) {
          // if file is empty, fetch the commits to rollback from rollback.requested file
          HoodieRollbackPlan rollbackPlan = TimelineMetadataUtils.deserializeAvroMetadata(
              timeline.readRollbackInfoAsBytes(factory.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION,
                  instant.getRequestTime())).get(), HoodieRollbackPlan.class);
          commitsToRollback = Collections.singletonList(rollbackPlan.getInstantToRollback().getCommitTime());
          LOG.warn("Had to fetch rollback info from requested instant since completed file is empty {}", instant);
        }
        return commitsToRollback;
      }

      List<String> rollbackedCommits = new LinkedList<>();
      if (instant.getAction().equals(HoodieTimeline.RESTORE_ACTION)) {
        // Restore is made up of several rollbacks
        HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
            timeline.getInstantDetails(instant).get());
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
      final StoragePath metadataBackupPath = new StoragePath(metadataTablePath.getParent(), ".metadata_" + dataMetaClient.createNewInstantTime(false));
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
          String.format(".metadata_%s_%s", partitionPath, dataMetaClient.createNewInstantTime(false)));
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
  public static String getFileIDForFileGroup(MetadataPartitionType partitionType, int index) {
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
   * Use {@link #readRecordKeysFromFileSlices(HoodieEngineContext, List, boolean, int, String, HoodieTableMetaClient, EngineType)} instead.
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
  public static HoodieData<HoodieRecord> readRecordKeysFromFileSlices(HoodieEngineContext engineContext,
                                                                      List<Pair<String, FileSlice>> partitionFileSlicePairs,
                                                                      boolean forDelete,
                                                                      int recordIndexMaxParallelism,
                                                                      String activeModule, HoodieTableMetaClient metaClient, EngineType engineType) {
    if (partitionFileSlicePairs.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    engineContext.setJobStatus(activeModule, "Record Index: reading record keys from " + partitionFileSlicePairs.size() + " file slices");
    final int parallelism = Math.min(partitionFileSlicePairs.size(), recordIndexMaxParallelism);
    final StoragePath basePath = metaClient.getBasePath();
    final StorageConfiguration<?> storageConf = metaClient.getStorageConf();
    return engineContext.parallelize(partitionFileSlicePairs, parallelism).flatMap(partitionAndBaseFile -> {
      final String partition = partitionAndBaseFile.getKey();
      final FileSlice fileSlice = partitionAndBaseFile.getValue();
      if (!fileSlice.getBaseFile().isPresent()) {
        List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
            .map(l -> l.getPath().toString()).collect(toList());
        HoodieMergedLogRecordScanner mergedLogRecordScanner = HoodieMergedLogRecordScanner.newBuilder()
            .withStorage(metaClient.getStorage())
            .withBasePath(basePath)
            .withLogFilePaths(logFilePaths)
            .withReaderSchema(HoodieAvroUtils.getRecordKeySchema())
            .withLatestInstantTime(metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::getRequestTime).orElse(""))
            .withReverseReader(false)
            .withMaxMemorySizeInBytes(storageConf.getLong(
                MAX_MEMORY_FOR_COMPACTION.key(), DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES))
            .withSpillableMapBasePath(FileIOUtils.getDefaultSpillableMapBasePath())
            .withPartition(fileSlice.getPartitionPath())
            .withOptimizedLogBlocksScan(storageConf.getBoolean(ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN.key(), false))
            .withDiskMapType(storageConf.getEnum(SPILLABLE_DISK_MAP_TYPE.key(), SPILLABLE_DISK_MAP_TYPE.defaultValue()))
            .withBitCaskDiskMapCompressionEnabled(storageConf.getBoolean(
                DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
            .withRecordMerger(HoodieRecordUtils.createRecordMerger(
                metaClient.getBasePath().toString(),
                engineType,
                Collections.emptyList(), // TODO: support different merger classes, which is currently only known to write config
                metaClient.getTableConfig().getRecordMergerStrategy()))
            .withTableMetaClient(metaClient)
            .build();
        ClosableIterator<String> recordKeyIterator = ClosableIterator.wrap(mergedLogRecordScanner.getRecords().keySet().iterator());
        return getHoodieRecordIterator(recordKeyIterator, forDelete, partition, fileSlice.getFileId(), fileSlice.getBaseInstantTime());
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
      return getHoodieRecordIterator(reader.getRecordKeyIterator(), forDelete, partition, fileId, instantTime);
    });
  }

  public static Schema getProjectedSchemaForFunctionalIndex(HoodieIndexDefinition indexDefinition, HoodieTableMetaClient metaClient) throws Exception {
    Schema tableSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    List<String> partitionFields = metaClient.getTableConfig().getPartitionFields()
        .map(Arrays::asList)
        .orElse(Collections.emptyList());
    List<String> sourceFields = indexDefinition.getSourceFields();
    List<String> mergedFields = new ArrayList<>(partitionFields.size() + sourceFields.size());
    mergedFields.addAll(partitionFields);
    mergedFields.addAll(sourceFields);
    return addMetadataFields(getSchemaForFields(tableSchema, mergedFields));
  }

  public static HoodieData<HoodieRecord> readSecondaryKeysFromBaseFiles(HoodieEngineContext engineContext,
                                                                        List<Pair<String, Pair<String, List<String>>>> partitionFiles,
                                                                        int secondaryIndexMaxParallelism,
                                                                        String activeModule, HoodieTableMetaClient metaClient, EngineType engineType,
                                                                        HoodieIndexDefinition indexDefinition) {
    if (partitionFiles.isEmpty()) {
      return engineContext.emptyHoodieData();
    }
    final int parallelism = Math.min(partitionFiles.size(), secondaryIndexMaxParallelism);
    final StoragePath basePath = metaClient.getBasePath();
    Schema tableSchema;
    try {
      tableSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    } catch (Exception e) {
      throw new HoodieException("Failed to get latest schema for " + metaClient.getBasePath(), e);
    }

    engineContext.setJobStatus(activeModule, "Secondary Index: reading secondary keys from " + partitionFiles.size() + " partitions");
    return engineContext.parallelize(partitionFiles, parallelism).flatMap(partitionAndBaseFile -> {
      final String partition = partitionAndBaseFile.getKey();
      final Pair<String, List<String>> baseAndLogFiles = partitionAndBaseFile.getValue();
      List<String> logFilePaths = new ArrayList<>();
      baseAndLogFiles.getValue().forEach(logFile -> logFilePaths.add(basePath + StoragePath.SEPARATOR + partition + StoragePath.SEPARATOR + logFile));
      String baseFilePath = baseAndLogFiles.getKey();
      Option<StoragePath> dataFilePath = baseFilePath.isEmpty() ? Option.empty() : Option.of(FSUtils.constructAbsolutePath(basePath, baseFilePath));
      Schema readerSchema;
      if (dataFilePath.isPresent()) {
        readerSchema = HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getFileFormatUtils(metaClient.getTableConfig().getBaseFileFormat())
            .readAvroSchema(metaClient.getStorage(), dataFilePath.get());
      } else {
        readerSchema = tableSchema;
      }
      return createSecondaryIndexGenerator(metaClient, engineType, logFilePaths, readerSchema, partition, dataFilePath, indexDefinition);
    });
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
      List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).map(l -> l.getPath().toString()).collect(toList());
      Option<StoragePath> dataFilePath = Option.ofNullable(fileSlice.getBaseFile().map(baseFile -> filePath(basePath, partition, baseFile.getFileName())).orElseGet(null));
      Schema readerSchema;
      if (dataFilePath.isPresent()) {
        readerSchema = HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getFileFormatUtils(metaClient.getTableConfig().getBaseFileFormat())
            .readAvroSchema(metaClient.getStorage(), dataFilePath.get());
      } else {
        readerSchema = tableSchema;
      }
      return createSecondaryIndexGenerator(metaClient, engineType, logFilePaths, readerSchema, partition, dataFilePath, indexDefinition);
    });
  }

  private static ClosableIterator<HoodieRecord> createSecondaryIndexGenerator(HoodieTableMetaClient metaClient,
                                                                              EngineType engineType, List<String> logFilePaths,
                                                                              Schema tableSchema, String partition,
                                                                              Option<StoragePath> dataFilePath,
                                                                              HoodieIndexDefinition indexDefinition) throws Exception {
    final String basePath = metaClient.getBasePath().toString();
    final StorageConfiguration<?> storageConf = metaClient.getStorageConf();

    HoodieRecordMerger recordMerger = HoodieRecordUtils.createRecordMerger(
        basePath,
        engineType,
        Collections.emptyList(),
        metaClient.getTableConfig().getRecordMergerStrategy());

    HoodieMergedLogRecordScanner mergedLogRecordScanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(metaClient.getStorage())
        .withBasePath(basePath)
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(tableSchema)
        .withLatestInstantTime(metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().map(HoodieInstant::getRequestTime).orElse(""))
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
        Option.empty());
    ClosableIterator<HoodieRecord> fileSliceIterator = ClosableIterator.wrap(fileSliceReader);
    return new ClosableIterator<HoodieRecord>() {
      @Override
      public void close() {
        fileSliceIterator.close();
      }

      @Override
      public boolean hasNext() {
        return fileSliceIterator.hasNext();
      }

      @Override
      public HoodieRecord next() {
        HoodieRecord record = fileSliceIterator.next();
        String recordKey = record.getRecordKey(tableSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD);
        String secondaryKeyFields = String.join(".", indexDefinition.getSourceFields());
        String secondaryKey;
        try {
          GenericRecord genericRecord = (GenericRecord) (record.toIndexedRecord(tableSchema, CollectionUtils.emptyProps()).get()).getData();
          secondaryKey = HoodieAvroUtils.getNestedFieldValAsString(genericRecord, secondaryKeyFields, true, false);
        } catch (IOException e) {
          throw new RuntimeException("Failed to fetch records." + e);
        }

        return HoodieMetadataPayload.createSecondaryIndex(recordKey, secondaryKey, indexDefinition.getIndexName(), false);
      }
    };
  }

  private static StoragePath filePath(StoragePath basePath, String partition, String filename) {
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
          String partitionPath) {

    // Step 1: Flatten and Group by Column Name
    Map<String, List<HoodieColumnRangeMetadata<Comparable>>> columnMetadataMap = fileColumnMetadata.stream()
            .flatMap(List::stream)
            .collect(Collectors.groupingBy(HoodieColumnRangeMetadata::getColumnName, Collectors.toList()));

    // Step 2: Aggregate Column Ranges
    Stream<HoodieColumnRangeMetadata<Comparable>> partitionStatsRangeMetadata = columnMetadataMap.entrySet().stream()
            .map(entry -> FileFormatUtils.getColumnRangeInPartition(partitionPath, entry.getValue()));

    // Create Partition Stats Records
    return HoodieMetadataPayload.createPartitionStatsRecords(partitionPath, partitionStatsRangeMetadata.collect(Collectors.toList()), false);
  }

  public static HoodieData<HoodieRecord> convertFilesToPartitionStatsRecords(HoodieEngineContext engineContext,
                                                                             List<DirectoryInfo> partitionInfoList,
                                                                             HoodieMetadataConfig metadataConfig,
                                                                             HoodieTableMetaClient dataTableMetaClient) {
    final List<String> columnsToIndex = getColumnsToIndex(
        metadataConfig.isPartitionStatsIndexEnabled(),
        metadataConfig.getColumnsEnabledForColumnStatsIndex(),
        Lazy.lazily(() -> tryResolveSchemaForTable(dataTableMetaClient)));
    if (columnsToIndex.isEmpty()) {
      LOG.warn("No columns to index for partition stats index");
      return engineContext.emptyHoodieData();
    }
    LOG.debug("Indexing following columns for partition stats index: {}", columnsToIndex);
    // Create records for MDT
    int parallelism = Math.max(Math.min(partitionInfoList.size(), metadataConfig.getPartitionStatsIndexParallelism()), 1);
    return engineContext.parallelize(partitionInfoList, parallelism).flatMap(partitionInfo -> {
      final String partitionPath = partitionInfo.getRelativePath();
      // Step 1: Collect Column Metadata for Each File
      List<List<HoodieColumnRangeMetadata<Comparable>>> fileColumnMetadata = partitionInfo.getFileNameToSizeMap().keySet().stream()
              .map(fileName -> getFileStatsRangeMetadata(partitionPath, partitionPath + "/" + fileName, dataTableMetaClient, columnsToIndex, false))
              .collect(Collectors.toList());

      return collectAndProcessColumnMetadata(fileColumnMetadata, partitionPath).iterator();
    });
  }

  private static List<HoodieColumnRangeMetadata<Comparable>> getFileStatsRangeMetadata(String partitionPath,
                                                                                       String filePath,
                                                                                       HoodieTableMetaClient datasetMetaClient,
                                                                                       List<String> columnsToIndex,
                                                                                       boolean isDeleted) {
    String filePartitionPath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
    String fileName = FSUtils.getFileName(filePath, partitionPath);
    if (isDeleted) {
      return columnsToIndex.stream()
          .map(entry -> HoodieColumnRangeMetadata.stub(fileName, entry))
          .collect(Collectors.toList());
    }
    return readColumnRangeMetadataFrom(filePartitionPath, datasetMetaClient, columnsToIndex);
  }

  public static HoodieData<HoodieRecord> convertMetadataToPartitionStatsRecords(HoodieCommitMetadata commitMetadata,
                                                                                HoodieEngineContext engineContext,
                                                                                HoodieTableMetaClient dataMetaClient,
                                                                                HoodieMetadataConfig metadataConfig) {
    List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).collect(Collectors.toList());
    if (allWriteStats.isEmpty()) {
      return engineContext.emptyHoodieData();
    }

    try {
      Option<Schema> writerSchema =
          Option.ofNullable(commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY))
              .flatMap(writerSchemaStr ->
                  isNullOrEmpty(writerSchemaStr)
                      ? Option.empty()
                      : Option.of(new Schema.Parser().parse(writerSchemaStr)));
      HoodieTableConfig tableConfig = dataMetaClient.getTableConfig();
      Option<Schema> tableSchema = writerSchema.map(schema -> tableConfig.populateMetaFields() ? addMetadataFields(schema) : schema);
      List<String> columnsToIndex = getColumnsToIndex(metadataConfig.isPartitionStatsIndexEnabled(), metadataConfig.getColumnsEnabledForColumnStatsIndex(), Lazy.eagerly(tableSchema));
      if (columnsToIndex.isEmpty()) {
        return engineContext.emptyHoodieData();
      }
      // Group by partitionPath and then gather write stats lists,
      // where each inner list contains HoodieWriteStat objects that have the same partitionPath.
      List<List<HoodieWriteStat>> partitionedWriteStats = allWriteStats.stream()
          .collect(Collectors.groupingBy(HoodieWriteStat::getPartitionPath))
          .values()
          .stream()
          .collect(Collectors.toList());

      int parallelism = Math.max(Math.min(partitionedWriteStats.size(), metadataConfig.getPartitionStatsIndexParallelism()), 1);
      return engineContext.parallelize(partitionedWriteStats, parallelism).flatMap(partitionedWriteStat -> {
        final String partitionName = partitionedWriteStat.get(0).getPartitionPath();
        // Step 1: Collect Column Metadata for Each File
        List<List<HoodieColumnRangeMetadata<Comparable>>> fileColumnMetadata = partitionedWriteStat.stream()
                .map(writeStat -> translateWriteStatToFileStats(writeStat, dataMetaClient, columnsToIndex))
                .collect(Collectors.toList());

        return collectAndProcessColumnMetadata(fileColumnMetadata, partitionName).iterator();
      });
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  private static List<HoodieColumnRangeMetadata<Comparable>> translateWriteStatToFileStats(HoodieWriteStat writeStat,
                                                                                           HoodieTableMetaClient datasetMetaClient,
                                                                                           List<String> columnsToIndex) {
    if (writeStat instanceof HoodieDeltaWriteStat && ((HoodieDeltaWriteStat) writeStat).getColumnStats().isPresent()) {
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMap = ((HoodieDeltaWriteStat) writeStat).getColumnStats().get();
      return columnRangeMap.values().stream().collect(Collectors.toList());
    }

    return getFileStatsRangeMetadata(writeStat.getPartitionPath(), writeStat.getPath(), datasetMetaClient, columnsToIndex, false);
  }

  public static String getPartitionStatsIndexKey(String partitionPath, String columnName) {
    final PartitionIndexID partitionIndexID = new PartitionIndexID(getColumnStatsIndexPartitionIdentifier(partitionPath));
    final ColumnIndexID columnIndexID = new ColumnIndexID(columnName);
    return columnIndexID.asBase64EncodedString().concat(partitionIndexID.asBase64EncodedString());
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

    return HoodieMetadataColumnStats.newBuilder(HoodieMetadataPayload.METADATA_COLUMN_STATS_BUILDER_STUB.get())
        .setFileName(newColumnStats.getFileName())
        .setColumnName(newColumnStats.getColumnName())
        .setMinValue(wrapValueIntoAvro(minValue))
        .setMaxValue(wrapValueIntoAvro(maxValue))
        .setValueCount(prevColumnStats.getValueCount() + newColumnStats.getValueCount())
        .setNullCount(prevColumnStats.getNullCount() + newColumnStats.getNullCount())
        .setTotalSize(prevColumnStats.getTotalSize() + newColumnStats.getTotalSize())
        .setTotalUncompressedSize(prevColumnStats.getTotalUncompressedSize() + newColumnStats.getTotalUncompressedSize())
        .setIsDeleted(newColumnStats.getIsDeleted())
        .build();
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
              // NOTE: We can’t assume that MT update records will be ordered the same way as actual
              //       FS operations (since they are not atomic), therefore MT record merging should be a
              //       _commutative_ & _associative_ operation (ie one that would work even in case records
              //       will get re-ordered), which is
              //          - Possible for file-sizes (since file-sizes will ever grow, we can simply
              //          take max of the old and new records)
              //          - Not possible for is-deleted flags*
              //
              //       *However, we’re assuming that the case of concurrent write and deletion of the same
              //       file is _impossible_ -- it would only be possible with concurrent upsert and
              //       rollback operation (affecting the same log-file), which is implausible, b/c either
              //       of the following have to be true:
              //          - We’re appending to failed log-file (then the other writer is trying to
              //          rollback it concurrently, before it’s own write)
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
      this.relativePath = relativePath;

      // Pre-allocate with the maximum length possible
      filenameToSizeMap = new HashMap<>(pathInfos.size());

      // Presence of partition meta file implies this is a HUDI partition
      isHoodiePartition = pathInfos.stream().anyMatch(status -> status.getPath().getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX));
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
}
