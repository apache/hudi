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
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.util.Lazy;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.avro.HoodieAvroUtils.convertValueForSpecificDataTypes;
import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldSchemaFromWriteSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.unwrapAvroValueWrapper;
import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.MILLIS_INSTANT_ID_LENGTH;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieTableMetadata.EMPTY_PARTITION_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;

/**
 * A utility to convert timeline information to metadata table records.
 */
public class HoodieTableMetadataUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableMetadataUtil.class);

  public static final String PARTITION_NAME_FILES = "files";
  public static final String PARTITION_NAME_COLUMN_STATS = "column_stats";
  public static final String PARTITION_NAME_BLOOM_FILTERS = "bloom_filters";
  public static final String PARTITION_NAME_RECORD_INDEX = "record_index";

  // Suffix to use for various operations on MDT
  private enum OperationSuffix {
    COMPACTION("001"),
    CLEAN("002"),
    RESTORE("003"),
    METADATA_INDEXER("004"),
    LOG_COMPACTION("005"),
    ROLLBACK("006");

    static final Set<String> ALL_SUFFIXES = Arrays.stream(OperationSuffix.values()).map(o -> o.getSuffix()).collect(Collectors.toSet());

    private final String suffix;

    OperationSuffix(String suffix) {
      this.suffix = suffix;
    }

    String getSuffix() {
      return suffix;
    }

    static boolean isValidSuffix(String suffix) {
      return ALL_SUFFIXES.contains(suffix);
    }
  }

  // This suffix and all after that are used for initialization of the various partitions. The unused suffixes lower than this value
  // are reserved for future operations on the MDT.
  private static final int PARTITION_INITIALIZATION_TIME_SUFFIX = 10; // corresponds to "010";

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
  public static Map<String, HoodieColumnRangeMetadata<Comparable>> collectColumnRangeMetadata(List<IndexedRecord> records,
                                                                                              List<Schema.Field> targetFields,
                                                                                              String filePath) {
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
        ColumnStats colStats = allColumnStats.computeIfAbsent(field.name(), (ignored) -> new ColumnStats());

        GenericRecord genericRecord = (GenericRecord) record;

        final Object fieldVal = convertValueForSpecificDataTypes(field.schema(), genericRecord.get(field.name()), false);
        final Schema fieldSchema = getNestedFieldSchemaFromWriteSchema(genericRecord.getSchema(), field.name());

        colStats.valueCount++;

        if (fieldVal != null && canCompare(fieldSchema)) {
          // Set the min value of the field
          if (colStats.minValue == null
              || ConvertingGenericData.INSTANCE.compare(fieldVal, colStats.minValue, fieldSchema) < 0) {
            colStats.minValue = fieldVal;
          }

          // Set the max value of the field
          if (colStats.maxValue == null || ConvertingGenericData.INSTANCE.compare(fieldVal, colStats.maxValue, fieldSchema) > 0) {
            colStats.maxValue = fieldVal;
          }
        } else {
          colStats.nullCount++;
        }
      });
    });

    Collector<HoodieColumnRangeMetadata<Comparable>, ?, Map<String, HoodieColumnRangeMetadata<Comparable>>> collector =
        Collectors.toMap(colRangeMetadata -> colRangeMetadata.getColumnName(), Function.identity());

    return (Map<String, HoodieColumnRangeMetadata<Comparable>>) targetFields.stream()
        .map(field -> {
          ColumnStats colStats = allColumnStats.get(field.name());
          return HoodieColumnRangeMetadata.<Comparable>create(
              filePath,
              field.name(),
              colStats == null ? null : coerceToComparable(field.schema(), colStats.minValue),
              colStats == null ? null : coerceToComparable(field.schema(), colStats.maxValue),
              colStats == null ? 0 : colStats.nullCount,
              colStats == null ? 0 : colStats.valueCount,
              // NOTE: Size and compressed size statistics are set to 0 to make sure we're not
              //       mixing up those provided by Parquet with the ones from other encodings,
              //       since those are not directly comparable
              0,
              0
          );
        })
        .collect(collector);
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

  /**
   * Delete the metadata table for the dataset. This will be invoked during upgrade/downgrade operation during which
   * no other
   * process should be running.
   *
   * @param basePath base path of the dataset
   * @param context  instance of {@link HoodieEngineContext}.
   */
  public static void deleteMetadataTable(String basePath, HoodieEngineContext context) {
    HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(context.getHadoopConf().get()).build();
    deleteMetadataTable(dataMetaClient, context, false);
  }

  /**
   * Deletes the metadata partition from the file system.
   *
   * @param basePath      - base path of the dataset
   * @param context       - instance of {@link HoodieEngineContext}
   * @param partitionType - {@link MetadataPartitionType} of the partition to delete
   */
  public static void deleteMetadataPartition(String basePath, HoodieEngineContext context, MetadataPartitionType partitionType) {
    HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(context.getHadoopConf().get()).build();
    deleteMetadataTablePartition(dataMetaClient, context, partitionType, false);
  }

  /**
   * Check if the given metadata partition exists.
   *
   * @param basePath base path of the dataset
   * @param context  instance of {@link HoodieEngineContext}.
   */
  public static boolean metadataPartitionExists(String basePath, HoodieEngineContext context, MetadataPartitionType partitionType) {
    final String metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    FileSystem fs = FSUtils.getFs(metadataTablePath, context.getHadoopConf().get());
    try {
      return fs.exists(new Path(metadataTablePath, partitionType.getPartitionPath()));
    } catch (Exception e) {
      throw new HoodieIOException(String.format("Failed to check metadata partition %s exists.", partitionType.getPartitionPath()));
    }
  }

  /**
   * Convert commit action to metadata records for the enabled partition types.
   *
   * @param commitMetadata          - Commit action metadata
   * @param instantTime             - Action instant time
   * @param recordsGenerationParams - Parameters for the record generation
   * @return Map of partition to metadata records for the commit action
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext context, HoodieCommitMetadata commitMetadata, String instantTime,
      MetadataRecordsGenerationParams recordsGenerationParams) {
    final Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = context.parallelize(
        convertMetadataToFilesPartitionRecords(commitMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES, filesPartitionRecordsRDD);

    if (recordsGenerationParams.getEnabledPartitionTypes().contains(MetadataPartitionType.BLOOM_FILTERS)) {
      final HoodieData<HoodieRecord> metadataBloomFilterRecords = convertMetadataToBloomFilterRecords(context, commitMetadata, instantTime, recordsGenerationParams);
      partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS, metadataBloomFilterRecords);
    }

    if (recordsGenerationParams.getEnabledPartitionTypes().contains(MetadataPartitionType.COLUMN_STATS)) {
      final HoodieData<HoodieRecord> metadataColumnStatsRDD = convertMetadataToColumnStatsRecords(commitMetadata, context, recordsGenerationParams);
      partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS, metadataColumnStatsRDD);
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

              String partition = getPartitionIdentifier(partitionStatName);

              HashMap<String, Long> updatedFilesToSizesMapping =
                  writeStats.stream().reduce(new HashMap<>(writeStats.size()),
                      (map, stat) -> {
                        String pathWithPartition = stat.getPath();
                        if (pathWithPartition == null) {
                          // Empty partition
                          LOG.warn("Unable to find path in write stat to update metadata table " + stat);
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
                          map.putAll(cdcPathAndSizes);
                        }
                        return map;
                      },
                      CollectionUtils::combine);

              newFileCount.add(updatedFilesToSizesMapping.size());
              return HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.of(updatedFilesToSizesMapping),
                  Option.empty());
            })
            .collect(Collectors.toList());

    records.addAll(updatedPartitionFilesRecords);

    LOG.info(String.format("Updating at %s from Commit/%s. #partitions_updated=%d, #files_added=%d", instantTime, commitMetadata.getOperationType(),
        records.size(), newFileCount.value()));

    return records;
  }

  private static List<String> getPartitionsAdded(HoodieCommitMetadata commitMetadata) {
    return commitMetadata.getPartitionToWriteStats().keySet().stream()
        // We need to make sure we properly handle case of non-partitioned tables
        .map(HoodieTableMetadataUtil::getPartitionIdentifier)
        .collect(Collectors.toList());
  }

  /**
   * Convert commit action metadata to bloom filter records.
   *
   * @param context                 - Engine context to use
   * @param commitMetadata          - Commit action metadata
   * @param instantTime             - Action instant time
   * @param recordsGenerationParams - Parameters for bloom filter record generation
   * @return HoodieData of metadata table records
   */
  public static HoodieData<HoodieRecord> convertMetadataToBloomFilterRecords(
      HoodieEngineContext context, HoodieCommitMetadata commitMetadata,
      String instantTime, MetadataRecordsGenerationParams recordsGenerationParams) {
    final List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(entry -> entry.stream()).collect(Collectors.toList());
    if (allWriteStats.isEmpty()) {
      return context.emptyHoodieData();
    }

    final int parallelism = Math.max(Math.min(allWriteStats.size(), recordsGenerationParams.getBloomIndexParallelism()), 1);
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
        LOG.error("Failed to find path in write stat to update metadata table " + hoodieWriteStat);
        return Collections.emptyListIterator();
      }

      String fileName = FSUtils.getFileName(pathWithPartition, partition);
      if (!FSUtils.isBaseFile(new Path(fileName))) {
        return Collections.emptyListIterator();
      }

      final Path writeFilePath = new Path(recordsGenerationParams.getDataMetaClient().getBasePath(), pathWithPartition);
      try (HoodieFileReader fileReader =
               HoodieFileReaderFactory.getReaderFactory(HoodieRecordType.AVRO).getFileReader(recordsGenerationParams.getDataMetaClient().getHadoopConf(), writeFilePath)) {
        try {
          final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
          if (fileBloomFilter == null) {
            LOG.error("Failed to read bloom filter for " + writeFilePath);
            return Collections.emptyListIterator();
          }
          ByteBuffer bloomByteBuffer = ByteBuffer.wrap(fileBloomFilter.serializeToString().getBytes());
          HoodieRecord record = HoodieMetadataPayload.createBloomFilterMetadataRecord(
              partition, fileName, instantTime, recordsGenerationParams.getBloomFilterType(), bloomByteBuffer, false);
          return Collections.singletonList(record).iterator();
        } catch (Exception e) {
          LOG.error("Failed to read bloom filter for " + writeFilePath);
          return Collections.emptyListIterator();
        } finally {
          fileReader.close();
        }
      } catch (IOException e) {
        LOG.error("Failed to get bloom filter for file: " + writeFilePath + ", write stat: " + hoodieWriteStat);
      }
      return Collections.emptyListIterator();
    });
  }

  /**
   * Convert the clean action to metadata records.
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(HoodieEngineContext engineContext,
                                                                                              HoodieCleanMetadata cleanMetadata,
                                                                                              MetadataRecordsGenerationParams recordsGenerationParams,
                                                                                              String instantTime) {
    final Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = engineContext.parallelize(
        convertMetadataToFilesPartitionRecords(cleanMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES, filesPartitionRecordsRDD);

    if (recordsGenerationParams.getEnabledPartitionTypes().contains(MetadataPartitionType.BLOOM_FILTERS)) {
      final HoodieData<HoodieRecord> metadataBloomFilterRecordsRDD =
          convertMetadataToBloomFilterRecords(cleanMetadata, engineContext, instantTime, recordsGenerationParams);
      partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS, metadataBloomFilterRecordsRDD);
    }

    if (recordsGenerationParams.getEnabledPartitionTypes().contains(MetadataPartitionType.COLUMN_STATS)) {
      final HoodieData<HoodieRecord> metadataColumnStatsRDD =
          convertMetadataToColumnStatsRecords(cleanMetadata, engineContext, recordsGenerationParams);
      partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS, metadataColumnStatsRDD);
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
      final String partition = getPartitionIdentifier(partitionName);
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(new ArrayList<>(deletedFiles)));
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
    LOG.info("Updating at " + instantTime + " from Clean. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0] + ", #partitions_deleted=" + deletedPartitions.size());
    return records;
  }

  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMissingPartitionRecords(HoodieEngineContext engineContext,
                                                                        List<String> deletedPartitions, Map<String, Map<String, Long>> filesAdded,
                                                                        Map<String, List<String>> filesDeleted, String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};
    int[] filesAddedCount = {0};

    filesAdded.forEach((k,v) -> {
      String partition = k;
      Map<String, Long> filestoAdd = v;
      filesAddedCount[0] += filestoAdd.size();
      Option<List<String>> filesToDelete = filesDeleted.containsKey(k) ? Option.of(filesDeleted.get(k)) : Option.empty();
      if (filesToDelete.isPresent()) {
        fileDeleteCount[0] += filesToDelete.get().size();
      }
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.of(filestoAdd), filesToDelete);
      records.add(record);
    });

    // there could be partitions which only has missing deleted files.
    filesDeleted.forEach((k,v) -> {
      String partition = k;
      Option<List<String>> filesToDelete = Option.of(v);
      if (!filesAdded.containsKey(partition)) {
        fileDeleteCount[0] += filesToDelete.get().size();
        HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(), filesToDelete);
        records.add(record);
      }
    });

    if (!deletedPartitions.isEmpty()) {
      // if there are partitions to be deleted, add them to delete list
      records.add(HoodieMetadataPayload.createPartitionListRecord(deletedPartitions, true));
    }

    LOG.info("Re-adding missing records at " + instantTime + " during Restore. #partitions_updated=" + records.size()
        + ", #files_added=" + filesAddedCount[0] + ", #files_deleted=" + fileDeleteCount[0] + ", #partitions_deleted=" + deletedPartitions.size());
    return Collections.singletonMap(MetadataPartitionType.FILES, engineContext.parallelize(records, 1));
  }

  /**
   * Convert clean metadata to bloom filter index records.
   *
   * @param cleanMetadata           - Clean action metadata
   * @param engineContext           - Engine context
   * @param instantTime             - Clean action instant time
   * @param recordsGenerationParams - Parameters for bloom filter record generation
   * @return List of bloom filter index records for the clean metadata
   */
  public static HoodieData<HoodieRecord> convertMetadataToBloomFilterRecords(HoodieCleanMetadata cleanMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             String instantTime,
                                                                             MetadataRecordsGenerationParams recordsGenerationParams) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> {
        final Path deletedFilePath = new Path(entry);
        if (FSUtils.isBaseFile(deletedFilePath)) {
          deleteFileList.add(Pair.of(partition, deletedFilePath.getName()));
        }
      });
    });

    final int parallelism = Math.max(Math.min(deleteFileList.size(), recordsGenerationParams.getBloomIndexParallelism()), 1);
    HoodieData<Pair<String, String>> deleteFileListRDD = engineContext.parallelize(deleteFileList, parallelism);
    return deleteFileListRDD.map(deleteFileInfoPair -> HoodieMetadataPayload.createBloomFilterMetadataRecord(
        deleteFileInfoPair.getLeft(), deleteFileInfoPair.getRight(), instantTime, StringUtils.EMPTY_STRING,
        ByteBuffer.allocate(0), true));
  }

  /**
   * Convert clean metadata to column stats index records.
   *
   * @param cleanMetadata           - Clean action metadata
   * @param engineContext           - Engine context
   * @param recordsGenerationParams - Parameters for bloom filter record generation
   * @return List of column stats index records for the clean metadata
   */
  public static HoodieData<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCleanMetadata cleanMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             MetadataRecordsGenerationParams recordsGenerationParams) {
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> deleteFileList.add(Pair.of(partition, entry)));
    });

    HoodieTableMetaClient dataTableMetaClient = recordsGenerationParams.getDataMetaClient();

    List<String> columnsToIndex =
        getColumnsToIndex(recordsGenerationParams,
            Lazy.lazily(() -> tryResolveSchemaForTable(dataTableMetaClient)));

    if (columnsToIndex.isEmpty()) {
      // In case there are no columns to index, bail
      return engineContext.emptyHoodieData();
    }

    int parallelism = Math.max(Math.min(deleteFileList.size(), recordsGenerationParams.getColumnStatsIndexParallelism()), 1);
    return engineContext.parallelize(deleteFileList, parallelism)
        .flatMap(deleteFileInfoPair -> {
          String partitionPath = deleteFileInfoPair.getLeft();
          String filePath = deleteFileInfoPair.getRight();

          if (filePath.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
            return getColumnStatsRecords(partitionPath, filePath, dataTableMetaClient, columnsToIndex, true).iterator();
          }
          return Collections.emptyListIterator();
        });
  }

  /**
   * Convert rollback action metadata to metadata table records.
   * <p>
   * We only need to handle FILES partition here as HUDI rollbacks on MOR table may end up adding a new log file. All other partitions
   * are handled by actual rollback of the deltacommit which added records to those partitions.
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext engineContext, HoodieTableMetaClient dataTableMetaClient, HoodieRollbackMetadata rollbackMetadata, String instantTime) {

    List<HoodieRecord> filesPartitionRecords = convertMetadataToRollbackRecords(rollbackMetadata, instantTime, dataTableMetaClient);
    final HoodieData<HoodieRecord> rollbackRecordsRDD = filesPartitionRecords.isEmpty() ? engineContext.emptyHoodieData()
        : engineContext.parallelize(filesPartitionRecords, filesPartitionRecords.size());

    return Collections.singletonMap(MetadataPartitionType.FILES, rollbackRecordsRDD);
  }

  private static void reAddLogFilesFromRollbackPlan(HoodieTableMetaClient dataTableMetaClient, String instantTime,
                                                    Map<String, Map<String, Long>> partitionToFilesMap) {
    HoodieInstant rollbackInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, instantTime);
    HoodieInstant requested = HoodieTimeline.getRollbackRequestedInstant(rollbackInstant);
    try {
      HoodieRollbackPlan rollbackPlan = TimelineMetadataUtils.deserializeAvroMetadata(
          dataTableMetaClient.getActiveTimeline().readRollbackInfoAsBytes(requested).get(), HoodieRollbackPlan.class);

      rollbackPlan.getRollbackRequests().forEach(rollbackRequest -> {
        final String partitionId = getPartitionIdentifier(rollbackRequest.getPartitionPath());
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
      throw new HoodieMetadataException("Parsing rollback plan for " + rollbackInstant.toString() + " failed ");
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
    return convertFilesToFilesPartitionRecords(Collections.EMPTY_MAP, partitionToAppendedFiles, instantTime, "Rollback");
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
      final String partitionId = getPartitionIdentifier(partition);

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
          String fileName = new Path(path).getName();
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
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileChangeCount = {0, 0}; // deletes, appends

    partitionToDeletedFiles.forEach((partitionName, deletedFiles) -> {
      fileChangeCount[0] += deletedFiles.size();
      final String partition = getPartitionIdentifier(partitionName);

      Option<Map<String, Long>> filesAdded = Option.empty();
      if (partitionToAppendedFiles.containsKey(partitionName)) {
        filesAdded = Option.of(partitionToAppendedFiles.remove(partitionName));
      }

      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, filesAdded,
          Option.of(new ArrayList<>(deletedFiles)));
      records.add(record);
    });

    partitionToAppendedFiles.forEach((partitionName, appendedFileMap) -> {
      final String partition = getPartitionIdentifier(partitionName);
      fileChangeCount[1] += appendedFileMap.size();

      // Validate that no appended file has been deleted
      checkState(
          !appendedFileMap.keySet().removeAll(partitionToDeletedFiles.getOrDefault(partition, Collections.emptyList())),
          "Rollback file cannot both be appended and deleted");

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.of(appendedFileMap),
          Option.empty());
      records.add(record);
    });

    LOG.info("Found at " + instantTime + " from " + operation + ". #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileChangeCount[0] + ", #files_appended=" + fileChangeCount[1]);

    return records;
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
                                                                          MetadataRecordsGenerationParams recordsGenerationParams,
                                                                          String instantTime) {
    HoodieData<HoodieRecord> allRecordsRDD = engineContext.emptyHoodieData();

    List<Pair<String, String>> partitionToDeletedFilesList = partitionToDeletedFiles.entrySet().stream().flatMap(entry -> {
      return entry.getValue().stream().map(file -> Pair.of(entry.getKey(), file));
    }).collect(Collectors.toList());

    int parallelism = Math.max(Math.min(partitionToDeletedFilesList.size(), recordsGenerationParams.getBloomIndexParallelism()), 1);
    HoodieData<Pair<String, String>> partitionToDeletedFilesRDD = engineContext.parallelize(partitionToDeletedFilesList, parallelism);

    HoodieData<HoodieRecord> deletedFilesRecordsRDD = partitionToDeletedFilesRDD.map(partitionToDeletedFilePair -> {
      String partitionName = partitionToDeletedFilePair.getLeft();
      String deletedFile = partitionToDeletedFilePair.getRight();
      if (!FSUtils.isBaseFile(new Path(deletedFile))) {
        return null;
      }
      final String partition = getPartitionIdentifier(partitionName);
      return (HoodieRecord) (HoodieMetadataPayload.createBloomFilterMetadataRecord(
          partition, deletedFile, instantTime, StringUtils.EMPTY_STRING, ByteBuffer.allocate(0), true));
    }).filter(Objects::nonNull);
    allRecordsRDD = allRecordsRDD.union(deletedFilesRecordsRDD);

    List<Pair<String, String>> partitionToAppendedFilesList = partitionToAppendedFiles.entrySet().stream().flatMap(entry -> {
      return entry.getValue().keySet().stream().map(file -> Pair.of(entry.getKey(), file));
    }).collect(Collectors.toList());

    parallelism = Math.max(Math.min(partitionToAppendedFilesList.size(), recordsGenerationParams.getBloomIndexParallelism()), 1);
    HoodieData<Pair<String, String>> partitionToAppendedFilesRDD = engineContext.parallelize(partitionToAppendedFilesList, parallelism);

    HoodieData<HoodieRecord> appendedFilesRecordsRDD = partitionToAppendedFilesRDD.map(partitionToAppendedFilesPair -> {
      String partitionName = partitionToAppendedFilesPair.getLeft();
      String appendedFile = partitionToAppendedFilesPair.getRight();
      String partition = getPartitionIdentifier(partitionName);
      if (!FSUtils.isBaseFile(new Path(appendedFile))) {
        return null;
      }
      final String pathWithPartition = partitionName + "/" + appendedFile;
      final Path appendedFilePath = new Path(recordsGenerationParams.getDataMetaClient().getBasePath(), pathWithPartition);
      try (HoodieFileReader fileReader =
               HoodieFileReaderFactory.getReaderFactory(HoodieRecordType.AVRO).getFileReader(recordsGenerationParams.getDataMetaClient().getHadoopConf(), appendedFilePath)) {
        final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
        if (fileBloomFilter == null) {
          LOG.error("Failed to read bloom filter for " + appendedFilePath);
          return null;
        }
        ByteBuffer bloomByteBuffer = ByteBuffer.wrap(fileBloomFilter.serializeToString().getBytes());
        return (HoodieRecord) (HoodieMetadataPayload.createBloomFilterMetadataRecord(
            partition, appendedFile, instantTime, recordsGenerationParams.getBloomFilterType(), bloomByteBuffer, false));
      } catch (IOException e) {
        LOG.error("Failed to get bloom filter for file: " + appendedFilePath);
      }
      return null;
    }).filter(Objects::nonNull);
    allRecordsRDD = allRecordsRDD.union(appendedFilesRecordsRDD);

    return allRecordsRDD;
  }

  /**
   * Convert added and deleted action metadata to column stats index records.
   */
  public static HoodieData<HoodieRecord> convertFilesToColumnStatsRecords(HoodieEngineContext engineContext,
                                                                          Map<String, List<String>> partitionToDeletedFiles,
                                                                          Map<String, Map<String, Long>> partitionToAppendedFiles,
                                                                          MetadataRecordsGenerationParams recordsGenerationParams) {
    HoodieData<HoodieRecord> allRecordsRDD = engineContext.emptyHoodieData();
    HoodieTableMetaClient dataTableMetaClient = recordsGenerationParams.getDataMetaClient();

    final List<String> columnsToIndex =
        getColumnsToIndex(recordsGenerationParams,
            Lazy.lazily(() -> tryResolveSchemaForTable(dataTableMetaClient)));

    if (columnsToIndex.isEmpty()) {
      // In case there are no columns to index, bail
      return engineContext.emptyHoodieData();
    }

    List<Pair<String, String>> partitionToDeletedFilesList = partitionToDeletedFiles.entrySet().stream().flatMap(entry -> {
      return entry.getValue().stream().map(file -> Pair.of(entry.getKey(), file));
    }).collect(Collectors.toList());

    int deletedFilesTargetParallelism = Math.max(Math.min(partitionToDeletedFilesList.size(), recordsGenerationParams.getColumnStatsIndexParallelism()), 1);
    final HoodieData<Pair<String, String>> partitionToDeletedFilesRDD =
        engineContext.parallelize(partitionToDeletedFilesList, deletedFilesTargetParallelism);

    HoodieData<HoodieRecord> deletedFilesRecordsRDD = partitionToDeletedFilesRDD.flatMap(partitionToDeletedFilesPair -> {
      String partitionPath = partitionToDeletedFilesPair.getLeft();
      String partitionId = getPartitionIdentifier(partitionPath);
      String deletedFile = partitionToDeletedFilesPair.getRight();
      String filePathWithPartition = partitionPath + "/" + deletedFile;
      return getColumnStatsRecords(partitionId, filePathWithPartition, dataTableMetaClient, columnsToIndex, true).iterator();
    });

    allRecordsRDD = allRecordsRDD.union(deletedFilesRecordsRDD);

    List<Pair<String, String>> partitionToAppendedFilesList = partitionToAppendedFiles.entrySet().stream().flatMap(entry -> {
      return entry.getValue().keySet().stream().map(file -> Pair.of(entry.getKey(), file));
    }).collect(Collectors.toList());

    int appendedFilesTargetParallelism = Math.max(Math.min(partitionToAppendedFilesList.size(), recordsGenerationParams.getColumnStatsIndexParallelism()), 1);
    final HoodieData<Pair<String, String>> partitionToAppendedFilesRDD =
        engineContext.parallelize(partitionToAppendedFilesList, appendedFilesTargetParallelism);

    HoodieData<HoodieRecord> appendedFilesRecordsRDD = partitionToAppendedFilesRDD.flatMap(partitionToAppendedFilesPair -> {
      String partitionPath = partitionToAppendedFilesPair.getLeft();
      String partitionId = getPartitionIdentifier(partitionPath);
      String appendedFile = partitionToAppendedFilesPair.getRight();
      if (!FSUtils.isBaseFile(new Path(appendedFile))
          || !appendedFile.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        return Stream.<HoodieRecord>empty().iterator();
      }
      final String filePathWithPartition = partitionPath + "/" + appendedFile;
      return getColumnStatsRecords(partitionId, filePathWithPartition, dataTableMetaClient, columnsToIndex, false).iterator();
    });

    allRecordsRDD = allRecordsRDD.union(appendedFilesRecordsRDD);

    return allRecordsRDD;
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
    LOG.info("Loading latest merged file slices for metadata table partition " + partition);
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
    LOG.info("Loading latest file slices for metadata table partition " + partition);
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
    if (timeline.empty()) {
      final HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION,
          HoodieActiveTimeline.createNewInstantTime());
      timeline = new HoodieDefaultTimeline(Stream.of(instant), metaClient.getActiveTimeline()::getInstantDetails);
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
    HoodieTableFileSystemView fsView = fileSystemView.orElse(getFileSystemView(metaClient));
    Stream<FileSlice> fileSliceStream;
    if (mergeFileSlices) {
      if (metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().isPresent()) {
        fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(
            partition, metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get().getTimestamp());
      } else {
        return Collections.EMPTY_LIST;
      }
    } else {
      fileSliceStream = fsView.getLatestFileSlices(partition);
    }
    return fileSliceStream.sorted(Comparator.comparing(FileSlice::getFileId)).collect(Collectors.toList());
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
    HoodieTableFileSystemView fsView = fileSystemView.orElse(getFileSystemView(metaClient));
    Stream<FileSlice> fileSliceStream = fsView.fetchLatestFileSlicesIncludingInflight(partition);
    return fileSliceStream
        .sorted(Comparator.comparing(FileSlice::getFileId))
        .collect(Collectors.toList());
  }

  public static HoodieData<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCommitMetadata commitMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             MetadataRecordsGenerationParams recordsGenerationParams) {
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

      HoodieTableMetaClient dataTableMetaClient = recordsGenerationParams.getDataMetaClient();
      HoodieTableConfig tableConfig = dataTableMetaClient.getTableConfig();

      // NOTE: Writer schema added to commit metadata will not contain Hudi's metadata fields
      Option<Schema> tableSchema = writerSchema.map(schema ->
          tableConfig.populateMetaFields() ? addMetadataFields(schema) : schema);

      List<String> columnsToIndex = getColumnsToIndex(recordsGenerationParams,
          Lazy.eagerly(tableSchema));

      if (columnsToIndex.isEmpty()) {
        // In case there are no columns to index, bail
        return engineContext.emptyHoodieData();
      }

      int parallelism = Math.max(Math.min(allWriteStats.size(), recordsGenerationParams.getColumnStatsIndexParallelism()), 1);
      return engineContext.parallelize(allWriteStats, parallelism)
          .flatMap(writeStat ->
              translateWriteStatToColumnStats(writeStat, dataTableMetaClient, columnsToIndex).iterator());
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  /**
   * Get the list of columns for the table for column stats indexing
   */
  private static List<String> getColumnsToIndex(MetadataRecordsGenerationParams recordsGenParams,
                                                Lazy<Option<Schema>> lazyWriterSchemaOpt) {
    checkState(recordsGenParams.isColumnStatsIndexEnabled());

    List<String> targetColumns = recordsGenParams.getTargetColumnsForColumnStatsIndex();
    if (!targetColumns.isEmpty()) {
      return targetColumns;
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
        Path fullFilePath = new Path(datasetMetaClient.getBasePath(), filePath);
        List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList =
            new ParquetUtils().readRangeFromParquetMetadata(datasetMetaClient.getHadoopConf(), fullFilePath, columnsToIndex);

        return columnRangeMetadataList;
      }

      LOG.warn("Column range index not supported for: " + filePath);
      return Collections.emptyList();
    } catch (Exception e) {
      // NOTE: In case reading column range metadata from individual file failed,
      //       we simply fall back, in lieu of failing the whole task
      LOG.error("Failed to fetch column range metadata for: " + filePath);
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
   * it could subsequently used in column stats
   *
   * NOTE: This method has to stay compatible with the semantic of
   *      {@link ParquetUtils#readRangeFromParquetMetadata} as they are used in tandem
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

  private static boolean canCompare(Schema schema) {
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
    Set<String> validInstantTimestamps = datasetTimeline.filterCompletedInstants().getInstantsAsStream()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

    // We should also add completed indexing delta commits in the metadata table, as they do not
    // have corresponding completed instant in the data table
    validInstantTimestamps.addAll(
        metadataMetaClient.getActiveTimeline()
            .filter(instant -> instant.isCompleted() && isValidInstant(instant))
            .getInstantsAsStream()
            .map(HoodieInstant::getTimestamp)
            .collect(Collectors.toList()));

    // For any rollbacks and restores, we cannot neglect the instants that they are rolling back.
    // The rollback instant should be more recent than the start of the timeline for it to have rolled back any
    // instant which we have a log block for.
    final String earliestInstantTime = validInstantTimestamps.isEmpty() ? SOLO_COMMIT_TIMESTAMP : Collections.min(validInstantTimestamps);
    datasetTimeline.getRollbackAndRestoreTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.GREATER_THAN, earliestInstantTime))
        .forEach(instant -> {
          validInstantTimestamps.addAll(getRollbackedCommits(instant, datasetTimeline));
        });

    // add restore instants from MDT.
    metadataMetaClient.getActiveTimeline().getRollbackAndRestoreTimeline().filterCompletedInstants()
        .filter(instant -> instant.getAction().equals(HoodieTimeline.RESTORE_ACTION))
        .getInstants().forEach(instant -> validInstantTimestamps.add(instant.getTimestamp()));

    // SOLO_COMMIT_TIMESTAMP is used during bootstrap so it is a valid timestamp
    validInstantTimestamps.add(createIndexInitTimestamp(SOLO_COMMIT_TIMESTAMP, PARTITION_INITIALIZATION_TIME_SUFFIX));
    return validInstantTimestamps;
  }

  /**
   * Checks if the Instant is a delta commit and has a valid suffix for operations on MDT.
   *
   * @param instant {@code HoodieInstant} to check.
   * @return {@code true} if the instant is valid.
   */
  public static boolean isValidInstant(HoodieInstant instant) {
    // Should be a deltacommit
    if (!instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
      return false;
    }

    // Check correct length. The timestamp should have a suffix over the timeline's timestamp format.
    final String instantTime = instant.getTimestamp();
    if (!(instantTime.length() == MILLIS_INSTANT_ID_LENGTH + OperationSuffix.METADATA_INDEXER.getSuffix().length())) {
      return false;
    }

    // Is this a fixed operations suffix
    final String suffix = instantTime.substring(instantTime.length() - 3);
    if (OperationSuffix.isValidSuffix(suffix)) {
      return true;
    }

    // Is this a index init suffix?
    if (suffix.compareTo(String.format("%03d", PARTITION_INITIALIZATION_TIME_SUFFIX)) >= 0) {
      return true;
    }

    return false;
  }

  /**
   * Checks if a delta commit in metadata table is written by async indexer.
   * <p>
   * TODO(HUDI-5733): This should be cleaned up once the proper fix of rollbacks in the
   *  metadata table is landed.
   *
   * @param instantTime Instant time to check.
   * @return {@code true} if from async indexer; {@code false} otherwise.
   */
  public static boolean isIndexingCommit(String instantTime) {
    return instantTime.length() == MILLIS_INSTANT_ID_LENGTH + OperationSuffix.METADATA_INDEXER.getSuffix().length()
            && instantTime.endsWith(OperationSuffix.METADATA_INDEXER.getSuffix());
  }

  /**
   * Returns a list of commits which were rolled back as part of a Rollback or Restore operation.
   *
   * @param instant  The Rollback operation to read
   * @param timeline instant of timeline from dataset.
   */
  private static List<String> getRollbackedCommits(HoodieInstant instant, HoodieActiveTimeline timeline) {
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
              timeline.readRollbackInfoAsBytes(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION,
                  instant.getTimestamp())).get(), HoodieRollbackPlan.class);
          commitsToRollback = Collections.singletonList(rollbackPlan.getInstantToRollback().getCommitTime());
          LOG.warn("Had to fetch rollback info from requested instant since completed file is empty " + instant.toString());
        }
        return commitsToRollback;
      }

      List<String> rollbackedCommits = new LinkedList<>();
      if (instant.getAction().equals(HoodieTimeline.RESTORE_ACTION)) {
        // Restore is made up of several rollbacks
        HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
            timeline.getInstantDetails(instant).get());
        restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
          rms.forEach(rm -> rollbackedCommits.addAll(rm.getCommitsRollback()));
        });
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
    final Path metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(dataMetaClient.getBasePathV2());
    FileSystem fs = FSUtils.getFs(metadataTablePath.toString(), context.getHadoopConf().get());
    dataMetaClient.getTableConfig().clearMetadataPartitions(dataMetaClient);
    try {
      if (!fs.exists(metadataTablePath)) {
        return null;
      }
    } catch (FileNotFoundException e) {
      // Ignoring exception as metadata table already does not exist
      return null;
    } catch (IOException e) {
      throw new HoodieMetadataException("Failed to check metadata table existence", e);
    }

    if (backup) {
      final Path metadataBackupPath = new Path(metadataTablePath.getParent(), ".metadata_" + HoodieActiveTimeline.createNewInstantTime());
      LOG.info("Backing up metadata directory to " + metadataBackupPath + " before deletion");
      try {
        if (fs.rename(metadataTablePath, metadataBackupPath)) {
          return metadataBackupPath.toString();
        }
      } catch (Exception e) {
        // If rename fails, we will ignore the backup and still delete the MDT
        LOG.error("Failed to backup metadata table using rename", e);
      }
    }

    LOG.info("Deleting metadata table from " + metadataTablePath);
    try {
      fs.delete(metadataTablePath, true);
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
   * @param partitionType  The partition to delete
   * @return The backup directory if backup was requested, null otherwise
   */
  public static String deleteMetadataTablePartition(HoodieTableMetaClient dataMetaClient, HoodieEngineContext context,
                                                    MetadataPartitionType partitionType, boolean backup) {
    if (partitionType.equals(MetadataPartitionType.FILES)) {
      return deleteMetadataTable(dataMetaClient, context, backup);
    }

    final Path metadataTablePartitionPath = new Path(HoodieTableMetadata.getMetadataTableBasePath(dataMetaClient.getBasePath()), partitionType.getPartitionPath());
    FileSystem fs = FSUtils.getFs(metadataTablePartitionPath.toString(), context.getHadoopConf().get());
    dataMetaClient.getTableConfig().setMetadataPartitionState(dataMetaClient, partitionType, false);
    try {
      if (!fs.exists(metadataTablePartitionPath)) {
        return null;
      }
    } catch (FileNotFoundException e) {
      // Ignoring exception as metadata table already does not exist
      LOG.debug("Metadata table partition " + partitionType + " not found at path " + metadataTablePartitionPath);
      return null;
    } catch (Exception e) {
      throw new HoodieMetadataException(String.format("Failed to check existence of MDT partition %s at path %s: ", partitionType, metadataTablePartitionPath), e);
    }

    if (backup) {
      final Path metadataPartitionBackupPath = new Path(metadataTablePartitionPath.getParent().getParent(),
              String.format(".metadata_%s_%s", partitionType.getPartitionPath(), HoodieActiveTimeline.createNewInstantTime()));
      LOG.info(String.format("Backing up MDT partition %s to %s before deletion", partitionType, metadataPartitionBackupPath));
      try {
        if (fs.rename(metadataTablePartitionPath, metadataPartitionBackupPath)) {
          return metadataPartitionBackupPath.toString();
        }
      } catch (Exception e) {
        // If rename fails, we will try to delete the table instead
        LOG.error(String.format("Failed to backup MDT partition %s using rename", partitionType), e);
      }
    } else {
      LOG.info("Deleting metadata table partition from " + metadataTablePartitionPath);
      try {
        fs.delete(metadataTablePartitionPath, true);
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
   * Create the timestamp for a clean operation on the metadata table.
   */
  public static String createCleanTimestamp(String timestamp) {
    return timestamp + OperationSuffix.CLEAN.getSuffix();
  }

  public static String createRollbackTimestamp(String timestamp) {
    return timestamp + OperationSuffix.ROLLBACK.getSuffix();
  }

  public static String createRestoreTimestamp(String timestamp) {
    return timestamp + OperationSuffix.RESTORE.getSuffix();
  }

  public static String createAsyncIndexerTimestamp(String timestamp) {
    return timestamp + OperationSuffix.METADATA_INDEXER.getSuffix();
  }

  /**
   * Create the timestamp for a compaction operation on the metadata table.
   */
  public static String createCompactionTimestamp(String timestamp) {
    return timestamp + OperationSuffix.COMPACTION.getSuffix();
  }

  /**
   * Create the timestamp for an index initialization operation on the metadata table.
   * <p>
   * Since many MDT partitions can be initialized one after other the offset parameter controls generating a
   * unique timestamp.
   */
  public static String createIndexInitTimestamp(String timestamp, int offset) {
    return String.format("%s%03d", timestamp, PARTITION_INITIALIZATION_TIME_SUFFIX + offset);
  }

  /**
   * Create the timestamp for a compaction operation on the metadata table.
   */
  public static String createLogCompactionTimestamp(String timestamp) {
    return timestamp + OperationSuffix.LOG_COMPACTION.getSuffix();
  }

  /**
   * Estimates the file group count to use for a MDT partition.
   *
   * @param partitionType         Type of the partition for which the file group count is to be estimated.
   * @param recordCount           The number of records expected to be written.
   * @param averageRecordSize     Average size of each record to be writen.
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

    LOG.info(String.format("Estimated file group count for MDT partition %s is %d "
            + "[recordCount=%d, avgRecordSize=%d, minFileGroupCount=%d, maxFileGroupCount=%d, growthFactor=%f, "
            + "maxFileGroupSizeBytes=%d]", partitionType.name(), fileGroupCount, recordCount, averageRecordSize, minFileGroupCount,
        maxFileGroupCount, growthFactor, maxFileGroupSizeBytes));
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
    if (config.enableRecordIndex()) {
      return true;
    }
    return false;
  }
}
