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

package org.apache.hudi.common.testutils.reader;

import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCDCDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.DELETE_BLOCK;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK;
import static org.apache.hudi.common.testutils.FileCreateUtilsLegacy.baseFileName;
import static org.apache.hudi.common.testutils.FileCreateUtilsLegacy.logFileName;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.DELETE;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.INSERT;

public class HoodieFileSliceTestUtils {
  public static final String FORWARD_SLASH = "/";
  public static final String PARQUET = ".parquet";

  public static final String DRIVER = "driver";
  public static final String PARTITION_PATH = "partition_path";
  public static final String RIDER = "rider";
  public static final String ROW_KEY = "_row_key";
  public static final int RECORD_KEY_INDEX = AVRO_SCHEMA.getField(ROW_KEY).pos();
  public static final String TIMESTAMP = "timestamp";
  public static final HoodieTestDataGenerator DATA_GEN =
      new HoodieTestDataGenerator(0xDEED);
  public static final TypedProperties PROPERTIES = new TypedProperties();
  private static String[] orderingFields = new String[] {"timestamp"};

  // We use a number to represent a record key, and a (start, end) range
  // to represent a set of record keys between start <= k <= end.
  public static class KeyRange {
    public int start;
    public int end;

    public KeyRange(int start, int end) {
      this.start = start;
      this.end = end;
    }
  }

  private static Path generateBaseFilePath(
      String basePath,
      String fileId,
      String instantTime
  ) {
    return new Path(
        basePath + FORWARD_SLASH
        + baseFileName(instantTime, fileId, PARQUET));
  }

  private static Path generateLogFilePath(
      String basePath,
      String fileId,
      String instantTime,
      int version) {
    return new Path(
        basePath + FORWARD_SLASH + logFileName(
        instantTime, fileId, version));
  }

  // Note:
  // "start < end" means start <= k <= end.
  // "start == end" means k = start.
  // "start > end" means no keys.
  private static List<String> generateKeys(KeyRange range) {
    List<String> keys = new ArrayList<>();
    if (range.start == range.end) {
      keys.add(String.valueOf(range.start));
    } else {
      keys = IntStream
          .rangeClosed(range.start, range.end)
          .boxed()
          .map(String::valueOf).collect(Collectors.toList());
    }
    return keys;
  }

  private static List<IndexedRecord> generateRecords(DataGenerationPlan plan) {
    List<IndexedRecord> records = new ArrayList<>();
    List<String> keys = plan.getRecordKeys();
    for (String key : keys) {
      records.add(DATA_GEN.generateGenericRecord(
          key,
          plan.getPartitionPath(),
          RIDER + "." + UUID.randomUUID(),
          DRIVER + "." + UUID.randomUUID(),
          plan.getTimestamp(),
          plan.getOperationType() == DELETE,
          false
      ));
    }
    return records;
  }

  private static HoodieDataBlock getDataBlock(
      HoodieLogBlock.HoodieLogBlockType dataBlockType,
      List<IndexedRecord> records,
      Map<HoodieLogBlock.HeaderMetadataType, String> header,
      StoragePath logFilePath,
      Map<String, Long> keyToPositionMap
  ) {
    return createDataBlock(
        dataBlockType,
        records.stream().map(r -> new HoodieAvroIndexedRecord(r, new HoodieRecordLocation("", "", keyToPositionMap.get(r.get(RECORD_KEY_INDEX)))))
            .collect(Collectors.toList()),
        header,
        logFilePath);
  }

  private static HoodieDataBlock createDataBlock(
      HoodieLogBlock.HoodieLogBlockType dataBlockType,
      List<HoodieRecord> records,
      Map<HoodieLogBlock.HeaderMetadataType, String> header,
      StoragePath pathForReader) {
    switch (dataBlockType) {
      case CDC_DATA_BLOCK:
        header.remove(BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS);
        return new HoodieCDCDataBlock(
            records,
            header,
            HoodieRecord.RECORD_KEY_METADATA_FIELD);
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(
            records,
            header,
            HoodieRecord.RECORD_KEY_METADATA_FIELD);
      case HFILE_DATA_BLOCK:
        header.remove(BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS);
        return new HoodieHFileDataBlock(
            records,
            header,
            HFILE_COMPRESSION_ALGORITHM_NAME.defaultValue(),
            pathForReader);
      case PARQUET_DATA_BLOCK:
        return new HoodieParquetDataBlock(
            records,
            header,
            HoodieRecord.RECORD_KEY_METADATA_FIELD,
            PARQUET_COMPRESSION_CODEC_NAME.defaultValue(),
            0.1,
            true);
      default:
        throw new RuntimeException(
            "Unknown data block type " + dataBlockType);
    }
  }

  public static HoodieDeleteBlock getDeleteBlock(
      List<IndexedRecord> records,
      Map<HoodieLogBlock.HeaderMetadataType, String> header,
      Schema schema,
      Properties props,
      Map<String, Long> keyToPositionMap
  ) {
    List<HoodieRecord> hoodieRecords = records.stream()
        .map(r -> {
          String rowKey = (String) r.get(r.getSchema().getField(ROW_KEY).pos());
          String partitionPath = (String) r.get(r.getSchema().getField(PARTITION_PATH).pos());
          return new HoodieAvroIndexedRecord(new HoodieKey(rowKey, partitionPath), r, null, new HoodieRecordLocation("", "",  keyToPositionMap.get(r.get(RECORD_KEY_INDEX))));
        })
        .collect(Collectors.toList());
    return new HoodieDeleteBlock(
        hoodieRecords.stream().map(
            r -> Pair.of(DeleteRecord.create(
                r.getKey(), r.getOrderingValue(HoodieSchema.fromAvroSchema(schema), props, orderingFields)), r.getCurrentLocation().getPosition()))
            .collect(Collectors.toList()),
        header);
  }

  public static HoodieBaseFile createBaseFile(
      String baseFilePath,
      List<IndexedRecord> records,
      Schema schema,
      String baseInstantTime
  ) throws IOException {
    HoodieStorage storage = HoodieTestUtils.getStorage(baseFilePath);

    // TODO: Optimize these hard-coded parameters for test purpose. (HUDI-7214)
    HoodieConfig cfg = new HoodieConfig();
    //enable bloom filter
    cfg.setValue(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true");
    cfg.setValue(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key(), "true");

    //set bloom filter values
    cfg.setValue(HoodieStorageConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE.key(), String.valueOf(1000));
    cfg.setValue(HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE.key(), String.valueOf(0.00001));
    cfg.setValue(HoodieStorageConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.key(), String.valueOf(10000));
    cfg.setValue(HoodieStorageConfig.BLOOM_FILTER_TYPE.key(), BloomFilterTypeCode.DYNAMIC_V0.name());

    //set parquet config values
    cfg.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), CompressionCodecName.GZIP.name());
    cfg.setValue(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(), String.valueOf(ParquetWriter.DEFAULT_BLOCK_SIZE));
    cfg.setValue(HoodieStorageConfig.PARQUET_PAGE_SIZE.key(), String.valueOf(ParquetWriter.DEFAULT_PAGE_SIZE));
    cfg.setValue(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key(), String.valueOf(1024 * 1024 * 1024));
    cfg.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION.key(), String.valueOf(0.1));
    cfg.setValue(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED.key(), "true");

    try (HoodieAvroFileWriter writer = (HoodieAvroFileWriter) HoodieFileWriterFactory
        .getFileWriter(baseInstantTime, new StoragePath(baseFilePath), storage, cfg,
                HoodieSchema.fromAvroSchema(schema), new LocalTaskContextSupplier(), HoodieRecord.HoodieRecordType.AVRO)) {
      for (IndexedRecord record : records) {
        writer.writeAvro(
            (String) record.get(schema.getField(ROW_KEY).pos()), record);
      }
    }
    return new HoodieBaseFile(baseFilePath);
  }

  public static HoodieLogFile createLogFile(
      HoodieStorage storage,
      String logFilePath,
      List<IndexedRecord> records,
      Schema schema,
      String fileId,
      String baseFileInstantTime,
      String logInstantTime,
      int version,
      HoodieLogBlock.HoodieLogBlockType blockType,
      boolean writePositions,
      Map<String, Long> keyToPositionMap
  ) throws InterruptedException, IOException {
    try (HoodieLogFormat.Writer writer =
             HoodieLogFormat.newWriterBuilder()
                 .onParentPath(new StoragePath(logFilePath).getParent())
                 .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
                 .withFileId(fileId)
                 .withInstantTime(logInstantTime)
                 .withLogVersion(version)
                 .withStorage(storage).build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, logInstantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      if (writePositions) {
        header.put(
            BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS,
            baseFileInstantTime);
      }

      if (blockType != DELETE_BLOCK) {
        HoodieDataBlock dataBlock = getDataBlock(
            blockType, records, header, new StoragePath(logFilePath), keyToPositionMap);
        writer.appendBlock(dataBlock);
      } else {
        HoodieDeleteBlock deleteBlock =
            getDeleteBlock(records, header, schema, PROPERTIES, keyToPositionMap);
        writer.appendBlock(deleteBlock);
      }
    }
    return new HoodieLogFile(logFilePath);
  }

  /**
   * Based on provided parameters to generate a {@link FileSlice} object.
   */
  public static FileSlice generateFileSlice(
      HoodieStorage storage,
      String basePath,
      String fileId,
      String partitionPath,
      Schema schema,
      List<DataGenerationPlan> plans
  ) throws IOException, InterruptedException {
    assert (!plans.isEmpty());

    HoodieBaseFile baseFile = null;
    List<HoodieLogFile> logFiles = new ArrayList<>();

    Map<String, Long> keyToPositionMap = new HashMap<>();
    // Generate a base file with records.
    DataGenerationPlan baseFilePlan = plans.get(0);
    if (!baseFilePlan.getRecordKeys().isEmpty()) {
      Path baseFilePath = generateBaseFilePath(
          basePath, fileId, baseFilePlan.getInstantTime());
      List<IndexedRecord> records = generateRecords(baseFilePlan);
      baseFile = createBaseFile(
          baseFilePath.toString(),
          records,
          schema,
          baseFilePlan.getInstantTime());
      for (int i = 0; i < baseFilePlan.getRecordKeys().size(); i++) {
        keyToPositionMap.put(baseFilePlan.getRecordKeys().get(i), (long) i);
      }
    }

    // Rest of plans are for log files.
    for (int i = 1; i < plans.size(); i++) {
      DataGenerationPlan logFilePlan = plans.get(i);
      if (logFilePlan.getRecordKeys().isEmpty()) {
        continue;
      }

      Path logFile = generateLogFilePath(
          basePath,fileId, logFilePlan.getInstantTime(), i);
      List<IndexedRecord> records = generateRecords(logFilePlan);
      HoodieLogBlock.HoodieLogBlockType blockType =
          logFilePlan.getOperationType() == DELETE ? DELETE_BLOCK : PARQUET_DATA_BLOCK;
      logFiles.add(createLogFile(
          storage,
          logFile.toString(),
          records,
          schema,
          fileId,
          baseFilePlan.getInstantTime(),
          logFilePlan.getInstantTime(),
          i,
          blockType,
          logFilePlan.isWritePositions(),
          keyToPositionMap));
    }

    // Assemble the FileSlice finally.
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partitionPath, fileId);
    String baseInstantTime = baseFile == null ? null : baseFile.getCommitTime();
    return new FileSlice(fileGroupId, baseInstantTime, baseFile, logFiles);
  }

  /**
   * Generate a {@link FileSlice} object which contains a {@link HoodieBaseFile} only.
   */
  public static Option<FileSlice> getBaseFileOnlyFileSlice(
      HoodieStorage storage,
      KeyRange range,
      long timestamp,
      String basePath,
      String partitionPath,
      String fileId,
      String baseInstantTime
  ) throws IOException, InterruptedException {
    List<String> keys = generateKeys(range);
    List<DataGenerationPlan> plans = new ArrayList<>();
    DataGenerationPlan baseFilePlan = DataGenerationPlan
        .newBuilder()
        .withRecordKeys(keys)
        .withOperationType(INSERT)
        .withPartitionPath(partitionPath)
        .withTimeStamp(timestamp)
        .withInstantTime(baseInstantTime)
        .withWritePositions(false)
        .build();
    plans.add(baseFilePlan);

    return Option.of(generateFileSlice(
        storage,
        basePath,
        fileId,
        partitionPath,
        AVRO_SCHEMA,
        plans));
  }

  /**
   * Generate a regular {@link FileSlice} containing both a base file and a number of log files.
   */
  public static Option<FileSlice> getFileSlice(
      HoodieStorage storage,
      List<KeyRange> ranges,
      List<Long> timestamps,
      List<DataGenerationPlan.OperationType> operationTypes,
      List<String> instantTimes,
      List<Boolean> shouldWritePositions,
      String basePath,
      String partitionPath,
      String fileId
  ) throws IOException, InterruptedException {
    List<DataGenerationPlan> plans = new ArrayList<>();
    for (int i = 0; i < ranges.size(); i++) {
      List<String> keys = generateKeys(ranges.get(i));
      plans.add(DataGenerationPlan.newBuilder()
          .withOperationType(operationTypes.get(i))
          .withPartitionPath(partitionPath)
          .withRecordKeys(keys)
          .withTimeStamp(timestamps.get(i))
          .withInstantTime(instantTimes.get(i))
          .withWritePositions(shouldWritePositions.get(i))
          .build());
    }

    return Option.of(generateFileSlice(
        storage,
        basePath,
        fileId,
        partitionPath,
        AVRO_SCHEMA,
        plans));
  }
}
