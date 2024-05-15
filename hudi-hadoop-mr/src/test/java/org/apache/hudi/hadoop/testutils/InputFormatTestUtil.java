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

package org.apache.hudi.hadoop.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.avro.AvroParquetWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;

public class InputFormatTestUtil {

  private static String TEST_WRITE_TOKEN = "1-0-1";

  public static File prepareTable(java.nio.file.Path basePath, HoodieFileFormat baseFileFormat, int numberOfFiles,
                                  String commitNumber) throws IOException {
    return prepareCustomizedTable(basePath, baseFileFormat, numberOfFiles, commitNumber, false, true, false, null);
  }

  public static File prepareCustomizedTable(java.nio.file.Path basePath, HoodieFileFormat baseFileFormat, int numberOfFiles,
                                            String commitNumber, boolean useNonPartitionedKeyGen, boolean populateMetaFields, boolean injectData, Schema schema)
      throws IOException {
    if (useNonPartitionedKeyGen) {
      HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), HoodieTableType.COPY_ON_WRITE,
          baseFileFormat, true, "org.apache.hudi.keygen.NonpartitionedKeyGenerator", populateMetaFields);
    } else {
      HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), HoodieTableType.COPY_ON_WRITE,
          baseFileFormat);
    }

    java.nio.file.Path partitionPath = useNonPartitionedKeyGen
        ? basePath
        : basePath.resolve(Paths.get("2016", "05", "01"));

    setupPartition(basePath, partitionPath);

    if (injectData) {
      try {
        createSimpleData(schema, partitionPath, numberOfFiles, 100, commitNumber);
        return partitionPath.toFile();
      } catch (Exception e) {
        throw new IOException("Exception thrown while writing data ", e);
      }
    } else {
      return simulateInserts(partitionPath.toFile(), baseFileFormat.getFileExtension(), "fileId1", numberOfFiles,
          commitNumber);
    }
  }

  public static File prepareMultiPartitionTable(java.nio.file.Path basePath, HoodieFileFormat baseFileFormat, int numberOfFiles,
                                                String commitNumber, String finalLevelPartitionName)
      throws IOException {
    HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), HoodieTableType.COPY_ON_WRITE,
        baseFileFormat);

    java.nio.file.Path partitionPath = basePath.resolve(Paths.get("2016", "05", finalLevelPartitionName));
    setupPartition(basePath, partitionPath);

    return simulateInserts(partitionPath.toFile(), baseFileFormat.getFileExtension(), "fileId1" + finalLevelPartitionName, numberOfFiles,
        commitNumber);
  }

  public static File simulateInserts(File partitionPath, String baseFileExtension, String fileId, int numberOfFiles,
                                     String commitNumber)
      throws IOException {
    for (int i = 0; i < numberOfFiles; i++) {
      Files.createFile(partitionPath.toPath()
          .resolve(FSUtils.makeBaseFileName(commitNumber, TEST_WRITE_TOKEN, fileId + i, baseFileExtension)));
    }
    return partitionPath;
  }

  public static void simulateUpdates(File directory, String baseFileExtension, final String originalCommit,
                                     int numberOfFilesUpdated, String newCommit, boolean randomize) throws IOException {
    List<File> dataFiles = Arrays.asList(Objects.requireNonNull(directory.listFiles((dir, name) -> {
      String commitTs = FSUtils.getCommitTime(name);
      return originalCommit.equals(commitTs);
    })));
    if (randomize) {
      Collections.shuffle(dataFiles);
    }
    List<File> toUpdateList = dataFiles.subList(0, Math.min(numberOfFilesUpdated, dataFiles.size()));
    for (File file : toUpdateList) {
      String fileId = FSUtils.getFileId(file.getName());
      Files.createFile(directory.toPath().resolve(FSUtils.makeBaseFileName(newCommit, TEST_WRITE_TOKEN, fileId,
          baseFileExtension)));
    }
  }

  public static void commit(java.nio.file.Path basePath, String commitNumber) throws IOException {
    // create the commit
    Files.createFile(basePath.resolve(Paths.get(".hoodie", commitNumber + ".commit")));
  }

  public static void deltaCommit(java.nio.file.Path basePath, String commitNumber) throws IOException {
    // create the commit
    Files.createFile(basePath.resolve(Paths.get(".hoodie", commitNumber + ".deltacommit")));
  }

  public static void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull) {
    setupIncremental(jobConf, startCommit, numberOfCommitsToPull, false);
  }

  public static void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull, boolean isIncrementalUseDatabase) {
    String modePropertyName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
        String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
        String.format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);

    jobConf.setBoolean(HoodieHiveUtils.HOODIE_INCREMENTAL_USE_DATABASE, isIncrementalUseDatabase);
  }

  public static void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull, String databaseName, boolean isIncrementalUseDatabase) {
    String modePropertyName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, databaseName + "." + HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
        String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN, databaseName + "." + HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
        String.format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, databaseName + "." + HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);

    jobConf.setBoolean(HoodieHiveUtils.HOODIE_INCREMENTAL_USE_DATABASE, isIncrementalUseDatabase);
  }

  public static void setupSnapshotIncludePendingCommits(JobConf jobConf, String instantTime) {
    setupSnapshotScanMode(jobConf, true);
    String validateTimestampName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_COMMIT, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(validateTimestampName, instantTime);
  }

  public static void setupSnapshotMaxCommitTimeQueryMode(JobConf jobConf, String maxInstantTime) {
    setUpScanMode(jobConf);
    String validateTimestampName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_COMMIT, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(validateTimestampName, maxInstantTime);
  }

  public static void setupSnapshotScanMode(JobConf jobConf) {
    setupSnapshotScanMode(jobConf, false);
  }

  private static void setupSnapshotScanMode(JobConf jobConf, boolean includePending) {
    setUpScanMode(jobConf);
    String includePendingCommitsName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_PENDING_COMMITS, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setBoolean(includePendingCommitsName, includePending);
  }

  private static void setUpScanMode(JobConf jobConf) {
    String modePropertyName =
        String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.SNAPSHOT_SCAN_MODE);
  }

  public static File prepareParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
                                         int numberOfRecords, String commitNumber) throws IOException {
    return prepareParquetTable(basePath, schema, numberOfFiles, numberOfRecords, commitNumber, HoodieTableType.COPY_ON_WRITE);
  }

  public static File prepareParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
                                         int numberOfRecords, String commitNumber, HoodieTableType tableType) throws IOException {
    HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), tableType, HoodieFileFormat.PARQUET);

    java.nio.file.Path partitionPath = basePath.resolve(Paths.get("2016", "05", "01"));
    setupPartition(basePath, partitionPath);

    createData(schema, partitionPath, numberOfFiles, numberOfRecords, commitNumber);

    return partitionPath.toFile();
  }

  public static File prepareSimpleParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
                                               int numberOfRecords, String commitNumber) throws Exception {
    return prepareSimpleParquetTable(basePath, schema, numberOfFiles, numberOfRecords, commitNumber, HoodieTableType.COPY_ON_WRITE);
  }

  public static File prepareSimpleParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
                                               int numberOfRecords, String commitNumber, HoodieTableType tableType) throws Exception {
    return prepareSimpleParquetTable(basePath, schema, numberOfFiles, numberOfRecords, commitNumber, tableType, "2016", "05", "01");
  }

  public static File prepareSimpleParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
                                               int numberOfRecords, String commitNumber, HoodieTableType tableType, String year, String month, String date) throws Exception {
    HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), tableType, HoodieFileFormat.PARQUET);

    java.nio.file.Path partitionPath = basePath.resolve(Paths.get(year, month, date));
    setupPartition(basePath, partitionPath);

    createSimpleData(schema, partitionPath, numberOfFiles, numberOfRecords, commitNumber);

    return partitionPath.toFile();
  }

  public static File prepareNonPartitionedParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
                                                       int numberOfRecords, String commitNumber) throws IOException {
    return prepareNonPartitionedParquetTable(basePath, schema, numberOfFiles, numberOfRecords, commitNumber, HoodieTableType.COPY_ON_WRITE);
  }

  public static File prepareNonPartitionedParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
                                                       int numberOfRecords, String commitNumber, HoodieTableType tableType) throws IOException {
    HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), tableType, HoodieFileFormat.PARQUET);
    createData(schema, basePath, numberOfFiles, numberOfRecords, commitNumber);
    return basePath.toFile();
  }

  public static List<File> prepareMultiPartitionedParquetTable(java.nio.file.Path basePath, Schema schema,
                                                               int numberPartitions, int numberOfRecordsPerPartition, String commitNumber, HoodieTableType tableType) throws IOException {
    List<File> result = new ArrayList<>();
    HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath.toString(), tableType, HoodieFileFormat.PARQUET);
    for (int i = 0; i < numberPartitions; i++) {
      java.nio.file.Path partitionPath = basePath.resolve(Paths.get(2016 + i + "", "05", "01"));
      setupPartition(basePath, partitionPath);

      createData(schema, partitionPath, 1, numberOfRecordsPerPartition, commitNumber);

      result.add(partitionPath.toFile());
    }
    return result;
  }

  private static void createData(Schema schema, java.nio.file.Path partitionPath, int numberOfFiles, int numberOfRecords,
                                 String commitNumber) throws IOException {
    AvroParquetWriter parquetWriter;
    for (int i = 0; i < numberOfFiles; i++) {
      String fileId = FSUtils.makeBaseFileName(commitNumber, TEST_WRITE_TOKEN, "fileid" + i, HoodieFileFormat.PARQUET.getFileExtension());
      parquetWriter = new AvroParquetWriter(new Path(partitionPath.resolve(fileId).toString()), schema);
      try {
        for (GenericRecord record : generateAvroRecords(schema, numberOfRecords, commitNumber, fileId)) {
          parquetWriter.write(record);
        }
      } finally {
        parquetWriter.close();
      }
    }
  }

  private static void createSimpleData(Schema schema, java.nio.file.Path partitionPath, int numberOfFiles, int numberOfRecords, String commitNumber) throws Exception {
    AvroParquetWriter parquetWriter;
    for (int i = 0; i < numberOfFiles; i++) {
      String fileId = FSUtils.makeBaseFileName(commitNumber, "1", "fileid" + i, HoodieFileFormat.PARQUET.getFileExtension());
      parquetWriter = new AvroParquetWriter(new Path(partitionPath.resolve(fileId).toString()), schema);
      try {
        List<IndexedRecord> records = SchemaTestUtil.generateTestRecords(0, numberOfRecords);
        Schema hoodieFieldsSchema = HoodieAvroUtils.addMetadataFields(schema);
        for (IndexedRecord record : records) {
          GenericRecord p = HoodieAvroUtils.rewriteRecord((GenericRecord) record, hoodieFieldsSchema);
          p.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, UUID.randomUUID().toString());
          p.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, "0000/00/00");
          p.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitNumber);
          parquetWriter.write(p);
        }
      } finally {
        parquetWriter.close();
      }
    }
  }

  private static Iterable<? extends GenericRecord> generateAvroRecords(Schema schema, int numberOfRecords,
                                                                       String instantTime, String fileId) throws IOException {
    List<GenericRecord> records = new ArrayList<>(numberOfRecords);
    for (int i = 0; i < numberOfRecords; i++) {
      records.add(SchemaTestUtil.generateAvroRecordFromJson(schema, i, instantTime, fileId));
    }
    return records;
  }

  public static void simulateParquetUpdates(File directory, Schema schema, String originalCommit,
                                            int totalNumberOfRecords, int numberOfRecordsToUpdate, String newCommit) throws IOException {
    File fileToUpdate = Objects.requireNonNull(directory.listFiles((dir, name) -> name.endsWith("parquet")))[0];
    String fileId = FSUtils.getFileId(fileToUpdate.getName());
    File dataFile = new File(directory,
        FSUtils.makeBaseFileName(newCommit, TEST_WRITE_TOKEN, fileId, HoodieFileFormat.PARQUET.getFileExtension()));
    try (AvroParquetWriter parquetWriter = new AvroParquetWriter(new Path(dataFile.getAbsolutePath()), schema)) {
      for (GenericRecord record : generateAvroRecords(schema, totalNumberOfRecords, originalCommit, fileId)) {
        if (numberOfRecordsToUpdate > 0) {
          // update this record
          record.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, newCommit);
          String oldSeqNo = (String) record.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD);
          record.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
              oldSeqNo.replace(originalCommit, newCommit));
          numberOfRecordsToUpdate--;
        }
        parquetWriter.write(record);
      }
    }
  }

  public static HoodieLogFormat.Writer writeRollback(File partitionDir, HoodieStorage storage,
                                                     String fileId,
                                                     String baseCommit,
                                                     String newCommit, String rolledBackInstant,
                                                     int logVersion)
      throws InterruptedException, IOException {
    HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(new StoragePath(partitionDir.getPath())).withFileId(fileId)
        .overBaseCommit(baseCommit).withStorage(storage).withLogVersion(logVersion).withRolloverLogWriteToken("1-0-1")
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
    // generate metadata
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, newCommit);
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, rolledBackInstant);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    // if update belongs to an existing log file
    writer.appendBlock(new HoodieCommandBlock(header));
    return writer;
  }

  public static HoodieLogFormat.Writer writeDataBlockToLogFile(File partitionDir,
                                                               HoodieStorage storage,
                                                               Schema schema, String fileId,
                                                               String baseCommit, String newCommit,
                                                               int numberOfRecords, int offset,
                                                               int logVersion)
      throws IOException, InterruptedException {
    return writeDataBlockToLogFile(partitionDir, storage, schema, fileId, baseCommit, newCommit,
        numberOfRecords, offset, logVersion, HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK);
  }

  public static HoodieLogFormat.Writer writeDataBlockToLogFile(File partitionDir,
                                                               HoodieStorage storage,
                                                               Schema schema, String fileId,
                                                               String baseCommit, String newCommit,
                                                               int numberOfRecords, int offset,
                                                               int logVersion,
                                                               HoodieLogBlock.HoodieLogBlockType logBlockType)
      throws InterruptedException, IOException {
    HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(new StoragePath(partitionDir.getPath()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(fileId).withLogVersion(logVersion)
        .withRolloverLogWriteToken("1-0-1").overBaseCommit(baseCommit).withStorage(storage).build();
    List<IndexedRecord> records = new ArrayList<>();
    for (int i = offset; i < offset + numberOfRecords; i++) {
      records.add(SchemaTestUtil.generateAvroRecordFromJson(schema, i, newCommit, "fileid0"));
    }
    Schema writeSchema = records.get(0).getSchema();
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, newCommit);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, writeSchema.toString());
    HoodieDataBlock dataBlock = null;
    List<HoodieRecord> hoodieRecords = records.stream().map(HoodieAvroIndexedRecord::new).collect(Collectors.toList());
    if (logBlockType == HoodieLogBlock.HoodieLogBlockType.HFILE_DATA_BLOCK) {
      dataBlock = new HoodieHFileDataBlock(
          hoodieRecords, header, HFILE_COMPRESSION_ALGORITHM_NAME.defaultValue(), writer.getLogFile().getPath(), HoodieReaderConfig.USE_NATIVE_HFILE_READER.defaultValue());
    } else if (logBlockType == HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK) {
      dataBlock = new HoodieParquetDataBlock(hoodieRecords, header, HoodieRecord.RECORD_KEY_METADATA_FIELD, PARQUET_COMPRESSION_CODEC_NAME.defaultValue(), 0.1, true);
    } else {
      dataBlock = new HoodieAvroDataBlock(hoodieRecords, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
    }
    writer.appendBlock(dataBlock);
    return writer;
  }

  public static HoodieLogFormat.Writer writeRollbackBlockToLogFile(File partitionDir,
                                                                   HoodieStorage storage,
                                                                   Schema schema,
                                                                   String fileId, String baseCommit,
                                                                   String newCommit,
                                                                   String oldCommit, int logVersion)
      throws InterruptedException, IOException {
    HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(new StoragePath(partitionDir.getPath()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(fileId).overBaseCommit(baseCommit)
        .withLogVersion(logVersion).withStorage(storage).build();

    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, newCommit);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, oldCommit);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    HoodieCommandBlock rollbackBlock = new HoodieCommandBlock(header);
    writer.appendBlock(rollbackBlock);
    return writer;
  }

  public static void setProjectFieldsForInputFormat(JobConf jobConf,
                                                    Schema schema, String hiveColumnTypes) {
    List<Schema.Field> fields = schema.getFields();
    String names = fields.stream().map(f -> f.name().toString()).collect(Collectors.joining(","));
    String positions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    Configuration conf = HoodieTestUtils.getDefaultStorageConf().unwrap();

    String hiveColumnNames = fields.stream().filter(field -> !field.name().equalsIgnoreCase("datestr"))
        .map(Schema.Field::name).collect(Collectors.joining(","));
    hiveColumnNames = hiveColumnNames + ",datestr";
    String modifiedHiveColumnTypes = HoodieAvroUtils.addMetadataColumnTypes(hiveColumnTypes);
    modifiedHiveColumnTypes = modifiedHiveColumnTypes + ",string";
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, modifiedHiveColumnTypes);
    // skip choose hoodie meta_columns, only choose one origin column to trigger HUDI-1722
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names.split(",")[5]);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions.split(",")[5]);
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    // skip choose hoodie meta_columns, only choose one origin column to trigger HUDI-1722
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names.split(",")[5]);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions.split(",")[5]);
    conf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, modifiedHiveColumnTypes);
    jobConf.addResource(conf);
  }

  public static void setPropsForInputFormat(JobConf jobConf,
                                            Schema schema, String hiveColumnTypes) {
    List<Schema.Field> fields = schema.getFields();
    String names = fields.stream().map(f -> f.name().toString()).collect(Collectors.joining(","));
    String positions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    Configuration conf = HoodieTestUtils.getDefaultStorageConf().unwrap();

    String hiveColumnNames = fields.stream().filter(field -> !field.name().equalsIgnoreCase("datestr"))
        .map(Schema.Field::name).collect(Collectors.joining(","));
    hiveColumnNames = hiveColumnNames + ",datestr";
    String modifiedHiveColumnTypes = HoodieAvroUtils.addMetadataColumnTypes(hiveColumnTypes);
    modifiedHiveColumnTypes = modifiedHiveColumnTypes + ",string";
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, modifiedHiveColumnTypes);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);
    conf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, modifiedHiveColumnTypes);
    jobConf.addResource(conf);
  }

  public static void setupPartition(java.nio.file.Path basePath, java.nio.file.Path partitionPath) throws IOException {
    Files.createDirectories(partitionPath);

    // Create partition metadata to properly setup table's partition
    try (RawLocalFileSystem lfs = new RawLocalFileSystem()) {
      lfs.setConf(HoodieTestUtils.getDefaultStorageConf().unwrap());

      HoodiePartitionMetadata partitionMetadata =
          new HoodiePartitionMetadata(
              new HoodieHadoopStorage(new LocalFileSystem(lfs)),
              "0",
              new StoragePath(basePath.toAbsolutePath().toString()),
              new StoragePath(partitionPath.toAbsolutePath().toString()),
              Option.of(HoodieFileFormat.PARQUET));

      partitionMetadata.trySave();
    }
  }

  public static void setInputPath(JobConf jobConf, String inputPath) {
    jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
    jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
    jobConf.set("map.input.dir", inputPath);
  }
}
