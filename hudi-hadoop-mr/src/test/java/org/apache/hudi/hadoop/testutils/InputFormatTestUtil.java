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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.hadoop.HoodieHiveUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class InputFormatTestUtil {

  private static String TEST_WRITE_TOKEN = "1-0-1";

  public static File prepareTable(java.nio.file.Path basePath, int numberOfFiles, String commitNumber)
      throws IOException {
    HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath.toString());
    java.nio.file.Path partitionPath = basePath.resolve(Paths.get("2016", "05", "01"));
    Files.createDirectories(partitionPath);
    return simulateInserts(partitionPath.toFile(), "fileId1", numberOfFiles, commitNumber);
  }

  public static File simulateInserts(File partitionPath, String fileId, int numberOfFiles, String commitNumber)
      throws IOException {
    for (int i = 0; i < numberOfFiles; i++) {
      Files.createFile(partitionPath.toPath()
          .resolve(FSUtils.makeDataFileName(commitNumber, TEST_WRITE_TOKEN, fileId + i)));
    }
    return partitionPath;
  }

  public static void simulateUpdates(File directory, final String originalCommit, int numberOfFilesUpdated,
      String newCommit, boolean randomize) throws IOException {
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
      Files.createFile(directory.toPath().resolve(FSUtils.makeDataFileName(newCommit, TEST_WRITE_TOKEN, fileId)));
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
    String modePropertyName =
        String.format(HoodieHiveUtil.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtil.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
        String.format(HoodieHiveUtil.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
        String.format(HoodieHiveUtil.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);
  }

  public static Schema readSchema(String location) throws IOException {
    return new Schema.Parser().parse(InputFormatTestUtil.class.getResourceAsStream(location));
  }

  public static File prepareParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
      int numberOfRecords, String commitNumber) throws IOException {
    HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath.toString());
    java.nio.file.Path partitionPath = basePath.resolve(Paths.get("2016", "05", "01"));
    createData(schema, partitionPath, numberOfFiles, numberOfRecords, commitNumber);
    return partitionPath.toFile();
  }

  public static File prepareSimpleParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
      int numberOfRecords, String commitNumber) throws Exception {
    HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath.toString());
    java.nio.file.Path partitionPath = basePath.resolve(Paths.get("2016", "05", "01"));
    createSimpleData(schema, partitionPath, numberOfFiles, numberOfRecords, commitNumber);
    return partitionPath.toFile();
  }

  public static File prepareNonPartitionedParquetTable(java.nio.file.Path basePath, Schema schema, int numberOfFiles,
      int numberOfRecords, String commitNumber) throws IOException {
    HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath.toString());
    createData(schema, basePath, numberOfFiles, numberOfRecords, commitNumber);
    return basePath.toFile();
  }

  private static void createData(Schema schema, java.nio.file.Path partitionPath, int numberOfFiles, int numberOfRecords,
      String commitNumber) throws IOException {
    AvroParquetWriter parquetWriter;
    for (int i = 0; i < numberOfFiles; i++) {
      String fileId = FSUtils.makeDataFileName(commitNumber, TEST_WRITE_TOKEN, "fileid" + i);
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

  private static void createSimpleData(Schema schema, java.nio.file.Path partitionPath, int numberOfFiles, int numberOfRecords,
      String commitNumber) throws Exception {
    AvroParquetWriter parquetWriter;
    for (int i = 0; i < numberOfFiles; i++) {
      String fileId = FSUtils.makeDataFileName(commitNumber, "1", "fileid" + i);
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
    File dataFile = new File(directory, FSUtils.makeDataFileName(newCommit, TEST_WRITE_TOKEN, fileId));
    try (AvroParquetWriter parquetWriter = new AvroParquetWriter(new Path(dataFile.getAbsolutePath()), schema)) {
      for (GenericRecord record : generateAvroRecords(schema, totalNumberOfRecords, originalCommit, fileId)) {
        if (numberOfRecordsToUpdate > 0) {
          // update this record
          record.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, newCommit);
          String oldSeqNo = (String) record.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD);
          record.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, oldSeqNo.replace(originalCommit, newCommit));
          numberOfRecordsToUpdate--;
        }
        parquetWriter.write(record);
      }
    }
  }

  public static HoodieLogFormat.Writer writeRollback(File partitionDir, FileSystem fs, String fileId, String baseCommit,
                                                     String newCommit, String rolledBackInstant, int logVersion)
      throws InterruptedException, IOException {
    HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(partitionDir.getPath())).withFileId(fileId)
        .overBaseCommit(baseCommit).withFs(fs).withLogVersion(logVersion).withLogWriteToken("1-0-1")
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
    // generate metadata
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, newCommit);
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, rolledBackInstant);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    // if update belongs to an existing log file
    writer = writer.appendBlock(new HoodieCommandBlock(header));
    return writer;
  }

  public static HoodieLogFormat.Writer writeDataBlockToLogFile(File partitionDir, FileSystem fs, Schema schema, String
      fileId,
                                                               String baseCommit, String newCommit, int numberOfRecords, int offset, int logVersion)
      throws InterruptedException, IOException {
    HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(partitionDir.getPath()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(fileId).withLogVersion(logVersion)
        .withLogWriteToken("1-0-1").overBaseCommit(baseCommit).withFs(fs).build();
    List<IndexedRecord> records = new ArrayList<>();
    for (int i = offset; i < offset + numberOfRecords; i++) {
      records.add(SchemaTestUtil.generateAvroRecordFromJson(schema, i, newCommit, "fileid0"));
    }
    Schema writeSchema = records.get(0).getSchema();
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, newCommit);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, writeSchema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
    writer = writer.appendBlock(dataBlock);
    return writer;
  }

  public static HoodieLogFormat.Writer writeRollbackBlockToLogFile(File partitionDir, FileSystem fs, Schema schema,
      String
          fileId, String baseCommit, String newCommit, String oldCommit, int logVersion)
      throws InterruptedException, IOException {
    HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(partitionDir.getPath()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(fileId).overBaseCommit(baseCommit)
        .withLogVersion(logVersion).withFs(fs).build();

    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, newCommit);
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, oldCommit);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    HoodieCommandBlock rollbackBlock = new HoodieCommandBlock(header);
    writer = writer.appendBlock(rollbackBlock);
    return writer;
  }

  public static void setPropsForInputFormat(JobConf jobConf,
      Schema schema, String hiveColumnTypes) {
    List<Schema.Field> fields = schema.getFields();
    String names = fields.stream().map(f -> f.name().toString()).collect(Collectors.joining(","));
    String postions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    Configuration conf = HoodieTestUtils.getDefaultHadoopConf();

    String hiveColumnNames = fields.stream().filter(field -> !field.name().equalsIgnoreCase("datestr"))
        .map(Schema.Field::name).collect(Collectors.joining(","));
    hiveColumnNames = hiveColumnNames + ",datestr";
    String modifiedHiveColumnTypes = HoodieAvroUtils.addMetadataColumnTypes(hiveColumnTypes);
    modifiedHiveColumnTypes = modifiedHiveColumnTypes + ",string";
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, modifiedHiveColumnTypes);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
    conf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "datestr");
    conf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, modifiedHiveColumnTypes);
    jobConf.addResource(conf);
  }

  public static void setInputPath(JobConf jobConf, String inputPath) {
    jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
    jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
    jobConf.set("map.input.dir", inputPath);
  }

}
