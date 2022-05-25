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

package org.apache.hudi.hive.testutils;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.minicluster.ZookeeperTestService;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.ddl.HiveQueryDDLExecutor;
import org.apache.hudi.hive.ddl.QueryBasedDDLExecutor;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.server.HiveServer2;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.platform.commons.JUnitException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("SameParameterValue")
public class HiveTestUtil {

  public static final String DB_NAME = "testdb";
  public static final String TABLE_NAME = "test1";
  public static String basePath;
  public static TypedProperties hiveSyncProps;
  public static HiveTestService hiveTestService;
  public static FileSystem fileSystem;
  public static QueryBasedDDLExecutor ddlExecutor;

  private static ZooKeeperServer zkServer;
  private static HiveServer2 hiveServer;
  private static ZookeeperTestService zkService;
  private static Configuration configuration;
  private static HiveSyncConfig hiveSyncConfig;
  private static DateTimeFormatter dtfOut;
  private static Set<String> createdTablesSet = new HashSet<>();

  public static void setUp() throws IOException, InterruptedException, HiveException, MetaException {
    configuration = new Configuration();
    if (zkServer == null) {
      zkService = new ZookeeperTestService(configuration);
      zkServer = zkService.start();
    }
    if (hiveServer == null) {
      hiveTestService = new HiveTestService(configuration);
      hiveServer = hiveTestService.start();
    }
    fileSystem = FileSystem.get(configuration);

    basePath = Files.createTempDirectory("hivesynctest" + Instant.now().toEpochMilli()).toUri().toString();

    hiveSyncProps = new TypedProperties();
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_URL.key(), hiveTestService.getJdbcHive2Url());
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_USER.key(), "");
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_PASS.key(), "");
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_DATABASE_NAME.key(), DB_NAME);
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_TABLE_NAME.key(), TABLE_NAME);
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_BASE_PATH.key(), basePath);
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_ASSUME_DATE_PARTITION.key(), "true");
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_USE_PRE_APACHE_INPUT_FORMAT.key(), "false");
    hiveSyncProps.setProperty(HiveSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "datestr");
    hiveSyncProps.setProperty(HiveSyncConfig.HIVE_BATCH_SYNC_PARTITION_NUM.key(), "3");

    hiveSyncConfig = new HiveSyncConfig(hiveSyncProps);

    dtfOut = DateTimeFormatter.ofPattern("yyyy/MM/dd");
    ddlExecutor = new HiveQueryDDLExecutor(hiveSyncConfig, fileSystem, getHiveConf());

    clear();
  }

  public static void clearIncrementalPullSetup(String path1, String path2) throws IOException, HiveException, MetaException {
    fileSystem.delete(new Path(path1), true);
    if (path2 != null) {
      fileSystem.delete(new Path(path2), true);
    }
    clear();
  }

  public static void clear() throws IOException, HiveException, MetaException {
    fileSystem.delete(new Path(basePath), true);
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(configuration, basePath);

    for (String tableName : createdTablesSet) {
      ddlExecutor.runSQL("drop table if exists " + tableName);
    }
    createdTablesSet.clear();
    ddlExecutor.runSQL("drop database if exists " + DB_NAME + " cascade");
  }

  public static HiveConf getHiveConf() {
    return hiveServer.getHiveConf();
  }

  public static void shutdown() {
    if (hiveServer != null) {
      hiveServer.stop();
    }
    if (hiveTestService != null) {
      hiveTestService.stop();
    }
    if (zkServer != null) {
      zkServer.shutdown();
    }
  }

  public static void createCOWTable(String instantTime, int numberOfPartitions, boolean useSchemaFromCommitMetadata,
                                    String basePath, String databaseName, String tableName) throws IOException, URISyntaxException {
    Path path = new Path(basePath);
    FileIOUtils.deleteDirectory(new File(basePath));
    HoodieTableMetaClient.withPropertyBuilder()
            .setTableType(HoodieTableType.COPY_ON_WRITE)
            .setTableName(tableName)
            .setPayloadClass(HoodieAvroPayload.class)
            .initTable(configuration, basePath);

    boolean result = fileSystem.mkdirs(path);
    checkResult(result);
    ZonedDateTime dateTime = ZonedDateTime.now();
    HoodieCommitMetadata commitMetadata = createPartitions(numberOfPartitions, true,
            useSchemaFromCommitMetadata, dateTime, instantTime, basePath);
    createdTablesSet.add(databaseName + "." + tableName);
    createCommitFile(commitMetadata, instantTime, basePath);
  }

  public static void createCOWTable(String instantTime, int numberOfPartitions, boolean useSchemaFromCommitMetadata)
      throws IOException, URISyntaxException {
    createCOWTable(instantTime, numberOfPartitions, useSchemaFromCommitMetadata, basePath, DB_NAME, TABLE_NAME);
  }

  public static void createReplaceCommit(String instantTime, String partitions, WriteOperationType type, boolean isParquetSchemaSimple, boolean useSchemaFromCommitMetadata)
      throws IOException {
    HoodieReplaceCommitMetadata replaceCommitMetadata = new HoodieReplaceCommitMetadata();
    addSchemaToCommitMetadata(replaceCommitMetadata, isParquetSchemaSimple, useSchemaFromCommitMetadata);
    replaceCommitMetadata.setOperationType(type);
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    partitionToReplaceFileIds.put(partitions, new ArrayList<>());
    replaceCommitMetadata.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
    createReplaceCommitFile(replaceCommitMetadata, instantTime);
  }

  public static void createCOWTableWithSchema(String instantTime, String schemaFileName)
      throws IOException, URISyntaxException {
    Path path = new Path(basePath);
    FileIOUtils.deleteDirectory(new File(basePath));
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(configuration, basePath);

    boolean result = fileSystem.mkdirs(path);
    checkResult(result);
    ZonedDateTime dateTime = ZonedDateTime.now().truncatedTo(ChronoUnit.DAYS);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    String partitionPath = dateTime.format(dtfOut);
    Path partPath = new Path(basePath + "/" + partitionPath);
    fileSystem.makeQualified(partPath);
    fileSystem.mkdirs(partPath);
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    String fileId = UUID.randomUUID().toString();
    Path filePath = new Path(partPath.toString() + "/" + FSUtils.makeBaseFileName(instantTime, "1-0-1", fileId));
    Schema schema = SchemaTestUtil.getSchemaFromResource(HiveTestUtil.class, schemaFileName);
    generateParquetDataWithSchema(filePath, schema);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setPath(filePath.toString());
    writeStats.add(writeStat);
    writeStats.forEach(s -> commitMetadata.addWriteStat(partitionPath, s));
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schema.toString());
    createdTablesSet.add(DB_NAME + "." + TABLE_NAME);
    createCommitFile(commitMetadata, instantTime, basePath);
  }

  public static void createMORTable(String commitTime, String deltaCommitTime, int numberOfPartitions,
                                    boolean createDeltaCommit, boolean useSchemaFromCommitMetadata)
      throws IOException, URISyntaxException, InterruptedException {
    Path path = new Path(basePath);
    FileIOUtils.deleteDirectory(new File(basePath));
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(configuration, basePath);

    boolean result = fileSystem.mkdirs(path);
    checkResult(result);
    ZonedDateTime dateTime = ZonedDateTime.now();
    HoodieCommitMetadata commitMetadata = createPartitions(numberOfPartitions, true,
        useSchemaFromCommitMetadata, dateTime, commitTime, basePath);
    createdTablesSet
        .add(DB_NAME + "." + TABLE_NAME + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE);
    createdTablesSet
        .add(DB_NAME + "." + TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE);
    HoodieCommitMetadata compactionMetadata = new HoodieCommitMetadata();
    commitMetadata.getPartitionToWriteStats()
        .forEach((key, value) -> value.forEach(l -> compactionMetadata.addWriteStat(key, l)));
    addSchemaToCommitMetadata(compactionMetadata, commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY),
        useSchemaFromCommitMetadata);
    createCompactionCommitFile(compactionMetadata, commitTime);
    if (createDeltaCommit) {
      // Write a delta commit
      HoodieCommitMetadata deltaMetadata = createLogFiles(commitMetadata.getPartitionToWriteStats(), true,
          useSchemaFromCommitMetadata);
      createDeltaCommitFile(deltaMetadata, deltaCommitTime);
    }
  }

  public static void addCOWPartitions(int numberOfPartitions, boolean isParquetSchemaSimple,
                                      boolean useSchemaFromCommitMetadata, ZonedDateTime startFrom, String instantTime) throws IOException, URISyntaxException {
    HoodieCommitMetadata commitMetadata =
        createPartitions(numberOfPartitions, isParquetSchemaSimple, useSchemaFromCommitMetadata, startFrom, instantTime, basePath);
    createdTablesSet.add(DB_NAME + "." + TABLE_NAME);
    createCommitFile(commitMetadata, instantTime, basePath);
  }

  public static void addCOWPartition(String partitionPath, boolean isParquetSchemaSimple,
                                     boolean useSchemaFromCommitMetadata, String instantTime) throws IOException, URISyntaxException {
    HoodieCommitMetadata commitMetadata =
        createPartition(partitionPath, isParquetSchemaSimple, useSchemaFromCommitMetadata, instantTime);
    createdTablesSet.add(DB_NAME + "." + TABLE_NAME);
    createCommitFile(commitMetadata, instantTime, basePath);
  }

  public static void addMORPartitions(int numberOfPartitions, boolean isParquetSchemaSimple, boolean isLogSchemaSimple,
                                      boolean useSchemaFromCommitMetadata, ZonedDateTime startFrom, String instantTime, String deltaCommitTime)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieCommitMetadata commitMetadata = createPartitions(numberOfPartitions, isParquetSchemaSimple,
        useSchemaFromCommitMetadata, startFrom, instantTime, basePath);
    createdTablesSet.add(DB_NAME + "." + TABLE_NAME + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE);
    createdTablesSet.add(DB_NAME + "." + TABLE_NAME + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE);
    HoodieCommitMetadata compactionMetadata = new HoodieCommitMetadata();
    commitMetadata.getPartitionToWriteStats()
        .forEach((key, value) -> value.forEach(l -> compactionMetadata.addWriteStat(key, l)));
    addSchemaToCommitMetadata(compactionMetadata, commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY),
        useSchemaFromCommitMetadata);
    createCompactionCommitFile(compactionMetadata, instantTime);
    HoodieCommitMetadata deltaMetadata = createLogFiles(commitMetadata.getPartitionToWriteStats(), isLogSchemaSimple,
        useSchemaFromCommitMetadata);
    createDeltaCommitFile(deltaMetadata, deltaCommitTime);
  }

  private static HoodieCommitMetadata createLogFiles(Map<String, List<HoodieWriteStat>> partitionWriteStats,
                                                     boolean isLogSchemaSimple, boolean useSchemaFromCommitMetadata)
      throws InterruptedException, IOException, URISyntaxException {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (Entry<String, List<HoodieWriteStat>> wEntry : partitionWriteStats.entrySet()) {
      String partitionPath = wEntry.getKey();
      for (HoodieWriteStat wStat : wEntry.getValue()) {
        Path path = new Path(wStat.getPath());
        HoodieBaseFile dataFile = new HoodieBaseFile(fileSystem.getFileStatus(path));
        HoodieLogFile logFile = generateLogData(path, isLogSchemaSimple);
        HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
        writeStat.setFileId(dataFile.getFileId());
        writeStat.setPath(logFile.getPath().toString());
        commitMetadata.addWriteStat(partitionPath, writeStat);
      }
    }
    addSchemaToCommitMetadata(commitMetadata, isLogSchemaSimple, useSchemaFromCommitMetadata);
    return commitMetadata;
  }

  private static HoodieCommitMetadata createPartitions(int numberOfPartitions, boolean isParquetSchemaSimple,
                                                       boolean useSchemaFromCommitMetadata, ZonedDateTime startFrom, String instantTime, String basePath) throws IOException, URISyntaxException {
    startFrom = startFrom.truncatedTo(ChronoUnit.DAYS);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (int i = 0; i < numberOfPartitions; i++) {
      String partitionPath = startFrom.format(dtfOut);
      Path partPath = new Path(basePath + "/" + partitionPath);
      fileSystem.makeQualified(partPath);
      fileSystem.mkdirs(partPath);
      List<HoodieWriteStat> writeStats = createTestData(partPath, isParquetSchemaSimple, instantTime);
      startFrom = startFrom.minusDays(1);
      writeStats.forEach(s -> commitMetadata.addWriteStat(partitionPath, s));
    }
    addSchemaToCommitMetadata(commitMetadata, isParquetSchemaSimple, useSchemaFromCommitMetadata);
    return commitMetadata;
  }

  private static HoodieCommitMetadata createPartition(String partitionPath, boolean isParquetSchemaSimple,
                                                      boolean useSchemaFromCommitMetadata, String instantTime) throws IOException, URISyntaxException {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    Path partPath = new Path(basePath + "/" + partitionPath);
    fileSystem.makeQualified(partPath);
    fileSystem.mkdirs(partPath);
    List<HoodieWriteStat> writeStats = createTestData(partPath, isParquetSchemaSimple, instantTime);
    writeStats.forEach(s -> commitMetadata.addWriteStat(partitionPath, s));
    addSchemaToCommitMetadata(commitMetadata, isParquetSchemaSimple, useSchemaFromCommitMetadata);
    return commitMetadata;
  }

  private static List<HoodieWriteStat> createTestData(Path partPath, boolean isParquetSchemaSimple, String instantTime)
      throws IOException, URISyntaxException {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      // Create 5 files
      String fileId = UUID.randomUUID().toString();
      Path filePath = new Path(partPath.toString() + "/" + FSUtils.makeBaseFileName(instantTime, "1-0-1", fileId));
      generateParquetData(filePath, isParquetSchemaSimple);
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setFileId(fileId);
      writeStat.setPath(filePath.toString());
      writeStats.add(writeStat);
    }
    return writeStats;
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  private static void generateParquetData(Path filePath, boolean isParquetSchemaSimple)
      throws IOException, URISyntaxException {
    Schema schema = getTestDataSchema(isParquetSchemaSimple);
    org.apache.parquet.schema.MessageType parquetSchema = new AvroSchemaConverter().convert(schema);
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, -1,
        BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(parquetSchema, schema, Option.of(filter));
    ParquetWriter writer = new ParquetWriter(filePath, writeSupport, CompressionCodecName.GZIP, 120 * 1024 * 1024,
        ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED, ParquetWriter.DEFAULT_WRITER_VERSION, fileSystem.getConf());

    List<IndexedRecord> testRecords = (isParquetSchemaSimple ? SchemaTestUtil.generateTestRecords(0, 100)
        : SchemaTestUtil.generateEvolvedTestRecords(100, 100));
    testRecords.forEach(s -> {
      try {
        writer.write(s);
      } catch (IOException e) {
        fail("IOException while writing test records as parquet" + e.toString());
      }
    });
    writer.close();
  }

  private static void generateParquetDataWithSchema(Path filePath, Schema schema)
      throws IOException {
    org.apache.parquet.schema.MessageType parquetSchema = new AvroSchemaConverter().convert(schema);
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, -1,
        BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(parquetSchema, schema, Option.of(filter));
    ParquetWriter writer = new ParquetWriter(filePath, writeSupport, CompressionCodecName.GZIP, 120 * 1024 * 1024,
        ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED, ParquetWriter.DEFAULT_WRITER_VERSION, fileSystem.getConf());

    List<IndexedRecord> testRecords = SchemaTestUtil.generateTestRecordsForSchema(schema);
    testRecords.forEach(s -> {
      try {
        writer.write(s);
      } catch (IOException e) {
        fail("IOException while writing test records as parquet" + e.toString());
      }
    });
    writer.close();
  }

  private static HoodieLogFile generateLogData(Path parquetFilePath, boolean isLogSchemaSimple)
      throws IOException, InterruptedException, URISyntaxException {
    Schema schema = getTestDataSchema(isLogSchemaSimple);
    HoodieBaseFile dataFile = new HoodieBaseFile(fileSystem.getFileStatus(parquetFilePath));
    // Write a log file for this parquet file
    Writer logWriter = HoodieLogFormat.newWriterBuilder().onParentPath(parquetFilePath.getParent())
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(dataFile.getFileId())
        .overBaseCommit(dataFile.getCommitTime()).withFs(fileSystem).build();
    List<IndexedRecord> records = (isLogSchemaSimple ? SchemaTestUtil.generateTestRecords(0, 100)
        : SchemaTestUtil.generateEvolvedTestRecords(100, 100));
    Map<HeaderMetadataType, String> header = new HashMap<>(2);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, dataFile.getCommitTime());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
    logWriter.appendBlock(dataBlock);
    logWriter.close();
    return logWriter.getLogFile();
  }

  private static Schema getTestDataSchema(boolean isSimpleSchema) throws IOException {
    return isSimpleSchema ? SchemaTestUtil.getSimpleSchema() : SchemaTestUtil.getEvolvedSchema();
  }

  private static void addSchemaToCommitMetadata(HoodieCommitMetadata commitMetadata, boolean isSimpleSchema,
                                                boolean useSchemaFromCommitMetadata) throws IOException {
    if (useSchemaFromCommitMetadata) {
      Schema dataSchema = getTestDataSchema(isSimpleSchema);
      commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, dataSchema.toString());
    }
  }

  private static void addSchemaToCommitMetadata(HoodieCommitMetadata commitMetadata, String schema,
                                                boolean useSchemaFromCommitMetadata) {
    if (useSchemaFromCommitMetadata) {
      commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schema);
    }
  }

  private static void checkResult(boolean result) {
    if (!result) {
      throw new JUnitException("Could not initialize");
    }
  }

  public static void createCommitFile(HoodieCommitMetadata commitMetadata, String instantTime, String basePath) throws IOException {
    byte[] bytes = commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeCommitFileName(instantTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  public static void createReplaceCommitFile(HoodieCommitMetadata commitMetadata, String instantTime) throws IOException {
    byte[] bytes = commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeReplaceFileName(instantTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  public static void createCommitFileWithSchema(HoodieCommitMetadata commitMetadata, String instantTime, boolean isSimpleSchema) throws IOException {
    addSchemaToCommitMetadata(commitMetadata, isSimpleSchema, true);
    createCommitFile(commitMetadata, instantTime, basePath);
  }

  private static void createCompactionCommitFile(HoodieCommitMetadata commitMetadata, String instantTime)
      throws IOException {
    byte[] bytes = commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeCommitFileName(instantTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  private static void createDeltaCommitFile(HoodieCommitMetadata deltaCommitMetadata, String deltaCommitTime)
      throws IOException {
    byte[] bytes = deltaCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeDeltaFileName(deltaCommitTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  public static Set<String> getCreatedTablesSet() {
    return createdTablesSet;
  }
}
