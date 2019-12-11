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

package org.apache.hudi.hive;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.BloomFilter;
import org.apache.hudi.common.minicluster.HdfsTestService;
import org.apache.hudi.common.minicluster.ZookeeperTestService;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.SchemaTestUtil;
import org.apache.hudi.hive.util.HiveTestService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.fail;

@SuppressWarnings("SameParameterValue")
public class TestUtil {

  private static MiniDFSCluster dfsCluster;
  private static ZooKeeperServer zkServer;
  private static HiveServer2 hiveServer;
  private static Configuration configuration;
  static HiveSyncConfig hiveSyncConfig;
  private static DateTimeFormatter dtfOut;
  static FileSystem fileSystem;
  private static Set<String> createdTablesSet = Sets.newHashSet();

  public static void setUp() throws IOException, InterruptedException, URISyntaxException {
    if (dfsCluster == null) {
      HdfsTestService service = new HdfsTestService();
      dfsCluster = service.start(true);
      configuration = service.getHadoopConf();
    }
    if (zkServer == null) {
      ZookeeperTestService zkService = new ZookeeperTestService(configuration);
      zkServer = zkService.start();
    }
    if (hiveServer == null) {
      HiveTestService hiveService = new HiveTestService(configuration);
      hiveServer = hiveService.start();
    }
    fileSystem = FileSystem.get(configuration);

    hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.jdbcUrl = "jdbc:hive2://127.0.0.1:9999/";
    hiveSyncConfig.databaseName = "hdrone_test";
    hiveSyncConfig.hiveUser = "";
    hiveSyncConfig.hivePass = "";
    hiveSyncConfig.databaseName = "testdb";
    hiveSyncConfig.tableName = "test1";
    hiveSyncConfig.basePath = "/tmp/hdfs/TestHiveSyncTool/";
    hiveSyncConfig.assumeDatePartitioning = true;
    hiveSyncConfig.usePreApacheInputFormat = false;
    hiveSyncConfig.partitionFields = Lists.newArrayList("datestr");

    dtfOut = DateTimeFormat.forPattern("yyyy/MM/dd");

    clear();
  }

  static void clear() throws IOException {
    fileSystem.delete(new Path(hiveSyncConfig.basePath), true);
    HoodieTableMetaClient.initTableType(configuration, hiveSyncConfig.basePath, HoodieTableType.COPY_ON_WRITE,
        hiveSyncConfig.tableName, HoodieAvroPayload.class.getName());

    HoodieHiveClient client = new HoodieHiveClient(hiveSyncConfig, hiveServer.getHiveConf(), fileSystem);
    for (String tableName : createdTablesSet) {
      client.updateHiveSQL("drop table if exists " + tableName);
    }
    createdTablesSet.clear();
    client.updateHiveSQL("drop database if exists " + hiveSyncConfig.databaseName);
    client.updateHiveSQL("create database " + hiveSyncConfig.databaseName);
  }

  static HiveConf getHiveConf() {
    return hiveServer.getHiveConf();
  }

  @SuppressWarnings("unused")
  public static void shutdown() {
    if (hiveServer != null) {
      hiveServer.stop();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
    if (zkServer != null) {
      zkServer.shutdown();
    }
  }

  static void createCOWDataset(String commitTime, int numberOfPartitions)
      throws IOException, InitializationError, URISyntaxException, InterruptedException {
    Path path = new Path(hiveSyncConfig.basePath);
    FileIOUtils.deleteDirectory(new File(hiveSyncConfig.basePath));
    HoodieTableMetaClient.initTableType(configuration, hiveSyncConfig.basePath, HoodieTableType.COPY_ON_WRITE,
        hiveSyncConfig.tableName, HoodieAvroPayload.class.getName());
    boolean result = fileSystem.mkdirs(path);
    checkResult(result);
    DateTime dateTime = DateTime.now();
    HoodieCommitMetadata commitMetadata = createPartitions(numberOfPartitions, true, dateTime, commitTime);
    createdTablesSet.add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);
    createCommitFile(commitMetadata, commitTime);
  }

  static void createMORDataset(String commitTime, String deltaCommitTime, int numberOfPartitions)
      throws IOException, InitializationError, URISyntaxException, InterruptedException {
    Path path = new Path(hiveSyncConfig.basePath);
    FileIOUtils.deleteDirectory(new File(hiveSyncConfig.basePath));
    HoodieTableMetaClient.initTableType(configuration, hiveSyncConfig.basePath, HoodieTableType.MERGE_ON_READ,
        hiveSyncConfig.tableName, HoodieAvroPayload.class.getName());

    boolean result = fileSystem.mkdirs(path);
    checkResult(result);
    DateTime dateTime = DateTime.now();
    HoodieCommitMetadata commitMetadata = createPartitions(numberOfPartitions, true, dateTime, commitTime);
    createdTablesSet.add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);
    createdTablesSet
        .add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_REALTIME_TABLE);
    HoodieCommitMetadata compactionMetadata = new HoodieCommitMetadata();
    commitMetadata.getPartitionToWriteStats()
        .forEach((key, value) -> value.stream().forEach(l -> compactionMetadata.addWriteStat(key, l)));
    createCompactionCommitFile(compactionMetadata, commitTime);
    // Write a delta commit
    HoodieCommitMetadata deltaMetadata = createLogFiles(commitMetadata.getPartitionToWriteStats(), true);
    createDeltaCommitFile(deltaMetadata, deltaCommitTime);
  }

  static void addCOWPartitions(int numberOfPartitions, boolean isParquetSchemaSimple, DateTime startFrom,
      String commitTime) throws IOException, URISyntaxException, InterruptedException {
    HoodieCommitMetadata commitMetadata =
        createPartitions(numberOfPartitions, isParquetSchemaSimple, startFrom, commitTime);
    createdTablesSet.add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);
    createCommitFile(commitMetadata, commitTime);
  }

  static void addMORPartitions(int numberOfPartitions, boolean isParquetSchemaSimple, boolean isLogSchemaSimple,
      DateTime startFrom, String commitTime, String deltaCommitTime)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieCommitMetadata commitMetadata =
        createPartitions(numberOfPartitions, isParquetSchemaSimple, startFrom, commitTime);
    createdTablesSet.add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);
    createdTablesSet
        .add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_REALTIME_TABLE);
    HoodieCommitMetadata compactionMetadata = new HoodieCommitMetadata();
    commitMetadata.getPartitionToWriteStats()
        .forEach((key, value) -> value.stream().forEach(l -> compactionMetadata.addWriteStat(key, l)));
    createCompactionCommitFile(compactionMetadata, commitTime);
    HoodieCommitMetadata deltaMetadata = createLogFiles(commitMetadata.getPartitionToWriteStats(), isLogSchemaSimple);
    createDeltaCommitFile(deltaMetadata, deltaCommitTime);
  }

  private static HoodieCommitMetadata createLogFiles(Map<String, List<HoodieWriteStat>> partitionWriteStats,
      boolean isLogSchemaSimple) throws InterruptedException, IOException, URISyntaxException {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (Entry<String, List<HoodieWriteStat>> wEntry : partitionWriteStats.entrySet()) {
      String partitionPath = wEntry.getKey();
      for (HoodieWriteStat wStat : wEntry.getValue()) {
        Path path = new Path(wStat.getPath());
        HoodieDataFile dataFile = new HoodieDataFile(fileSystem.getFileStatus(path));
        HoodieLogFile logFile = generateLogData(path, isLogSchemaSimple);
        HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
        writeStat.setFileId(dataFile.getFileId());
        writeStat.setPath(logFile.getPath().toString());
        commitMetadata.addWriteStat(partitionPath, writeStat);
      }
    }
    return commitMetadata;
  }

  private static HoodieCommitMetadata createPartitions(int numberOfPartitions, boolean isParquetSchemaSimple,
      DateTime startFrom, String commitTime) throws IOException, URISyntaxException, InterruptedException {
    startFrom = startFrom.withTimeAtStartOfDay();

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (int i = 0; i < numberOfPartitions; i++) {
      String partitionPath = dtfOut.print(startFrom);
      Path partPath = new Path(hiveSyncConfig.basePath + "/" + partitionPath);
      fileSystem.makeQualified(partPath);
      fileSystem.mkdirs(partPath);
      List<HoodieWriteStat> writeStats = createTestData(partPath, isParquetSchemaSimple, commitTime);
      startFrom = startFrom.minusDays(1);
      writeStats.forEach(s -> commitMetadata.addWriteStat(partitionPath, s));
    }
    return commitMetadata;
  }

  private static List<HoodieWriteStat> createTestData(Path partPath, boolean isParquetSchemaSimple, String commitTime)
      throws IOException, URISyntaxException, InterruptedException {
    List<HoodieWriteStat> writeStats = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      // Create 5 files
      String fileId = UUID.randomUUID().toString();
      Path filePath = new Path(partPath.toString() + "/" + FSUtils.makeDataFileName(commitTime, "1-0-1", fileId));
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
      throws IOException, URISyntaxException, InterruptedException {
    Schema schema = (isParquetSchemaSimple ? SchemaTestUtil.getSimpleSchema() : SchemaTestUtil.getEvolvedSchema());
    org.apache.parquet.schema.MessageType parquetSchema = new AvroSchemaConverter().convert(schema);
    BloomFilter filter = new BloomFilter(1000, 0.0001);
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(parquetSchema, schema, filter);
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

  private static HoodieLogFile generateLogData(Path parquetFilePath, boolean isLogSchemaSimple)
      throws IOException, InterruptedException, URISyntaxException {
    Schema schema = (isLogSchemaSimple ? SchemaTestUtil.getSimpleSchema() : SchemaTestUtil.getEvolvedSchema());
    HoodieDataFile dataFile = new HoodieDataFile(fileSystem.getFileStatus(parquetFilePath));
    // Write a log file for this parquet file
    Writer logWriter = HoodieLogFormat.newWriterBuilder().onParentPath(parquetFilePath.getParent())
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(dataFile.getFileId())
        .overBaseCommit(dataFile.getCommitTime()).withFs(fileSystem).build();
    List<IndexedRecord> records = (isLogSchemaSimple ? SchemaTestUtil.generateTestRecords(0, 100)
        : SchemaTestUtil.generateEvolvedTestRecords(100, 100));
    Map<HeaderMetadataType, String> header = Maps.newHashMap();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, dataFile.getCommitTime());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
    logWriter.appendBlock(dataBlock);
    logWriter.close();
    return logWriter.getLogFile();
  }

  private static void checkResult(boolean result) throws InitializationError {
    if (!result) {
      throw new InitializationError("Could not initialize");
    }
  }

  private static void createCommitFile(HoodieCommitMetadata commitMetadata, String commitTime) throws IOException {
    byte[] bytes = commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(hiveSyncConfig.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeCommitFileName(commitTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  private static void createCompactionCommitFile(HoodieCommitMetadata commitMetadata, String commitTime)
      throws IOException {
    byte[] bytes = commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(hiveSyncConfig.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeCommitFileName(commitTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  private static void createDeltaCommitFile(HoodieCommitMetadata deltaCommitMetadata, String deltaCommitTime)
      throws IOException {
    byte[] bytes = deltaCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(hiveSyncConfig.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeDeltaFileName(deltaCommitTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  public static Set<String> getCreatedTablesSet() {
    return createdTablesSet;
  }
}
