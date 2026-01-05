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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hive.service.server.HiveServer2;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.junit.jupiter.api.Assertions.fail;

public class HiveTestCluster implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {
  public MiniDFSCluster dfsCluster;
  private HdfsTestService hdfsTestService;
  private HiveTestService hiveTestService;
  private HiveConf conf;
  private HiveServer2 server2;
  private DateTimeFormatter dtfOut;
  private File hiveSiteXml;
  private IMetaStoreClient client;

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    setup();
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    shutDown();
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
  }

  public void setup() throws Exception {
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);

    Configuration hadoopConf = hdfsTestService.getHadoopConf();
    hadoopConf.setInt(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, NetworkTestUtils.nextFreePort());
    hiveTestService = new HiveTestService(hadoopConf);
    server2 = hiveTestService.start();
    dtfOut = DateTimeFormatter.ofPattern("yyyy/MM/dd");
    hiveSiteXml = File.createTempFile("hive-site", ".xml");
    hiveSiteXml.deleteOnExit();
    conf = hiveTestService.getHiveConf();
    try (OutputStream os = new FileOutputStream(hiveSiteXml)) {
      conf.writeXml(os);
    }
    client = HiveMetaStoreClient.newSynchronizedClient(
        RetryingMetaStoreClient.getProxy(conf, true));
  }

  public String getHiveSiteXmlLocation() {
    return hiveSiteXml.getAbsolutePath();
  }

  public IMetaStoreClient getHMSClient() {
    return client;
  }

  public String getHiveJdBcUrl() {
    return hiveTestService.getJdbcHive2Url();
  }

  public String tablePath(String dbName, String tableName) throws Exception {
    return dbPath(dbName) + "/" + tableName;
  }

  private String dbPath(String dbName) throws Exception {
    return dfsCluster.getFileSystem().getWorkingDirectory().toString() + "/" + dbName;
  }

  public void forceCreateDb(String dbName) throws Exception {
    try {
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException ignored) {
      // expected
    }
    Database db = new Database(dbName, "", dbPath(dbName), new HashMap<>());
    client.createDatabase(db);
  }

  public void createCOWTable(String commitTime, int numberOfPartitions, String dbName, String tableName)
      throws Exception {
    String tablePathStr = tablePath(dbName, tableName);
    Path path = new Path(tablePathStr);
    FileIOUtils.deleteDirectory(new File(path.toString()));
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(tableName)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(conf), path.toString());
    dfsCluster.getFileSystem().mkdirs(path);
    ZonedDateTime dateTime = ZonedDateTime.now();
    HoodieCommitMetadata commitMetadata = createPartitions(numberOfPartitions, true, dateTime, commitTime, path.toString());
    createCommitFile(commitMetadata, commitTime, path.toString());
  }

  private void createCommitFile(HoodieCommitMetadata commitMetadata, String commitTime, String basePath) throws IOException {
    Path fullPath = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/"
        + INSTANT_FILE_NAME_GENERATOR.makeCommitFileName(commitTime + "_" + InProcessTimeGenerator.createNewInstantTime()));
    try (OutputStream fsout = dfsCluster.getFileSystem().create(fullPath, true)) {
      COMMIT_METADATA_SER_DE.getInstantWriter(commitMetadata).get().writeToStream(fsout);
    }
  }

  private HoodieCommitMetadata createPartitions(int numberOfPartitions, boolean isParquetSchemaSimple,
      ZonedDateTime startFrom, String commitTime, String basePath) throws IOException, URISyntaxException {
    startFrom = startFrom.truncatedTo(ChronoUnit.DAYS);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (int i = 0; i < numberOfPartitions; i++) {
      String partitionPath = startFrom.format(dtfOut);
      Path partPath = new Path(basePath + "/" + partitionPath);
      dfsCluster.getFileSystem().makeQualified(partPath);
      dfsCluster.getFileSystem().mkdirs(partPath);
      List<HoodieWriteStat> writeStats = createTestData(partPath, isParquetSchemaSimple, commitTime);
      startFrom = startFrom.minusDays(1);
      writeStats.forEach(s -> commitMetadata.addWriteStat(partitionPath, s));
    }
    return commitMetadata;
  }

  private List<HoodieWriteStat> createTestData(Path partPath, boolean isParquetSchemaSimple, String commitTime)
      throws IOException, URISyntaxException {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      // Create 5 files
      String fileId = UUID.randomUUID().toString();
      Path filePath = new Path(partPath.toString() + "/" + FSUtils
          .makeBaseFileName(commitTime, "1-0-1", fileId, HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension()));
      generateParquetData(filePath, isParquetSchemaSimple);
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setFileId(fileId);
      writeStat.setPath(filePath.toString());
      writeStat.setNumInserts(10);
      writeStats.add(writeStat);
    }
    return writeStats;
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  private void generateParquetData(Path filePath, boolean isParquetSchemaSimple)
      throws IOException, URISyntaxException {
    HoodieSchema schema = (isParquetSchemaSimple ? SchemaTestUtil.getSimpleSchema() : SchemaTestUtil.getEvolvedSchema());
    org.apache.parquet.schema.MessageType parquetSchema = new AvroSchemaConverter().convert(schema.toAvroSchema());
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, -1,
        BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(parquetSchema, schema.toAvroSchema(), Option.of(filter), new Properties());
    ParquetWriter writer = new ParquetWriter(filePath, writeSupport, CompressionCodecName.GZIP, 120 * 1024 * 1024,
        ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED, ParquetWriter.DEFAULT_WRITER_VERSION, dfsCluster.getFileSystem().getConf());

    List<IndexedRecord> testRecords = (isParquetSchemaSimple ? SchemaTestUtil.generateTestRecords(0, 100)
        : SchemaTestUtil.generateEvolvedTestRecords(100, 100));
    testRecords.forEach(s -> {
      try {
        writer.write(s);
      } catch (IOException e) {
        fail("IOException while writing test records as parquet", e);
      }
    });
    writer.close();
  }

  public HiveConf getHiveConf() {
    return server2.getHiveConf();
  }

  public void stopHiveServer2() {
    if (server2 != null) {
      server2.stop();
      server2 = null;
    }
  }

  public void startHiveServer2() {
    if (server2 == null) {
      server2 = new HiveServer2();
      server2.init(hiveTestService.getHiveConf());
      server2.start();
    }
  }

  public void shutDown() throws IOException {
    Files.deleteIfExists(hiveSiteXml.toPath());
    Hive.closeCurrent();
    hiveTestService.stop();
    hdfsTestService.stop();
    FileSystem.closeAll();
  }
}
