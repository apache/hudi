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

package org.apache.hudi.utilities;

import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.TestRawTripPayload;
import org.apache.hudi.common.minicluster.HdfsTestService;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveClient;
import org.apache.hudi.hive.util.HiveTestService;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Abstract test that provides a dfs & spark contexts.
 *
 * TODO(vc): this needs to be done across the board.
 */
public class UtilitiesTestBase {

  protected static String dfsBasePath;
  protected static HdfsTestService hdfsTestService;
  protected static MiniDFSCluster dfsCluster;
  protected static DistributedFileSystem dfs;
  protected transient JavaSparkContext jsc = null;
  protected transient SparkSession sparkSession = null;
  protected transient SQLContext sqlContext;
  protected static HiveServer2 hiveServer;

  @BeforeClass
  public static void initClass() throws Exception {
    initClass(false);
  }

  static void initClass(boolean startHiveService) throws Exception {
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));
    if (startHiveService) {
      HiveTestService hiveService = new HiveTestService(hdfsTestService.getHadoopConf());
      hiveServer = hiveService.start();
      clearHiveDb();
    }
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
    if (hiveServer != null) {
      hiveServer.stop();
    }
  }

  @Before
  public void setup() throws Exception {
    TestDataSource.initDataGen();
    jsc = UtilHelpers.buildSparkContext(this.getClass().getName() + "-hoodie", "local[2]");
    sqlContext = new SQLContext(jsc);
    sparkSession = SparkSession.builder().config(jsc.getConf()).getOrCreate();
  }

  @After
  public void teardown() throws Exception {
    TestDataSource.resetDataGen();
    if (jsc != null) {
      jsc.stop();
    }
  }

  /**
   * Helper to get hive sync config
   * 
   * @param basePath
   * @param tableName
   * @return
   */
  protected static HiveSyncConfig getHiveSyncConfig(String basePath, String tableName) {
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.jdbcUrl = "jdbc:hive2://127.0.0.1:9999/";
    hiveSyncConfig.hiveUser = "";
    hiveSyncConfig.hivePass = "";
    hiveSyncConfig.databaseName = "testdb1";
    hiveSyncConfig.tableName = tableName;
    hiveSyncConfig.basePath = basePath;
    hiveSyncConfig.assumeDatePartitioning = false;
    hiveSyncConfig.usePreApacheInputFormat = false;
    hiveSyncConfig.partitionFields = new ImmutableList.Builder<String>().add("datestr").build();
    return hiveSyncConfig;
  }

  /**
   * Initialize Hive DB
   * 
   * @throws IOException
   */
  private static void clearHiveDb() throws IOException {
    HiveConf hiveConf = new HiveConf();
    // Create Dummy hive sync config
    HiveSyncConfig hiveSyncConfig = getHiveSyncConfig("/dummy", "dummy");
    hiveConf.addResource(hiveServer.getHiveConf());
    HoodieTableMetaClient.initTableType(dfs.getConf(), hiveSyncConfig.basePath, HoodieTableType.COPY_ON_WRITE,
        hiveSyncConfig.tableName, null);
    HoodieHiveClient client = new HoodieHiveClient(hiveSyncConfig, hiveConf, dfs);
    client.updateHiveSQL("drop database if exists " + hiveSyncConfig.databaseName);
    client.updateHiveSQL("create database " + hiveSyncConfig.databaseName);
    client.close();
  }

  public static class Helpers {

    public static void copyToDFS(ClassLoader classLoader, String testResourcePath, FileSystem fs, String targetPath)
        throws IOException {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(testResourcePath)));
      PrintStream os = new PrintStream(fs.create(new Path(targetPath), true));
      String line;
      while ((line = reader.readLine()) != null) {
        os.println(line);
      }
      os.flush();
      os.close();
    }

    public static void savePropsToDFS(TypedProperties props, FileSystem fs, String targetPath) throws IOException {
      String[] lines = props.keySet().stream().map(k -> String.format("%s=%s", k, props.get(k))).toArray(String[]::new);
      saveStringsToDFS(lines, fs, targetPath);
    }

    public static void saveStringsToDFS(String[] lines, FileSystem fs, String targetPath) throws IOException {
      PrintStream os = new PrintStream(fs.create(new Path(targetPath), true));
      for (String l : lines) {
        os.println(l);
      }
      os.flush();
      os.close();
    }

    public static void saveParquetToDFS(List<GenericRecord> records, Path targetFile) throws IOException {
      try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(targetFile)
          .withSchema(HoodieTestDataGenerator.avroSchema).withConf(HoodieTestUtils.getDefaultHadoopConf()).build()) {
        for (GenericRecord record : records) {
          writer.write(record);
        }
      }
    }

    public static TypedProperties setupSchemaOnDFS(String filePath) throws IOException {
      UtilitiesTestBase.Helpers.copyToDFS(Helpers.class.getClassLoader(), filePath, dfs, dfsBasePath + "/" + filePath);
      TypedProperties props = new TypedProperties();
      props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/" + filePath);
      return props;
    }

    public static GenericRecord toGenericRecord(HoodieRecord hoodieRecord, HoodieTestDataGenerator dataGenerator) {
      try {
        Option<IndexedRecord> recordOpt = hoodieRecord.getData().getInsertValue(dataGenerator.avroSchema);
        return (GenericRecord) recordOpt.get();
      } catch (IOException e) {
        return null;
      }
    }

    public static List<GenericRecord> toGenericRecords(List<HoodieRecord> hoodieRecords,
        HoodieTestDataGenerator dataGenerator) {
      List<GenericRecord> records = new ArrayList<GenericRecord>();
      for (HoodieRecord hoodieRecord : hoodieRecords) {
        records.add(toGenericRecord(hoodieRecord, dataGenerator));
      }
      return records;
    }

    public static String toJsonString(HoodieRecord hr) {
      try {
        return ((TestRawTripPayload) hr.getData()).getJsonData();
      } catch (IOException ioe) {
        return null;
      }
    }

    public static String[] jsonifyRecords(List<HoodieRecord> records) throws IOException {
      return records.stream().map(Helpers::toJsonString).toArray(String[]::new);
    }
  }
}
