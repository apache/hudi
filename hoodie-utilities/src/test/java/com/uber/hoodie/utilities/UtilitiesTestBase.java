/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities;

import com.google.common.collect.ImmutableList;
import com.uber.hoodie.common.TestRawTripPayload;
import com.uber.hoodie.common.minicluster.HdfsTestService;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.hive.HiveSyncConfig;
import com.uber.hoodie.hive.HoodieHiveClient;
import com.uber.hoodie.hive.util.HiveTestService;
import com.uber.hoodie.utilities.sources.TestDataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
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
    hiveSyncConfig.partitionFields = new ImmutableList.Builder<String>().add("datestr").build();
    return hiveSyncConfig;
  }

  /**
   * Initialize Hive DB
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

    // to get hold of resources bundled with jar
    private static ClassLoader classLoader = Helpers.class.getClassLoader();

    public static void copyToDFS(String testResourcePath, FileSystem fs, String targetPath) throws IOException {
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(classLoader.getResourceAsStream(testResourcePath)));
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

    public static TypedProperties setupSchemaOnDFS() throws IOException {
      UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source.avsc", dfs, dfsBasePath + "/source.avsc");
      TypedProperties props = new TypedProperties();
      props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
      return props;
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
