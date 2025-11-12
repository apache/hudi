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

package org.apache.hudi.testutils;

import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.testutils.providers.DFSProvider;
import org.apache.hudi.testutils.providers.HoodieMetaClientProvider;
import org.apache.hudi.testutils.providers.HoodieWriteClientProvider;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.HoodieSparkKryoRegistrar$;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;

/**
 * @deprecated Deprecated. Use {@link SparkClientFunctionalTestHarness} instead.
 */
public class FunctionalTestHarness implements SparkProvider, DFSProvider, HoodieMetaClientProvider, HoodieWriteClientProvider {

  protected static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  protected static transient HoodieSparkEngineContext context;

  private static transient HdfsTestService hdfsTestService;
  private static transient MiniDFSCluster dfsCluster;
  private static transient HoodieStorage storage;

  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

  public String basePath() {
    return tempDir.toAbsolutePath().toString();
  }

  @Override
  public SparkSession spark() {
    return spark;
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public JavaSparkContext jsc() {
    return jsc;
  }

  @Override
  public MiniDFSCluster dfsCluster() {
    return dfsCluster;
  }

  @Override
  public HoodieStorage hoodieStorage() {
    return storage;
  }

  @Override
  public Path dfsBasePath() {
    return new Path("/tmp");
  }

  @Override
  public HoodieEngineContext context() {
    return context;
  }

  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath) throws IOException {
    return getHoodieMetaClient(storageConf, basePath, new Properties());
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath, Properties props,
      HoodieTableType tableType) throws IOException {
    return HoodieTableMetaClient.newTableBuilder()
      .setTableName(RAW_TRIPS_TEST_NAME)
      .setTableType(tableType)
      .setPayloadClass(HoodieAvroPayload.class)
      .fromProperties(props)
      .initTable(storageConf.newInstance(), basePath);
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath, Properties props) throws IOException {
    return getHoodieMetaClient(storageConf, basePath, props, COPY_ON_WRITE);
  }

  @Override
  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) throws IOException {
    return new SparkRDDWriteClient(context(), cfg);
  }

  @BeforeEach
  public synchronized void runBeforeEach() throws Exception {
    initialized = spark != null && hdfsTestService != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      HoodieSparkKryoRegistrar$.MODULE$.register(sparkConf);
      SparkRDDReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);

      hdfsTestService = new HdfsTestService();
      dfsCluster = hdfsTestService.start(true);
      storage = new HoodieHadoopStorage(dfsCluster.getFileSystem());
      storage.createDirectory(new StoragePath("/tmp"));

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        hdfsTestService.stop();
        hdfsTestService = null;

        jsc.close();
        jsc = null;
        spark.stop();
        spark = null;
      }));
    }
  }

  @AfterEach
  public synchronized void tearDown() throws Exception {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @AfterAll
  public static synchronized void cleanUpAfterAll() throws IOException {
    StoragePath workDir = new StoragePath("/tmp");
    HoodieStorage storage = new HoodieHadoopStorage(
        workDir, HadoopFSUtils.getStorageConf(hdfsTestService.getHadoopConf()));
    List<StoragePathInfo> pathInfoList = storage.listDirectEntries(workDir);
    for (StoragePathInfo f : pathInfoList) {
      if (f.isDirectory()) {
        storage.deleteDirectory(f.getPath());
      } else {
        storage.deleteFile(f.getPath());
      }
    }
    if (hdfsTestService != null) {
      hdfsTestService.stop();
      hdfsTestService = null;
    }
    if (spark != null) {
      spark.stop();
      spark = null;
    }
    if (jsc != null) {
      jsc.close();
      jsc = null;
    }
    sqlContext = null;
    context = null;
  }
}
