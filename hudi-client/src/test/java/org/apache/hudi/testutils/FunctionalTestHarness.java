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

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.providers.DFSProvider;
import org.apache.hudi.testutils.providers.HoodieMetaClientProvider;
import org.apache.hudi.testutils.providers.HoodieWriteClientProvider;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;

public class FunctionalTestHarness implements SparkProvider, DFSProvider, HoodieMetaClientProvider, HoodieWriteClientProvider {

  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;

  private static transient HdfsTestService hdfsTestService;
  private static transient MiniDFSCluster dfsCluster;
  private static transient DistributedFileSystem dfs;

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
  public DistributedFileSystem dfs() {
    return dfs;
  }

  @Override
  public Path dfsBasePath() {
    return dfs.getWorkingDirectory();
  }

  public HoodieTableMetaClient getHoodieMetaClient(Configuration hadoopConf, String basePath) throws IOException {
    return getHoodieMetaClient(hadoopConf, basePath, new Properties());
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(Configuration hadoopConf, String basePath, Properties props) throws IOException {
    props.putIfAbsent(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP_NAME, PARQUET.toString());
    props.putIfAbsent(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, RAW_TRIPS_TEST_NAME);
    props.putIfAbsent(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, COPY_ON_WRITE.name());
    props.putIfAbsent(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, HoodieAvroPayload.class.getName());
    return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, props);
  }

  @Override
  public HoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) throws IOException {
    return new HoodieWriteClient(jsc, cfg, false, HoodieIndex.createIndex(cfg));
  }

  @BeforeEach
  public synchronized void runBeforeEach() throws Exception {
    initialized = spark != null && hdfsTestService != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      HoodieWriteClient.registerClasses(sparkConf);
      HoodieReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());

      hdfsTestService = new HdfsTestService();
      dfsCluster = hdfsTestService.start(true);
      dfs = dfsCluster.getFileSystem();
      dfs.mkdirs(dfs.getWorkingDirectory());

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        hdfsTestService.stop();
        hdfsTestService = null;

        spark.stop();
        spark = null;
      }));
    }
  }

  @AfterAll
  public static synchronized void cleanUpAfterAll() throws IOException {
    Path workDir = dfs.getWorkingDirectory();
    FileSystem fs = workDir.getFileSystem(hdfsTestService.getHadoopConf());
    FileStatus[] fileStatuses = dfs.listStatus(workDir);
    for (FileStatus f : fileStatuses) {
      fs.delete(f.getPath(), true);
    }
  }
}
