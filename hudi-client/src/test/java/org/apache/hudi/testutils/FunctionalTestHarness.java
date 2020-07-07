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

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

public class FunctionalTestHarness implements SparkProvider, DFSProvider {

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

  @BeforeEach
  public synchronized void runBeforeEach() throws Exception {
    initialized = spark != null && hdfsTestService != null;
    if (!initialized) {
      FileSystem.closeAll();

      spark = SparkSession.builder()
          .config(HoodieWriteClient.registerClasses(conf()))
          .getOrCreate();
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
