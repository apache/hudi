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

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.HoodieClientTestHarness;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.utilities.HoodieSnapshotExporter.Config;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHoodieSnapshotExporter extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestHoodieSnapshotExporter.class);
  private static final int NUM_RECORDS = 100;
  private static final String COMMIT_TIME = "20200101000000";
  private static final String PARTITION_PATH = "2020/01/01";
  private static final String TABLE_NAME = "testing";
  private String sourcePath;
  private String targetPath;

  @Before
  public void setUp() throws Exception {
    initSparkContexts();
    initDFS();
    dataGen = new HoodieTestDataGenerator(new String[] {PARTITION_PATH});

    // Initialize test data dirs
    sourcePath = dfsBasePath + "/source/";
    targetPath = dfsBasePath + "/target/";
    dfs.mkdirs(new Path(sourcePath));
    dfs.mkdirs(new Path(targetPath));
    HoodieTableMetaClient
        .initTableType(jsc.hadoopConfiguration(), sourcePath, HoodieTableType.COPY_ON_WRITE, TABLE_NAME,
            HoodieAvroPayload.class.getName());

    // Prepare data as source Hudi dataset
    HoodieWriteConfig cfg = getHoodieWriteConfig(sourcePath);
    HoodieWriteClient hdfsWriteClient = new HoodieWriteClient(jsc, cfg);
    hdfsWriteClient.startCommitWithTime(COMMIT_TIME);
    List<HoodieRecord> records = dataGen.generateInserts(COMMIT_TIME, NUM_RECORDS);
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
    hdfsWriteClient.bulkInsert(recordsRDD, COMMIT_TIME);
    hdfsWriteClient.close();

    RemoteIterator<LocatedFileStatus> itr = dfs.listFiles(new Path(sourcePath), true);
    while (itr.hasNext()) {
      LOG.info(">>> Prepared test file: " + itr.next().getPath());
    }
  }

  @After
  public void tearDown() throws Exception {
    cleanupSparkContexts();
    cleanupDFS();
    cleanupTestDataGenerator();
  }

  private HoodieWriteConfig getHoodieWriteConfig(String basePath) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .forTable(TABLE_NAME)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.BLOOM).build())
        .build();
  }

  @Test
  public void testExportAsParquet() throws IOException {
    HoodieSnapshotExporter.Config cfg = new Config();
    cfg.sourceBasePath = sourcePath;
    cfg.targetOutputPath = targetPath;
    cfg.outputFormat = "parquet";
    new HoodieSnapshotExporter().export(SparkSession.builder().config(jsc.getConf()).getOrCreate(), cfg);
    assertEquals(NUM_RECORDS, sqlContext.read().parquet(targetPath).count());
    assertTrue(dfs.exists(new Path(targetPath + "/_SUCCESS")));
  }

  @Test
  public void testExportAsHudi() throws IOException {
    HoodieSnapshotExporter.Config cfg = new Config();
    cfg.sourceBasePath = sourcePath;
    cfg.targetOutputPath = targetPath;
    cfg.outputFormat = "hudi";
    new HoodieSnapshotExporter().export(SparkSession.builder().config(jsc.getConf()).getOrCreate(), cfg);

    // Check results
    assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".clean")));
    assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".clean.inflight")));
    assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".clean.requested")));
    assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".commit")));
    assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".commit.requested")));
    assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/" + COMMIT_TIME + ".inflight")));
    assertTrue(dfs.exists(new Path(targetPath + "/.hoodie/hoodie.properties")));
    String partition = targetPath + "/" + PARTITION_PATH;
    long numParquetFiles = Arrays.stream(dfs.listStatus(new Path(partition)))
        .filter(fileStatus -> fileStatus.getPath().toString().endsWith(".parquet"))
        .count();
    assertTrue("There should exist at least 1 parquet file.", numParquetFiles >= 1);
    assertEquals(NUM_RECORDS, sqlContext.read().parquet(partition).count());
    assertTrue(dfs.exists(new Path(partition + "/.hoodie_partition_metadata")));
  }

  @Test
  public void testExportEmptyDataset() throws IOException {
    // delete all source data
    dfs.delete(new Path(sourcePath + "/" + PARTITION_PATH), true);

    // export
    HoodieSnapshotExporter.Config cfg = new Config();
    cfg.sourceBasePath = sourcePath;
    cfg.targetOutputPath = targetPath;
    cfg.outputFormat = "hudi";
    new HoodieSnapshotExporter().export(SparkSession.builder().config(jsc.getConf()).getOrCreate(), cfg);

    // Check results
    assertEquals("Target path should be empty.", 0, dfs.listStatus(new Path(targetPath)).length);
  }
}
