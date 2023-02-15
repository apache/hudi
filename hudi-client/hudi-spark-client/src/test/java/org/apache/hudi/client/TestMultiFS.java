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

package org.apache.hudi.client;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMultiFS extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestMultiFS.class);
  private static final String TABLE_TYPE = HoodieTableType.COPY_ON_WRITE.name();
  private static final String TABLE_NAME = "hoodie_rt";
  private static HdfsTestService hdfsTestService;
  private static FileSystem dfs;
  private String tablePath;
  private String dfsBasePath;

  @BeforeAll
  public static void setUpAll() throws IOException {
    hdfsTestService = new HdfsTestService();
    MiniDFSCluster dfsCluster = hdfsTestService.start(true);
    dfs = dfsCluster.getFileSystem();
  }

  @AfterAll
  public static void cleanUpAll() {
    hdfsTestService.stop();
  }

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    tablePath = baseUri + "/sample-table";
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));
    hadoopConf = dfs.getConf();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  protected HoodieWriteConfig getHoodieWriteConfig(String basePath) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withEmbeddedTimelineServerEnabled(true)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable(TABLE_NAME)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .build();
  }

  @Test
  public void readLocalWriteHDFS() throws Exception {
    // Initialize table and filesystem
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(TABLE_TYPE)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(hadoopConf, dfsBasePath);

    // Create write client to write some records in
    HoodieWriteConfig cfg = getHoodieWriteConfig(dfsBasePath);
    HoodieWriteConfig localConfig = getHoodieWriteConfig(tablePath);

    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(TABLE_TYPE)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .setRecordKeyFields(localConfig.getProps().getProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()))
        .setPartitionFields(localConfig.getProps().getProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()))
        .initTable(hadoopConf, tablePath);


    try (SparkRDDWriteClient hdfsWriteClient = getHoodieWriteClient(cfg);
         SparkRDDWriteClient localWriteClient = getHoodieWriteClient(localConfig)) {

      // Write generated data to hdfs (only inserts)
      String readCommitTime = hdfsWriteClient.startCommit();
      LOG.info("Starting commit " + readCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(readCommitTime, 10);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);
      hdfsWriteClient.upsert(writeRecords, readCommitTime);

      // Read from hdfs
      FileSystem fs = FSUtils.getFs(dfsBasePath, HoodieTestUtils.getDefaultHadoopConf());
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(dfsBasePath).build();
      HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
      Dataset<Row> readRecords = HoodieClientTestUtils.readCommit(dfsBasePath, sqlContext, timeline, readCommitTime);
      assertEquals(readRecords.count(), records.size());

      // Write to local
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(TABLE_TYPE)
          .setTableName(TABLE_NAME)
          .setPayloadClass(HoodieAvroPayload.class)
          .initTable(hadoopConf, tablePath);

      String writeCommitTime = localWriteClient.startCommit();
      LOG.info("Starting write commit " + writeCommitTime);
      List<HoodieRecord> localRecords = dataGen.generateInserts(writeCommitTime, 10);
      JavaRDD<HoodieRecord> localWriteRecords = jsc.parallelize(localRecords, 2);
      LOG.info("Writing to path: " + tablePath);
      localWriteClient.upsert(localWriteRecords, writeCommitTime);

      LOG.info("Reading from path: " + tablePath);
      fs = FSUtils.getFs(tablePath, HoodieTestUtils.getDefaultHadoopConf());
      metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(tablePath).build();
      timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
      Dataset<Row> localReadRecords =
          HoodieClientTestUtils.readCommit(tablePath, sqlContext, timeline, writeCommitTime);
      assertEquals(localReadRecords.count(), localRecords.size());
    }
  }
}
