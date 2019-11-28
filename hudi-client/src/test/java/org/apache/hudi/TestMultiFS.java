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

package org.apache.hudi;

import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestMultiFS extends HoodieClientTestHarness {

  private static Logger logger = LogManager.getLogger(TestMultiFS.class);
  private String tablePath = "file:///tmp/hoodie/sample-table";
  protected String tableName = "hoodie_rt";
  private String tableType = HoodieTableType.COPY_ON_WRITE.name();

  @Before
  public void setUp() throws Exception {
    initSparkContexts();
    initDFS();
    initTestDataGenerator();
  }

  @After
  public void tearDown() throws Exception {
    cleanupSparkContexts();
    cleanupDFS();
    cleanupTestDataGenerator();
  }

  private HoodieWriteClient getHoodieWriteClient(HoodieWriteConfig config) throws Exception {
    return new HoodieWriteClient(jsc, config);
  }

  protected HoodieWriteConfig getHoodieWriteConfig(String basePath) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withEmbeddedTimelineServerEnabled(true)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable(tableName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
  }

  @Test
  public void readLocalWriteHDFS() throws Exception {
    // Initialize table and filesystem
    HoodieTableMetaClient.initTableType(jsc.hadoopConfiguration(), dfsBasePath, HoodieTableType.valueOf(tableType),
        tableName, HoodieAvroPayload.class.getName());

    // Create write client to write some records in
    HoodieWriteConfig cfg = getHoodieWriteConfig(dfsBasePath);
    HoodieWriteConfig localConfig = getHoodieWriteConfig(tablePath);

    try (HoodieWriteClient hdfsWriteClient = getHoodieWriteClient(cfg);
        HoodieWriteClient localWriteClient = getHoodieWriteClient(localConfig);) {

      // Write generated data to hdfs (only inserts)
      String readCommitTime = hdfsWriteClient.startCommit();
      logger.info("Starting commit " + readCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(readCommitTime, 100);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      hdfsWriteClient.upsert(writeRecords, readCommitTime);

      // Read from hdfs
      FileSystem fs = FSUtils.getFs(dfsBasePath, HoodieTestUtils.getDefaultHadoopConf());
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs.getConf(), dfsBasePath);
      HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
      Dataset<Row> readRecords = HoodieClientTestUtils.readCommit(dfsBasePath, sqlContext, timeline, readCommitTime);
      assertEquals("Should contain 100 records", readRecords.count(), records.size());

      // Write to local
      HoodieTableMetaClient.initTableType(jsc.hadoopConfiguration(), tablePath, HoodieTableType.valueOf(tableType),
          tableName, HoodieAvroPayload.class.getName());

      String writeCommitTime = localWriteClient.startCommit();
      logger.info("Starting write commit " + writeCommitTime);
      List<HoodieRecord> localRecords = dataGen.generateInserts(writeCommitTime, 100);
      JavaRDD<HoodieRecord> localWriteRecords = jsc.parallelize(localRecords, 1);
      logger.info("Writing to path: " + tablePath);
      localWriteClient.upsert(localWriteRecords, writeCommitTime);

      logger.info("Reading from path: " + tablePath);
      fs = FSUtils.getFs(tablePath, HoodieTestUtils.getDefaultHadoopConf());
      metaClient = new HoodieTableMetaClient(fs.getConf(), tablePath);
      timeline = new HoodieActiveTimeline(metaClient).getCommitTimeline();
      Dataset<Row> localReadRecords =
          HoodieClientTestUtils.readCommit(tablePath, sqlContext, timeline, writeCommitTime);
      assertEquals("Should contain 100 records", localReadRecords.count(), localRecords.size());
    }
  }
}
