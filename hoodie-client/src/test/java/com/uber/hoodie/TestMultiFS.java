/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie;


import static org.junit.Assert.assertEquals;

import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.minicluster.HdfsTestService;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import java.io.Serializable;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultiFS implements Serializable {

  private static String dfsBasePath;
  private static HdfsTestService hdfsTestService;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;
  private static Logger logger = LogManager.getLogger(TestMultiFS.class);
  private String tablePath = "file:///tmp/hoodie/sample-table";
  private String tableName = "hoodie_rt";
  private String tableType = HoodieTableType.COPY_ON_WRITE.name();
  private static JavaSparkContext jsc;
  private static SQLContext sqlContext;

  @BeforeClass
  public static void initClass() throws Exception {
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);

    // Create a temp folder as the base path
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));

    SparkConf sparkConf = new SparkConf().setAppName("hoodie-client-example");
    sparkConf.setMaster("local[1]");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryoserializer.buffer.max", "512m");
    jsc = new JavaSparkContext(sparkConf);
    sqlContext = new SQLContext(jsc);
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    if (jsc != null) {
      jsc.stop();
    }

    if (hdfsTestService != null) {
      hdfsTestService.stop();
      dfsCluster.shutdown();
    }
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the same JVM
    FileSystem.closeAll();
  }

  @Test
  public void readLocalWriteHDFS() throws Exception {

    // Generator of some records to be loaded in.
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    // Initialize table and filesystem
    FileSystem hdfs = FSUtils.getFs(dfsBasePath, jsc.hadoopConfiguration());
    HoodieTableMetaClient
        .initTableType(hdfs, dfsBasePath, HoodieTableType.valueOf(tableType), tableName,
            HoodieAvroPayload.class.getName());

    //Create write client to write some records in
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(dfsBasePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .forTable(tableName).withIndexConfig(
            HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .build();
    HoodieWriteClient hdfsWriteClient = new HoodieWriteClient(jsc, cfg);

    // Write generated data to hdfs (only inserts)
    String readCommitTime = hdfsWriteClient.startCommit();
    logger.info("Starting commit " + readCommitTime);
    List<HoodieRecord> records = dataGen.generateInserts(readCommitTime, 100);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    hdfsWriteClient.upsert(writeRecords, readCommitTime);

    // Read from hdfs
    FileSystem fs = FSUtils.getFs(dfsBasePath, HoodieTestUtils.getDefaultHadoopConf());
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs.getConf(), dfsBasePath);
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient)
        .getCommitTimeline();
    Dataset<Row> readRecords = HoodieClientTestUtils
        .readCommit(dfsBasePath, sqlContext, timeline, readCommitTime);
    assertEquals("Should contain 100 records", readRecords.count(), records.size());

    // Write to local
    FileSystem local = FSUtils.getFs(tablePath, jsc.hadoopConfiguration());
    HoodieTableMetaClient
        .initTableType(local, tablePath, HoodieTableType.valueOf(tableType), tableName,
            HoodieAvroPayload.class.getName());
    HoodieWriteConfig localConfig = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .forTable(tableName).withIndexConfig(
            HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .build();
    HoodieWriteClient localWriteClient = new HoodieWriteClient(jsc, localConfig);

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
    Dataset<Row> localReadRecords = HoodieClientTestUtils
        .readCommit(tablePath, sqlContext, timeline, writeCommitTime);
    assertEquals("Should contain 100 records", localReadRecords.count(), localRecords.size());
  }
}
