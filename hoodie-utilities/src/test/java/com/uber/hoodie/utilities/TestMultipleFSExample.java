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

package com.uber.hoodie.utilities;


import static org.junit.Assert.assertEquals;

import com.beust.jcommander.Parameter;
import com.uber.hoodie.HoodieReadClient;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.minicluster.HdfsTestService;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import java.io.Serializable;
import java.util.*;

import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import org.apache.hadoop.fs.*;
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

public class TestMultipleFSExample implements Serializable {
    private static String dfsBasePath;
    private static HdfsTestService hdfsTestService;
    private static MiniDFSCluster dfsCluster;
    private static DistributedFileSystem dfs;
    private static Logger logger = LogManager.getLogger(TestMultipleFSExample.class);
    private String tablePath = "file:///tmp/hoodie/sample-table";
    private String tableName =  "hoodie_rt";
    private String tableType =  HoodieTableType.COPY_ON_WRITE.name();

    @BeforeClass
    public static void initClass() throws Exception {
        hdfsTestService = new HdfsTestService();
        dfsCluster = hdfsTestService.start(true);

        // Create a temp folder as the base path
        dfs = dfsCluster.getFileSystem();
        dfsBasePath = dfs.getWorkingDirectory().toString();
        dfs.mkdirs(new Path(dfsBasePath));
    }

    @AfterClass
    public static void cleanupClass() throws Exception {
        if (hdfsTestService != null) {
            hdfsTestService.stop();
        }
    }

    @Test
    public void readLocalWriteHDFS() throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("hoodie-client-example");
        sparkConf.setMaster("local[1]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryoserializer.buffer.max", "512m");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        // Generator of some records to be loaded in.
        HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

        // Initialize table and filesystem
        FileSystem hdfs = FSUtils.getFs(dfsBasePath);
        HoodieTableMetaClient.initTableType(hdfs, dfsBasePath, HoodieTableType.valueOf(tableType), tableName);

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
        HoodieReadClient hdfsReadClient = new HoodieReadClient(jsc, dfsBasePath, sqlContext);
        Dataset<Row> readRecords = hdfsReadClient.readCommit(readCommitTime);
        assertEquals("Should contain 100 records", readRecords.count(), records.size());

        // Write to local
        FileSystem local = FSUtils.getFs(tablePath);
        HoodieTableMetaClient.initTableType(local, tablePath, HoodieTableType.valueOf(tableType), tableName);
        HoodieWriteConfig localConfig = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
                .forTable(tableName).withIndexConfig(
                        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                .build();
        HoodieWriteClient localWriteClient = new HoodieWriteClient(jsc, localConfig);

        String writeCommitTime = localWriteClient.startCommit();
        logger.info("Starting write commit "  + writeCommitTime);
        List<HoodieRecord> localRecords = dataGen.generateInserts(writeCommitTime, 100);
        JavaRDD<HoodieRecord> localWriteRecords = jsc.parallelize(localRecords, 1);
        logger.info("Writing to path: " + tablePath);
        localWriteClient.upsert(localWriteRecords, writeCommitTime);

        logger.info("Reading from path: " + tablePath);
        HoodieReadClient localReadClient = new HoodieReadClient(jsc, tablePath, sqlContext);
        Dataset<Row> localReadRecords = localReadClient.readCommit(writeCommitTime);
        assertEquals("Should contain 100 records", localReadRecords.count(), localRecords.size());

    }
}
