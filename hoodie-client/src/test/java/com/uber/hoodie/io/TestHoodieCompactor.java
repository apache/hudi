/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.io;

import com.uber.hoodie.HoodieReadClient;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieCompactionMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieBloomIndex;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.compact.HoodieCompactor;
import com.uber.hoodie.io.compact.HoodieRealtimeTableCompactor;
import com.uber.hoodie.table.HoodieTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHoodieCompactor {
    private transient JavaSparkContext jsc = null;
    private String basePath = null;
    private HoodieCompactor compactor;
    private transient HoodieTestDataGenerator dataGen = null;

    @Before
    public void init() throws IOException {
        // Initialize a local spark env
        SparkConf sparkConf =
            new SparkConf().setAppName("TestHoodieCompactor").setMaster("local[4]");
        jsc = new JavaSparkContext(HoodieReadClient.addHoodieSupport(sparkConf));

        // Create a temp folder as the base path
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        basePath = folder.getRoot().getAbsolutePath();
        HoodieTestUtils.initTableType(basePath, HoodieTableType.MERGE_ON_READ);

        dataGen = new HoodieTestDataGenerator();
        compactor = new HoodieRealtimeTableCompactor();
    }

    @After
    public void clean() {
        if (basePath != null) {
            new File(basePath).delete();
        }
        if (jsc != null) {
            jsc.stop();
        }
    }

    private HoodieWriteConfig getConfig() {
        return getConfigBuilder().build();
    }

    private HoodieWriteConfig.Builder getConfigBuilder() {
        return HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .withCompactionConfig(
                HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
                    .withInlineCompaction(false).build())
            .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
            .forTable("test-trip-table").withIndexConfig(
                HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompactionOnCopyOnWriteFail() throws Exception {
        HoodieTestUtils.initTableType(basePath, HoodieTableType.COPY_ON_WRITE);

        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());

        compactor.compact(jsc, getConfig(), table);
    }

    @Test
    public void testCompactionEmpty() throws Exception {
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
        HoodieWriteConfig config = getConfig();
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, config);
        HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);

        String newCommitTime = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
        JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
        writeClient.insert(recordsRDD, newCommitTime).collect();

        HoodieCompactionMetadata result =
            compactor.compact(jsc, getConfig(), table);
        assertTrue("If there is nothing to compact, result will be empty",
            result.getFileIdAndFullPaths().isEmpty());
    }

    @Test
    public void testLogFileCountsAfterCompaction() throws Exception {
        FileSystem fs = FSUtils.getFs();
        // insert 100 records
        HoodieWriteConfig config = getConfig();
        HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
        String newCommitTime = "100";
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
        JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
        List<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime).collect();

        // Update all the 100 records
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, config);

        newCommitTime = "101";
        List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
        JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
        HoodieIndex index = new HoodieBloomIndex<>(config, jsc);
        updatedRecords = index.tagLocation(updatedRecordsRDD, table).collect();

        // Write them to corresponding avro logfiles
        HoodieTestUtils
            .writeRecordsToLogFiles(metaClient.getBasePath(), HoodieTestDataGenerator.avroSchema,
                updatedRecords);

        // Verify that all data file has one log file
        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, config);
        for (String partitionPath : dataGen.getPartitionPaths()) {
            Map<HoodieDataFile, List<HoodieLogFile>> groupedLogFiles =
                table.getFileSystemView().groupLatestDataFileWithLogFiles(partitionPath);
            for (List<HoodieLogFile> logFiles : groupedLogFiles.values()) {
                assertEquals("There should be 1 log file written for every data file", 1,
                    logFiles.size());
            }
        }

        // Do a compaction
        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, config);

        HoodieCompactionMetadata result =
            compactor.compact(jsc, getConfig(), table);

        // Verify that recently written compacted data file has no log file
        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, config);
        HoodieActiveTimeline timeline = metaClient.getActiveTimeline();

        assertTrue("Compaction commit should be > than last insert", timeline
            .compareTimestamps(timeline.lastInstant().get().getTimestamp(), newCommitTime,
                HoodieTimeline.GREATER));

        for (String partitionPath : dataGen.getPartitionPaths()) {
            Map<HoodieDataFile, List<HoodieLogFile>> groupedLogFiles =
                table.getFileSystemView().groupLatestDataFileWithLogFiles(partitionPath);
            for (List<HoodieLogFile> logFiles : groupedLogFiles.values()) {
                assertTrue(
                    "After compaction there should be no log files visiable on a Realtime view",
                    logFiles.isEmpty());
            }
            assertTrue(result.getPartitionToCompactionWriteStats().containsKey(partitionPath));
        }
    }

    // TODO - after modifying HoodieReadClient to support realtime tables - add more tests to make sure the data read is the updated data (compaction correctness)
    // TODO - add more test cases for compactions after a failed commit/compaction
}
