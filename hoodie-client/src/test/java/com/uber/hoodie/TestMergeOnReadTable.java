/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.compact.HoodieCompactor;
import com.uber.hoodie.io.compact.HoodieRealtimeTableCompactor;
import com.uber.hoodie.table.HoodieTable;
import org.apache.hadoop.fs.FileStatus;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMergeOnReadTable {
    private transient JavaSparkContext jsc = null;
    private transient SQLContext sqlContext;
    private String basePath = null;
    private HoodieCompactor compactor;
    private FileSystem fs;

    @Before
    public void init() throws IOException {
        this.fs = FSUtils.getFs();

        // Initialize a local spark env
        SparkConf sparkConf =
            new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setAppName("TestHoodieCompactor").setMaster("local[4]");
        jsc = new JavaSparkContext(HoodieReadClient.addHoodieSupport(sparkConf));

        // Create a temp folder as the base path
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        basePath = folder.getRoot().getAbsolutePath();
        HoodieTestUtils.initTableType(basePath, HoodieTableType.MERGE_ON_READ);

        compactor = new HoodieRealtimeTableCompactor();

        //SQLContext stuff
        sqlContext = new SQLContext(jsc);
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


    @Test
    public void testSimpleInsertAndUpdate() throws Exception {
        HoodieWriteConfig cfg = getConfig();
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

        /**
         * Write 1 (only inserts)
         */
        String newCommitTime = "001";
        HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
        assertNoWriteErrors(statuses);

        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, cfg.getBasePath());
        HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg);

        Optional<HoodieInstant> deltaCommit =
            metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
        assertTrue(deltaCommit.isPresent());
        assertEquals("Delta commit should be 001", "001", deltaCommit.get().getTimestamp());

        Optional<HoodieInstant> commit =
            metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
        assertFalse(commit.isPresent());

        TableFileSystemView fsView = hoodieTable.getCompactedFileSystemView();
        FileStatus[] allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
        Stream<HoodieDataFile> dataFilesToRead = fsView.getLatestVersions(allFiles);
        assertTrue(!dataFilesToRead.findAny().isPresent());

        fsView = hoodieTable.getFileSystemView();
        dataFilesToRead = fsView.getLatestVersions(allFiles);
        assertTrue("RealtimeTableView should list the parquet files we wrote in the delta commit",
            dataFilesToRead.findAny().isPresent());

        /**
         * Write 2 (updates)
         */
        newCommitTime = "004";
        records = dataGen.generateUpdates(newCommitTime, 100);
        Map<HoodieKey, HoodieRecord> recordsMap = new HashMap<>();
        for (HoodieRecord rec : records) {
            if (!recordsMap.containsKey(rec.getKey())) {
                recordsMap.put(rec.getKey(), rec);
            }
        }


        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);
        metaClient = new HoodieTableMetaClient(fs, cfg.getBasePath());
        deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
        assertTrue(deltaCommit.isPresent());
        assertEquals("Latest Delta commit should be 004", "004", deltaCommit.get().getTimestamp());

        commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
        assertFalse(commit.isPresent());


        HoodieCompactor compactor = new HoodieRealtimeTableCompactor();
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());

        compactor.compact(jsc, getConfig(), table);

        allFiles = HoodieTestUtils.listAllDataFilesInPath(fs, cfg.getBasePath());
        dataFilesToRead = fsView.getLatestVersions(allFiles);
        assertTrue(dataFilesToRead.findAny().isPresent());

        // verify that there is a commit
        HoodieReadClient readClient = new HoodieReadClient(jsc, basePath, sqlContext);
        assertEquals("Expecting a single commit.", 1, readClient.listCommitsSince("000").size());
        String latestCompactionCommitTime = readClient.latestCommit();
        assertTrue(HoodieTimeline
            .compareTimestamps("000", latestCompactionCommitTime, HoodieTimeline.LESSER));
        assertEquals("Must contain 200 records", 200, readClient.readSince("000").count());
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

    private void assertNoWriteErrors(List<WriteStatus> statuses) {
        // Verify there are no errors
        for (WriteStatus status : statuses) {
            assertFalse("Errors found in write of " + status.getFileId(), status.hasErrors());
        }
    }



}
