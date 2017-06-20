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

import com.google.common.collect.Iterables;

import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.ParquetUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;

import java.util.Collection;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.util.AccumulatorV2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import scala.collection.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHoodieClient implements Serializable {
    private transient JavaSparkContext jsc = null;
    private transient SQLContext sqlContext;
    private String basePath = null;
    private transient HoodieTestDataGenerator dataGen = null;
    private String[] partitionPaths = {"2016/01/01", "2016/02/02", "2016/06/02"};

    @Before
    public void init() throws IOException {
        // Initialize a local spark env
        SparkConf sparkConf = new SparkConf().setAppName("TestHoodieClient").setMaster("local[4]");
        jsc = new JavaSparkContext(HoodieReadClient.addHoodieSupport(sparkConf));

        //SQLContext stuff
        sqlContext = new SQLContext(jsc);

        // Create a temp folder as the base path
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        basePath = folder.getRoot().getAbsolutePath();
        HoodieTestUtils.init(basePath);
        dataGen = new HoodieTestDataGenerator();
    }


    private HoodieWriteConfig getConfig() {
        return getConfigBuilder().build();
    }

    private HoodieWriteConfig.Builder getConfigBuilder() {
        return HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
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

    private void assertPartitionMetadata(String[] partitionPaths, FileSystem fs) throws IOException {
        for (String partitionPath: partitionPaths) {
            assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(fs, new Path(basePath, partitionPath)));
            HoodiePartitionMetadata pmeta = new HoodiePartitionMetadata(fs, new Path(basePath, partitionPath));
            pmeta.readFromFS();
            assertEquals(3, pmeta.getPartitionDepth());
        }
    }

    private void checkTaggedRecords(List<HoodieRecord> taggedRecords, String commitTime) {
        for (HoodieRecord rec : taggedRecords) {
            assertTrue("Record " + rec + " found with no location.", rec.isCurrentLocationKnown());
            assertEquals("All records should have commit time "+ commitTime+", since updates were made",
                    rec.getCurrentLocation().getCommitTime(), commitTime);
        }
    }

    @Test
    public void testFilterExist() throws Exception {
        HoodieWriteConfig config = getConfig();
        HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
        String newCommitTime = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
        JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);

        HoodieReadClient readClient = new HoodieReadClient(jsc, config.getBasePath());
        JavaRDD<HoodieRecord> filteredRDD = readClient.filterExists(recordsRDD);

        // Should not find any files
        assertTrue(filteredRDD.collect().size() == 100);

        JavaRDD<HoodieRecord> smallRecordsRDD = jsc.parallelize(records.subList(0, 75), 1);
        // We create three parquet file, each having one record. (two different partitions)
        List<WriteStatus> statuses = writeClient.bulkInsert(smallRecordsRDD, newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        readClient = new HoodieReadClient(jsc, config.getBasePath());
        filteredRDD = readClient.filterExists(recordsRDD);
        List<HoodieRecord> result = filteredRDD.collect();
        // Check results
        assertTrue(result.size() == 25);
    }

    @Test
    public void testAutoCommit() throws Exception {
        // Set autoCommit false
        HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

        String newCommitTime = "001";
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, newCommitTime);

        assertFalse("If Autocommit is false, then commit should not be made automatically",
            HoodieTestUtils.doesCommitExist(basePath, newCommitTime));
        assertTrue("Commit should succeed", client.commit(newCommitTime, result));
        assertTrue("After explicit commit, commit file should be created",
            HoodieTestUtils.doesCommitExist(basePath, newCommitTime));

        newCommitTime = "002";
        records = dataGen.generateUpdates(newCommitTime, 100);
        JavaRDD<HoodieRecord> updateRecords = jsc.parallelize(records, 1);
        result = client.upsert(updateRecords, newCommitTime);
        assertFalse("If Autocommit is false, then commit should not be made automatically",
            HoodieTestUtils.doesCommitExist(basePath, newCommitTime));
        assertTrue("Commit should succeed", client.commit(newCommitTime, result));
        assertTrue("After explicit commit, commit file should be created",
            HoodieTestUtils.doesCommitExist(basePath, newCommitTime));
    }

    @Test
    public void testUpserts() throws Exception {
        HoodieWriteConfig cfg = getConfig();
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
        FileSystem fs = FSUtils.getFs();

        /**
         * Write 1 (only inserts)
         */
        String newCommitTime = "001";
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
        assertNoWriteErrors(statuses);

        // check the partition metadata is written out
        assertPartitionMetadata(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, fs);

        // verify that there is a commit
        HoodieReadClient readClient = new HoodieReadClient(jsc, basePath, sqlContext);
        assertEquals("Expecting a single commit.", readClient.listCommitsSince("000").size(), 1);
        assertEquals("Latest commit should be 001",readClient.latestCommit(), newCommitTime);
        assertEquals("Must contain 200 records", readClient.readCommit(newCommitTime).count(), records.size());
        // Should have 100 records in table (check using Index), all in locations marked at commit
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());

        List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(records, 1), table).collect();
        checkTaggedRecords(taggedRecords, "001");

        /**
         * Write 2 (updates)
         */
        newCommitTime = "004";
        records = dataGen.generateUpdates(newCommitTime, 100);
        LinkedHashMap<HoodieKey, HoodieRecord> recordsMap = new LinkedHashMap<>();
        for (HoodieRecord rec : records) {
            if (!recordsMap.containsKey(rec.getKey())) {
                recordsMap.put(rec.getKey(), rec);
            }
        }
        List<HoodieRecord> dedupedRecords = new ArrayList<>(recordsMap.values());

        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // verify there are now 2 commits
        readClient = new HoodieReadClient(jsc, basePath, sqlContext);
        assertEquals("Expecting two commits.", readClient.listCommitsSince("000").size(), 2);
        assertEquals("Latest commit should be 004",readClient.latestCommit(), newCommitTime);

        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());

        // Index should be able to locate all updates in correct locations.
        taggedRecords = index.tagLocation(jsc.parallelize(dedupedRecords, 1), table).collect();
        checkTaggedRecords(taggedRecords, "004");

        // Check the entire dataset has 100 records still
        String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
        for (int i=0; i < fullPartitionPaths.length; i++) {
            fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
        }
        assertEquals("Must contain 200 records", readClient.read(fullPartitionPaths).count(), 200);


        // Check that the incremental consumption from time 000
        assertEquals("Incremental consumption from time 002, should give all records in commit 004",
            readClient.readCommit(newCommitTime).count(),
            readClient.readSince("002").count());
        assertEquals("Incremental consumption from time 001, should give all records in commit 004",
            readClient.readCommit(newCommitTime).count(),
            readClient.readSince("001").count());
    }

    @Test
    public void testDeletes() throws Exception {

        HoodieWriteConfig cfg = getConfig();
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
        FileSystem fs = FSUtils.getFs();

        /**
         * Write 1 (inserts and deletes)
         * Write actual 200 insert records and ignore 100 delete records
         */
        String newCommitTime = "001";
        List<HoodieRecord> fewRecordsForInsert = dataGen.generateInserts(newCommitTime, 200);
        List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletes(newCommitTime, 100);

        List<HoodieRecord> records = new ArrayList(fewRecordsForInsert);
        records.addAll(fewRecordsForDelete);

        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
        assertNoWriteErrors(statuses);

        // verify that there is a commit
        HoodieReadClient readClient = new HoodieReadClient(jsc, basePath, sqlContext);
        assertEquals("Expecting a single commit.", readClient.listCommitsSince("000").size(), 1);
        assertEquals("Latest commit should be 001",readClient.latestCommit(), newCommitTime);
        assertEquals("Must contain 200 records", readClient.readCommit(newCommitTime).count(), fewRecordsForInsert.size());
        // Should have 100 records in table (check using Index), all in locations marked at commit
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());

        List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(fewRecordsForInsert, 1), table).collect();
        checkTaggedRecords(taggedRecords, "001");

        /**
         * Write 2 (deletes+writes)
         */
        newCommitTime = "004";
        fewRecordsForDelete = records.subList(0,50);
        List<HoodieRecord> fewRecordsForUpdate = records.subList(50,100);
        records = dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete);

        records.addAll(fewRecordsForUpdate);

        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // verify there are now 2 commits
        readClient = new HoodieReadClient(jsc, basePath, sqlContext);
        assertEquals("Expecting two commits.", readClient.listCommitsSince("000").size(), 2);
        assertEquals("Latest commit should be 004",readClient.latestCommit(), newCommitTime);

        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());

        // Check the entire dataset has 150 records(200-50) still
        String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
        for (int i=0; i < fullPartitionPaths.length; i++) {
            fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
        }
        assertEquals("Must contain 150 records", readClient.read(fullPartitionPaths).count(), 150);


        // Check that the incremental consumption from time 000
        assertEquals("Incremental consumption from latest commit, should give 50 updated records",
                readClient.readCommit(newCommitTime).count(),
                50);
        assertEquals("Incremental consumption from time 001, should give 50 updated records",
                50,
                readClient.readSince("001").count());
        assertEquals("Incremental consumption from time 000, should give 150",
                150,
                readClient.readSince("000").count());
    }


    @Test
    public void testCreateSavepoint() throws Exception {
        HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1)
                .build()).build();
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        FileSystem fs = FSUtils.getFs();
        HoodieTestDataGenerator.writePartitionMetadata(fs, HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, basePath);

        /**
         * Write 1 (only inserts)
         */
        String newCommitTime = "001";
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
        List<WriteStatus> statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        assertNoWriteErrors(statuses);

        /**
         * Write 2 (updates)
         */
        newCommitTime = "002";
        records = dataGen.generateUpdates(newCommitTime, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        client.savepoint("hoodie-unit-test", "test");
        try {
            client.rollback(newCommitTime);
            fail("Rollback of a savepoint was allowed " + newCommitTime);
        } catch (HoodieRollbackException e) {
            // this is good
        }

        /**
         * Write 3 (updates)
         */
        newCommitTime = "003";
        records = dataGen.generateUpdates(newCommitTime, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        /**
         * Write 4 (updates)
         */
        newCommitTime = "004";
        records = dataGen.generateUpdates(newCommitTime, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        List<String> partitionPaths = FSUtils.getAllPartitionPaths(fs, cfg.getBasePath(), getConfig().shouldAssumeDatePartitioning());
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());
        final TableFileSystemView.ReadOptimizedView view = table.getROFileSystemView();
        List<HoodieDataFile> dataFiles = partitionPaths.stream().flatMap(s -> {
            return view.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("002"));
        }).collect(Collectors.toList());

        assertEquals("The data files for commit 002 should not be cleaned", 3, dataFiles.size());

        // Delete savepoint
        assertFalse(table.getCompletedSavepointTimeline().empty());
        client.deleteSavepoint(
            table.getCompletedSavepointTimeline().getInstants().findFirst().get().getTimestamp());
        // rollback and reupsert 004
        client.rollback(newCommitTime);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());
        final TableFileSystemView.ReadOptimizedView view1 = table.getROFileSystemView();
        dataFiles = partitionPaths.stream().flatMap(s -> {
            return view1.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("002"));
        }).collect(Collectors.toList());

        assertEquals("The data files for commit 002 should be cleaned now", 0, dataFiles.size());
    }


    @Test
    public void testRollbackToSavepoint() throws Exception {
        HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1)
                .build()).build();
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        FileSystem fs = FSUtils.getFs();
        HoodieTestDataGenerator.writePartitionMetadata(fs, HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, basePath);

        /**
         * Write 1 (only inserts)
         */
        String newCommitTime = "001";
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
        assertNoWriteErrors(statuses);

        /**
         * Write 2 (updates)
         */
        newCommitTime = "002";
        records = dataGen.generateUpdates(newCommitTime, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        client.savepoint("hoodie-unit-test", "test");

        /**
         * Write 3 (updates)
         */
        newCommitTime = "003";
        records = dataGen.generateUpdates(newCommitTime, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);
        List<String> partitionPaths = FSUtils.getAllPartitionPaths(fs, cfg.getBasePath(), getConfig().shouldAssumeDatePartitioning());
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());
        final TableFileSystemView.ReadOptimizedView view1 = table.getROFileSystemView();

        List<HoodieDataFile> dataFiles = partitionPaths.stream().flatMap(s -> {
            return view1.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("003"));
        }).collect(Collectors.toList());
        assertEquals("The data files for commit 003 should be present", 3, dataFiles.size());


        /**
         * Write 4 (updates)
         */
        newCommitTime = "004";
        records = dataGen.generateUpdates(newCommitTime, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());
        final TableFileSystemView.ReadOptimizedView view2 = table.getROFileSystemView();

        dataFiles = partitionPaths.stream().flatMap(s -> {
            return view2.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("004"));
        }).collect(Collectors.toList());
        assertEquals("The data files for commit 004 should be present", 3, dataFiles.size());


        // rolling back to a non existent savepoint must not succeed
        try {
            client.rollbackToSavepoint("001");
            fail("Rolling back to non-existent savepoint should not be allowed");
        } catch (HoodieRollbackException e) {
            // this is good
        }

        // rollback to savepoint 002
        HoodieInstant savepoint =
            table.getCompletedSavepointTimeline().getInstants().findFirst().get();
        client.rollbackToSavepoint(savepoint.getTimestamp());

        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());
        final TableFileSystemView.ReadOptimizedView view3 = table.getROFileSystemView();
        dataFiles = partitionPaths.stream().flatMap(s -> {
            return view3.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("002"));
        }).collect(Collectors.toList());
        assertEquals("The data files for commit 002 be available", 3, dataFiles.size());

        dataFiles = partitionPaths.stream().flatMap(s -> {
            return view3.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("003"));
        }).collect(Collectors.toList());
        assertEquals("The data files for commit 003 should be rolled back", 0, dataFiles.size());

        dataFiles = partitionPaths.stream().flatMap(s -> {
            return view3.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("004"));
        }).collect(Collectors.toList());
        assertEquals("The data files for commit 004 should be rolled back", 0, dataFiles.size());
    }


    @Test
    public void testInsertAndCleanByVersions() throws Exception {
        int maxVersions = 2; // keep upto 2 versions for each file
        HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
                .retainFileVersions(maxVersions).build()).build();
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
        FileSystem fs = FSUtils.getFs();

        /**
         * do a big insert
         * (this is basically same as insert part of upsert, just adding it here so we can
         * catch breakages in insert(), if the implementation diverges.)
         */
        String newCommitTime = client.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 500);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 5);

        List<WriteStatus> statuses = client.insert(writeRecords, newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // verify that there is a commit

        assertEquals("Expecting a single commit.", new HoodieReadClient(jsc, basePath).listCommitsSince("000").size(), 1);
        // Should have 100 records in table (check using Index), all in locations marked at commit
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());
        assertFalse(table.getCompletedCommitTimeline().empty());
        String commitTime =
            table.getCompletedCommitTimeline().getInstants().findFirst().get().getTimestamp();
        assertFalse(table.getCompletedCleanTimeline().empty());
        assertEquals("The clean instant should be the same as the commit instant", commitTime,
            table.getCompletedCleanTimeline().getInstants().findFirst().get().getTimestamp());

        List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(records, 1), table).collect();
        checkTaggedRecords(taggedRecords, newCommitTime);

        // Keep doing some writes and clean inline. Make sure we have expected number of files remaining.
        for (int writeCnt = 2; writeCnt < 10; writeCnt++) {

            Thread.sleep(1100); // make sure commits are unique
            newCommitTime = client.startCommit();
            records = dataGen.generateUpdates(newCommitTime, 100);

            statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
            // Verify there are no errors
            assertNoWriteErrors(statuses);

            HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
            table = HoodieTable.getHoodieTable(metadata, getConfig());
            HoodieTimeline timeline = table.getCommitTimeline();

            TableFileSystemView fsView = table.getFileSystemView();
            // Need to ensure the following
            for (String partitionPath : dataGen.getPartitionPaths()) {
                // compute all the versions of all files, from time 0
                HashMap<String, TreeSet<String>> fileIdToVersions = new HashMap<>();
                for (HoodieInstant entry : timeline.getInstants().collect(Collectors.toList())) {
                    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(entry).get());

                    for (HoodieWriteStat wstat : commitMetadata.getWriteStats(partitionPath)) {
                        if (!fileIdToVersions.containsKey(wstat.getFileId())) {
                            fileIdToVersions.put(wstat.getFileId(), new TreeSet<>());
                        }
                        fileIdToVersions.get(wstat.getFileId()).add(FSUtils.getCommitTime(new Path(wstat.getPath()).getName()));
                    }
                }


                List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());

                for (HoodieFileGroup fileGroup : fileGroups) {
                    // No file has no more than max versions
                    String fileId = fileGroup.getId();
                    List<HoodieDataFile> dataFiles = fileGroup.getAllDataFiles().collect(Collectors.toList());

                    assertTrue("fileId " + fileId + " has more than " + maxVersions + " versions",
                            dataFiles.size() <= maxVersions);

                    // Each file, has the latest N versions (i.e cleaning gets rid of older versions)
                    List<String> commitedVersions = new ArrayList<>(fileIdToVersions.get(fileId));
                    for (int i = 0; i < dataFiles.size(); i++) {
                        assertEquals("File " + fileId + " does not have latest versions on commits" + commitedVersions,
                            Iterables.get(dataFiles, i).getCommitTime(),
                                commitedVersions.get(commitedVersions.size() - 1 - i));
                    }
                }
            }
        }
    }

    @Test
    public void testInsertAndCleanByCommits() throws Exception {
        int maxCommits = 3; // keep upto 3 commits from the past
        HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
                .retainCommits(maxCommits).build()).build();
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
        FileSystem fs = FSUtils.getFs();

        /**
         * do a big insert
         * (this is basically same as insert part of upsert, just adding it here so we can
         * catch breakages in insert(), if the implementation diverges.)
         */
        String newCommitTime = client.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 500);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 5);

        List<WriteStatus> statuses = client.insert(writeRecords, newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // verify that there is a commit
        assertEquals("Expecting a single commit.", new HoodieReadClient(jsc, basePath).listCommitsSince("000").size(), 1);
        // Should have 100 records in table (check using Index), all in locations marked at commit
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());

        assertFalse(table.getCompletedCommitTimeline().empty());
        String commitTime =
            table.getCompletedCommitTimeline().getInstants().findFirst().get().getTimestamp();
        assertFalse(table.getCompletedCleanTimeline().empty());
        assertEquals("The clean instant should be the same as the commit instant", commitTime,
            table.getCompletedCleanTimeline().getInstants().findFirst().get().getTimestamp());

        List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(records, 1), table).collect();
        checkTaggedRecords(taggedRecords, newCommitTime);

        // Keep doing some writes and clean inline. Make sure we have expected number of files remaining.
        for (int writeCnt = 2; writeCnt < 10; writeCnt++) {
            Thread.sleep(1100); // make sure commits are unique
            newCommitTime = client.startCommit();
            records = dataGen.generateUpdates(newCommitTime, 100);

            statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
            // Verify there are no errors
            assertNoWriteErrors(statuses);

            HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
            HoodieTable table1 = HoodieTable.getHoodieTable(metadata, cfg);
            HoodieTimeline activeTimeline = table1.getCompletedCommitTimeline();
            Optional<HoodieInstant>
                earliestRetainedCommit = activeTimeline.nthFromLastInstant(maxCommits - 1);
            Set<HoodieInstant> acceptableCommits =
                activeTimeline.getInstants().collect(Collectors.toSet());
            if (earliestRetainedCommit.isPresent()) {
                acceptableCommits.removeAll(
                    activeTimeline.findInstantsInRange("000", earliestRetainedCommit.get().getTimestamp()).getInstants()
                        .collect(Collectors.toSet()));
                acceptableCommits.add(earliestRetainedCommit.get());
            }

            TableFileSystemView fsView = table1.getFileSystemView();
            // Need to ensure the following
            for (String partitionPath : dataGen.getPartitionPaths()) {
                List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
                for (HoodieFileGroup fileGroup : fileGroups) {
                    Set<String> commitTimes = new HashSet<>();
                    fileGroup.getAllDataFiles().forEach(value ->  {
                        System.out.println("Data File - " + value);
                        commitTimes.add(value.getCommitTime());
                    });
                    assertEquals("Only contain acceptable versions of file should be present",
                        acceptableCommits.stream().map(HoodieInstant::getTimestamp)
                            .collect(Collectors.toSet()), commitTimes);
                }
            }
        }
    }

    @Test
    public void testRollbackCommit() throws Exception {
        // Let's create some commit files and parquet files
        String commitTime1 = "20160501010101";
        String commitTime2 = "20160502020601";
        String commitTime3 = "20160506030611";
        new File(basePath + "/.hoodie").mkdirs();
        HoodieTestDataGenerator.writePartitionMetadata(FSUtils.getFs(),
                new String[] {"2016/05/01", "2016/05/02", "2016/05/06"},
                basePath);


        // Only first two have commit files
        HoodieTestUtils.createCommitFiles(basePath, commitTime1, commitTime2);
        // Third one has a .inflight intermediate commit file
        HoodieTestUtils.createInflightCommitFiles(basePath, commitTime3);

        // Make commit1
        String file11 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime1, "id11");
        String file12 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime1, "id12");
        String file13 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime1, "id13");

        // Make commit2
        String file21 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime2, "id21");
        String file22 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime2, "id22");
        String file23 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime2, "id23");

        // Make commit3
        String file31 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime3, "id31");
        String file32 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime3, "id32");
        String file33 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime3, "id33");

        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withIndexConfig(
                HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY)
                    .build()).build();

        HoodieWriteClient client = new HoodieWriteClient(jsc, config, false);

        // Rollback commit 1 (this should fail, since commit2 is still around)
        try {
            client.rollback(commitTime1);
            assertTrue("Should have thrown an exception ", false);
        } catch (HoodieRollbackException hrbe) {
            // should get here
        }

        // Rollback commit3
        client.rollback(commitTime3);
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime3));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime3, file31) ||
                    HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime3, file32) ||
                    HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime3, file33));

        // simulate partial failure, where .inflight was not deleted, but data files were.
        HoodieTestUtils.createInflightCommitFiles(basePath, commitTime3);
        client.rollback(commitTime3);
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime3));


        // Rollback commit2
        client.rollback(commitTime2);
        assertFalse(HoodieTestUtils.doesCommitExist(basePath, commitTime2));
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21) ||
                    HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22) ||
                    HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));

        // simulate partial failure, where only .commit => .inflight renaming succeeded, leaving a
        // .inflight commit and a bunch of data files around.
        HoodieTestUtils.createInflightCommitFiles(basePath, commitTime2);
        file21 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime2, "id21");
        file22 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime2, "id22");
        file23 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime2, "id23");

        client.rollback(commitTime2);
        assertFalse(HoodieTestUtils.doesCommitExist(basePath, commitTime2));
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21) ||
                    HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22) ||
                    HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));


        // Let's rollback commit1, Check results
        client.rollback(commitTime1);
        assertFalse(HoodieTestUtils.doesCommitExist(basePath, commitTime1));
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime1));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime1, file11) ||
                    HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime1, file12) ||
                    HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime1, file13));
    }


    @Test
    public void testAutoRollbackCommit() throws Exception {
        // Let's create some commit files and parquet files
        String commitTime1 = "20160501010101";
        String commitTime2 = "20160502020601";
        String commitTime3 = "20160506030611";
        new File(basePath + "/.hoodie").mkdirs();
        HoodieTestDataGenerator.writePartitionMetadata(FSUtils.getFs(),
                new String[] {"2016/05/01", "2016/05/02", "2016/05/06"},
                basePath);

        // One good commit
        HoodieTestUtils.createCommitFiles(basePath, commitTime1);
        // Two inflight commits
        HoodieTestUtils.createInflightCommitFiles(basePath, commitTime2, commitTime3);

        // Make commit1
        String file11 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime1, "id11");
        String file12 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime1, "id12");
        String file13 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime1, "id13");

        // Make commit2
        String file21 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime2, "id21");
        String file22 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime2, "id22");
        String file23 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime2, "id23");

        // Make commit3
        String file31 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime3, "id31");
        String file32 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime3, "id32");
        String file33 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime3, "id33");

        // Turn auto rollback off
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withIndexConfig(
                HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY)
                    .build()).build();

        new HoodieWriteClient(jsc, config, false);

        // Check results, nothing changed
        assertTrue(HoodieTestUtils.doesCommitExist(basePath, commitTime1));
        assertTrue(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
        assertTrue(HoodieTestUtils.doesInflightExist(basePath, commitTime3));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime3, file31) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime3, file32) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime3, file33));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime1, file11) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime1, file12) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime1, file13));

        // Turn auto rollback on
        new HoodieWriteClient(jsc, config, true);
        assertTrue(HoodieTestUtils.doesCommitExist(basePath, commitTime1));
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime3));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime3, file31) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime3, file32) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime3, file33));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime1, file11) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime1, file12) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime1, file13));
    }


    private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize) {
        HoodieWriteConfig.Builder builder = getConfigBuilder();
        return builder.withCompactionConfig(
                HoodieCompactionConfig.newBuilder()
                        .compactionSmallFileSize(HoodieTestDataGenerator.SIZE_PER_RECORD * 15)
                        .insertSplitSize(insertSplitSize).build()) // tolerate upto 15 records
                .withStorageConfig(HoodieStorageConfig.newBuilder()
                        .limitFileSize(HoodieTestDataGenerator.SIZE_PER_RECORD * 20)
                        .build())
                .build();
    }


    @Test
    public void testSmallInsertHandlingForUpserts() throws Exception {

        FileSystem fs = FSUtils.getFs();
        final String TEST_PARTITION_PATH = "2016/09/26";
        final int INSERT_SPLIT_LIMIT = 10;
        // setup the small file handling params
        HoodieWriteConfig config = getSmallInsertWriteConfig(INSERT_SPLIT_LIMIT); // hold upto 20 records max
        dataGen = new HoodieTestDataGenerator(new String[] {TEST_PARTITION_PATH});

        HoodieWriteClient client = new HoodieWriteClient(jsc, config);

        // Inserts => will write file1
        String commitTime1 = "001";
        List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, INSERT_SPLIT_LIMIT); // this writes ~500kb
        Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);

        JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
        List<WriteStatus> statuses= client.upsert(insertRecordsRDD1, commitTime1).collect();

        assertNoWriteErrors(statuses);

        assertEquals("Just 1 file needs to be added.", 1, statuses.size());
        String file1 = statuses.get(0).getFileId();
        assertEquals("file should contain 10 records",
                ParquetUtils.readRowKeysFromParquet(new Path(basePath, TEST_PARTITION_PATH + "/" + FSUtils.makeDataFileName(commitTime1, 0, file1))).size(),
                10);

        // Update + Inserts such that they just expand file1
        String commitTime2 = "002";
        List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 4);
        Set<String> keys2 = HoodieClientTestUtils.getRecordKeys(inserts2);
        List<HoodieRecord> insertsAndUpdates2 = new ArrayList<>();
        insertsAndUpdates2.addAll(inserts2);
        insertsAndUpdates2.addAll(dataGen.generateUpdates(commitTime2, inserts1));

        JavaRDD<HoodieRecord> insertAndUpdatesRDD2 = jsc.parallelize(insertsAndUpdates2, 1);
        statuses = client.upsert(insertAndUpdatesRDD2, commitTime2).collect();
        assertNoWriteErrors(statuses);

        assertEquals("Just 1 file needs to be updated.", 1, statuses.size());
        assertEquals("Existing file should be expanded", file1, statuses.get(0).getFileId());
        assertEquals("Existing file should be expanded", commitTime1, statuses.get(0).getStat().getPrevCommit());
        Path newFile = new Path(basePath, TEST_PARTITION_PATH + "/" + FSUtils.makeDataFileName(commitTime2, 0, file1));
        assertEquals("file should contain 14 records", ParquetUtils.readRowKeysFromParquet(newFile).size(), 14);

        List<GenericRecord> records = ParquetUtils.readAvroRecords(newFile);
        for (GenericRecord record: records) {
            String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
            assertEquals("only expect commit2", commitTime2, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
            assertTrue("key expected to be part of commit2", keys2.contains(recordKey) || keys1.contains(recordKey));
        }

        // update + inserts such that file1 is updated and expanded, a new file2 is created.
        String commitTime3 = "003";
        List<HoodieRecord> insertsAndUpdates3 = dataGen.generateInserts(commitTime3, 20);
        Set<String> keys3 = HoodieClientTestUtils.getRecordKeys(insertsAndUpdates3);
        List<HoodieRecord> updates3 = dataGen.generateUpdates(commitTime3, inserts2);
        insertsAndUpdates3.addAll(updates3);

        JavaRDD<HoodieRecord> insertAndUpdatesRDD3 = jsc.parallelize(insertsAndUpdates3, 1);
        statuses = client.upsert(insertAndUpdatesRDD3, commitTime3).collect();
        assertNoWriteErrors(statuses);

        assertEquals("2 files needs to be committed.", 2, statuses.size());
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metadata, config);
        TableFileSystemView.ReadOptimizedView fileSystemView = table.getROFileSystemView();
        List<HoodieDataFile> files = fileSystemView.getLatestDataFilesBeforeOrOn(TEST_PARTITION_PATH, commitTime3).collect(
            Collectors.toList());
        int numTotalInsertsInCommit3 = 0;
        for (HoodieDataFile file:  files) {
            if (file.getFileName().contains(file1)) {
                assertEquals("Existing file should be expanded", commitTime3, file.getCommitTime());
                records = ParquetUtils.readAvroRecords(new Path(file.getPath()));
                for (GenericRecord record: records) {
                    String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
                    String recordCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
                    if (recordCommitTime.equals(commitTime3)) {
                        if (keys2.contains(recordKey)) {
                            assertEquals("only expect commit3", commitTime3, recordCommitTime);
                            keys2.remove(recordKey);
                        } else {
                            numTotalInsertsInCommit3++;
                        }
                    }
                }
                assertEquals("All keys added in commit 2 must be updated in commit3 correctly", 0, keys2.size());
            } else {
                assertEquals("New file must be written for commit 3", commitTime3, file.getCommitTime());
                records = ParquetUtils.readAvroRecords(new Path(file.getPath()));
                for (GenericRecord record: records) {
                    String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
                    assertEquals("only expect commit3", commitTime3, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
                    assertTrue("key expected to be part of commit3", keys3.contains(recordKey));
                }
                numTotalInsertsInCommit3 += records.size();
            }
        }
        assertEquals("Total inserts in commit3 must add up", keys3.size(), numTotalInsertsInCommit3);
    }

    @Test
    public void testSmallInsertHandlingForInserts() throws Exception {

        final String TEST_PARTITION_PATH = "2016/09/26";
        final int INSERT_SPLIT_LIMIT = 10;
        // setup the small file handling params
        HoodieWriteConfig config = getSmallInsertWriteConfig(INSERT_SPLIT_LIMIT); // hold upto 20 records max
        dataGen = new HoodieTestDataGenerator(new String[] {TEST_PARTITION_PATH});
        HoodieWriteClient client = new HoodieWriteClient(jsc, config);

        // Inserts => will write file1
        String commitTime1 = "001";
        List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, INSERT_SPLIT_LIMIT); // this writes ~500kb
        Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);
        JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
        List<WriteStatus> statuses= client.insert(insertRecordsRDD1, commitTime1).collect();

        assertNoWriteErrors(statuses);
        assertPartitionMetadata(new String[]{TEST_PARTITION_PATH}, FSUtils.getFs());

        assertEquals("Just 1 file needs to be added.", 1, statuses.size());
        String file1 = statuses.get(0).getFileId();
        assertEquals("file should contain 10 records",
                ParquetUtils.readRowKeysFromParquet(new Path(basePath, TEST_PARTITION_PATH + "/" + FSUtils.makeDataFileName(commitTime1, 0, file1))).size(),
                10);

        // Second, set of Inserts should just expand file1
        String commitTime2 = "002";
        List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 4);
        Set<String> keys2 = HoodieClientTestUtils.getRecordKeys(inserts2);
        JavaRDD<HoodieRecord> insertRecordsRDD2 = jsc.parallelize(inserts2, 1);
        statuses = client.insert(insertRecordsRDD2, commitTime2).collect();
        assertNoWriteErrors(statuses);

        assertEquals("Just 1 file needs to be updated.", 1, statuses.size());
        assertEquals("Existing file should be expanded", file1, statuses.get(0).getFileId());
        assertEquals("Existing file should be expanded", commitTime1, statuses.get(0).getStat().getPrevCommit());
        Path newFile = new Path(basePath, TEST_PARTITION_PATH + "/" + FSUtils.makeDataFileName(commitTime2, 0, file1));
        assertEquals("file should contain 14 records", ParquetUtils.readRowKeysFromParquet(newFile).size(), 14);

        List<GenericRecord> records = ParquetUtils.readAvroRecords(newFile);
        for (GenericRecord record: records) {
            String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
            String recCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
            assertTrue("Record expected to be part of commit 1 or commit2", commitTime1.equals(recCommitTime) || commitTime2.equals(recCommitTime));
            assertTrue("key expected to be part of commit 1 or commit2", keys2.contains(recordKey) || keys1.contains(recordKey));
        }

        // Lots of inserts such that file1 is updated and expanded, a new file2 is created.
        String commitTime3 = "003";
        List<HoodieRecord> insert3 = dataGen.generateInserts(commitTime3, 20);
        JavaRDD<HoodieRecord> insertRecordsRDD3 = jsc.parallelize(insert3, 1);
        statuses = client.insert(insertRecordsRDD3, commitTime3).collect();
        assertNoWriteErrors(statuses);
        assertEquals("2 files needs to be committed.", 2, statuses.size());


        FileSystem fs = FSUtils.getFs();
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, config);
        List<HoodieDataFile> files =
            table.getROFileSystemView().getLatestDataFilesBeforeOrOn(TEST_PARTITION_PATH, commitTime3)
                .collect(Collectors.toList());
        assertEquals("Total of 2 valid data files", 2, files.size());


        int totalInserts = 0;
        for (HoodieDataFile file:  files) {
            assertEquals("All files must be at commit 3", commitTime3, file.getCommitTime());
            records = ParquetUtils.readAvroRecords(new Path(file.getPath()));
            totalInserts += records.size();
        }
        assertEquals("Total number of records must add up", totalInserts, inserts1.size() + inserts2.size() + insert3.size());
    }

    @Test
    public void testKeepLatestFileVersions() throws IOException {
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withAssumeDatePartitioning(true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
                .retainFileVersions(1).build()).build();

        // make 1 commit, with 1 file per partition
        HoodieTestUtils.createCommitFiles(basePath, "000");

        String file1P0C0 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "000");
        String file1P1C0 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[1], "000");
        HoodieTable table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(FSUtils.getFs(), config.getBasePath(), true), config);

        List<HoodieCleanStat> hoodieCleanStatsOne = table.clean(jsc);
        assertEquals("Must not clean any files" , 0, getCleanStat(hoodieCleanStatsOne, partitionPaths[0]).getSuccessDeleteFiles().size());
        assertEquals("Must not clean any files" , 0, getCleanStat(hoodieCleanStatsOne, partitionPaths[1]).getSuccessDeleteFiles().size());
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "000", file1P1C0));

        // make next commit, with 1 insert & 1 update per partition
        HoodieTestUtils.createCommitFiles(basePath, "001");
        table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(FSUtils.getFs(), config.getBasePath(), true), config);

        String file2P0C1 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "001"); // insert
        String file2P1C1 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[1], "001"); // insert
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "001", file1P0C0); // update
        HoodieTestUtils.createDataFile(basePath, partitionPaths[1], "001", file1P1C0); // update

        List<HoodieCleanStat> hoodieCleanStatsTwo = table.clean(jsc);
        assertEquals("Must clean 1 file" , 1, getCleanStat(hoodieCleanStatsTwo, partitionPaths[0]).getSuccessDeleteFiles().size());
        assertEquals("Must clean 1 file" , 1, getCleanStat(hoodieCleanStatsTwo, partitionPaths[1]).getSuccessDeleteFiles().size());
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "001", file2P1C1));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "000", file1P1C0));

        // make next commit, with 2 updates to existing files, and 1 insert
        HoodieTestUtils.createCommitFiles(basePath, "002");
        table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(FSUtils.getFs(), config.getBasePath(), true), config);

        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "002", file1P0C0); // update
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "002", file2P0C1); // update
        String file3P0C2 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "002");

        List<HoodieCleanStat> hoodieCleanStatsThree = table.clean(jsc);
        assertEquals("Must clean two files" , 2, getCleanStat(hoodieCleanStatsThree, partitionPaths[0]).getSuccessDeleteFiles().size());
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file1P0C0));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file3P0C2));

        // No cleaning on partially written file, with no commit.
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "003", file3P0C2); // update
        List<HoodieCleanStat> hoodieCleanStatsFour = table.clean(jsc);
        assertEquals("Must not clean any files" , 0, getCleanStat(hoodieCleanStatsFour, partitionPaths[0]).getSuccessDeleteFiles().size());
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file3P0C2));
    }

    @Test
    public void testKeepLatestCommits() throws IOException {
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withAssumeDatePartitioning(true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
                .retainCommits(2).build()).build();

        // make 1 commit, with 1 file per partition
        HoodieTestUtils.createCommitFiles(basePath, "000");

        String file1P0C0 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "000");
        String file1P1C0 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[1], "000");

        HoodieTable table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(FSUtils.getFs(), config.getBasePath(), true), config);

        List<HoodieCleanStat> hoodieCleanStatsOne = table.clean(jsc);
        assertEquals("Must not clean any files" , 0, getCleanStat(hoodieCleanStatsOne, partitionPaths[0]).getSuccessDeleteFiles().size());
        assertEquals("Must not clean any files" , 0, getCleanStat(hoodieCleanStatsOne, partitionPaths[1]).getSuccessDeleteFiles().size());
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "000", file1P1C0));

        // make next commit, with 1 insert & 1 update per partition
        HoodieTestUtils.createCommitFiles(basePath, "001");
        table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(FSUtils.getFs(), config.getBasePath(), true), config);

        String file2P0C1 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "001"); // insert
        String file2P1C1 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[1], "001"); // insert
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "001", file1P0C0); // update
        HoodieTestUtils.createDataFile(basePath, partitionPaths[1], "001", file1P1C0); // update

        List<HoodieCleanStat> hoodieCleanStatsTwo = table.clean(jsc);
        assertEquals("Must not clean any files" , 0, getCleanStat(hoodieCleanStatsTwo, partitionPaths[0]).getSuccessDeleteFiles().size());
        assertEquals("Must not clean any files" , 0, getCleanStat(hoodieCleanStatsTwo, partitionPaths[1]).getSuccessDeleteFiles().size());
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "001", file2P1C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "000", file1P1C0));

        // make next commit, with 2 updates to existing files, and 1 insert
        HoodieTestUtils.createCommitFiles(basePath, "002");
        table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(FSUtils.getFs(), config.getBasePath(), true), config);

        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "002", file1P0C0); // update
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "002", file2P0C1); // update
        String file3P0C2 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "002");

        List<HoodieCleanStat> hoodieCleanStatsThree = table.clean(jsc);
        assertEquals(
            "Must not clean any file. We have to keep 1 version before the latest commit time to keep",
            0,  getCleanStat(hoodieCleanStatsThree, partitionPaths[0]).getSuccessDeleteFiles().size());

        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));

        // make next commit, with 2 updates to existing files, and 1 insert
        HoodieTestUtils.createCommitFiles(basePath, "003");
        table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(FSUtils.getFs(), config.getBasePath(), true), config);

        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "003", file1P0C0); // update
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "003", file2P0C1); // update
        String file4P0C3 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "003");

        List<HoodieCleanStat> hoodieCleanStatsFour = table.clean(jsc);
        assertEquals(
            "Must not clean one old file", 1,  getCleanStat(hoodieCleanStatsFour, partitionPaths[0]).getSuccessDeleteFiles().size());

        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file2P0C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file3P0C2));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "003", file4P0C3));

        // No cleaning on partially written file, with no commit.
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "004", file3P0C2); // update
        List<HoodieCleanStat> hoodieCleanStatsFive = table.clean(jsc);
        assertEquals("Must not clean any files" , 0,  getCleanStat(hoodieCleanStatsFive, partitionPaths[0]).getSuccessDeleteFiles().size());
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
    }

    @Test
    public void testCleaningWithZeroPartitonPaths() throws IOException {
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withAssumeDatePartitioning(true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
                .retainCommits(2).build()).build();

        // Make a commit, although there are no partitionPaths.
        // Example use-case of this is when a client wants to create a table
        // with just some commit metadata, but no data/partitionPaths.
        HoodieTestUtils.createCommitFiles(basePath, "000");

        HoodieTable table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(FSUtils.getFs(), config.getBasePath(), true),
                config);

        List<HoodieCleanStat> hoodieCleanStatsOne = table.clean(jsc);
        assertTrue("HoodieCleanStats should be empty for a table with empty partitionPaths",
            hoodieCleanStatsOne.isEmpty());
    }

    @Test
    public void testCleaningSkewedPartitons() throws IOException {
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withAssumeDatePartitioning(true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
                .retainCommits(2).build()).build();
        Map<Long, Long> stageOneShuffleReadTaskRecordsCountMap = new HashMap<>();

        // Since clean involves repartition in order to uniformly distribute data,
        // we can inspect the number of records read by various tasks in stage 1.
        // There should not be skew in the number of records read in the task.

        // SparkListener below listens to the stage end events and captures number of
        // records read by various tasks in stage-1.
        jsc.sc().addSparkListener(new SparkListener() {

            @Override
            public void onTaskEnd(SparkListenerTaskEnd taskEnd) {

                Iterator<AccumulatorV2<?, ?>> iterator = taskEnd.taskMetrics().accumulators()
                    .iterator();
                while(iterator.hasNext()) {
                    AccumulatorV2 accumulator = iterator.next();
                    if (taskEnd.stageId() == 1 &&
                        accumulator.isRegistered() &&
                        accumulator.name().isDefined() &&
                        accumulator.name().get().equals("internal.metrics.shuffle.read.recordsRead")) {
                        stageOneShuffleReadTaskRecordsCountMap.put(taskEnd.taskInfo().taskId(), (Long) accumulator.value());
                    }
                }
            }
        });

        // make 1 commit, with 100 files in one partition and 10 in other two
        HoodieTestUtils.createCommitFiles(basePath, "000");
        List<String> filesP0C0 = createFilesInPartition(partitionPaths[0], "000", 100);
        List<String> filesP1C0 = createFilesInPartition(partitionPaths[1], "000", 10);
        List<String> filesP2C0 = createFilesInPartition(partitionPaths[2], "000", 10);

        HoodieTestUtils.createCommitFiles(basePath, "001");
        updateAllFilesInPartition(filesP0C0, partitionPaths[0], "001");
        updateAllFilesInPartition(filesP1C0, partitionPaths[1], "001");
        updateAllFilesInPartition(filesP2C0, partitionPaths[2], "001");

        HoodieTestUtils.createCommitFiles(basePath, "002");
        updateAllFilesInPartition(filesP0C0, partitionPaths[0], "002");
        updateAllFilesInPartition(filesP1C0, partitionPaths[1], "002");
        updateAllFilesInPartition(filesP2C0, partitionPaths[2], "002");

        HoodieTestUtils.createCommitFiles(basePath, "003");
        updateAllFilesInPartition(filesP0C0, partitionPaths[0], "003");
        updateAllFilesInPartition(filesP1C0, partitionPaths[1], "003");
        updateAllFilesInPartition(filesP2C0, partitionPaths[2], "003");

        HoodieTable table = HoodieTable
            .getHoodieTable(new HoodieTableMetaClient(FSUtils.getFs(), config.getBasePath(), true), config);
        List<HoodieCleanStat> hoodieCleanStats = table.clean(jsc);

        assertEquals(100,  getCleanStat(hoodieCleanStats, partitionPaths[0]).getSuccessDeleteFiles().size());
        assertEquals(10, getCleanStat(hoodieCleanStats, partitionPaths[1]).getSuccessDeleteFiles().size());
        assertEquals(10, getCleanStat(hoodieCleanStats, partitionPaths[2]).getSuccessDeleteFiles().size());

        // 3 tasks are expected since the number of partitions is 3
        assertEquals(3, stageOneShuffleReadTaskRecordsCountMap.keySet().size());
        // Sum of all records processed = total number of files to clean
        assertEquals(120, stageOneShuffleReadTaskRecordsCountMap
            .values().stream().reduce((a,b) -> a + b).get().intValue());
        assertTrue("The skew in handling files to clean is not removed. "
                + "Each task should handle more records than the partitionPath with least files "
                + "and less records than the partitionPath with most files.",
            stageOneShuffleReadTaskRecordsCountMap.values().stream().filter(a -> a > 10 && a < 100).count() == 3);
    }

    public void testCommitWritesRelativePaths() throws Exception {

        HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        FileSystem fs = FSUtils.getFs();
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, cfg);

        String commitTime = "000";
        List<HoodieRecord> records = dataGen.generateInserts(commitTime, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, commitTime);

        assertTrue("Commit should succeed", client.commit(commitTime, result));
        assertTrue("After explicit commit, commit file should be created",
                HoodieTestUtils.doesCommitExist(basePath, commitTime));

        // Get parquet file paths from commit metadata
        String actionType = table.getCompactedCommitActionType();
        HoodieInstant commitInstant =
                new HoodieInstant(false, actionType, commitTime);
        HoodieTimeline commitTimeline = table.getCompletedCompactionCommitTimeline();
        HoodieCommitMetadata commitMetadata =
                HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commitInstant).get());
        String basePath = table.getMetaClient().getBasePath();
        Collection<String> commitPathNames = commitMetadata.getFileIdAndFullPaths(basePath).values();

        // Read from commit file
        String filename = HoodieTestUtils.getCommitFilePath(basePath, commitTime);
        FileInputStream inputStream = new FileInputStream(filename);
        String everything = IOUtils.toString(inputStream);
        HoodieCommitMetadata metadata = HoodieCommitMetadata.fromJsonString(everything.toString());
        HashMap<String, String> paths = metadata.getFileIdAndFullPaths(basePath);
        inputStream.close();

        // Compare values in both to make sure they are equal.
        for (String pathName : paths.values()) {
            assertTrue(commitPathNames.contains(pathName));
        }
    }

    private HoodieCleanStat getCleanStat(List<HoodieCleanStat> hoodieCleanStatsTwo,
        String partitionPath) {
        return hoodieCleanStatsTwo.stream()
            .filter(e -> e.getPartitionPath().equals(partitionPath))
            .findFirst().get();
    }

    private void updateAllFilesInPartition(List<String> files, String partitionPath,
        String commitTime) throws IOException {
        for (String fileId : files) {
            HoodieTestUtils.createDataFile(basePath, partitionPath, commitTime, fileId);
        }
    }

    private List<String> createFilesInPartition(String partitionPath, String commitTime, int numFiles) throws IOException {
        List<String> files = new ArrayList<>();
        for (int i = 0; i < numFiles; i++) {
            files.add(HoodieTestUtils.createNewDataFile(basePath, partitionPath, commitTime));
        }
        return files;
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
}
