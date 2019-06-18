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

package com.uber.hoodie.table;

import static com.uber.hoodie.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.uber.hoodie.HoodieReadClient;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieMergeOnReadTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.TestRawTripPayload.MetadataMergeWriteStatus;
import com.uber.hoodie.common.minicluster.HdfsTestService;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRollingStat;
import com.uber.hoodie.common.model.HoodieRollingStatMetadata;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.SyncableFileSystemView;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.index.HoodieIndex.IndexType;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMergeOnReadTable {

  protected String basePath = null;
  //NOTE : Be careful in using DFS (FileSystem.class) vs LocalFs(RawLocalFileSystem.class)
  //The implementation and gurantees of many API's differ, for example check rename(src,dst)
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;
  private static HdfsTestService hdfsTestService;
  private transient JavaSparkContext jsc = null;
  private transient SQLContext sqlContext;
  private HoodieWriteClient writeClient;

  @AfterClass
  public static void cleanUp() throws Exception {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
      dfsCluster.shutdown();
      dfsCluster = null;
      dfs = null;
      hdfsTestService = null;
    }
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the
    // same JVM
    FileSystem.closeAll();
  }

  @BeforeClass
  public static void setUpDFS() throws IOException {
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the
    // same JVM
    FileSystem.closeAll();
    if (hdfsTestService == null) {
      hdfsTestService = new HdfsTestService();
      dfsCluster = hdfsTestService.start(true);
      // Create a temp folder as the base path
      dfs = dfsCluster.getFileSystem();
    }
  }

  @Before
  public void init() throws IOException {
    // Initialize a local spark env
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestHoodieMergeOnReadTable"));

    // Create a temp folder as the base path
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
    jsc.hadoopConfiguration().addResource(dfs.getConf());

    dfs.mkdirs(new Path(basePath));
    HoodieTestUtils.initTableType(jsc.hadoopConfiguration(), basePath, HoodieTableType.MERGE_ON_READ);

    sqlContext = new SQLContext(jsc); // SQLContext stuff
  }

  @After
  public void clean() {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }

    if (basePath != null) {
      new File(basePath).delete();
    }
    if (jsc != null) {
      jsc.stop();
    }
  }

  private HoodieWriteClient getWriteClient(HoodieWriteConfig config) throws Exception {
    if (null != writeClient) {
      writeClient.close();
    }
    writeClient = new HoodieWriteClient(jsc, config);
    return writeClient;
  }

  @Test
  public void testSimpleInsertAndUpdate() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = getWriteClient(cfg);

    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);

    Optional<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals("Delta commit should be 001", "001", deltaCommit.get().getTimestamp());

    Optional<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());

    FileStatus[] allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    TableFileSystemView.ReadOptimizedView roView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    Stream<HoodieDataFile> dataFilesToRead = roView.getLatestDataFiles();
    assertTrue(!dataFilesToRead.findAny().isPresent());

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    assertTrue("RealtimeTableView should list the parquet files we wrote in the delta commit",
        dataFilesToRead.findAny().isPresent());

    /**
     * Write 2 (updates)
     */
    newCommitTime = "004";
    client.startCommitWithTime(newCommitTime);

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
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals("Latest Delta commit should be 004", "004", deltaCommit.get().getTimestamp());

    commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());

    String compactionCommitTime = client.scheduleCompaction(Optional.empty()).get().toString();
    client.compact(compactionCommitTime);

    allFiles = HoodieTestUtils.listAllDataFilesInPath(dfs, cfg.getBasePath());
    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    assertTrue(dataFilesToRead.findAny().isPresent());

    // verify that there is a commit
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath(), true);
    HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
    assertEquals("Expecting a single commit.", 1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants());
    String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
    assertTrue(HoodieTimeline.compareTimestamps("000", latestCompactionCommitTime, HoodieTimeline.LESSER));

    assertEquals("Must contain 200 records", 200,
        HoodieClientTestUtils.readSince(basePath, sqlContext, timeline, "000").count());
  }

  // Check if record level metadata is aggregated properly at the end of write.
  @Test
  public void testMetadataAggregateFromWriteStatus() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false).withWriteStatusClass(MetadataMergeWriteStatus.class).build();
    HoodieWriteClient client = getWriteClient(cfg);

    String newCommitTime = "001";
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    client.startCommit();

    List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);
    Map<String, String> allWriteStatusMergedMetadataMap = MetadataMergeWriteStatus
        .mergeMetadataForWriteStatuses(statuses);
    assertTrue(allWriteStatusMergedMetadataMap.containsKey("InputRecordCount_1506582000"));
    // For metadata key InputRecordCount_1506582000, value is 2 for each record. So sum of this
    // should be 2 * records.size()
    assertEquals(String.valueOf(2 * records.size()),
        allWriteStatusMergedMetadataMap.get("InputRecordCount_1506582000"));
  }

  @Test
  public void testSimpleInsertUpdateAndDelete() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = getWriteClient(cfg);

    /**
     * Write 1 (only inserts, written as parquet file)
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);

    Optional<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals("Delta commit should be 001", "001", deltaCommit.get().getTimestamp());

    Optional<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());

    FileStatus[] allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    TableFileSystemView.ReadOptimizedView roView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    Stream<HoodieDataFile> dataFilesToRead = roView.getLatestDataFiles();
    assertTrue(!dataFilesToRead.findAny().isPresent());

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    assertTrue("RealtimeTableView should list the parquet files we wrote in the delta commit",
        dataFilesToRead.findAny().isPresent());

    /**
     * Write 2 (only updates, written to .log file)
     */
    newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);

    records = dataGen.generateUpdates(newCommitTime, records);
    writeRecords = jsc.parallelize(records, 1);
    statuses = client.upsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);

    /**
     * Write 2 (only deletes, written to .log file)
     */
    newCommitTime = "004";
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletesFromExistingRecords(records);

    statuses = client.upsert(jsc.parallelize(fewRecordsForDelete, 1), newCommitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals("Latest Delta commit should be 004", "004", deltaCommit.get().getTimestamp());

    commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());

    allFiles = HoodieTestUtils.listAllDataFilesInPath(dfs, cfg.getBasePath());
    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    assertTrue(dataFilesToRead.findAny().isPresent());

    List<String> dataFiles = roView.getLatestDataFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
    List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath);
    //Wrote 20 records and deleted 20 records, so remaining 20-20 = 0
    assertEquals("Must contain 0 records", 0, recordsRead.size());
  }

  @Test
  public void testCOWToMORConvertedDatasetRollback() throws Exception {

    //Set TableType to COW
    HoodieTestUtils.initTableType(jsc.hadoopConfiguration(), basePath, HoodieTableType.COPY_ON_WRITE);

    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = getWriteClient(cfg);

    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
    //verify there are no errors
    assertNoWriteErrors(statuses);

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    Optional<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertTrue(commit.isPresent());
    assertEquals("commit should be 001", "001", commit.get().getTimestamp());

    /**
     * Write 2 (updates)
     */
    newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);

    records = dataGen.generateUpdates(newCommitTime, records);

    statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    //Set TableType to MOR
    HoodieTestUtils.initTableType(jsc.hadoopConfiguration(), basePath, HoodieTableType.MERGE_ON_READ);

    //rollback a COW commit when TableType is MOR
    client.rollback(newCommitTime);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    FileStatus[] allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    HoodieTableFileSystemView roView = new HoodieTableFileSystemView(metaClient,
        hoodieTable.getCompletedCommitsTimeline(), allFiles);

    final String absentCommit = newCommitTime;
    assertFalse(roView.getLatestDataFiles().filter(file -> {
      if (absentCommit.equals(file.getCommitTime())) {
        return true;
      } else {
        return false;
      }
    }).findAny().isPresent());
  }

  @Test
  public void testRollbackWithDeltaAndCompactionCommit() throws Exception {

    HoodieWriteConfig cfg = getConfig(false);
    HoodieWriteClient client = getWriteClient(cfg);

    // Test delta commit rollback
    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
    client.commit(newCommitTime, writeStatusJavaRDD);
    List<WriteStatus> statuses = writeStatusJavaRDD.collect();
    assertNoWriteErrors(statuses);

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);

    Optional<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals("Delta commit should be 001", "001", deltaCommit.get().getTimestamp());

    Optional<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());

    FileStatus[] allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    TableFileSystemView.ReadOptimizedView roView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    Stream<HoodieDataFile> dataFilesToRead = roView.getLatestDataFiles();
    assertTrue(!dataFilesToRead.findAny().isPresent());

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    assertTrue("RealtimeTableView should list the parquet files we wrote in the delta commit",
        dataFilesToRead.findAny().isPresent());

    /**
     * Write 2 (inserts + updates - testing failed delta commit)
     */
    final String commitTime1 = "002";
    // WriteClient with custom config (disable small file handling)
    client = getWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff());
    client.startCommitWithTime(commitTime1);

    List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
    copyOfRecords = dataGen.generateUpdates(commitTime1, copyOfRecords);
    copyOfRecords.addAll(dataGen.generateInserts(commitTime1, 200));

    List<String> dataFiles = roView.getLatestDataFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
    List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath);
    assertEquals(recordsRead.size(), 200);

    statuses = client.upsert(jsc.parallelize(copyOfRecords, 1), commitTime1).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    // Test failed delta commit rollback
    client.rollback(commitTime1);
    allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    // After rollback, there should be no parquet file with the failed commit time
    Assert.assertEquals(Arrays.asList(allFiles).stream().filter(file -> file.getPath().getName()
        .contains(commitTime1)).collect(Collectors.toList()).size(), 0);
    dataFiles = roView.getLatestDataFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
    recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath);
    assertEquals(recordsRead.size(), 200);


    /**
     * Write 3 (inserts + updates - testing successful delta commit)
     */
    final String commitTime2 = "002";
    client = getWriteClient(cfg);
    client.startCommitWithTime(commitTime2);

    copyOfRecords = new ArrayList<>(records);
    copyOfRecords = dataGen.generateUpdates(commitTime2, copyOfRecords);
    copyOfRecords.addAll(dataGen.generateInserts(commitTime2, 200));

    dataFiles = roView.getLatestDataFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
    recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath);
    assertEquals(recordsRead.size(), 200);

    writeRecords = jsc.parallelize(copyOfRecords, 1);
    writeStatusJavaRDD = client.upsert(writeRecords, commitTime2);
    client.commit(commitTime2, writeStatusJavaRDD);
    statuses = writeStatusJavaRDD.collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    // Test successful delta commit rollback
    client.rollback(commitTime2);
    allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    // After rollback, there should be no parquet file with the failed commit time
    Assert.assertEquals(Arrays.asList(allFiles).stream().filter(file -> file.getPath().getName()
        .contains(commitTime2)).collect(Collectors.toList()).size(), 0);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFiles = roView.getLatestDataFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
    recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath);
    // check that the number of records read is still correct after rollback operation
    assertEquals(recordsRead.size(), 200);

    // Test compaction commit rollback
    /**
     * Write 4 (updates)
     */
    newCommitTime = "003";
    client.startCommitWithTime(newCommitTime);

    records = dataGen.generateUpdates(newCommitTime, records);

    writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
    client.commit(newCommitTime, writeStatusJavaRDD);
    statuses = writeStatusJavaRDD.collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());

    String compactionInstantTime = client.scheduleCompaction(Optional.empty()).get().toString();
    JavaRDD<WriteStatus> ws = client.compact(compactionInstantTime);
    client.commitCompaction(compactionInstantTime, ws, Optional.empty());

    allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    roView = new HoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);
    List<HoodieDataFile> dataFiles2 = roView.getLatestDataFiles().collect(Collectors.toList());

    final String compactedCommitTime = metaClient.getActiveTimeline().reload().getCommitsTimeline().lastInstant().get()
        .getTimestamp();

    assertTrue(roView.getLatestDataFiles().filter(file -> {
      if (compactedCommitTime.equals(file.getCommitTime())) {
        return true;
      } else {
        return false;
      }
    }).findAny().isPresent());

    client.rollback(compactedCommitTime);

    allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    roView = new HoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

    assertFalse(roView.getLatestDataFiles().filter(file -> {
      if (compactedCommitTime.equals(file.getCommitTime())) {
        return true;
      } else {
        return false;
      }
    }).findAny().isPresent());
  }

  @Test
  public void testMultiRollbackWithDeltaAndCompactionCommit() throws Exception {

    HoodieWriteConfig cfg = getConfig(false);
    final HoodieWriteClient client = getWriteClient(cfg);
    List<String> allCommits = new ArrayList<>();
    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = "001";
    allCommits.add(newCommitTime);
    client.startCommitWithTime(newCommitTime);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
    client.commit(newCommitTime, writeStatusJavaRDD);
    List<WriteStatus> statuses = writeStatusJavaRDD.collect();
    assertNoWriteErrors(statuses);

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);

    Optional<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals("Delta commit should be 001", "001", deltaCommit.get().getTimestamp());

    Optional<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());

    FileStatus[] allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    TableFileSystemView.ReadOptimizedView roView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    Stream<HoodieDataFile> dataFilesToRead = roView.getLatestDataFiles();
    assertTrue(!dataFilesToRead.findAny().isPresent());

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    assertTrue("ReadOptimizedTableView should list the parquet files we wrote in the delta commit",
        dataFilesToRead.findAny().isPresent());

    /**
     * Write 2 (inserts + updates)
     */
    newCommitTime = "002";
    allCommits.add(newCommitTime);
    // WriteClient with custom config (disable small file handling)
    HoodieWriteClient nClient = getWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff());
    nClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
    copyOfRecords = dataGen.generateUpdates(newCommitTime, copyOfRecords);
    copyOfRecords.addAll(dataGen.generateInserts(newCommitTime, 200));

    List<String> dataFiles = roView.getLatestDataFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
    List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath);
    assertEquals(recordsRead.size(), 200);

    statuses = nClient.upsert(jsc.parallelize(copyOfRecords, 1), newCommitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);
    nClient.commit(newCommitTime, writeStatusJavaRDD);
    copyOfRecords.clear();


    // Schedule a compaction
    /**
     * Write 3 (inserts + updates)
     */
    newCommitTime = "003";
    allCommits.add(newCommitTime);
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> newInserts = dataGen.generateInserts(newCommitTime, 100);
    records = dataGen.generateUpdates(newCommitTime, records);
    records.addAll(newInserts);
    writeRecords = jsc.parallelize(records, 1);

    writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
    client.commit(newCommitTime, writeStatusJavaRDD);
    statuses = writeStatusJavaRDD.collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());

    String compactionInstantTime = "004";
    allCommits.add(compactionInstantTime);
    client.scheduleCompactionAtInstant(compactionInstantTime, Optional.empty());

    // Compaction commit
    /**
     * Write 4 (updates)
     */
    newCommitTime = "005";
    allCommits.add(newCommitTime);
    client.startCommitWithTime(newCommitTime);

    records = dataGen.generateUpdates(newCommitTime, records);
    writeRecords = jsc.parallelize(records, 1);

    writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
    client.commit(newCommitTime, writeStatusJavaRDD);
    statuses = writeStatusJavaRDD.collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());

    compactionInstantTime = "006";
    allCommits.add(compactionInstantTime);
    client.scheduleCompactionAtInstant(compactionInstantTime, Optional.empty());
    JavaRDD<WriteStatus> ws = client.compact(compactionInstantTime);
    client.commitCompaction(compactionInstantTime, ws, Optional.empty());

    allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    roView = new HoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

    final String compactedCommitTime = metaClient.getActiveTimeline().reload().getCommitsTimeline().lastInstant().get()
        .getTimestamp();

    assertTrue(roView.getLatestDataFiles().filter(file -> {
      if (compactedCommitTime.equals(file.getCommitTime())) {
        return true;
      } else {
        return false;
      }
    }).findAny().isPresent());

    /**
     * Write 5 (updates)
     */
    newCommitTime = "007";
    allCommits.add(newCommitTime);
    client.startCommitWithTime(newCommitTime);
    copyOfRecords = new ArrayList<>(records);
    copyOfRecords = dataGen.generateUpdates(newCommitTime, copyOfRecords);
    copyOfRecords.addAll(dataGen.generateInserts(newCommitTime, 200));

    statuses = client.upsert(jsc.parallelize(copyOfRecords, 1), newCommitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);
    client.commit(newCommitTime, writeStatusJavaRDD);
    copyOfRecords.clear();

    // Rollback latest commit first
    client.restoreToInstant("000");

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    roView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    assertTrue(!dataFilesToRead.findAny().isPresent());
    HoodieTableFileSystemView.RealtimeView rtView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    List<HoodieFileGroup> fileGroups = ((HoodieTableFileSystemView) rtView).getAllFileGroups().collect(Collectors
        .toList());
    assertTrue(fileGroups.isEmpty());
  }

  protected HoodieWriteConfig getHoodieWriteConfigWithSmallFileHandlingOff() {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withAutoCommit(false).withAssumeDatePartitioning(true).withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(1 * 1024).withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withEmbeddedTimelineServerEnabled(true)
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1 * 1024).build())
        .forTable("test-trip-table").build();
  }

  @Test
  public void testUpsertPartitioner() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = getWriteClient(cfg);

    /**
     * Write 1 (only inserts, written as parquet file)
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);

    Optional<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals("Delta commit should be 001", "001", deltaCommit.get().getTimestamp());

    Optional<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());

    FileStatus[] allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    TableFileSystemView.ReadOptimizedView roView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitsTimeline().filterCompletedInstants(), allFiles);
    Stream<HoodieDataFile> dataFilesToRead = roView.getLatestDataFiles();
    Map<String, Long> parquetFileIdToSize = dataFilesToRead.collect(
        Collectors.toMap(HoodieDataFile::getFileId, HoodieDataFile::getFileSize));

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    List<HoodieDataFile> dataFilesList = dataFilesToRead.collect(Collectors.toList());
    assertTrue("RealtimeTableView should list the parquet files we wrote in the delta commit",
        dataFilesList.size() > 0);

    /**
     * Write 2 (only updates + inserts, written to .log file + correction of existing parquet
     * file size)
     */
    newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> newRecords = dataGen.generateUpdates(newCommitTime, records);
    newRecords.addAll(dataGen.generateInserts(newCommitTime, 20));

    statuses = client.upsert(jsc.parallelize(newRecords), newCommitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals("Latest Delta commit should be 002", "002", deltaCommit.get().getTimestamp());

    commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());

    allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    roView = new HoodieTableFileSystemView(metaClient,
        hoodieTable.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    List<HoodieDataFile> newDataFilesList = dataFilesToRead.collect(Collectors.toList());
    Map<String, Long> parquetFileIdToNewSize = newDataFilesList.stream().collect(
        Collectors.toMap(HoodieDataFile::getFileId, HoodieDataFile::getFileSize));

    assertTrue(parquetFileIdToNewSize.entrySet().stream()
        .filter(entry -> parquetFileIdToSize.get(entry.getKey()) < entry.getValue()).count() > 0);

    List<String> dataFiles = roView.getLatestDataFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
    List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath);
    //Wrote 20 records in 2 batches
    assertEquals("Must contain 40 records", 40, recordsRead.size());
  }

  @Test
  public void testLogFileCountsAfterCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfig(true);
    HoodieWriteClient writeClient = getWriteClient(config);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    String newCommitTime = "100";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
    List<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime).collect();

    // Update all the 100 records
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    HoodieTimeline timeline2 = metaClient.getActiveTimeline();
    newCommitTime = "101";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
    JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
    HoodieReadClient readClient = new HoodieReadClient(jsc, config);
    updatedRecords = readClient.tagLocation(updatedRecordsRDD).collect();

    // Write them to corresponding avro logfiles
    HoodieTestUtils
        .writeRecordsToLogFiles(metaClient.getFs(), metaClient.getBasePath(),
            HoodieTestDataGenerator.avroSchemaWithMetadataFields, updatedRecords);

    // Verify that all data file has one log file
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    // In writeRecordsToLogFiles, no commit files are getting added, so resetting file-system view state
    ((SyncableFileSystemView)(table.getRTFileSystemView())).reset();

    for (String partitionPath : dataGen.getPartitionPaths()) {
      List<FileSlice> groupedLogFiles = table.getRTFileSystemView().getLatestFileSlices(partitionPath)
          .collect(Collectors.toList());
      for (FileSlice fileSlice : groupedLogFiles) {
        assertEquals("There should be 1 log file written for every data file", 1, fileSlice.getLogFiles().count());
      }
    }

    // Mark 2nd delta-instant as completed
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Optional.empty());

    // Do a compaction
    String compactionInstantTime = writeClient.scheduleCompaction(Optional.empty()).get().toString();
    JavaRDD<WriteStatus> result = writeClient.compact(compactionInstantTime);

    // Verify that recently written compacted data file has no log file
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();

    assertTrue("Compaction commit should be > than last insert", HoodieTimeline.compareTimestamps(
        timeline.lastInstant().get().getTimestamp(), newCommitTime, HoodieTimeline.GREATER));

    for (String partitionPath : dataGen.getPartitionPaths()) {
      List<FileSlice> groupedLogFiles = table.getRTFileSystemView().getLatestFileSlices(partitionPath)
          .collect(Collectors.toList());
      for (FileSlice slice : groupedLogFiles) {
        assertTrue("After compaction there should be no log files visiable on a Realtime view",
            slice.getLogFiles().collect(Collectors.toList()).isEmpty());
      }
      List<WriteStatus> writeStatuses = result.collect();
      assertTrue(writeStatuses.stream()
          .filter(writeStatus -> writeStatus.getStat().getPartitionPath().contentEquals(partitionPath))
          .count() > 0);
    }
  }

  @Test
  public void testMetadataValuesAfterInsertUpsertAndCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfig(false);
    HoodieWriteClient writeClient = getWriteClient(config);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    String newCommitTime = "100";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
    writeClient.commit(newCommitTime, statuses);

    // total time taken for creating files should be greater than 0
    long totalCreateTime = statuses.map(writeStatus -> writeStatus.getStat().getRuntimeStats().getTotalCreateTime())
        .reduce((a, b) -> a + b).intValue();
    Assert.assertTrue(totalCreateTime > 0);

    // Update all the 100 records
    newCommitTime = "101";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
    JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
    statuses = writeClient.upsert(updatedRecordsRDD, newCommitTime);
    writeClient.commit(newCommitTime, statuses);
    // total time taken for upsert all records should be greater than 0
    long totalUpsertTime = statuses.map(writeStatus -> writeStatus.getStat().getRuntimeStats().getTotalUpsertTime())
        .reduce((a, b) -> a + b).intValue();
    Assert.assertTrue(totalUpsertTime > 0);

    // Do a compaction
    String compactionInstantTime = writeClient.scheduleCompaction(Optional.empty()).get().toString();
    statuses = writeClient.compact(compactionInstantTime);
    writeClient.commitCompaction(compactionInstantTime, statuses, Optional.empty());
    // total time taken for scanning log files should be greater than 0
    long timeTakenForScanner = statuses.map(writeStatus -> writeStatus.getStat().getRuntimeStats().getTotalScanTime())
        .reduce((a, b) -> a + b).longValue();
    Assert.assertTrue(timeTakenForScanner > 0);
  }

  @Test
  public void testSimpleInsertsGeneratedIntoLogFiles() throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, IndexType.INMEMORY).build();
    HoodieWriteClient writeClient = getWriteClient(config);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    String newCommitTime = "100";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
    writeClient.commit(newCommitTime, statuses);

    HoodieTable table = HoodieTable
        .getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath), config,
            jsc);
    TableFileSystemView.RealtimeView tableRTFileSystemView = table.getRTFileSystemView();

    long numLogFiles = 0;
    for (String partitionPath : dataGen.getPartitionPaths()) {
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getDataFile().isPresent()).count() == 0);
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getLogFiles().count() > 0).count() > 0);
      numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getLogFiles().count() > 0).count();
    }

    Assert.assertTrue(numLogFiles > 0);
    // Do a compaction
    String commitTime = writeClient.scheduleCompaction(Optional.empty()).get().toString();
    statuses = writeClient.compact(commitTime);
    Assert.assertTrue(statuses.map(status -> status.getStat().getPath().contains("parquet")).count() == numLogFiles);
    Assert.assertEquals(statuses.count(), numLogFiles);
    writeClient.commitCompaction(commitTime, statuses, Optional.empty());
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollback() throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, IndexType.INMEMORY).build();
    HoodieWriteClient writeClient = getWriteClient(config);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    String newCommitTime = "100";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
    // trigger an action
    List<WriteStatus> writeStatuses = statuses.collect();

    // Ensure that inserts are written to only log files
    Assert.assertEquals(writeStatuses.stream().filter(writeStatus -> !writeStatus.getStat().getPath().contains("log")
    ).count(), 0);
    Assert.assertTrue(writeStatuses.stream().filter(writeStatus -> writeStatus.getStat().getPath().contains("log")
    ).count() > 0);

    // rollback a failed commit
    boolean rollback = writeClient.rollback(newCommitTime);
    Assert.assertTrue(rollback);
    newCommitTime = "101";
    writeClient.startCommitWithTime(newCommitTime);

    // insert 100 records
    records = dataGen.generateInserts(newCommitTime, 100);
    recordsRDD = jsc.parallelize(records, 1);
    statuses = writeClient.insert(recordsRDD, newCommitTime);
    writeClient.commit(newCommitTime, statuses);

    // rollback a successful commit
    // Sleep for small interval (at least 1 second) to force a new rollback start time.
    Thread.sleep(1000);
    writeClient.rollback(newCommitTime);
    final HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    TableFileSystemView.RealtimeView tableRTFileSystemView = table.getRTFileSystemView();

    long numLogFiles = 0;
    for (String partitionPath : dataGen.getPartitionPaths()) {
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getDataFile().isPresent()).count() == 0);
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getLogFiles().count() > 0).count() == 0);
      numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getLogFiles().count() > 0).count();
    }
    Assert.assertTrue(numLogFiles == 0);
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackAfterCompaction() throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, IndexType.INMEMORY).build();
    HoodieWriteClient writeClient = getWriteClient(config);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    String newCommitTime = "100";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
    writeClient.commit(newCommitTime, statuses);
    // trigger an action
    statuses.collect();

    HoodieTable table = HoodieTable
        .getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath), config,
            jsc);
    TableFileSystemView.RealtimeView tableRTFileSystemView = table.getRTFileSystemView();

    long numLogFiles = 0;
    for (String partitionPath : dataGen.getPartitionPaths()) {
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getDataFile().isPresent()).count() == 0);
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getLogFiles().count() > 0).count() > 0);
      numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getLogFiles().count() > 0).count();
    }

    Assert.assertTrue(numLogFiles > 0);
    // Do a compaction
    newCommitTime = writeClient.scheduleCompaction(Optional.empty()).get().toString();
    statuses = writeClient.compact(newCommitTime);
    // Ensure all log files have been compacted into parquet files
    Assert.assertTrue(statuses.map(status -> status.getStat().getPath().contains("parquet")).count() == numLogFiles);
    Assert.assertEquals(statuses.count(), numLogFiles);
    writeClient.commitCompaction(newCommitTime, statuses, Optional.empty());
    // Trigger a rollback of compaction
    writeClient.rollback(newCommitTime);
    table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath), config, jsc);
    tableRTFileSystemView = table.getRTFileSystemView();
    ((SyncableFileSystemView)tableRTFileSystemView).reset();
    for (String partitionPath : dataGen.getPartitionPaths()) {
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getDataFile().isPresent()).count() == 0);
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getLogFiles().count() > 0).count() > 0);
    }
  }

  /**
   * Test to ensure rolling stats are correctly written to metadata file
   */
  @Test
  public void testRollingStatsInMetadata() throws Exception {

    HoodieWriteConfig cfg = getConfigBuilder(false, IndexType.INMEMORY).withAutoCommit(false).build();
    HoodieWriteClient client = getWriteClient(cfg);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);

    // Create a commit without rolling stats in metadata to test backwards compatibility
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitActionType = table.getMetaClient().getCommitActionType();
    HoodieInstant instant = new HoodieInstant(true, commitActionType, "000");
    activeTimeline.createInflight(instant);
    activeTimeline.saveAsComplete(instant, Optional.empty());

    String commitTime = "001";
    client.startCommitWithTime(commitTime);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    JavaRDD<WriteStatus> statuses = client.insert(writeRecords, commitTime);
    assertTrue("Commit should succeed", client.commit(commitTime, statuses));

    // Read from commit file
    table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(table.getActiveTimeline().getInstantDetails(table
        .getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(), HoodieCommitMetadata.class);
    HoodieRollingStatMetadata rollingStatMetadata = HoodieCommitMetadata.fromBytes(metadata.getExtraMetadata()
        .get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(), HoodieRollingStatMetadata.class);
    int inserts = 0;
    for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat :
        rollingStatMetadata.getPartitionToRollingStats().entrySet()) {
      for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
        inserts += stat.getValue().getInserts();
      }
    }
    Assert.assertEquals(inserts, 200);

    commitTime = "002";
    client.startCommitWithTime(commitTime);
    records = dataGen.generateUpdates(commitTime, records);
    writeRecords = jsc.parallelize(records, 1);
    statuses = client.upsert(writeRecords, commitTime);
    assertTrue("Commit should succeed", client.commit(commitTime, statuses));

    // Read from commit file
    table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    metadata = HoodieCommitMetadata.fromBytes(table.getActiveTimeline().getInstantDetails(table
        .getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(), HoodieCommitMetadata.class);
    rollingStatMetadata = HoodieCommitMetadata.fromBytes(metadata.getExtraMetadata()
        .get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(), HoodieRollingStatMetadata.class);
    inserts = 0;
    int upserts = 0;
    for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
        .entrySet()) {
      for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
        inserts += stat.getValue().getInserts();
        upserts += stat.getValue().getUpserts();
      }
    }

    Assert.assertEquals(inserts, 200);
    Assert.assertEquals(upserts, 200);

    client.rollback(commitTime);

    // Read from commit file
    table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    metadata = HoodieCommitMetadata.fromBytes(table.getActiveTimeline().getInstantDetails(table
        .getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(), HoodieCommitMetadata.class);
    rollingStatMetadata = HoodieCommitMetadata.fromBytes(metadata.getExtraMetadata()
        .get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(), HoodieRollingStatMetadata.class);
    inserts = 0;
    upserts = 0;
    for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
        .entrySet()) {
      for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
        inserts += stat.getValue().getInserts();
        upserts += stat.getValue().getUpserts();
      }
    }
    Assert.assertEquals(inserts, 200);
    Assert.assertEquals(upserts, 0);
  }

  /**
   * Test to ensure rolling stats are correctly written to the metadata file, identifies small files and corrects them
   */
  @Test
  public void testRollingStatsWithSmallFileHandling() throws Exception {

    HoodieWriteConfig cfg = getConfigBuilder(false, IndexType.INMEMORY).withAutoCommit(false).build();
    HoodieWriteClient client = getWriteClient(cfg);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    Map<String, Long> fileIdToInsertsMap = new HashMap<>();
    Map<String, Long> fileIdToUpsertsMap = new HashMap<>();
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);

    String commitTime = "000";
    client.startCommitWithTime(commitTime);

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    JavaRDD<WriteStatus> statuses = client.insert(writeRecords, commitTime);
    assertTrue("Commit should succeed", client.commit(commitTime, statuses));

    // Read from commit file
    table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(table.getActiveTimeline().getInstantDetails(table
        .getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(), HoodieCommitMetadata.class);
    HoodieRollingStatMetadata rollingStatMetadata = HoodieCommitMetadata.fromBytes(metadata.getExtraMetadata()
        .get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(), HoodieRollingStatMetadata.class);
    int inserts = 0;
    for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat :
        rollingStatMetadata.getPartitionToRollingStats().entrySet()) {
      for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
        inserts += stat.getValue().getInserts();
        fileIdToInsertsMap.put(stat.getKey(), stat.getValue().getInserts());
        fileIdToUpsertsMap.put(stat.getKey(), stat.getValue().getUpserts());
      }
    }
    Assert.assertEquals(inserts, 200);

    commitTime = "001";
    client.startCommitWithTime(commitTime);
    // generate updates + inserts. inserts should be handled into small files
    records = dataGen.generateUpdates(commitTime, records);
    records.addAll(dataGen.generateInserts(commitTime, 200));
    writeRecords = jsc.parallelize(records, 1);
    statuses = client.upsert(writeRecords, commitTime);
    assertTrue("Commit should succeed", client.commit(commitTime, statuses));

    // Read from commit file
    table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    metadata = HoodieCommitMetadata.fromBytes(table.getActiveTimeline().getInstantDetails(table
        .getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(), HoodieCommitMetadata.class);
    rollingStatMetadata = HoodieCommitMetadata.fromBytes(metadata.getExtraMetadata()
        .get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(), HoodieRollingStatMetadata.class);
    inserts = 0;
    int upserts = 0;
    for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
        .entrySet()) {
      for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
        // No new file id should be created, all the data should be written to small files already there
        assertTrue(fileIdToInsertsMap.containsKey(stat.getKey()));
        assertTrue(fileIdToUpsertsMap.containsKey(stat.getKey()));
        inserts += stat.getValue().getInserts();
        upserts += stat.getValue().getUpserts();
      }
    }

    Assert.assertEquals(inserts, 400);
    Assert.assertEquals(upserts, 200);

    // Test small file handling after compaction
    commitTime = "002";
    client.scheduleCompactionAtInstant(commitTime, Optional.of(metadata.getExtraMetadata()));
    statuses = client.compact(commitTime);
    client.commitCompaction(commitTime, statuses, Optional.empty());

    // Read from commit file
    table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    metadata = HoodieCommitMetadata.fromBytes(table.getActiveTimeline().getInstantDetails(table
        .getActiveTimeline().getCommitsTimeline().lastInstant().get()).get(), HoodieCommitMetadata.class);
    HoodieRollingStatMetadata rollingStatMetadata1 = HoodieCommitMetadata.fromBytes(metadata.getExtraMetadata()
        .get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(), HoodieRollingStatMetadata.class);

    // Ensure that the rolling stats from the extra metadata of delta commits is copied over to the compaction commit
    for (Map.Entry<String, Map<String, HoodieRollingStat>> entry : rollingStatMetadata.getPartitionToRollingStats()
        .entrySet()) {
      Assert.assertTrue(rollingStatMetadata1.getPartitionToRollingStats().containsKey(entry.getKey()));
      Assert.assertEquals(rollingStatMetadata1.getPartitionToRollingStats().get(entry.getKey()).size(), entry
          .getValue().size());
    }

    // Write inserts + updates
    commitTime = "003";
    client.startCommitWithTime(commitTime);
    // generate updates + inserts. inserts should be handled into small files
    records = dataGen.generateUpdates(commitTime, records);
    records.addAll(dataGen.generateInserts(commitTime, 200));
    writeRecords = jsc.parallelize(records, 1);
    statuses = client.upsert(writeRecords, commitTime);
    assertTrue("Commit should succeed", client.commit(commitTime, statuses));

    // Read from commit file
    table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
    metadata = HoodieCommitMetadata.fromBytes(table.getActiveTimeline().getInstantDetails(table
        .getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(), HoodieCommitMetadata.class);
    rollingStatMetadata = HoodieCommitMetadata.fromBytes(metadata.getExtraMetadata()
        .get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY).getBytes(), HoodieRollingStatMetadata.class);
    inserts = 0;
    upserts = 0;
    for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
        .entrySet()) {
      for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
        // No new file id should be created, all the data should be written to small files already there
        assertTrue(fileIdToInsertsMap.containsKey(stat.getKey()));
        inserts += stat.getValue().getInserts();
        upserts += stat.getValue().getUpserts();
      }
    }

    Assert.assertEquals(inserts, 600);
    Assert.assertEquals(upserts, 600);

  }

  private HoodieWriteConfig getConfig(Boolean autoCommit) {
    return getConfigBuilder(autoCommit).build();
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return getConfigBuilder(autoCommit, IndexType.BLOOM);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, HoodieIndex.IndexType indexType) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withAutoCommit(autoCommit).withAssumeDatePartitioning(true)
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024).withInlineCompaction(false)
                .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true)
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build());
  }

  private void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse("Errors found in write of " + status.getFileId(), status.hasErrors());
    }
  }
}
