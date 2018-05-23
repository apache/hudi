/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.table;

import static com.uber.hoodie.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieMergeOnReadTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.TestRawTripPayload.MetadataMergeWriteStatus;
import com.uber.hoodie.common.minicluster.HdfsTestService;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.index.HoodieIndex.IndexType;
import com.uber.hoodie.index.bloom.HoodieBloomIndex;
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

  private static String basePath = null;
  //NOTE : Be careful in using DFS (FileSystem.class) vs LocalFs(RawLocalFileSystem.class)
  //The implementation and gurantees of many API's differ, for example check rename(src,dst)
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;
  private static HdfsTestService hdfsTestService;
  private transient JavaSparkContext jsc = null;
  private transient SQLContext sqlContext;

  @AfterClass
  public static void cleanUp() throws Exception {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
      dfsCluster.shutdown();
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
    if (basePath != null) {
      new File(basePath).delete();
    }
    if (jsc != null) {
      jsc.stop();
    }
  }

  @Test
  public void testSimpleInsertAndUpdate() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

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

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitTimeline(), allFiles);
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

    String compactionCommitTime = client.startCompaction();
    client.compact(compactionCommitTime);

    allFiles = HoodieTestUtils.listAllDataFilesInPath(dfs, cfg.getBasePath());
    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitTimeline(), allFiles);
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
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

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
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

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

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitTimeline(), allFiles);
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
    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitTimeline(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    assertTrue(dataFilesToRead.findAny().isPresent());

    List<String> dataFiles = roView.getLatestDataFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
    List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath);
    //Wrote 40 records and deleted 20 records, so remaining 40-20 = 20
    assertEquals("Must contain 20 records", 20, recordsRead.size());
  }

  @Test
  public void testCOWToMORConvertedDatasetRollback() throws Exception {

    //Set TableType to COW
    HoodieTestUtils.initTableType(jsc.hadoopConfiguration(), basePath, HoodieTableType.COPY_ON_WRITE);

    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

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
        hoodieTable.getCompletedCommitTimeline(), allFiles);

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
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

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

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitTimeline(), allFiles);
    dataFilesToRead = roView.getLatestDataFiles();
    assertTrue("RealtimeTableView should list the parquet files we wrote in the delta commit",
        dataFilesToRead.findAny().isPresent());

    /**
     * Write 2 (inserts + updates - testing failed delta commit)
     */
    final String commitTime1 = "002";
    // WriteClient with custom config (disable small file handling)
    client = new HoodieWriteClient(jsc, HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withAutoCommit(false).withAssumeDatePartitioning(true).withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(1 * 1024).withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1 * 1024).build())
        .forTable("test-trip-table").build());
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
    client = new HoodieWriteClient(jsc, cfg);
    client.startCommitWithTime(commitTime2);

    copyOfRecords = new ArrayList<>(records);
    copyOfRecords = dataGen.generateUpdates(commitTime2, copyOfRecords);
    copyOfRecords.addAll(dataGen.generateInserts(commitTime2, 200));

    dataFiles = roView.getLatestDataFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
    recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(dataFiles, basePath);
    assertEquals(recordsRead.size(), 200);

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
    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitTimeline(), allFiles);
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

    String compactionCommit = client.startCompaction();
    JavaRDD<WriteStatus> writeStatus = client.compact(compactionCommit);
    client.commitCompaction(compactionCommit, writeStatus);

    allFiles = HoodieTestUtils.listAllDataFilesInPath(metaClient.getFs(), cfg.getBasePath());
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.getBasePath());
    hoodieTable = HoodieTable.getHoodieTable(metaClient, cfg, jsc);
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
  public void testUpsertPartitioner() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

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

    roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitTimeline(), allFiles);
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
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    String newCommitTime = "100";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
    List<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime).collect();

    // Update all the 100 records
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    newCommitTime = "101";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
    JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
    HoodieIndex index = new HoodieBloomIndex<>(config);
    updatedRecords = index.tagLocation(updatedRecordsRDD, jsc, table).collect();

    // Write them to corresponding avro logfiles
    HoodieTestUtils
        .writeRecordsToLogFiles(metaClient.getFs(), metaClient.getBasePath(), HoodieTestDataGenerator.avroSchema,
            updatedRecords);

    // Verify that all data file has one log file
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    for (String partitionPath : dataGen.getPartitionPaths()) {
      List<FileSlice> groupedLogFiles = table.getRTFileSystemView().getLatestFileSlices(partitionPath)
          .collect(Collectors.toList());
      for (FileSlice fileSlice : groupedLogFiles) {
        assertEquals("There should be 1 log file written for every data file", 1, fileSlice.getLogFiles().count());
      }
    }

    // Do a compaction
    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    String commitTime = writeClient.startCompaction();
    JavaRDD<WriteStatus> result = writeClient.compact(commitTime);

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
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
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
    String commitTime = writeClient.startCompaction();
    statuses = writeClient.compact(commitTime);
    writeClient.commitCompaction(commitTime, statuses);
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
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
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
    String commitTime = writeClient.startCompaction();
    statuses = writeClient.compact(commitTime);
    Assert.assertTrue(statuses.map(status -> status.getStat().getPath().contains("parquet")).count() == numLogFiles);
    Assert.assertEquals(statuses.count(), numLogFiles);
    writeClient.commitCompaction(commitTime, statuses);
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollback() throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, IndexType.INMEMORY).build();
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
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
    HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
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
    newCommitTime = writeClient.startCompaction();
    statuses = writeClient.compact(newCommitTime);
    // Ensure all log files have been compacted into parquet files
    Assert.assertTrue(statuses.map(status -> status.getStat().getPath().contains("parquet")).count() == numLogFiles);
    Assert.assertEquals(statuses.count(), numLogFiles);
    writeClient.commitCompaction(newCommitTime, statuses);
    // Trigger a rollback of compaction
    writeClient.rollback(newCommitTime);
    for (String partitionPath : dataGen.getPartitionPaths()) {
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getDataFile().isPresent()).count() == 0);
      Assert.assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).filter(fileSlice ->
          fileSlice.getLogFiles().count() > 0).count() > 0);
    }
  }


  private HoodieWriteConfig getConfig(Boolean autoCommit) {
    return getConfigBuilder(autoCommit).build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return getConfigBuilder(autoCommit, IndexType.BLOOM);
  }

  private HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, HoodieIndex.IndexType indexType) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withAutoCommit(autoCommit).withAssumeDatePartitioning(true).withCompactionConfig(
            HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024).withInlineCompaction(false)
                .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024 * 1024).build())
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
