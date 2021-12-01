/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.testutils;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The test harness for resource initialization and cleanup.
 */
public abstract class HoodieClientTestHarness extends HoodieCommonTestHarness implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieClientTestHarness.class);

  protected static int timelineServicePort =
      FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue();
  private String testMethodName;
  protected transient JavaSparkContext jsc = null;
  protected transient HoodieSparkEngineContext context = null;
  protected transient Configuration hadoopConf = null;
  protected transient SQLContext sqlContext;
  protected transient FileSystem fs;
  protected transient ExecutorService executorService;
  protected transient HoodieTableMetaClient metaClient;
  protected transient SparkRDDWriteClient writeClient;
  protected transient HoodieReadClient readClient;
  protected transient HoodieTableFileSystemView tableView;
  protected transient TimelineService timelineService;

  protected final SparkTaskContextSupplier supplier = new SparkTaskContextSupplier();

  // dfs
  protected String dfsBasePath;
  protected transient HdfsTestService hdfsTestService;
  protected transient MiniDFSCluster dfsCluster;
  protected transient DistributedFileSystem dfs;

  @AfterAll
  public static void tearDownAll() throws IOException {
    FileSystem.closeAll();
  }

  @BeforeEach
  public void setTestMethodName(TestInfo testInfo) {
    if (testInfo.getTestMethod().isPresent()) {
      testMethodName = testInfo.getTestMethod().get().getName();
    } else {
      testMethodName = "Unknown";
    }
  }

  /**
   * Initializes resource group for the subclasses of {@link HoodieClientTestBase}.
   */
  public void initResources() throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initFileSystem();
    initMetaClient();
    initTimelineService();
  }

  /**
   * Cleanups resource group for the subclasses of {@link HoodieClientTestBase}.
   */
  public void cleanupResources() throws IOException {
    cleanupTimelineService();
    cleanupClients();
    cleanupSparkContexts();
    cleanupTestDataGenerator();
    cleanupFileSystem();
    cleanupDFS();
    cleanupExecutorService();
    System.gc();
  }

  /**
   * Initializes the Spark contexts ({@link JavaSparkContext} and {@link SQLContext}) with the given application name.
   *
   * @param appName The specified application name.
   */
  protected void initSparkContexts(String appName) {
    // Initialize a local spark env
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(appName + "#" + testMethodName));
    jsc.setLogLevel("ERROR");
    hadoopConf = jsc.hadoopConfiguration();

    // SQLContext stuff
    sqlContext = new SQLContext(jsc);
    context = new HoodieSparkEngineContext(jsc);
    hadoopConf = context.getHadoopConf().get();
  }

  /**
   * Initializes the Spark contexts ({@link JavaSparkContext} and {@link SQLContext})
   * with a default name matching the name of the class.
   */
  protected void initSparkContexts() {
    initSparkContexts(this.getClass().getSimpleName());
  }

  /**
   * Cleanups Spark contexts ({@link JavaSparkContext} and {@link SQLContext}).
   */
  protected void cleanupSparkContexts() {
    if (sqlContext != null) {
      LOG.info("Clearing sql context cache of spark-session used in previous test-case");
      sqlContext.clearCache();
      sqlContext = null;
    }

    if (jsc != null) {
      LOG.info("Closing spark context used in previous test-case");
      jsc.close();
      jsc.stop();
      jsc = null;
    }

    if (context != null) {
      LOG.info("Closing spark engine context used in previous test-case");
      context = null;
    }
  }

  /**
   * Initializes a file system with the hadoop configuration of Spark context.
   */
  protected void initFileSystem() {
    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    initFileSystemWithConfiguration(hadoopConf);
  }

  /**
   * Initializes file system with a default empty configuration.
   */
  protected void initFileSystemWithDefaultConfiguration() {
    initFileSystemWithConfiguration(new Configuration());
  }

  /**
   * Cleanups file system.
   *
   * @throws IOException
   */
  protected void cleanupFileSystem() throws IOException {
    if (fs != null) {
      LOG.warn("Closing file-system instance used in previous test-run");
      fs.close();
      fs = null;
    }
  }

  /**
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified by
   * {@code getTableType()}.
   *
   * @throws IOException
   */
  @Override
  protected void initMetaClient() throws IOException {
    initMetaClient(getTableType());
  }

  protected void initMetaClient(Properties properties) throws IOException {
    initMetaClient(getTableType(), properties);
  }

  protected void initMetaClient(HoodieTableType tableType) throws IOException {
    initMetaClient(tableType, new Properties());
  }

  protected void initMetaClient(HoodieTableType tableType, Properties properties) throws IOException {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    if (tableName != null && !tableName.isEmpty()) {
      properties.put(HoodieTableConfig.NAME.key(), tableName);
    }
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, tableType, properties);
  }

  /**
   * Initializes timeline service based on the write config.
   */
  protected void initTimelineService() {
    timelineService = HoodieClientTestUtils.initTimelineService(
        context, basePath, incrementTimelineServicePortToUse());
    timelineServicePort = timelineService.getServerPort();
  }

  protected void cleanupTimelineService() {
    if (timelineService != null) {
      timelineService.close();
    }
  }

  protected int incrementTimelineServicePortToUse() {
    // Increment the timeline service port for each individual test
    // to avoid port reuse causing failures
    timelineServicePort = (timelineServicePort + 1 - 1024) % (65536 - 1024) + 1024;
    return timelineServicePort;
  }

  protected Properties getPropertiesForKeyGen() {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    properties.put("hoodie.datasource.write.recordkey.field", "_row_key");
    properties.put("hoodie.datasource.write.partitionpath.field", "partition_path");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    properties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), SimpleKeyGenerator.class.getName());
    return properties;
  }

  protected Properties getPropertiesForMetadataTable() {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    properties.put("hoodie.datasource.write.recordkey.field", "key");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "key");
    return properties;
  }

  protected void addConfigsForPopulateMetaFields(HoodieWriteConfig.Builder configBuilder, boolean populateMetaFields,
                                                 boolean isMetadataTable) {
    if (!populateMetaFields) {
      configBuilder.withProperties((isMetadataTable ? getPropertiesForMetadataTable() : getPropertiesForKeyGen()))
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build());
    }
  }

  protected void addConfigsForPopulateMetaFields(HoodieWriteConfig.Builder configBuilder, boolean populateMetaFields) {
    addConfigsForPopulateMetaFields(configBuilder, populateMetaFields, false);
  }

  /**
   * Cleanups hoodie clients.
   */
  protected void cleanupClients() throws IOException {
    if (metaClient != null) {
      metaClient = null;
    }
    if (readClient != null) {
      readClient = null;
    }
    if (writeClient != null) {
      writeClient.close();
      writeClient = null;
    }
    if (tableView != null) {
      tableView.close();
      tableView = null;
    }
  }

  /**
   * Initializes a distributed file system and base directory.
   *
   * @throws IOException
   */
  protected void initDFS() throws IOException {
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);

    // Create a temp folder as the base path
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    this.basePath = dfsBasePath;
    this.hadoopConf = dfs.getConf();
    dfs.mkdirs(new Path(dfsBasePath));
  }

  /**
   * Initializes an instance of {@link HoodieTableMetaClient} with a special table type specified by
   * {@code getTableType()}.
   *
   * @throws IOException
   */
  protected void initDFSMetaClient() throws IOException {
    if (dfsBasePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }
    metaClient = HoodieTestUtils.init(dfs.getConf(), dfsBasePath, getTableType());
  }

  /**
   * Cleanups the distributed file system.
   *
   * @throws IOException
   */
  protected void cleanupDFS() throws IOException {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
      dfsCluster.shutdown();
      hdfsTestService = null;
      dfsCluster = null;
      dfs = null;
    }
    // Need to closeAll to clear FileSystem.Cache, required because DFS and LocalFS used in the
    // same JVM
    FileSystem.closeAll();
  }

  /**
   * Initializes executor service with a fixed thread pool.
   *
   * @param threadNum specify the capacity of the fixed thread pool
   */
  protected void initExecutorServiceWithFixedThreadPool(int threadNum) {
    executorService = Executors.newFixedThreadPool(threadNum);
  }

  /**
   * Cleanups the executor service.
   */
  protected void cleanupExecutorService() {
    if (this.executorService != null) {
      this.executorService.shutdownNow();
      this.executorService = null;
    }
  }

  private void initFileSystemWithConfiguration(Configuration configuration) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    fs = FSUtils.getFs(basePath, configuration);
    if (fs instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  public HoodieReadClient getHoodieReadClient(String basePath) {
    readClient = new HoodieReadClient(context, basePath, SQLContext.getOrCreate(jsc.sc()));
    return readClient;
  }

  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }
    writeClient = new SparkRDDWriteClient(context, cfg);
    return writeClient;
  }

  public HoodieTableMetaClient getHoodieMetaClient(Configuration conf, String basePath) {
    metaClient = HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).build();
    return metaClient;
  }

  public HoodieTableFileSystemView getHoodieTableFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline,
                                                                FileStatus[] fileStatuses) {
    if (tableView == null) {
      tableView = new HoodieTableFileSystemView(metaClient, visibleActiveTimeline, fileStatuses);
    } else {
      tableView.init(metaClient, visibleActiveTimeline, fileStatuses);
    }
    return tableView;
  }

  public JavaRDD<HoodieRecord> tagLocation(
      HoodieIndex index, JavaRDD<HoodieRecord> records, HoodieTable table) {
    return HoodieJavaRDD.getJavaRDD(
        index.tagLocation(HoodieJavaRDD.of(records), context, table));
  }

  public static Pair<HashMap<String, WorkloadStat>, WorkloadStat> buildProfile(JavaRDD<HoodieRecord> inputRecordsRDD) {
    HashMap<String, WorkloadStat> partitionPathStatMap = new HashMap<>();
    WorkloadStat globalStat = new WorkloadStat();

    // group the records by partitionPath + currentLocation combination, count the number of
    // records in each partition
    Map<Tuple2<String, Option<HoodieRecordLocation>>, Long> partitionLocationCounts = inputRecordsRDD
        .mapToPair(record -> new Tuple2<>(
            new Tuple2<>(record.getPartitionPath(), Option.ofNullable(record.getCurrentLocation())), record))
        .countByKey();

    // count the number of both inserts and updates in each partition, update the counts to workLoadStats
    for (Map.Entry<Tuple2<String, Option<HoodieRecordLocation>>, Long> e : partitionLocationCounts.entrySet()) {
      String partitionPath = e.getKey()._1();
      Long count = e.getValue();
      Option<HoodieRecordLocation> locOption = e.getKey()._2();

      if (!partitionPathStatMap.containsKey(partitionPath)) {
        partitionPathStatMap.put(partitionPath, new WorkloadStat());
      }

      if (locOption.isPresent()) {
        // update
        partitionPathStatMap.get(partitionPath).addUpdates(locOption.get(), count);
        globalStat.addUpdates(locOption.get(), count);
      } else {
        // insert
        partitionPathStatMap.get(partitionPath).addInserts(count);
        globalStat.addInserts(count);
      }
    }
    return Pair.of(partitionPathStatMap, globalStat);
  }

  /**
   * Validate the metadata tables contents to ensure it matches what is on the file system.
   */
  public void validateMetadata(HoodieTestTable testTable, List<String> inflightCommits, HoodieWriteConfig writeConfig,
                               String metadataTableBasePath, boolean doFullValidation) throws IOException {
    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    assertNotNull(tableMetadata, "MetadataReader should have been initialized");
    if (!writeConfig.isMetadataTableEnabled()) {
      return;
    }

    if (!tableMetadata.getSyncedInstantTime().isPresent() || tableMetadata instanceof FileSystemBackedTableMetadata) {
      throw new IllegalStateException("Metadata should have synced some commits or tableMetadata should not be an instance "
          + "of FileSystemBackedTableMetadata");
    }
    assertEquals(inflightCommits, testTable.inflightCommits());

    HoodieTimer timer = new HoodieTimer().startTimer();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Partitions should match
    List<java.nio.file.Path> fsPartitionPaths = testTable.getAllPartitionPaths();
    List<String> fsPartitions = new ArrayList<>();
    fsPartitionPaths.forEach(entry -> fsPartitions.add(entry.getFileName().toString()));
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertEquals(fsPartitions, metadataPartitions, "Partitions should match");

    // Files within each partition should match
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(writeConfig, engineContext, true);
    TableFileSystemView tableView = table.getHoodieView();
    List<String> fullPartitionPaths = fsPartitions.stream().map(partition -> basePath + "/" + partition).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = tableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    assertEquals(fsPartitions.size(), partitionToFilesMap.size());

    fsPartitions.forEach(partition -> {
      try {
        validateFilesPerPartition(testTable, tableMetadata, tableView, partitionToFilesMap, partition);
      } catch (IOException e) {
        fail("Exception should not be raised: " + e);
      }
    });
    if (doFullValidation) {
      runFullValidation(writeConfig, metadataTableBasePath, engineContext);
    }

    LOG.info("Validation time=" + timer.endTimer());
  }

  public void syncTableMetadata(HoodieWriteConfig writeConfig) {
    if (!writeConfig.getMetadataConfig().enabled()) {
      return;
    }
    // Open up the metadata table again, for syncing
    try (HoodieTableMetadataWriter writer = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, writeConfig, context)) {
      LOG.info("Successfully synced to metadata table");
    } catch (Exception e) {
      throw new HoodieMetadataException("Error syncing to metadata table.", e);
    }
  }

  public HoodieBackedTableMetadataWriter metadataWriter(HoodieWriteConfig clientConfig) {
    return (HoodieBackedTableMetadataWriter) SparkHoodieBackedTableMetadataWriter
        .create(hadoopConf, clientConfig, new HoodieSparkEngineContext(jsc));
  }

  public HoodieTableMetadata metadata(HoodieWriteConfig clientConfig, HoodieEngineContext hoodieEngineContext) {
    return HoodieTableMetadata.create(hoodieEngineContext, clientConfig.getMetadataConfig(), clientConfig.getBasePath(),
        clientConfig.getSpillableMapBasePath());
  }

  protected void validateFilesPerPartition(HoodieTestTable testTable, HoodieTableMetadata tableMetadata, TableFileSystemView tableView,
                                           Map<String, FileStatus[]> partitionToFilesMap, String partition) throws IOException {
    Path partitionPath;
    if (partition.equals("")) {
      // Should be the non-partitioned case
      partitionPath = new Path(basePath);
    } else {
      partitionPath = new Path(basePath, partition);
    }

    FileStatus[] fsStatuses = testTable.listAllFilesInPartition(partition);
    FileStatus[] metaStatuses = tableMetadata.getAllFilesInPartition(partitionPath);
    List<String> fsFileNames = Arrays.stream(fsStatuses)
        .map(s -> s.getPath().getName()).collect(Collectors.toList());
    List<String> metadataFilenames = Arrays.stream(metaStatuses)
        .map(s -> s.getPath().getName()).collect(Collectors.toList());
    Collections.sort(fsFileNames);
    Collections.sort(metadataFilenames);

    if ((fsFileNames.size() != metadataFilenames.size()) || (!fsFileNames.equals(metadataFilenames))) {
      LOG.info("*** File system listing = " + Arrays.toString(fsFileNames.toArray()));
      LOG.info("*** Metadata listing = " + Arrays.toString(metadataFilenames.toArray()));

      for (String fileName : fsFileNames) {
        if (!metadataFilenames.contains(fileName)) {
          LOG.error(partition + "FsFilename " + fileName + " not found in Meta data");
        }
      }
      for (String fileName : metadataFilenames) {
        if (!fsFileNames.contains(fileName)) {
          LOG.error(partition + "Metadata file " + fileName + " not found in original FS");
        }
      }
    }
    assertEquals(fsStatuses.length, partitionToFilesMap.get(basePath + "/" + partition).length);

    // Block sizes should be valid
    Arrays.stream(metaStatuses).forEach(s -> assertTrue(s.getBlockSize() > 0));
    List<Long> fsBlockSizes = Arrays.stream(fsStatuses).map(FileStatus::getBlockSize).sorted().collect(Collectors.toList());
    List<Long> metadataBlockSizes = Arrays.stream(metaStatuses).map(FileStatus::getBlockSize).sorted().collect(Collectors.toList());
    assertEquals(fsBlockSizes, metadataBlockSizes);

    assertEquals(fsFileNames.size(), metadataFilenames.size(), "Files within partition " + partition + " should match");
    assertEquals(fsFileNames, metadataFilenames, "Files within partition " + partition + " should match");

    // FileSystemView should expose the same data
    List<HoodieFileGroup> fileGroups = tableView.getAllFileGroups(partition).collect(Collectors.toList());
    fileGroups.addAll(tableView.getAllReplacedFileGroups(partition).collect(Collectors.toList()));

    fileGroups.forEach(g -> LogManager.getLogger(getClass()).info(g));
    fileGroups.forEach(g -> g.getAllBaseFiles().forEach(b -> LogManager.getLogger(getClass()).info(b)));
    fileGroups.forEach(g -> g.getAllFileSlices().forEach(s -> LogManager.getLogger(getClass()).info(s)));

    long numFiles = fileGroups.stream()
        .mapToLong(g -> g.getAllBaseFiles().count() + g.getAllFileSlices().mapToLong(s -> s.getLogFiles().count()).sum())
        .sum();
    assertEquals(metadataFilenames.size(), numFiles);
  }

  private void runFullValidation(HoodieWriteConfig writeConfig, String metadataTableBasePath, HoodieSparkEngineContext engineContext) {
    HoodieBackedTableMetadataWriter metadataWriter = metadataWriter(writeConfig);
    assertNotNull(metadataWriter, "MetadataWriter should have been initialized");

    // Validate write config for metadata table
    HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
    assertFalse(metadataWriteConfig.isMetadataTableEnabled(), "No metadata table for metadata table");

    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();

    // Metadata table is MOR
    assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

    // Metadata table is HFile format
    assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
        "Metadata Table base file format should be HFile");

    // Metadata table has a fixed number of partitions
    // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
    // in the .hoodie folder.
    List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, HoodieTableMetadata.getMetadataTableBasePath(basePath),
        false, false);
    Assertions.assertEquals(MetadataPartitionType.values().length, metadataTablePartitions.size());

    // Metadata table should automatically compact and clean
    // versions are +1 as autoclean / compaction happens end of commits
    int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metadataMetaClient, metadataMetaClient.getActiveTimeline());
    metadataTablePartitions.forEach(partition -> {
      List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());
      assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).count() <= 1, "Should have a single latest base file");
      assertTrue(latestSlices.size() <= 1, "Should have a single latest file slice");
      assertTrue(latestSlices.size() <= numFileVersions, "Should limit file slice to "
          + numFileVersions + " but was " + latestSlices.size());
    });
  }
}
