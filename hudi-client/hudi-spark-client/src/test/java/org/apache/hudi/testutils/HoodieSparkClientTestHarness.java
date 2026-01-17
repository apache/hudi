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

import org.apache.hudi.HoodieConversionUtils;
import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.timeline.service.TimelineService;
import org.apache.hudi.util.JFunction;
import org.apache.hudi.utils.HoodieWriterClientTestHarness;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.common.util.CleanerUtils.convertCleanMetadata;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The test harness for resource initialization and cleanup.
 */
@Slf4j
public abstract class HoodieSparkClientTestHarness extends HoodieWriterClientTestHarness {

  @AfterAll
  public static void tearDownAll() throws IOException {
    FileSystem.closeAll();
  }

  protected JavaSparkContext jsc;
  protected HoodieSparkEngineContext context;
  protected SparkSession sparkSession;
  protected SQLContext sqlContext;
  protected ExecutorService executorService;
  protected SparkRDDWriteClient writeClient;
  protected SparkRDDReadClient readClient;
  protected HoodieTableFileSystemView tableView;
  protected Map<String, String> extraConf = new HashMap<>();

  protected TimelineService timelineService;
  protected final SparkTaskContextSupplier supplier = new SparkTaskContextSupplier();

  private String testMethodName;

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
    initHoodieStorage();
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
    cleanupExecutorService();
    System.gc();
  }

  protected Option<Consumer<SparkSessionExtensions>> getSparkSessionExtensionsInjector() {
    return Option.empty();
  }

  /**
   * Initializes the Spark contexts ({@link JavaSparkContext} and {@link SQLContext}) with the given application name.
   *
   * @param appName The specified application name.
   */
  protected void initSparkContexts(String appName) {
    Option<Consumer<SparkSessionExtensions>> sparkSessionExtensionsInjector =
        getSparkSessionExtensionsInjector();

    if (sparkSessionExtensionsInjector.isPresent()) {
      // In case we need to inject extensions into Spark Session, we have
      // to stop any session that might still be active, since Spark will try
      // to re-use it
      HoodieConversionUtils.toJavaOption(SparkSession.getActiveSession())
          .ifPresent(SparkSession::stop);
    }

    // Initialize a local spark env
    SparkConf sc = HoodieClientTestUtils.getSparkConfForTest(appName + "#" + testMethodName);
    extraConf.forEach(sc::set);
    SparkContext sparkContext = new SparkContext(sc);
    HoodieClientTestUtils.overrideSparkHadoopConfiguration(sparkContext);
    jsc = new JavaSparkContext(sparkContext);
    jsc.setLogLevel("ERROR");
    storageConf = HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration());
    sparkSession = SparkSession.builder()
        .withExtensions(JFunction.toScala(sparkSessionExtensions -> {
          sparkSessionExtensionsInjector.ifPresent(injector -> injector.accept(sparkSessionExtensions));
          return null;
        }))
        .config(jsc.getConf())
        .getOrCreate();

    sqlContext = SQLContext.getOrCreate(sparkContext);
    context = new HoodieSparkEngineContext(jsc, sqlContext);

    // NOTE: It's important to set Spark's `Tests.IS_TESTING` so that our tests are recognized
    //       as such by Spark
    System.setProperty("spark.testing", "true");
    sparkSession.sparkContext().persistentRdds().foreach(rdd -> rdd._2.persist());
  }

  /**
   * Initializes the Spark contexts ({@link JavaSparkContext} and {@link SQLContext})
   * with a default name matching the name of the class.
   */
  protected void initSparkContexts() {
    initSparkContexts(this.getClass().getSimpleName());
  }

  protected void initQueryIndexConf() {
    extraConf.put("hoodie.fileIndex.dataSkippingFailureMode", "strict");
  }

  /**
   * Cleanups Spark contexts ({@link JavaSparkContext} and {@link SQLContext}).
   */
  protected void cleanupSparkContexts() {
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession = null;
    }

    // SparkSession.stop() should clean the necessary resources.
    if (sqlContext != null) {
      sqlContext = null;
    }

    if (jsc != null) {
      log.info("Closing spark context used in previous test-case");
      jsc.stop();
      jsc = null;
    }

    if (context != null) {
      log.info("Closing spark engine context used in previous test-case");
      context = null;
    }
  }

  /**
   * Initializes a file system with the hadoop configuration of Spark context.
   */
  protected void initHoodieStorage() {
    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    initFileSystemWithConfiguration(storageConf);
  }

  /**
   * Initializes file system with a default empty configuration.
   */
  protected void initFileSystemWithDefaultConfiguration() {
    initFileSystemWithConfiguration(getDefaultStorageConf());
  }

  /**
   * Cleanups file system.
   *
   * @throws IOException
   */
  protected void cleanupFileSystem() throws IOException {
    if (storage != null) {
      log.warn("Closing file-system instance used in previous test-run");
      storage.close();
      storage = null;
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
    metaClient = HoodieTestUtils.init(storageConf, basePath, tableType, properties);
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

  private void initFileSystemWithConfiguration(StorageConfiguration<?> configuration) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    storage = HoodieStorageUtils.getStorage(basePath, configuration);
    FileSystem fs = (FileSystem) storage.getFileSystem();
    if (fs instanceof LocalFileSystem) {
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      ((LocalFileSystem) fs).setVerifyChecksum(true);
    }
  }

  public SparkRDDReadClient getHoodieReadClient(String basePath) {
    readClient = new SparkRDDReadClient(context, basePath, SQLContext.getOrCreate(jsc.sc()));
    return readClient;
  }

  @Override
  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    return getHoodieWriteClient(cfg, true);
  }

  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg, boolean shouldCloseOlderClient) {
    if (null != writeClient && shouldCloseOlderClient) {
      writeClient.close();
      writeClient = null;
    }
    writeClient = new SparkRDDWriteClient(context, cfg);
    return writeClient;
  }

  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> conf, String basePath) {
    metaClient = HoodieTestUtils.createMetaClient(conf, basePath);
    return metaClient;
  }

  public HoodieTableFileSystemView getHoodieTableFileSystemView(HoodieTableMetaClient metaClient,
                                                                HoodieTimeline visibleActiveTimeline,
                                                                List<StoragePathInfo> pathInfoList) {
    if (tableView == null) {
      tableView = new HoodieTableFileSystemView(metaClient, visibleActiveTimeline, pathInfoList);
    } else {
      tableView.init(metaClient, visibleActiveTimeline, pathInfoList);
    }
    return tableView;
  }

  /**
   * @deprecated Use {@link #tagLocation(HoodieIndex, HoodieEngineContext, JavaRDD, HoodieTable)} instead.
   */
  @Deprecated
  public JavaRDD<HoodieRecord> tagLocation(
      HoodieIndex index, JavaRDD<HoodieRecord> records, HoodieTable table) {
    return HoodieJavaRDD.getJavaRDD(
        index.tagLocation(HoodieJavaRDD.of(records), context, table));
  }

  public static JavaRDD<HoodieRecord> tagLocation(
      HoodieIndex index, HoodieEngineContext context, JavaRDD<HoodieRecord> records, HoodieTable table) {
    return HoodieJavaRDD.getJavaRDD(index.tagLocation(HoodieJavaRDD.of(records), context, table));
  }

  public static Pair<Map<String, WorkloadStat>, WorkloadStat> buildProfile(JavaRDD<HoodieRecord> inputRecordsRDD) {
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

  @Override
  protected List<WriteStatus> writeAndVerifyBatch(BaseHoodieWriteClient client, List<HoodieRecord> inserts, String commitTime, boolean populateMetaFields, boolean autoCommitOff) {
    WriteClientTestUtils.startCommitWithTime(client, commitTime);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts, 2);
    List<WriteStatus> statusList = ((SparkRDDWriteClient) client).upsert(insertRecordsRDD1, commitTime).collect();
    client.commit(commitTime, jsc.parallelize(statusList, 1));
    assertNoWriteErrors(statusList);
    verifyRecordsWritten(commitTime, populateMetaFields, inserts, statusList, client.getConfig(),
        HoodieSparkKeyGeneratorFactory.createKeyGenerator(client.getConfig().getProps()));
    return statusList;
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

    if (tableMetadata instanceof FileSystemBackedTableMetadata || !tableMetadata.getSyncedInstantTime().isPresent()) {
      throw new IllegalStateException("Metadata should have synced some commits or tableMetadata should not be an instance "
          + "of FileSystemBackedTableMetadata");
    }
    assertEquals(inflightCommits, testTable.inflightCommits());

    HoodieTimer timer = HoodieTimer.start();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Partitions should match
    List<java.nio.file.Path> fsPartitionPaths = testTable.getAllPartitionPaths();
    List<String> fsPartitions = new ArrayList<>();
    fsPartitionPaths.forEach(entry -> fsPartitions.add(entry.getFileName().toString()));
    if (fsPartitions.isEmpty() && testTable.isNonPartitioned()) {
      fsPartitions.add("");
    }
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertEquals(fsPartitions, metadataPartitions, "Partitions should match");

    // Files within each partition should match
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(writeConfig, engineContext);
    TableFileSystemView tableView = table.getHoodieView();
    List<String> fullPartitionPaths =
        fsPartitions.stream().map(partition -> basePath + "/" + partition)
            .collect(Collectors.toList());
    Map<String, List<StoragePathInfo>> partitionToFilesMap =
        tableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    assertEquals(fsPartitions.size(), partitionToFilesMap.size());

    fsPartitions.forEach(partition -> {
      try {
        validateFilesPerPartition(testTable, tableMetadata, tableView, partitionToFilesMap,
            partition);
      } catch (IOException e) {
        fail("Exception should not be raised: " + e);
      }
    });
    if (doFullValidation) {
      runFullValidation(table.getConfig().getMetadataConfig(), writeConfig, metadataTableBasePath, engineContext);
    }

    log.info("Validation time={}", timer.endTimer());
  }

  public void syncTableMetadata(HoodieWriteConfig writeConfig) {
    if (!writeConfig.getMetadataConfig().isEnabled()) {
      return;
    }
    // Open up the metadata table again, for syncing
    try (HoodieTableMetadataWriter writer = SparkHoodieBackedTableMetadataWriter.create(storageConf, writeConfig, context)) {
      log.info("Successfully synced to metadata table");
    } catch (Exception e) {
      throw new HoodieMetadataException("Error syncing to metadata table.", e);
    }
  }

  public HoodieBackedTableMetadataWriter metadataWriter(HoodieWriteConfig clientConfig) {
    return (HoodieBackedTableMetadataWriter) SparkHoodieBackedTableMetadataWriter
        .create(storageConf, clientConfig, new HoodieSparkEngineContext(jsc));
  }

  public HoodieTableMetadata metadata(HoodieWriteConfig clientConfig,
                                      HoodieEngineContext hoodieEngineContext) {
    return metaClient.getTableFormat().getMetadataFactory().create(
        hoodieEngineContext, storage, clientConfig.getMetadataConfig(), clientConfig.getBasePath());
  }

  protected void validateFilesPerPartition(HoodieTestTable testTable,
                                           HoodieTableMetadata tableMetadata,
                                           TableFileSystemView tableView,
                                           Map<String, List<StoragePathInfo>> partitionToFilesMap,
                                           String partition) throws IOException {
    StoragePath partitionPath;
    if (partition.equals("")) {
      // Should be the non-partitioned case
      partitionPath = new StoragePath(basePath);
    } else {
      partitionPath = new StoragePath(basePath, partition);
    }

    FileStatus[] fsStatuses = testTable.listAllFilesInPartition(partition);
    List<StoragePathInfo> metaFilesList = tableMetadata.getAllFilesInPartition(partitionPath);
    List<String> fsFileNames = Arrays.stream(fsStatuses)
        .map(s -> s.getPath().getName()).collect(Collectors.toList());
    List<String> metadataFilenames = metaFilesList.stream()
        .map(s -> s.getPath().getName()).collect(Collectors.toList());
    Collections.sort(fsFileNames);
    Collections.sort(metadataFilenames);

    assertLinesMatch(fsFileNames, metadataFilenames);
    assertEquals(fsStatuses.length, partitionToFilesMap.get(partitionPath.toString()).size());

    // Block sizes should be valid
    metaFilesList.forEach(s -> assertTrue(s.getBlockSize() > 0));
    List<Long> fsBlockSizes = Arrays.stream(fsStatuses).map(FileStatus::getBlockSize).sorted().collect(Collectors.toList());
    List<Long> metadataBlockSizes = metaFilesList.stream().map(StoragePathInfo::getBlockSize).sorted().collect(Collectors.toList());
    assertEquals(fsBlockSizes, metadataBlockSizes);

    assertEquals(fsFileNames.size(), metadataFilenames.size(),
        "Files within partition " + partition + " should match");
    assertEquals(fsFileNames, metadataFilenames,
        "Files within partition " + partition + " should match");

    // FileSystemView should expose the same data
    List<HoodieFileGroup> fileGroups =
        tableView.getAllFileGroups(partition).collect(Collectors.toList());
    fileGroups.addAll(tableView.getAllReplacedFileGroups(partition).collect(Collectors.toList()));

    long numFiles = fileGroups.stream()
        .mapToLong(g -> g.getAllBaseFiles().count()
            + g.getAllFileSlices().mapToLong(s -> s.getLogFiles().count()).sum())
        .sum();
    assertEquals(metadataFilenames.size(), numFiles);
  }

  private void runFullValidation(HoodieMetadataConfig metadataConfig,
                                 HoodieWriteConfig writeConfig,
                                 String metadataTableBasePath,
                                 HoodieSparkEngineContext engineContext) {
    HoodieBackedTableMetadataWriter metadataWriter = metadataWriter(writeConfig);
    assertNotNull(metadataWriter, "MetadataWriter should have been initialized");

    // Validate write config for metadata table
    HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
    assertFalse(metadataWriteConfig.isMetadataTableEnabled(), "No metadata table for metadata table");

    HoodieTableMetaClient metadataMetaClient = HoodieTestUtils.createMetaClient(storageConf, metadataTableBasePath);

    // Metadata table is MOR
    assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

    // Metadata table has a fixed number of partitions
    // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
    // in the .hoodie folder.
    List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, metadataMetaClient, false);

    // Metadata table should automatically compact and clean
    // versions are +1 as autoClean / compaction happens end of commits
    int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(engineContext, metadataMetaClient, metadataMetaClient.getActiveTimeline());
    metadataTablePartitions.forEach(partition -> {
      List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());
      assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).filter(Objects::nonNull).count() > 0, "Should have a single latest base file");
      assertTrue(latestSlices.size() > 0, "Should have a single latest file slice");
      assertTrue(latestSlices.size() <= numFileVersions, "Should limit file slice to "
              + numFileVersions + " but was " + latestSlices.size());
    });
  }

  public HoodieInstant createCleanMetadata(String instantTime, boolean inflightOnly) throws IOException {
    return createCleanMetadata(instantTime, inflightOnly, false, false);
  }

  public HoodieInstant createEmptyCleanMetadata(String instantTime, boolean inflightOnly) throws IOException {
    return createCleanMetadata(instantTime, inflightOnly, true, true);
  }

  public HoodieInstant createCleanMetadata(String instantTime, boolean inflightOnly, boolean isEmptyForAll, boolean isEmptyCompleted) throws IOException {
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""), "", "",
            new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.EMPTY_MAP);
    if (inflightOnly) {
      HoodieTestTable.of(metaClient).addInflightClean(instantTime, cleanerPlan);
    } else {
      HoodieCleanStat cleanStats = new HoodieCleanStat(
              HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
              HoodieTestUtils.DEFAULT_PARTITION_PATHS[new Random().nextInt(HoodieTestUtils.DEFAULT_PARTITION_PATHS.length)],
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              instantTime,
              "");
      HoodieCleanMetadata cleanMetadata = convertCleanMetadata(instantTime, Option.of(0L), Collections.singletonList(cleanStats), Collections.EMPTY_MAP);
      HoodieTestTable.of(metaClient).addClean(instantTime, cleanerPlan, cleanMetadata, isEmptyForAll, isEmptyCompleted);
    }
    return INSTANT_GENERATOR.createNewInstant(inflightOnly ? HoodieInstant.State.INFLIGHT : HoodieInstant.State.COMPLETED, "clean", instantTime);
  }

  protected HoodieTableMetaClient createMetaClient(SparkSession spark, String basePath) {
    return HoodieClientTestUtils.createMetaClient(spark, basePath);
  }

  protected HoodieTableMetaClient createMetaClient(JavaSparkContext context, String basePath) {
    return HoodieClientTestUtils.createMetaClient(context, basePath);
  }
}
