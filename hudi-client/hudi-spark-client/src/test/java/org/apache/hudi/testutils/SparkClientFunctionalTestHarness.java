/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.testutils;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.providers.HoodieMetaClientProvider;
import org.apache.hudi.testutils.providers.HoodieWriteClientProvider;
import org.apache.hudi.testutils.providers.SparkProvider;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.HoodieSparkKryoRegistrar$;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.apache.hudi.testutils.Assertions.assertFileSizesEqual;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkClientFunctionalTestHarness implements SparkProvider, HoodieMetaClientProvider, HoodieWriteClientProvider {

  protected static int timelineServicePort =
      FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue();
  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  private static transient HoodieSparkEngineContext context;
  private static transient TimelineService timelineService;
  private HoodieStorage storage;
  private FileSystem fileSystem;

  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

  public static Map<String, String> getSparkSqlConf() {
    Map<String, String> sqlConf = new HashMap<>();
    sqlConf.put("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
    sqlConf.put("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog");
    return sqlConf;
  }

  public String basePath() {
    return tempDir.toAbsolutePath().toUri().toString();
  }

  @Override
  public SparkSession spark() {
    return spark;
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public JavaSparkContext jsc() {
    return jsc;
  }

  public StorageConfiguration<Configuration> storageConf() {
    return HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration());
  }

  public HoodieStorage hoodieStorage() {
    if (storage == null) {
      storage = HoodieStorageUtils.getStorage(basePath(), storageConf());
    }
    return storage;
  }

  public FileSystem fs() {
    if (fileSystem == null) {
      fileSystem = (FileSystem) hoodieStorage().getFileSystem();
    }
    return fileSystem;
  }

  @Override
  public HoodieSparkEngineContext context() {
    return context;
  }

  public HoodieTableMetaClient getHoodieMetaClient(HoodieTableType tableType) throws IOException {
    return getHoodieMetaClient(tableType, new Properties());
  }

  public HoodieTableMetaClient getHoodieMetaClient(HoodieTableType tableType, Properties props) throws IOException {
    return getHoodieMetaClient(storageConf(), basePath(), tableType, props);
  }

  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath, HoodieTableType tableType, Properties props) {
    try {
      return HoodieTableMetaClient.newTableBuilder()
          .setTableName(RAW_TRIPS_TEST_NAME)
          .setTableType(tableType)
          .fromProperties(props)
          .initTable(storageConf.newInstance(), basePath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create HoodieTableMetaClient for basePath: " + basePath, e);
    }
  }

  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath) throws IOException {
    return getHoodieMetaClient(storageConf, basePath, getPropertiesForKeyGen(true));
  }

  public HoodieTableMetaClient getHoodieMetaClientWithTableVersion(StorageConfiguration<?> storageConf, String basePath, String tableVersion) throws IOException {
    Properties props = getPropertiesForKeyGen(true);
    props.put(WRITE_TABLE_VERSION.key(), tableVersion);
    return getHoodieMetaClient(storageConf, basePath, props);
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath, Properties props) throws IOException {
    return getHoodieMetaClient(storageConf, basePath, props, COPY_ON_WRITE);
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath, Properties props,
                                                   HoodieTableType tableType) throws IOException {
    return HoodieTableMetaClient.newTableBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(tableType)
        .setPayloadClass(HoodieAvroPayload.class)
        .setTableVersion(ConfigUtils.getIntWithAltKeys(props, WRITE_TABLE_VERSION))
        .fromProperties(props)
        .initTable(storageConf.newInstance(), basePath);
  }

  @Override
  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) throws IOException {
    return new SparkRDDWriteClient(context(), cfg);
  }

  @BeforeEach
  public synchronized void runBeforeEach() {
    initialized = spark != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      HoodieSparkKryoRegistrar$.MODULE$.register(sparkConf);
      SparkRDDReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      HoodieClientTestUtils.overrideSparkHadoopConfiguration(spark.sparkContext());
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
      timelineService = HoodieClientTestUtils.initTimelineService(
          context, basePath(), incrementTimelineServicePortToUse());
      timelineServicePort = timelineService.getServerPort();
    }
    spark.sparkContext().persistentRdds().foreach(rdd -> rdd._2.unpersist(false));
  }

  /**
   * To clean up Spark resources after all testcases have run in functional tests.
   *
   * Spark session and contexts were reused for testcases in the same test class. Some
   * testcase may invoke this specifically to clean up in case of repeated test runs.
   */
  @AfterAll
  public static synchronized void resetSpark() throws IOException {
    if (spark != null) {
      spark.close();
      spark = null;
    }
    if (timelineService != null) {
      timelineService.close();
    }
  }

  @AfterEach
  public void closeFileSystem() throws IOException {
    if (fileSystem != null) {
      fileSystem.close();
      fileSystem = null;
    }
  }

  protected JavaRDD<HoodieRecord> tagLocation(
      HoodieIndex index, JavaRDD<HoodieRecord> records, HoodieTable table) {
    return HoodieJavaRDD.getJavaRDD(
        index.tagLocation(HoodieJavaRDD.of(records), context, table));
  }

  protected JavaRDD<WriteStatus> updateLocation(
      HoodieIndex index, JavaRDD<WriteStatus> writeStatus, HoodieTable table) {
    return HoodieJavaRDD.getJavaRDD(
        index.updateLocation(HoodieJavaRDD.of(writeStatus), context, table));
  }

  protected Stream<HoodieBaseFile> insertRecordsToMORTable(HoodieTableMetaClient metaClient, List<HoodieRecord> records,
                                                           SparkRDDWriteClient client, HoodieWriteConfig cfg, String commitTime) throws IOException {
    return insertRecordsToMORTable(metaClient, records, client, cfg, commitTime, false);
  }

  protected Stream<HoodieBaseFile> insertRecordsToMORTable(HoodieTableMetaClient metaClient, List<HoodieRecord> records,
                                                 SparkRDDWriteClient client, HoodieWriteConfig cfg, String commitTime,
                                                           boolean doExplicitCommit) throws IOException {
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    JavaRDD<WriteStatus> statusesRdd = client.insert(writeRecords, commitTime);
    List<WriteStatus> statuses = statusesRdd.collect();
    assertNoWriteErrors(statuses);
    client.commit(commitTime, jsc().parallelize(statuses));

    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);
    assertFileSizesEqual(statuses, status -> FSUtils.getFileSize(
        reloadedMetaClient.getStorage(),
        new StoragePath(reloadedMetaClient.getBasePath(), status.getStat().getPath())));

    HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), reloadedMetaClient);

    Option<HoodieInstant> deltaCommit =
        reloadedMetaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals(commitTime, deltaCommit.get().requestedTime(),
        "Delta commit should be specified value");

    Option<HoodieInstant> commit =
        reloadedMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().lastInstant();
    assertFalse(commit.isPresent());

    List<StoragePathInfo> allFiles = listAllBaseFilesInPath(hoodieTable);
    TableFileSystemView.BaseFileOnlyView roView =
        getHoodieTableFileSystemView(reloadedMetaClient,
            reloadedMetaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    Stream<HoodieBaseFile> dataFilesToRead = roView.getLatestBaseFiles();
    assertTrue(!dataFilesToRead.findAny().isPresent());

    roView = getHoodieTableFileSystemView(reloadedMetaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestBaseFiles();
    return dataFilesToRead;
  }

  protected void updateRecordsInMORTable(HoodieTableMetaClient metaClient, List<HoodieRecord> records, SparkRDDWriteClient client, HoodieWriteConfig cfg, String commitTime) throws IOException {
    updateRecordsInMORTable(metaClient, records, client, cfg, commitTime, true);
  }

  protected void updateRecordsInMORTable(HoodieTableMetaClient metaClient, List<HoodieRecord> records, SparkRDDWriteClient client, HoodieWriteConfig cfg, String commitTime,
                                         boolean doExplicitCommit) throws IOException {
    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);

    Map<HoodieKey, HoodieRecord> recordsMap = new HashMap<>();
    for (HoodieRecord rec : records) {
      if (!recordsMap.containsKey(rec.getKey())) {
        recordsMap.put(rec.getKey(), rec);
      }
    }

    JavaRDD<WriteStatus> statusesRdd = client.upsert(jsc().parallelize(records, 1), commitTime);
    List<WriteStatus> statuses = statusesRdd.collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);
    client.commit(commitTime, statusesRdd);
    assertFileSizesEqual(statuses, status -> FSUtils.getFileSize(
        reloadedMetaClient.getStorage(),
        new StoragePath(reloadedMetaClient.getBasePath(), status.getStat().getPath())));

    Option<HoodieInstant> deltaCommit =
        reloadedMetaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals(commitTime, deltaCommit.get().requestedTime(),
        "Latest Delta commit should match specified time");

    Option<HoodieInstant> commit =
        reloadedMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().firstInstant();
    assertFalse(commit.isPresent());
  }

  protected List<StoragePathInfo> listAllBaseFilesInPath(HoodieTable table) throws IOException {
    return HoodieTestTable.of(table.getMetaClient()).listAllBaseFiles(table.getBaseFileExtension());
  }

  protected Properties getPropertiesForKeyGen() {
    return getPropertiesForKeyGen(false);
  }

  protected Properties getPropertiesForKeyGen(boolean populateMetaFields) {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaFields));
    properties.put("hoodie.datasource.write.recordkey.field", "_row_key");
    properties.put("hoodie.datasource.write.partitionpath.field", "partition_path");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    return properties;
  }

  protected void addConfigsForPopulateMetaFields(HoodieWriteConfig.Builder configBuilder, boolean populateMetaFields) {
    configBuilder.withProperties(getPropertiesForKeyGen(populateMetaFields));
    if (!populateMetaFields) {
      configBuilder.withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build());
    }
  }

  protected HoodieWriteConfig getConfig(Boolean autoCommit) {
    return getConfigBuilder(autoCommit).build();
  }

  protected HoodieWriteConfig getConfig(Boolean autoCommit, Boolean rollbackUsingMarkers) {
    return getConfigBuilder(autoCommit, rollbackUsingMarkers, HoodieIndex.IndexType.BLOOM).build();
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return getConfigBuilder(autoCommit, HoodieIndex.IndexType.BLOOM);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, HoodieIndex.IndexType indexType) {
    return getConfigBuilder(autoCommit, false, indexType);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, long compactionSmallFileSize, HoodieClusteringConfig clusteringConfig) {
    return getConfigBuilder(autoCommit, false, HoodieIndex.IndexType.BLOOM, compactionSmallFileSize, clusteringConfig);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, Boolean rollbackUsingMarkers, HoodieIndex.IndexType indexType) {
    return getConfigBuilder(autoCommit, rollbackUsingMarkers, indexType, 1024 * 1024 * 1024L, HoodieClusteringConfig.newBuilder().build());
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, Boolean rollbackUsingMarkers, HoodieIndex.IndexType indexType,
      long compactionSmallFileSize, HoodieClusteringConfig clusteringConfig) {
    return HoodieWriteConfig.newBuilder().withPath(basePath()).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(compactionSmallFileSize)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).parquetMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withRemoteServerPort(timelineServicePort)
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withClusteringConfig(clusteringConfig)
        .withRollbackUsingMarkers(rollbackUsingMarkers)
        // Enable lock provider and concurrency control for functional tests to catch any potential deadlock issues
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build());
  }

  protected Dataset<Row> toDataset(List<HoodieRecord> records, Schema schema) {
    List<GenericRecord> avroRecords = records.stream()
        .map(r -> (GenericRecord) r.getData())
        .collect(Collectors.toList());
    JavaRDD<GenericRecord> jrdd = jsc.parallelize(avroRecords, 2);
    return AvroConversionUtils.createDataFrame(jrdd.rdd(), schema.toString(), spark);
  }

  protected int incrementTimelineServicePortToUse() {
    // Increment the timeline service port for each individual test
    // to avoid port reuse causing failures
    timelineServicePort = (timelineServicePort + 1 - 1024) % (65536 - 1024) + 1024;
    return timelineServicePort;
  }

  /**
   * Check if two dataframes are equal.
   *
   * @param expectedDf      expected dataframe
   * @param actualDf        actual dataframe
   * @param validateColumns columns to validate
   * @return true if dataframes are equal, false otherwise
   */
  public static boolean areDataframesEqual(Dataset<Row> expectedDf,
                                           Dataset<Row> actualDf,
                                           Set<String> validateColumns) {
    // Normalize schema order
    String[] sortedColumnNames = Arrays.stream(expectedDf.columns())
        .filter(validateColumns::contains).sorted().toArray(String[]::new);

    // Reorder columns in both datasets
    Dataset<Row> df1Normalized = expectedDf.selectExpr(sortedColumnNames);
    Dataset<Row> df2Normalized = actualDf.selectExpr(sortedColumnNames);

    // Check for differences
    return df1Normalized.except(df2Normalized).isEmpty()
        && df2Normalized.except(df1Normalized).isEmpty();
  }
}
