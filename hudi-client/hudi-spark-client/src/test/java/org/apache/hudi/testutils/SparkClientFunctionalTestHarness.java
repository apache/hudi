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

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.providers.HoodieMetaClientProvider;
import org.apache.hudi.testutils.providers.HoodieWriteClientProvider;
import org.apache.hudi.testutils.providers.SparkProvider;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
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

  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

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

  public Configuration hadoopConf() {
    return jsc.hadoopConfiguration();
  }

  public FileSystem fs() {
    return FSUtils.getFs(basePath(), hadoopConf());
  }

  @Override
  public HoodieSparkEngineContext context() {
    return context;
  }

  public HoodieTableMetaClient getHoodieMetaClient(HoodieTableType tableType) throws IOException {
    return getHoodieMetaClient(tableType, new Properties());
  }

  public HoodieTableMetaClient getHoodieMetaClient(HoodieTableType tableType, Properties props) throws IOException {
    return getHoodieMetaClient(hadoopConf(), basePath(), tableType, props);
  }

  public HoodieTableMetaClient getHoodieMetaClient(Configuration hadoopConf, String basePath, HoodieTableType tableType, Properties props) throws IOException {
    props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(tableType)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .build();
    return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, props);
  }

  public HoodieTableMetaClient getHoodieMetaClient(Configuration hadoopConf, String basePath) throws IOException {
    return getHoodieMetaClient(hadoopConf, basePath, new Properties());
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(Configuration hadoopConf, String basePath, Properties props) throws IOException {
    props = HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(props)
        .build();
    return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, props);
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
      SparkRDDWriteClient.registerClasses(sparkConf);
      HoodieReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
      timelineService = HoodieClientTestUtils.initTimelineService(
          context, basePath(), incrementTimelineServicePortToUse());
      timelineServicePort = timelineService.getServerPort();
    }
  }

  /**
   * To clean up Spark resources after all testcases have run in functional tests.
   *
   * Spark session and contexts were reused for testcases in the same test class. Some
   * testcase may invoke this specifically to clean up in case of repeated test runs.
   */
  @AfterAll
  public static synchronized void resetSpark() {
    if (spark != null) {
      spark.close();
      spark = null;
    }
    if (timelineService != null) {
      timelineService.close();
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

  protected Stream<HoodieBaseFile> insertRecords(HoodieTableMetaClient metaClient, List<HoodieRecord> records,
                                                 SparkRDDWriteClient client, HoodieWriteConfig cfg, String commitTime) throws IOException {
    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);

    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
    List<WriteStatus> statuses = client.insert(writeRecords, commitTime).collect();
    assertNoWriteErrors(statuses);
    assertFileSizesEqual(statuses, status -> FSUtils.getFileSize(reloadedMetaClient.getFs(), new Path(reloadedMetaClient.getBasePath(), status.getStat().getPath())));

    HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), reloadedMetaClient);

    Option<HoodieInstant> deltaCommit = reloadedMetaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals(commitTime, deltaCommit.get().getTimestamp(), "Delta commit should be specified value");

    Option<HoodieInstant> commit = reloadedMetaClient.getActiveTimeline().getCommitTimeline().lastInstant();
    assertFalse(commit.isPresent());

    FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
    TableFileSystemView.BaseFileOnlyView roView =
        getHoodieTableFileSystemView(reloadedMetaClient, reloadedMetaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    Stream<HoodieBaseFile> dataFilesToRead = roView.getLatestBaseFiles();
    assertTrue(!dataFilesToRead.findAny().isPresent());

    roView = getHoodieTableFileSystemView(reloadedMetaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestBaseFiles();
    return dataFilesToRead;
  }

  protected void updateRecords(HoodieTableMetaClient metaClient, List<HoodieRecord> records, SparkRDDWriteClient client, HoodieWriteConfig cfg, String commitTime) throws IOException {
    HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);

    Map<HoodieKey, HoodieRecord> recordsMap = new HashMap<>();
    for (HoodieRecord rec : records) {
      if (!recordsMap.containsKey(rec.getKey())) {
        recordsMap.put(rec.getKey(), rec);
      }
    }

    List<WriteStatus> statuses = client.upsert(jsc().parallelize(records, 1), commitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);
    assertFileSizesEqual(statuses, status -> FSUtils.getFileSize(reloadedMetaClient.getFs(), new Path(reloadedMetaClient.getBasePath(), status.getStat().getPath())));

    Option<HoodieInstant> deltaCommit = reloadedMetaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals(commitTime, deltaCommit.get().getTimestamp(),
        "Latest Delta commit should match specified time");

    Option<HoodieInstant> commit = reloadedMetaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());
  }

  protected FileStatus[] listAllBaseFilesInPath(HoodieTable table) throws IOException {
    return HoodieTestTable.of(table.getMetaClient()).listAllBaseFiles(table.getBaseFileExtension());
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

  protected void addConfigsForPopulateMetaFields(HoodieWriteConfig.Builder configBuilder, boolean populateMetaFields) {
    if (!populateMetaFields) {
      configBuilder.withProperties(getPropertiesForKeyGen())
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build());
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
        .withAutoCommit(autoCommit)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(compactionSmallFileSize)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).parquetMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withRemoteServerPort(timelineServicePort)
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withClusteringConfig(clusteringConfig)
        .withRollbackUsingMarkers(rollbackUsingMarkers);
  }

  protected int incrementTimelineServicePortToUse() {
    // Increment the timeline service port for each individual test
    // to avoid port reuse causing failures
    timelineServicePort = (timelineServicePort + 1 - 1024) % (65536 - 1024) + 1024;
    return timelineServicePort;
  }
}
