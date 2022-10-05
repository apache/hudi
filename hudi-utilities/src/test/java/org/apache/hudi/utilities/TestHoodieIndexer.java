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

package org.apache.hudi.utilities;

import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableMetaClient.reload;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.metadataPartitionExists;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.HoodieIndexer.DROP_INDEX;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE_AND_EXECUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieIndexer extends HoodieCommonTestHarness implements SparkProvider {

  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  private static transient HoodieSparkEngineContext context;
  private static int colStatsFileGroupCount;

  @BeforeEach
  public void init() throws IOException {
    boolean initialized = spark != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      SparkRDDWriteClient.registerClasses(sparkConf);
      SparkRDDReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
      colStatsFileGroupCount = HoodieMetadataConfig.METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT.defaultValue();
    }
    initPath();
    initMetaClient();
  }

  protected void initMetaClient() throws IOException {
    String rootPathStr = "file://" + tempDir.toAbsolutePath().toString();
    Path rootPath = new Path(rootPathStr);
    rootPath.getFileSystem(jsc.hadoopConfiguration()).mkdirs(rootPath);
    metaClient = HoodieTestUtils.init(rootPathStr, getTableType());
    basePath = metaClient.getBasePath();
  }

  @Test
  public void testGetRequestedPartitionTypes() {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    config.basePath = basePath;
    config.tableName = "indexer_test";
    config.indexTypes = "FILES,BLOOM_FILTERS,COLUMN_STATS";
    HoodieIndexer indexer = new HoodieIndexer(jsc, config);
    List<MetadataPartitionType> partitionTypes = indexer.getRequestedPartitionTypes(config.indexTypes, Option.empty());
    assertTrue(partitionTypes.contains(FILES));
    assertTrue(partitionTypes.contains(BLOOM_FILTERS));
    assertTrue(partitionTypes.contains(COLUMN_STATS));
  }

  @Test
  public void testIsIndexBuiltForAllRequestedTypes() {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    config.basePath = basePath;
    config.tableName = "indexer_test";
    config.indexTypes = "BLOOM_FILTERS,COLUMN_STATS";
    HoodieIndexer indexer = new HoodieIndexer(jsc, config);
    HoodieIndexCommitMetadata commitMetadata = HoodieIndexCommitMetadata.newBuilder()
        .setIndexPartitionInfos(Arrays.asList(new HoodieIndexPartitionInfo(
            1,
            COLUMN_STATS.getPartitionPath(),
            "0000")))
        .build();
    assertFalse(indexer.isIndexBuiltForAllRequestedTypes(commitMetadata.getIndexPartitionInfos()));

    config.indexTypes = "COLUMN_STATS";
    indexer = new HoodieIndexer(jsc, config);
    assertTrue(indexer.isIndexBuiltForAllRequestedTypes(commitMetadata.getIndexPartitionInfos()));
  }

  @Test
  public void testIndexerWithNotAllIndexesEnabled() {
    initTestDataGenerator();
    tableName = "indexer_test";
    // enable files and bloom_filters on the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(true, false).withMetadataIndexBloomFilter(true);
    initializeWriteClient(metadataConfigBuilder.build());

    // validate table config
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));

    // build indexer config which has only column_stats enabled (files and bloom filter is already enabled)
    indexMetadataPartitionsAndAssert(COLUMN_STATS, Arrays.asList(new MetadataPartitionType[] {FILES, BLOOM_FILTERS}), Collections.emptyList());
  }

  @Test
  public void testIndexerWithFilesPartition() {
    initTestDataGenerator();
    tableName = "indexer_test";
    // enable files and bloom_filters on the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(false, false).withMetadataIndexBloomFilter(true);
    initializeWriteClient(metadataConfigBuilder.build());

    // validate table config
    assertFalse(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));

    // build indexer config which has only files enabled
    indexMetadataPartitionsAndAssert(FILES, Collections.emptyList(), Arrays.asList(new MetadataPartitionType[] {COLUMN_STATS, BLOOM_FILTERS}));
  }

  private static Stream<Arguments> colStatsFileGroupCountParams() {
    return Stream.of(
        Arguments.of(1),
        Arguments.of(2),
        Arguments.of(4),
        Arguments.of(8)
    );
  }

  @ParameterizedTest
  @MethodSource("colStatsFileGroupCountParams")
  public void testColStatsFileGroupCount(int colStatsFileGroupCount) {
    TestHoodieIndexer.colStatsFileGroupCount = colStatsFileGroupCount;
    initTestDataGenerator();
    tableName = "indexer_test";
    // enable files and bloom_filters on the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(false, false).withMetadataIndexBloomFilter(true);
    initializeWriteClient(metadataConfigBuilder.build());

    // validate table config
    assertFalse(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));

    // build indexer config which has only files enabled
    indexMetadataPartitionsAndAssert(FILES, Collections.emptyList(), Arrays.asList(new MetadataPartitionType[] {COLUMN_STATS, BLOOM_FILTERS}));

    // build indexer config which has only col stats enabled
    indexMetadataPartitionsAndAssert(COLUMN_STATS, Collections.singletonList(FILES), Arrays.asList(new MetadataPartitionType[] {BLOOM_FILTERS}));

    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(metaClient.getHadoopConf()).setBasePath(metaClient.getMetaPath() + "/metadata").build();
    List<FileSlice> partitionFileSlices =
        HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, COLUMN_STATS.getPartitionPath());
    assertEquals(partitionFileSlices.size(), colStatsFileGroupCount);
  }

  /**
   * If first time indexing is done for any other partition other than FILES partition, exception will be thrown, given metadata table is not initialized in synchronous code path
   * with regular writers.
   */
  @Test
  public void testIndexerForExceptionWithNonFilesPartition() {
    initTestDataGenerator();
    tableName = "indexer_test";
    // enable files and bloom_filters on the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(false, false);
    initializeWriteClient(metadataConfigBuilder.build());
    // validate table config
    assertFalse(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));

    // build indexer config which has only column stats enabled. expected to throw exception.
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource("delta-streamer-config/indexer.properties")).getPath();
    config.basePath = basePath;
    config.tableName = tableName;
    config.indexTypes = COLUMN_STATS.name();
    config.runningMode = SCHEDULE_AND_EXECUTE;
    config.propsFilePath = propsPath;
    // start the indexer and validate index building fails
    HoodieIndexer indexer = new HoodieIndexer(jsc, config);
    assertEquals(-1, indexer.start(0));

    // validate table config
    metaClient = reload(metaClient);
    assertFalse(metaClient.getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertFalse(metaClient.getTableConfig().getMetadataPartitions().contains(COLUMN_STATS.getPartitionPath()));
    assertFalse(metaClient.getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    // validate metadata partitions actually exist
    assertFalse(metadataPartitionExists(basePath, context, FILES));

    // trigger FILES partition and indexing should succeed.
    indexMetadataPartitionsAndAssert(FILES, Collections.emptyList(), Arrays.asList(new MetadataPartitionType[] {COLUMN_STATS, BLOOM_FILTERS}));

    // build indexer config which has only col stats enabled
    indexMetadataPartitionsAndAssert(COLUMN_STATS, Collections.singletonList(FILES), Arrays.asList(new MetadataPartitionType[] {BLOOM_FILTERS}));

    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(metaClient.getHadoopConf()).setBasePath(metaClient.getMetaPath() + "/metadata").build();
    List<FileSlice> partitionFileSlices =
        HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, COLUMN_STATS.getPartitionPath());
    assertEquals(partitionFileSlices.size(), HoodieMetadataConfig.METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT.defaultValue());
  }

  private void initializeWriteClient(HoodieMetadataConfig metadataConfig) {
    HoodieWriteConfig.Builder writeConfigBuilder = getWriteConfigBuilder(basePath, tableName);
    HoodieWriteConfig writeConfig = writeConfigBuilder.withMetadataConfig(metadataConfig).build();
    // do one upsert with synchronous metadata update
    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, writeConfig);
    String instant = "0001";
    writeClient.startCommitWithTime(instant);
    List<HoodieRecord> records = dataGen.generateInserts(instant, 100);
    JavaRDD<WriteStatus> result = writeClient.upsert(jsc.parallelize(records, 1), instant);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);
  }

  private void indexMetadataPartitionsAndAssert(MetadataPartitionType partitionTypeToIndex, List<MetadataPartitionType> alreadyCompletedPartitions, List<MetadataPartitionType> nonExistantPartitions) {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource("delta-streamer-config/indexer.properties")).getPath();
    config.basePath = basePath;
    config.tableName = tableName;
    config.indexTypes = partitionTypeToIndex.name();
    config.runningMode = SCHEDULE_AND_EXECUTE;
    config.propsFilePath = propsPath;
    if (partitionTypeToIndex.getPartitionPath().equals(COLUMN_STATS.getPartitionPath())) {
      config.configs.add(HoodieMetadataConfig.METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT.key() + "=" + colStatsFileGroupCount);
    }
    // start the indexer and validate files index is completely built out
    HoodieIndexer indexer = new HoodieIndexer(jsc, config);
    assertEquals(0, indexer.start(0));

    // validate table config
    metaClient = reload(metaClient);
    Set<String> completedPartitions = metaClient.getTableConfig().getMetadataPartitions();
    assertTrue(completedPartitions.contains(partitionTypeToIndex.getPartitionPath()));
    alreadyCompletedPartitions.forEach(entry -> assertTrue(completedPartitions.contains(entry.getPartitionPath())));
    nonExistantPartitions.forEach(entry -> assertFalse(completedPartitions.contains(entry.getPartitionPath())));

    // validate metadata partitions actually exist
    assertTrue(metadataPartitionExists(basePath, context, partitionTypeToIndex));
    alreadyCompletedPartitions.forEach(entry -> assertTrue(metadataPartitionExists(basePath, context, entry)));
  }

  @Test
  public void testIndexerDropPartitionDeletesInstantFromTimeline() {
    initTestDataGenerator();
    String tableName = "indexer_test";
    HoodieWriteConfig.Builder writeConfigBuilder = getWriteConfigBuilder(basePath, tableName);
    // enable files on the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(true, false).withMetadataIndexBloomFilter(true);
    HoodieWriteConfig writeConfig = writeConfigBuilder.withMetadataConfig(metadataConfigBuilder.build()).build();
    // do one upsert with synchronous metadata update
    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, writeConfig);
    String instant = "0001";
    writeClient.startCommitWithTime(instant);
    List<HoodieRecord> records = dataGen.generateInserts(instant, 100);
    JavaRDD<WriteStatus> result = writeClient.upsert(jsc.parallelize(records, 1), instant);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    // validate partitions built successfully
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath, context, FILES));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath, context, BLOOM_FILTERS));

    // build indexer config which has only column_stats enabled (files is enabled by default)
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource("delta-streamer-config/indexer.properties")).getPath();
    config.basePath = basePath;
    config.tableName = tableName;
    config.indexTypes = COLUMN_STATS.name();
    config.runningMode = SCHEDULE;
    config.propsFilePath = propsPath;

    // schedule indexing and validate column_stats index is also initialized
    HoodieIndexer indexer = new HoodieIndexer(jsc, config);
    assertEquals(0, indexer.start(0));
    Option<HoodieInstant> indexInstantInTimeline = metaClient.reloadActiveTimeline().filterPendingIndexTimeline().lastInstant();
    assertTrue(indexInstantInTimeline.isPresent());
    assertEquals(REQUESTED, indexInstantInTimeline.get().getState());
    assertTrue(metadataPartitionExists(basePath, context, COLUMN_STATS));

    // drop column_stats and validate indexing.requested is also removed from the timeline
    config.runningMode = DROP_INDEX;
    indexer = new HoodieIndexer(jsc, config);
    assertEquals(0, indexer.start(0));
    indexInstantInTimeline = metaClient.reloadActiveTimeline().filterPendingIndexTimeline().lastInstant();
    assertFalse(indexInstantInTimeline.isPresent());
    assertFalse(metadataPartitionExists(basePath, context, COLUMN_STATS));

    // check other partitions are intact
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath, context, FILES));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath, context, BLOOM_FILTERS));
  }

  @Test
  public void testTwoIndexersOneCreateOneDropPartition() {
    initTestDataGenerator();
    String tableName = "indexer_test";
    HoodieWriteConfig.Builder writeConfigBuilder = getWriteConfigBuilder(basePath, tableName);
    // enable files on the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(true, false);
    HoodieWriteConfig writeConfig = writeConfigBuilder.withMetadataConfig(metadataConfigBuilder.build()).build();
    // do one upsert with synchronous metadata update
    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, writeConfig);
    String instant = "0001";
    writeClient.startCommitWithTime(instant);
    List<HoodieRecord> records = dataGen.generateInserts(instant, 100);
    JavaRDD<WriteStatus> result = writeClient.upsert(jsc.parallelize(records, 1), instant);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    // validate files partition built successfully
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath, context, FILES));

    // build indexer config which has only bloom_filters enabled
    HoodieIndexer.Config config = getHoodieIndexConfig(BLOOM_FILTERS.name(), SCHEDULE_AND_EXECUTE, "delta-streamer-config/indexer-only-bloom.properties");
    // start the indexer and validate bloom_filters index is also complete
    HoodieIndexer indexer = new HoodieIndexer(jsc, config);
    assertEquals(0, indexer.start(0));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath, context, BLOOM_FILTERS));

    // completed index timeline for later validation
    Option<HoodieInstant> bloomIndexInstant = metaClient.reloadActiveTimeline().filterCompletedIndexTimeline().lastInstant();
    assertTrue(bloomIndexInstant.isPresent());

    // build indexer config which has only column_stats enabled
    config = getHoodieIndexConfig(COLUMN_STATS.name(), SCHEDULE, "delta-streamer-config/indexer.properties");

    // schedule indexing and validate column_stats index is also initialized
    // and indexing.requested instant is present
    indexer = new HoodieIndexer(jsc, config);
    assertEquals(0, indexer.start(0));
    Option<HoodieInstant> columnStatsIndexInstant = metaClient.reloadActiveTimeline().filterPendingIndexTimeline().lastInstant();
    assertTrue(columnStatsIndexInstant.isPresent());
    assertEquals(REQUESTED, columnStatsIndexInstant.get().getState());
    assertTrue(metadataPartitionExists(basePath, context, COLUMN_STATS));

    // drop column_stats and validate indexing.requested is also removed from the timeline
    // and completed indexing instant corresponding to bloom_filters index is still present
    dropIndexAndAssert(COLUMN_STATS, "delta-streamer-config/indexer.properties", Option.empty());

    // check other partitions are intact
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath, context, FILES));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath, context, BLOOM_FILTERS));

    // drop bloom filter partition. timeline files should not be deleted since the index building is complete.
    dropIndexAndAssert(BLOOM_FILTERS, "delta-streamer-config/indexer-only-bloom.properties", bloomIndexInstant);
  }

  private void dropIndexAndAssert(MetadataPartitionType indexType, String resourceFilePath, Option<HoodieInstant> completedIndexInstant) {
    HoodieIndexer.Config config = getHoodieIndexConfig(indexType.name(), DROP_INDEX, resourceFilePath);
    HoodieIndexer indexer = new HoodieIndexer(jsc, config);
    assertEquals(0, indexer.start(0));
    Option<HoodieInstant> pendingFlights = metaClient.reloadActiveTimeline().filterPendingIndexTimeline().lastInstant();
    assertFalse(pendingFlights.isPresent());
    assertFalse(metadataPartitionExists(basePath, context, indexType));
    if (completedIndexInstant.isPresent()) {
      assertEquals(completedIndexInstant, metaClient.reloadActiveTimeline().filterCompletedIndexTimeline().lastInstant());
    }
  }

  private HoodieIndexer.Config getHoodieIndexConfig(String indexType, String runMode, String resourceFilePath) {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource(resourceFilePath)).getPath();
    config.basePath = basePath;
    config.tableName = tableName;
    config.indexTypes = indexType;
    config.runningMode = runMode;
    config.propsFilePath = propsPath;
    return config;
  }

  private static HoodieWriteConfig.Builder getWriteConfigBuilder(String basePath, String tableName) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable(tableName);
  }

  private static HoodieMetadataConfig.Builder getMetadataConfigBuilder(boolean enable, boolean asyncIndex) {
    return HoodieMetadataConfig.newBuilder()
        .enable(enable)
        .withAsyncIndex(asyncIndex);
  }

  @Override
  public HoodieEngineContext context() {
    return context;
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
}
