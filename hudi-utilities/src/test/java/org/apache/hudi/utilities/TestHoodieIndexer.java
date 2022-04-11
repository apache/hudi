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
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.hudi.common.table.HoodieTableMetaClient.reload;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieIndexer extends HoodieCommonTestHarness implements SparkProvider {

  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  private static transient HoodieSparkEngineContext context;

  @BeforeEach
  public void init() throws IOException {
    boolean initialized = spark != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      SparkRDDWriteClient.registerClasses(sparkConf);
      HoodieReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
    }
    initPath();
    initMetaClient();
  }

  @Test
  public void testGetRequestedPartitionTypes() {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    config.basePath = basePath;
    config.tableName = "indexer_test";
    config.indexTypes = "FILES,BLOOM_FILTERS,COLUMN_STATS";
    HoodieIndexer indexer = new HoodieIndexer(jsc, config);
    List<MetadataPartitionType> partitionTypes = indexer.getRequestedPartitionTypes(config.indexTypes);
    assertFalse(partitionTypes.contains(FILES));
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
    String tableName = "indexer_test";
    HoodieWriteConfig.Builder writeConfigBuilder = getWriteConfigBuilder(basePath, tableName);
    // enable files and bloom_filters on the regular write client
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

    // validate table config
    assertTrue(HoodieTableMetadataUtil.getCompletedMetadataPartitions(reload(metaClient).getTableConfig()).contains(FILES.getPartitionPath()));
    assertTrue(HoodieTableMetadataUtil.getCompletedMetadataPartitions(reload(metaClient).getTableConfig()).contains(BLOOM_FILTERS.getPartitionPath()));

    // build indexer config which has only column_stats enabled (files is enabled by default)
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource("delta-streamer-config/indexer.properties")).getPath();
    config.basePath = basePath;
    config.tableName = tableName;
    config.indexTypes = "COLUMN_STATS";
    config.runningMode = "scheduleAndExecute";
    config.propsFilePath = propsPath;
    // start the indexer and validate column_stats index is also complete
    HoodieIndexer indexer = new HoodieIndexer(jsc, config);
    assertEquals(0, indexer.start(0));

    // validate table config
    assertTrue(HoodieTableMetadataUtil.getCompletedMetadataPartitions(reload(metaClient).getTableConfig()).contains(FILES.getPartitionPath()));
    assertTrue(HoodieTableMetadataUtil.getCompletedMetadataPartitions(reload(metaClient).getTableConfig()).contains(BLOOM_FILTERS.getPartitionPath()));
    assertTrue(HoodieTableMetadataUtil.getCompletedMetadataPartitions(reload(metaClient).getTableConfig()).contains(COLUMN_STATS.getPartitionPath()));
    // validate metadata partitions actually exist
    assertTrue(HoodieTableMetadataUtil.metadataPartitionExists(basePath, context, FILES));
    assertTrue(HoodieTableMetadataUtil.metadataPartitionExists(basePath, context, COLUMN_STATS));
    assertTrue(HoodieTableMetadataUtil.metadataPartitionExists(basePath, context, BLOOM_FILTERS));
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
