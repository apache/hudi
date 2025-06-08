/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.utilities.multitable;

import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.providers.SparkProvider;
import org.apache.hudi.utilities.HoodieCompactor;

import org.apache.hadoop.fs.Path;
import org.apache.spark.HoodieSparkKryoRegistrar$;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.multitable.MultiTableServiceUtils.Constants.TABLES_SKIP_WRONG_PATH;
import static org.apache.hudi.utilities.multitable.MultiTableServiceUtils.Constants.TABLES_TO_BE_SERVED_PROP;

/**
 * Tests for HoodieMultiTableServicesMain
 * @see HoodieMultiTableServicesMain
 */
class TestHoodieMultiTableServicesMain extends HoodieCommonTestHarness implements SparkProvider {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieMultiTableServicesMain.class);

  protected boolean initialized = false;

  private static SparkSession spark;
  private static SQLContext sqlContext;
  private static JavaSparkContext jsc;
  private static HoodieSparkEngineContext context;

  protected transient HoodieTestDataGenerator dataGen = null;

  @BeforeEach
  public void init() throws IOException, ExecutionException, InterruptedException {
    boolean initialized = spark != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      HoodieSparkKryoRegistrar$.MODULE$.register(sparkConf);
      SparkRDDReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
    }
    initPath();
    prepareData();
  }

  @Test
  public void testRunAllServices() throws IOException, ExecutionException, InterruptedException {
    HoodieMultiTableServicesMain.Config cfg = getHoodieMultiServiceConfig();
    cfg.batch = true;
    HoodieTableMetaClient metaClient1 = getMetaClient("table1");
    HoodieTableMetaClient metaClient2 = getMetaClient("table2");
    HoodieMultiTableServicesMain main = new HoodieMultiTableServicesMain(jsc, cfg);
    main.startServices();
    // Verify cleans
    Assertions.assertEquals(1, metaClient1.reloadActiveTimeline().getCleanerTimeline().countInstants());
    Assertions.assertEquals(1, metaClient2.reloadActiveTimeline().getCleanerTimeline().countInstants());
    // Verify delta commits
    Assertions.assertEquals(2, metaClient1.reloadActiveTimeline().getDeltaCommitTimeline().countInstants());
    Assertions.assertEquals(2, metaClient2.reloadActiveTimeline().getDeltaCommitTimeline().countInstants());
    // Verify replace commits
    Assertions.assertEquals(1, metaClient1.reloadActiveTimeline().getCompletedReplaceTimeline().countInstants());
    Assertions.assertEquals(1, metaClient2.reloadActiveTimeline().getCompletedReplaceTimeline().countInstants());
    // Verify compactions, delta commits and replace commits
    Assertions.assertEquals(4, metaClient1.reloadActiveTimeline().getCommitsTimeline().countInstants());
    Assertions.assertEquals(4, metaClient2.reloadActiveTimeline().getCommitsTimeline().countInstants());
  }

  @Test
  public void testRunAllServicesForSingleTable() throws IOException, ExecutionException, InterruptedException {
    HoodieMultiTableServicesMain.Config cfg = getHoodieMultiServiceConfig();
    HoodieTableMetaClient metaClient1 = getMetaClient("table1");
    cfg.batch = true;
    cfg.basePath = Collections.singletonList(metaClient1.getBasePath().toString());
    HoodieMultiTableServicesMain main = new HoodieMultiTableServicesMain(jsc, cfg);
    main.startServices();
    // Verify cleans
    Assertions.assertEquals(1, metaClient1.reloadActiveTimeline().getCleanerTimeline().countInstants());
    // Verify delta commits
    Assertions.assertEquals(2, metaClient1.reloadActiveTimeline().getDeltaCommitTimeline().countInstants());
    // Verify replace commits
    Assertions.assertEquals(1, metaClient1.reloadActiveTimeline().getCompletedReplaceTimeline().countInstants());
    // Verify compactions, delta commits and replace commits
    Assertions.assertEquals(4, metaClient1.reloadActiveTimeline().getCommitsTimeline().countInstants());
  }

  @Test
  public void testStreamRunAllServices() throws IOException, ExecutionException, InterruptedException {
    HoodieMultiTableServicesMain.Config cfg = getHoodieMultiServiceConfig();
    HoodieMultiTableServicesMain main = new HoodieMultiTableServicesMain(jsc, cfg);
    new Thread(() -> {
      try {
        Thread.sleep(10000);
        LOG.info("Shutdown the table services");
        main.cancel();
      } catch (InterruptedException e) {
        LOG.warn("InterruptedException: ", e);
      }
    }).start();
    main.startServices();
    HoodieTableMetaClient metaClient1 = getMetaClient("table1");
    HoodieTableMetaClient metaClient2 = getMetaClient("table2");
    // Verify cleans
    Assertions.assertEquals(1, metaClient1.reloadActiveTimeline().getCleanerTimeline().countInstants());
    Assertions.assertEquals(1, metaClient2.reloadActiveTimeline().getCleanerTimeline().countInstants());
    // Verify compactions, delta commits and replace commits
    Assertions.assertEquals(4, metaClient1.reloadActiveTimeline().getCommitsTimeline().countInstants());
    Assertions.assertEquals(4, metaClient2.reloadActiveTimeline().getCommitsTimeline().countInstants());
  }

  @Test
  public void testRunMultiTableServicesWithOneWrongPath() throws IOException {
    // batch run table service
    HoodieMultiTableServicesMain.Config cfg = getHoodieMultiServiceConfig();
    cfg.autoDiscovery = false;
    cfg.batch = true;
    HoodieTableMetaClient metaClient1 = getMetaClient("table1");
    cfg.configs.add(String.format("%s=%s", TABLES_SKIP_WRONG_PATH, "true"));
    cfg.configs.add(String.format("%s=%s", TABLES_TO_BE_SERVED_PROP, metaClient1.getBasePath() + ",file:///fakepath"));
    HoodieMultiTableServicesMain main = new HoodieMultiTableServicesMain(jsc, cfg);
    try {
      main.startServices();
    } catch (Exception e) {
      Assertions.assertFalse(e instanceof TableNotFoundException);
    }

    // stream run table service
    cfg.batch = false;
    new Thread(() -> {
      try {
        Thread.sleep(10000);
        LOG.info("Shutdown the table services");
        main.cancel();
      } catch (InterruptedException e) {
        LOG.warn("InterruptedException: ", e);
      }
    }).start();
    try {
      main.startServices();
    } catch (Exception e) {
      Assertions.assertFalse(e instanceof TableNotFoundException);
    }

    // When we disable the skip wrong path, throw the exception.
    cfg.batch = true;
    cfg.configs.add(String.format("%s=%s", TABLES_SKIP_WRONG_PATH, "false"));
    try {
      main.startServices();
    } catch (Exception e) {
      Assertions.assertTrue(e instanceof TableNotFoundException);
    }
  }

  private void prepareData() throws IOException {
    initTestDataGenerator();
    HoodieTableMetaClient metaClient1 = getMetaClient("table1");
    HoodieTableMetaClient metaClient2 = getMetaClient("table2");
    String instant1 = InProcessTimeGenerator.createNewInstantTime(0);
    writeToTable(metaClient1.getBasePath(), instant1, false);
    writeToTable(metaClient2.getBasePath(), instant1, false);
    String instant2 = InProcessTimeGenerator.createNewInstantTime(1);
    writeToTable(metaClient1.getBasePath(), instant2, true);
    writeToTable(metaClient2.getBasePath(), instant2, true);
    Assertions.assertEquals(0, metaClient1.reloadActiveTimeline().getCleanerTimeline().countInstants());
    Assertions.assertEquals(0, metaClient2.reloadActiveTimeline().getCleanerTimeline().countInstants());
  }

  private void writeToTable(StoragePath basePath, String instant, boolean update) throws IOException {
    String tableName = "test";
    HoodieWriteConfig.Builder writeConfigBuilder = getWriteConfigBuilder(basePath, tableName);
    // enable files and bloom_filters on the regular write client
    HoodieWriteConfig writeConfig = writeConfigBuilder.build();
    // do one upsert with synchronous metadata update
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, writeConfig)) {
      List<HoodieRecord> records;
      WriteClientTestUtils.startCommitWithTime(writeClient, instant);
      if (update) {
        records = dataGen.generateUpdates(instant, 100);
      } else {
        records = dataGen.generateInserts(instant, 100);
      }
      JavaRDD<WriteStatus> result = writeClient.upsert(jsc.parallelize(records, 8), instant);
      List<WriteStatus> statuses = result.collect();
      assertNoWriteErrors(statuses);
      writeClient.commit(instant, jsc().parallelize(statuses));
    }
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(StoragePath basePath, String tableName) {
    Properties properties = new Properties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(4, 4)
        .withBulkInsertParallelism(4)
        .withFinalizeWriteParallelism(2)
        .withProps(makeIndexConfig(HoodieIndex.IndexType.BUCKET))
        .withTableServicesEnabled(false)
        .withLayoutConfig(HoodieLayoutConfig.newBuilder()
            .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
            .withLayoutPartitioner("org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner")
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .forTable(tableName);
  }

  protected HoodieTableMetaClient getMetaClient(String tableName) throws IOException {
    String rootPathStr = "file://" + tempDir.toAbsolutePath() + "/" + tableName;
    Path rootPath = new Path(rootPathStr);
    rootPath.getFileSystem(jsc.hadoopConfiguration()).mkdirs(rootPath);
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    props.setProperty(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "_row_key");
    return HoodieTestUtils.init(
        HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), rootPathStr, getTableType(), props);
  }

  private Properties makeIndexConfig(HoodieIndex.IndexType indexType) {
    Properties props = new Properties();
    HoodieIndexConfig.Builder indexConfig = HoodieIndexConfig.newBuilder()
        .withIndexType(indexType);
    if (indexType.equals(HoodieIndex.IndexType.BUCKET)) {
      props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
      indexConfig.fromProperties(props)
          .withIndexKeyField("_row_key")
          .withBucketNum("1")
          .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE);
      props.putAll(indexConfig.build().getProps());
      props.putAll(HoodieLayoutConfig.newBuilder().fromProperties(props)
          .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
          .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build().getProps());
    }
    return props;
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private HoodieMultiTableServicesMain.Config getHoodieMultiServiceConfig() {
    HoodieMultiTableServicesMain.Config cfg = new HoodieMultiTableServicesMain.Config();
    cfg.autoDiscovery = true;
    cfg.enableCompaction = true;
    cfg.enableClustering = true;
    cfg.enableClean = true;
    cfg.enableArchive = true;
    List<String> configs = new ArrayList<>();
    configs.add(String.format("%s=%s", HoodieCleanConfig.CLEANER_POLICY.key(), HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS));
    configs.add(String.format("%s=%s", HoodieCleanConfig.AUTO_CLEAN.key(), "false"));
    configs.add(String.format("%s=%s", HoodieCleanConfig.CLEANER_FILE_VERSIONS_RETAINED.key(), "1"));
    configs.add(String.format("%s=%s", HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "0"));
    cfg.configs = configs;
    cfg.compactionRunningMode = HoodieCompactor.SCHEDULE_AND_EXECUTE;
    cfg.compactionStrategyClassName = LogFileSizeBasedCompactionStrategy.class.getName();
    cfg.clusteringRunningMode = HoodieCompactor.SCHEDULE_AND_EXECUTE;
    cfg.basePath = Collections.singletonList(tempDir.toAbsolutePath().toString());
    cfg.scheduleDelay = 50000;
    return cfg;
  }

  /**
   * Initializes a test data generator which used to generate test datas.
   */
  protected void initTestDataGenerator() {
    dataGen = new HoodieTestDataGenerator();
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

  @AfterAll
  public static synchronized void cleanUpAfterAll() {
    if (spark != null) {
      spark.close();
      spark = null;
    }
  }

}
