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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncClient;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.utilities.ingestion.HoodieIngestionService;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.sources.TestParquetDFSSourceEmptyBatch;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.StreamSync;
import org.apache.hudi.utilities.streamer.StreamerCheckpointUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.Transformer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Basic tests against {@link HoodieDeltaStreamer}, by issuing bulk_inserts, upserts, inserts. Check counts at the end.
 */
@Slf4j
public class TestHoodieDeltaStreamer extends HoodieDeltaStreamerTestBase {

  // Bounds the stop a failure triggers, so a wedged streamer cannot hang the test it already failed. Kept
  // well inside what the @Timeout(600) continuous-mode tests have left after the 360s they already spend in
  // the wait: once that budget blows, JUnit replaces the test's own failure with its timeout.
  private static final long STREAMER_STOP_TIMEOUT_SECS = 30;

  @AfterEach
  public void perTestAfterEach() {
    testNum++;
  }

  @Test
  public void testProps() {
    TypedProperties props =
        new DFSPropertiesConfiguration(fs.getConf(), new StoragePath(basePath + "/" + PROPS_FILENAME_TEST_SOURCE)).getProps();
    assertEquals(2, props.getInteger("hoodie.upsert.shuffle.parallelism"));
    assertEquals("_row_key", props.getString("hoodie.datasource.write.recordkey.field"));
    assertEquals("org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer$TestGenerator",
        props.getString("hoodie.datasource.write.keygenerator.class"));
  }

  @Test
  public void testCombinePropertiesWithSourceOrderingFields() {
    HoodieStreamer.Config cfg = getBaseConfig();
    cfg.sourceOrderingFields = "ts,seq_id";

    TypedProperties props = HoodieStreamer.combineProperties(cfg, Option.empty(), jsc.hadoopConfiguration());

    assertEquals("ts,seq_id", props.getString(HoodieTableConfig.ORDERING_FIELDS.key()));
  }

  @Test
  public void testCombinePropertiesWithoutSourceOrderingFields() {
    HoodieStreamer.Config cfg = getBaseConfig();
    cfg.sourceOrderingFields = null;

    TypedProperties props = HoodieStreamer.combineProperties(cfg, Option.empty(), jsc.hadoopConfiguration());

    assertFalse(props.containsKey(HoodieTableConfig.ORDERING_FIELDS.key()));
  }

  private static HoodieStreamer.Config getBaseConfig() {
    // Base config with all required fields
    HoodieStreamer.Config base = new HoodieStreamer.Config();
    base.targetBasePath = TGT_BASE_PATH_VALUE;
    base.tableType = TABLE_TYPE_VALUE;
    base.targetTableName = TARGET_TABLE_VALUE;
    return base;
  }

  private static Stream<Arguments> provideValidCliArgs() {

    HoodieStreamer.Config base = getBaseConfig();
    // String parameter
    HoodieStreamer.Config conf1 = getBaseConfig();
    conf1.baseFileFormat = BASE_FILE_FORMAT_VALUE;

    // Integer parameter
    HoodieStreamer.Config conf2 = getBaseConfig();
    conf2.sourceLimit = Long.parseLong(SOURCE_LIMIT_VALUE);

    // Boolean Parameter
    HoodieStreamer.Config conf3 = getBaseConfig();
    conf3.enableHiveSync = true;

    // ArrayList Parameter with 1 value
    HoodieStreamer.Config conf4 = getBaseConfig();
    conf4.configs = Arrays.asList(HOODIE_CONF_VALUE1);

    // ArrayList Parameter with comma separated values
    HoodieStreamer.Config conf5 = getBaseConfig();
    conf5.configs = Arrays.asList(HOODIE_CONF_VALUE2);

    // Multiple ArrayList values
    HoodieStreamer.Config conf6 = getBaseConfig();
    conf6.configs = Arrays.asList(HOODIE_CONF_VALUE1, HOODIE_CONF_VALUE2);

    // Super set of all cases
    HoodieStreamer.Config conf = getBaseConfig();
    conf.baseFileFormat = BASE_FILE_FORMAT_VALUE;
    conf.sourceLimit = Long.parseLong(SOURCE_LIMIT_VALUE);
    conf.enableHiveSync = true;
    conf.configs = Arrays.asList(HOODIE_CONF_VALUE1, HOODIE_CONF_VALUE2);

    String[] allConfig = new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE, SOURCE_LIMIT_PARAM,
        SOURCE_LIMIT_VALUE, TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
        BASE_FILE_FORMAT_PARAM, BASE_FILE_FORMAT_VALUE, ENABLE_HIVE_SYNC_PARAM, HOODIE_CONF_PARAM, HOODIE_CONF_VALUE1,
        HOODIE_CONF_PARAM, HOODIE_CONF_VALUE2};

    return Stream.of(
        // Base
        Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
            TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE}, base),
        // String
        Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
            TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
            BASE_FILE_FORMAT_PARAM, BASE_FILE_FORMAT_VALUE}, conf1),
        // Integer
        Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
            TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
            SOURCE_LIMIT_PARAM, SOURCE_LIMIT_VALUE}, conf2),
        // Boolean
        Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
            TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
            ENABLE_HIVE_SYNC_PARAM}, conf3),
        // Array List 1
        Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
            TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
            HOODIE_CONF_PARAM, HOODIE_CONF_VALUE1}, conf4),
        // Array List with comma
        Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
            TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
            HOODIE_CONF_PARAM, HOODIE_CONF_VALUE2}, conf5),
        // Array list with multiple values
        Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
            TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
            HOODIE_CONF_PARAM, HOODIE_CONF_VALUE1, HOODIE_CONF_PARAM, HOODIE_CONF_VALUE2}, conf6),
        // All
        Arguments.of(allConfig, conf)
    );
  }

  @ParameterizedTest
  @MethodSource("provideValidCliArgs")
  public void testValidCommandLineArgs(String[] args, HoodieStreamer.Config expected) {
    assertEquals(expected, HoodieDeltaStreamer.getConfig(args));
  }

  @Test
  public void testKafkaConnectCheckpointProvider() throws IOException {
    String tableBasePath = basePath + "/test_table";
    String bootstrapPath = basePath + "/kafka_topic1";
    String partitionPath = bootstrapPath + "/year=2016/month=05/day=01";
    String filePath = partitionPath + "/kafka_topic1+0+100+200.parquet";
    String checkpointProviderClass = "org.apache.hudi.utilities.checkpointing.KafkaConnectHdfsProvider";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeDropAllConfig(tableBasePath, WriteOperationType.UPSERT);
    TypedProperties props =
        new DFSPropertiesConfiguration(fs.getConf(), new StoragePath(basePath + "/" + PROPS_FILENAME_TEST_SOURCE)).getProps();
    props.put("hoodie.streamer.checkpoint.provider.path", bootstrapPath);
    cfg.initialCheckpointProvider = checkpointProviderClass;
    // create regular kafka connect hdfs dirs
    fs.mkdirs(new Path(bootstrapPath));
    fs.mkdirs(new Path(partitionPath));
    // generate parquet files using kafka connect naming convention
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    Helpers.saveParquetToDFS(Helpers.toGenericRecords(dataGenerator.generateInserts("000", 100)), new Path(filePath));
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg, jsc, fs, jsc.hadoopConfiguration(), Option.ofNullable(props));
    assertEquals("kafka_topic1,0:200", deltaStreamer.getConfig().checkpoint);
  }

  @Test
  public void testPropsWithInvalidKeyGenerator() {
    Exception e = assertThrows(HoodieException.class, () -> {
      String tableBasePath = basePath + "/test_table_invalid_key_gen";
      HoodieDeltaStreamer deltaStreamer =
          new HoodieDeltaStreamer(TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT,
              Collections.singletonList(TripsWithDistanceTransformer.class.getName()), PROPS_FILENAME_TEST_INVALID, false), jsc);
      deltaStreamer.sync();
    }, "Should error out when setting the key generator class property to an invalid value");
    // expected
    log.warn("Expected error during getting the key generator", e);
    assertTrue(e.getMessage().contains("Unable to load class"));
  }

  private static Stream<Arguments> provideInferKeyGenArgs() {
    return Stream.of(
        Arguments.of(
            PROPS_FILENAME_INFER_COMPLEX_KEYGEN,
            ComplexKeyGenerator.class.getName()),
        Arguments.of(
            PROPS_FILENAME_INFER_NONPARTITIONED_KEYGEN,
            NonpartitionedKeyGenerator.class.getName())
    );
  }

  @ParameterizedTest
  @MethodSource("provideInferKeyGenArgs")
  public void testInferKeyGenerator(String propsFilename,
                                    String expectedKeyGeneratorClassName) throws Exception {
    String[] splitNames = propsFilename.split("\\.");
    String tableBasePath = basePath + "/" + splitNames[0];
    syncOnce(TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        Collections.singletonList(TripsWithDistanceTransformer.class.getName()),
        propsFilename, false));
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HoodieTestUtils.getDefaultStorageConf()).setBasePath(tableBasePath).build();
    assertEquals(
        expectedKeyGeneratorClassName, metaClient.getTableConfig().getKeyGeneratorClassName());
    Dataset<Row> res = sqlContext.read().format("hudi").load(tableBasePath);
    assertEquals(1000, res.count());
    assertCheckpointVersion(metaClient);
  }

  static void assertCheckpointVersion(HoodieTableMetaClient metaClient) {
    metaClient.reloadActiveTimeline();
    Option<HoodieCommitMetadata> metadata = HoodieClientTestUtils.getCommitMetadataForInstant(
        metaClient, metaClient.getActiveTimeline().lastInstant().get());
    assertFalse(metadata.isEmpty());
    Map<String, String> extraMetadata = metadata.get().getExtraMetadata();
    assertTrue(extraMetadata.containsKey(STREAMER_CHECKPOINT_KEY_V1));
    assertFalse(extraMetadata.containsKey(STREAMER_CHECKPOINT_KEY_V2));
  }

  @Test
  public void testTableCreation() throws Exception {
    Exception e = assertThrows(TableNotFoundException.class, () -> {
      fs.mkdirs(new Path(basePath + "/not_a_table"));
      syncOnce(TestHelpers.makeConfig(basePath + "/not_a_table", WriteOperationType.BULK_INSERT));
    }, "Should error out when pointed out at a dir thats not a table");
    // expected
    log.debug("Expected error during table creation", e);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTableCreationContainsHiveStylePartitioningEnable(boolean configFlag) throws Exception {
    String tablePath = basePath + "/url_encode_and_hive_style_partitioning_enable_" + configFlag;
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    // Update DeltaStreamer configs for with parameterized test input
    cfg.configs.add(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key() + "=" + configFlag);
    cfg.configs.add(HoodieTableConfig.URL_ENCODE_PARTITIONING.key() + "=" + configFlag);
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamer.getIngestionService().ingestOnce();
    // Create new metaClient from tablePath
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(context, tablePath);
    assertEquals(configFlag, Boolean.parseBoolean(metaClient.getTableConfig().getHiveStylePartitioningEnable()));
    assertEquals(configFlag, Boolean.parseBoolean(metaClient.getTableConfig().getUrlEncodePartitioning()));
    deltaStreamer.shutdownGracefully();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieRecordType.class, names = {"AVRO", "SPARK"})
  public void testBulkInsertsAndUpsertsWithBootstrap(HoodieRecordType recordType) throws Exception {
    String tableBasePath = basePath + "/test_table";

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    addRecordMerger(recordType, cfg.configs);
    syncAndAssertRecordCount(cfg, 1000, tableBasePath, "00000", 1);

    // No new data => no commits.
    cfg.sourceLimit = 0;
    syncAndAssertRecordCount(cfg, 1000, tableBasePath, "00000", 1);

    // upsert() #1
    cfg.sourceLimit = 2000;
    cfg.operation = WriteOperationType.UPSERT;
    syncAndAssertRecordCount(cfg, 1950, tableBasePath, "00001", 2);
    List<Row> counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1950, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());

    // Perform bootstrap with tableBasePath as source
    String bootstrapSourcePath = basePath + "/src_bootstrapped";
    Dataset<Row> sourceDf = sqlContext.read()
        .format("org.apache.hudi")
        .load(tableBasePath);
    // TODO(HUDI-4944): fix the test to use a partition column with slashes (`/`) included
    //  in the value.  Currently it fails the tests due to slash encoding.
    sourceDf.write().format("parquet").partitionBy("rider").save(bootstrapSourcePath);

    String newDatasetBasePath = basePath + "/test_dataset_bootstrapped";
    cfg.runBootstrap = true;
    cfg.configs.add(String.format("hoodie.bootstrap.base.path=%s", bootstrapSourcePath));
    cfg.configs.add(String.format("%s=%s", DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "rider"));
    cfg.configs.add(String.format("hoodie.datasource.write.keygenerator.class=%s", SimpleKeyGenerator.class.getName()));
    cfg.configs.add("hoodie.datasource.write.hive_style_partitioning=true");
    cfg.configs.add("hoodie.bootstrap.parallelism=5");
    cfg.configs.add(String.format("%s=false", HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key()));
    cfg.targetBasePath = newDatasetBasePath;
    syncOnce(cfg);
    Dataset<Row> res = sqlContext.read().format("org.apache.hudi").load(newDatasetBasePath);
    log.info("Schema : {}", res.schema());

    assertRecordCount(1950, newDatasetBasePath, sqlContext);
    res.registerTempTable("bootstrapped");
    assertEquals(1950, sqlContext.sql("select distinct _hoodie_record_key from bootstrapped").count());
    // NOTE: To fetch record's count Spark will optimize the query fetching minimal possible amount
    //       of data, which might not provide adequate amount of test coverage
    assertDoesNotThrow(() -> sqlContext.sql("select * from bootstrapped").collect());

    StructField[] fields = res.schema().fields();
    List<String> fieldNames = Arrays.asList(res.schema().fieldNames());
    List<String> expectedFieldNames = Arrays.asList(sourceDf.schema().fieldNames());
    assertEquals(expectedFieldNames.size(), fields.length);
    assertTrue(fieldNames.containsAll(HoodieRecord.HOODIE_META_COLUMNS));
    assertTrue(fieldNames.containsAll(expectedFieldNames));
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, bootstrapSourcePath);
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, newDatasetBasePath);
  }

  @Test
  public void testModifiedTableConfigs() throws Exception {
    String tableBasePath = basePath + "/test_table_modified_configs";

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    syncAndAssertRecordCount(cfg, 1000, tableBasePath, "00000", 1);

    // No new data => no commits.
    cfg.sourceLimit = 0;
    syncAndAssertRecordCount(cfg, 1000, tableBasePath, "00000", 1);

    // add disallowed config update to recordkey field. An exception should be thrown
    cfg.sourceLimit = 2000;
    cfg.operation = WriteOperationType.UPSERT;
    cfg.configs.add(HoodieTableConfig.RECORDKEY_FIELDS.key() + "=differentval");
    assertThrows(HoodieException.class, () -> syncAndAssertRecordCount(cfg, 1000, tableBasePath, "00000", 1));
    List<Row> counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1000, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());

    //perform the upsert and now with the original config, the commit should go through
    HoodieDeltaStreamer.Config newCfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    newCfg.sourceLimit = 2000;
    newCfg.operation = WriteOperationType.UPSERT;
    syncAndAssertRecordCount(newCfg, 1950, tableBasePath, "00001", 2);
    List<Row> counts2 = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1950, counts2.stream().mapToLong(entry -> entry.getLong(1)).sum());
  }

  static void deltaStreamerTestRunner(HoodieDeltaStreamer ds, HoodieDeltaStreamer.Config cfg, Function<Boolean, Boolean> condition) throws Exception {
    deltaStreamerTestRunner(ds, cfg, condition, "single_ds_job");
  }

  static void deltaStreamerTestRunner(HoodieDeltaStreamer ds, HoodieDeltaStreamer.Config cfg, Function<Boolean, Boolean> condition, String jobId) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future dsFuture = null;
    boolean stoppedCleanly = false;
    try {
      dsFuture = executor.submit(() -> {
        try {
          ds.sync();
        } catch (Exception ex) {
          log.warn("DS continuous job failed, hence not proceeding with condition check for {}", jobId);
          throw new RuntimeException(ex.getMessage(), ex);
        }
      });
      TestHelpers.waitTillCondition(condition, dsFuture, 360);
      if (cfg != null && !cfg.postWriteTerminationStrategyClass.isEmpty()) {
        // If the streamer died, waitTillCondition returns as soon as the future completes. Surface that
        // failure here rather than letting awaitDeltaStreamerShutdown time out and report the misleading
        // "Deltastreamer should have shutdown by now" two minutes later.
        if (dsFuture.isDone()) {
          dsFuture.get();
        }
        awaitDeltaStreamerShutdown(ds);
      } else {
        ds.shutdownGracefully();
        dsFuture.get();
      }
      stoppedCleanly = true;
    } finally {
      if (!stoppedCleanly) {
        try {
          stopLeakedStreamer(ds, dsFuture);
        } catch (Throwable cleanupFailure) {
          // Never let the cleanup replace the failure the caller is already propagating.
          log.warn("Failed to stop the streamer after a failure", cleanupFailure);
        }
        // The ingest task has already had its one interrupt from cancel(true). If it swallowed that,
        // an orderly shutdown() would never reach it and the pool thread would outlive the fork.
        executor.shutdownNow();
      } else {
        executor.shutdown();
      }
    }
  }

  /**
   * Stops a streamer that a failure left running, without letting the stop hang the test.
   * <p>
   * Surefire runs this module with forkCount=1 and reuseForks=true, so a live streamer reads on into the
   * next test, whose setup deletes basePath and whose teardown closes the data generators underneath it.
   * The stop has to be bounded: shutdownGracefully awaits the ingest executor for up to 24 hours, and it
   * returns immediately without waiting when shutdown was already requested, so neither the wait nor the
   * absence of one can be relied on here.
   * <p>
   * Each of the three waits - the stop itself, the join of the ingest task, and the close that runs on the
   * stopper thread after the interrupt is swallowed - is bounded by {@code stopTimeoutSecs}, and at most two
   * of them run in sequence on any one path (a stop that times out skips the join; a stop that returns leaves
   * nothing for the close-wait), so a wedged streamer holds this for at most twice that.
   */
  private static void stopLeakedStreamer(HoodieDeltaStreamer ds, Future dsFuture) {
    stopLeakedStreamer(ds, dsFuture, STREAMER_STOP_TIMEOUT_SECS);
  }

  /** The bound is a parameter only so this helper's own tests need not spend the production one. */
  static void stopLeakedStreamer(HoodieDeltaStreamer ds, Future dsFuture, long stopTimeoutSecs) {
    ExecutorService stopper = Executors.newSingleThreadExecutor();
    try {
      Future<?> stop = stopper.submit(ds::shutdownGracefully);
      try {
        stop.get(stopTimeoutSecs, TimeUnit.SECONDS);
      } catch (ExecutionException stopThrew) {
        // The stop itself failing does not excuse leaving the ingest task running, so fall through to the join
        // below rather than take the outer clause, which tolerates only the ingest task's own failure.
        log.warn("Stopping the streamer threw after a failure", stopThrew);
      }
      if (dsFuture != null) {
        dsFuture.get(stopTimeoutSecs, TimeUnit.SECONDS);
      }
    } catch (ExecutionException ingestFailure) {
      // Expected rather than anomalous: the ingest task failing is usually why the caller is unwinding at
      // all, and the caller reports it. Nothing to warn about here.
    } catch (Exception stopFailure) {
      // Swallowed on purpose: this runs while another failure is propagating, and replacing that failure
      // with this one would hide the diagnostic the caller is about to report.
      if (stopFailure instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.warn("Could not stop the streamer cleanly after a failure, cancelling the ingest task", stopFailure);
      // The bound only stops this thread waiting: HoodieAsyncService.shutdown(false) swallows the interrupt
      // that stopper.shutdownNow() sends, and HoodieStreamer.shutdownGracefully runs ds.close() regardless, so
      // forcing the executor down at least interrupts the ingest round before the close.
      forceStopIngestion(ds);
      if (dsFuture != null) {
        dsFuture.cancel(true);
      }
    } finally {
      stopper.shutdownNow();
      // shutdownNow only interrupts the stopper out of awaitTermination. HoodieAsyncService.shutdown(false)
      // swallows that interrupt without restoring the flag, so shutdownGracefully carries on into ds.close()
      // on that thread. Give the close a bounded chance to finish here, rather than let it run on into the
      // next test's setup, which deletes basePath underneath it.
      // An interrupted caller would make awaitTermination throw at once and skip the wait, so the flag is
      // cleared for the wait and restored afterwards.
      boolean callerInterrupted = Thread.interrupted();
      try {
        if (!stopper.awaitTermination(stopTimeoutSecs, TimeUnit.SECONDS)) {
          log.warn("The streamer stop did not finish closing within {}s, letting it run on", stopTimeoutSecs);
        }
      } catch (InterruptedException interrupted) {
        callerInterrupted = true;
      } finally {
        if (callerInterrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private static void forceStopIngestion(HoodieDeltaStreamer ds) {
    try {
      HoodieIngestionService ingestionService = ds.getIngestionService();
      if (ingestionService != null) {
        ingestionService.shutdown(true);
      }
    } catch (Exception noService) {
      // Nothing to force down: a streamer that never started an ingestion service. On a real streamer
      // getIngestionService is an Option.get(), so absence arrives as an exception; a mock returns null
      // instead, which the guard above covers.
      log.debug("No ingestion service to force-stop", noService);
    }
  }

  static void awaitDeltaStreamerShutdown(HoodieDeltaStreamer ds) throws InterruptedException {
    // await until deltastreamer shuts down on its own
    boolean shutDownRequested = false;
    int timeSoFar = 0;
    while (!shutDownRequested) {
      shutDownRequested = ds.getIngestionService().isShutdownRequested();
      Thread.sleep(500);
      timeSoFar += 500;
      if (timeSoFar > (2 * 60 * 1000)) {
        Assertions.fail("Deltastreamer should have shutdown by now");
      }
    }
    boolean shutdownComplete = false;
    while (!shutdownComplete) {
      shutdownComplete = ds.getIngestionService().isShutdown();
      Thread.sleep(500);
      timeSoFar += 500;
      if (timeSoFar > (2 * 60 * 1000)) {
        Assertions.fail("Deltastreamer should have shutdown by now");
      }
    }
  }

  static void deltaStreamerTestRunner(HoodieDeltaStreamer ds, Function<Boolean, Boolean> condition) throws Exception {
    deltaStreamerTestRunner(ds, null, condition);
  }

  private static long getNumUpdates(HoodieCommitMetadata metadata) {
    return metadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream)
        .mapToLong(HoodieWriteStat::getNumUpdateWrites)
        .sum();
  }

  static void prepareJsonKafkaDFSFiles(int numRecords, boolean createTopic, String topicName) {
    prepareJsonKafkaDFSFiles(numRecords, createTopic, topicName, 2, HoodieTestDataGenerator.TRIP_SCHEMA, System.nanoTime());
  }

  static void prepareJsonKafkaDFSFiles(int numRecords, boolean createTopic, String topicName, int numPartitions) {
    prepareJsonKafkaDFSFiles(numRecords, createTopic, topicName, numPartitions, HoodieTestDataGenerator.TRIP_SCHEMA, System.nanoTime());
  }

  static void prepareJsonKafkaDFSFiles(int numRecords, boolean createTopic, String topicName, int numPartitions, String schemaStr, long seed) {
    if (createTopic) {
      try {
        testUtils.createTopic(topicName, numPartitions);
      } catch (TopicExistsException e) {
        // no op
      }
    }
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(seed);
    testUtils.sendMessages(topicName,
        UtilitiesTestBase.Helpers.jsonifyRecordsByPartitions(
            dataGenerator.generateInsertsAsPerSchema(
                "000", numRecords, schemaStr), numPartitions));
  }

  @Test
  public void testDeltaStreamerRestartAfterMissingHoodieProps() throws Exception {
    testDeltaStreamerRestartAfterMissingHoodieProps(true);
  }

  @Test
  public void testDeltaStreamerRestartAfterMissingHoodiePropsAfterValidCommit() throws Exception {
    testDeltaStreamerRestartAfterMissingHoodieProps(false);
  }

  private void testDeltaStreamerRestartAfterMissingHoodieProps(boolean testInitFailure) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 10;
    boolean hasTransformer = false;
    boolean useSchemaProvider = false;
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "0");

    String tableBasePath = basePath + "/test_parquet_table" + testNum;
    syncOnce(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, testInitFailure ? TestParquetDFSSourceEmptyBatch.class.getName() : ParquetDFSSource.class.getName(),
            null, PROPS_FILENAME_TEST_PARQUET, false,
            useSchemaProvider, 100000, false, null, null, "timestamp", null));

    if (testInitFailure) {
      FileStatus[] fileStatuses = fs.listStatus(new Path(tableBasePath + "/.hoodie/timeline/"));
      Arrays.stream(fileStatuses).filter(entry -> entry.getPath().getName().contains("commit") || entry.getPath().getName().contains("inflight")).forEach(entry -> {
        try {
          fs.delete(entry.getPath());
        } catch (IOException e) {
          log.warn("Failed to delete: {}", entry.getPath().toString(), e);
        }
      });
    }
    // delete hoodie.properties
    fs.delete(new Path(tableBasePath + "/.hoodie/hoodie.properties"));

    // restart the pipeline.
    if (testInitFailure) { // should succeed.
      syncOnce(
          TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
              null, PROPS_FILENAME_TEST_PARQUET, false,
              useSchemaProvider, 100000, false, null, null, "timestamp", null));
      assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    } else {
      assertThrows(HoodieIOException.class, () -> syncOnce(
          TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
              null, PROPS_FILENAME_TEST_PARQUET, false,
              useSchemaProvider, 100000, false, null, null, "timestamp", null)));
    }
    testNum++;
  }

  @Test
  public void testToSortedTruncatedStringSecretsMasked() {
    TypedProperties props =
        new DFSPropertiesConfiguration(fs.getConf(), new StoragePath(basePath + "/" + PROPS_FILENAME_TEST_SOURCE)).getProps();
    props.put("ssl.trustore.location", "SSL SECRET KEY");
    props.put("sasl.jaas.config", "SASL SECRET KEY");
    props.put("auth.credentials", "AUTH CREDENTIALS");
    props.put("auth.user.info", "AUTH USER INFO");

    String truncatedKeys = HoodieDeltaStreamer.toSortedTruncatedString(props);
    assertFalse(truncatedKeys.contains("SSL SECRET KEY"));
    assertFalse(truncatedKeys.contains("SASL SECRET KEY"));
    assertFalse(truncatedKeys.contains("AUTH CREDENTIALS"));
    assertFalse(truncatedKeys.contains("AUTH USER INFO"));
    assertTrue(truncatedKeys.contains("SENSITIVE_INFO_MASKED"));
  }

  @Test
  public void testFetchingCheckpointFromPreviousCommits() throws IOException {
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(basePath + "/testFetchPreviousCheckpoint", WriteOperationType.BULK_INSERT);

    TypedProperties properties = new TypedProperties();
    properties.setProperty("hoodie.datasource.write.recordkey.field", "key");
    properties.setProperty("hoodie.datasource.write.partitionpath.field", "pp");
    DummyStreamSync testDeltaSync = new DummyStreamSync(cfg, sparkSession, null, properties,
        jsc, fs, jsc.hadoopConfiguration(), null);

    properties.put(HoodieTableConfig.NAME.key(), "sample_tbl");
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), basePath, HoodieTableType.COPY_ON_WRITE, properties);

    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY, "abc");
    addCommitToTimeline(metaClient, extraMetadata);
    metaClient.reloadActiveTimeline();
    assertEquals(StreamerCheckpointUtils.getLatestCommitMetadataWithValidCheckpointInfo(metaClient.getActiveTimeline()
        .getCommitsTimeline()).get().getMetadata(HoodieDeltaStreamer.CHECKPOINT_KEY), "abc");

    extraMetadata.put(HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY, "def");
    addCommitToTimeline(metaClient, extraMetadata);
    metaClient.reloadActiveTimeline();
    assertEquals(StreamerCheckpointUtils.getLatestCommitMetadataWithValidCheckpointInfo(metaClient.getActiveTimeline()
        .getCommitsTimeline()).get().getMetadata(HoodieDeltaStreamer.CHECKPOINT_KEY), "def");

    // add a cluster commit which does not have CHECKPOINT_KEY. Deltastreamer should be able to go back and pick the right checkpoint.
    addClusterCommitToTimeline(metaClient, Collections.emptyMap());
    metaClient.reloadActiveTimeline();
    assertEquals(StreamerCheckpointUtils.getLatestCommitMetadataWithValidCheckpointInfo(metaClient.getActiveTimeline()
        .getCommitsTimeline()).get().getMetadata(HoodieDeltaStreamer.CHECKPOINT_KEY), "def");
  }

  @Test
  public void testForceEmptyMetaSync() throws Exception {
    String tableBasePath = basePath + "/test_force_empty_meta_sync";

    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    cfg.sourceLimit = 0;
    cfg.allowCommitOnNoCheckpointChange = true;
    cfg.enableMetaSync = true;
    cfg.forceEmptyMetaSync = true;

    syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf()));
    assertRecordCount(0, tableBasePath, sqlContext);

    // make sure hive table is present
    HiveSyncConfig hiveSyncConfig = getHiveSyncConfig(tableBasePath, "hive_trips");
    hiveSyncConfig.setHadoopConf(hiveServer.getHiveConf());
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf())
        .setBasePath(tableBasePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
    try (HoodieHiveSyncClient hiveClient = new HoodieHiveSyncClient(hiveSyncConfig, metaClient)) {
      final String tableName = hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_TABLE_NAME);
      assertTrue(hiveClient.tableExists(tableName), "Table " + tableName + " should exist");
    }
  }

  @Test
  public void testResumeCheckpointAfterChangingCOW2MOR() throws Exception {
    String tableBasePath = basePath + "/test_resume_checkpoint_after_changing_cow_to_mor";
    // default table type is COW
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    syncOnce(cfg);
    assertRecordCount(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);
    TestHelpers.assertAtLeastNCommits(1, tableBasePath);

    // change cow to mor
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(storage.getConf().newInstance())
        .setBasePath(cfg.targetBasePath)
        .setLoadActiveTimelineOnLoad(false)
        .build();
    Properties hoodieProps = new Properties();
    hoodieProps.load(fs.open(new Path(cfg.targetBasePath + "/.hoodie/hoodie.properties")));
    log.info("old props: {}", hoodieProps);
    hoodieProps.put("hoodie.table.type", HoodieTableType.MERGE_ON_READ.name());
    log.info("new props: {}", hoodieProps);
    StoragePath metaPathDir = new StoragePath(metaClient.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME);
    HoodieTableConfig.create(metaClient.getStorage(), metaPathDir, hoodieProps);

    // continue deltastreamer
    cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    syncOnce(cfg);
    // out of 1000 new records, 500 are inserts, 450 are updates and 50 are deletes.
    assertRecordCount(1450, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);
    List<Row> counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1450, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());
    TestHelpers.assertAtLeastNCommits(1, tableBasePath);
    // currently there should be 1 deltacommits now
    TestHelpers.assertAtleastNDeltaCommits(1, tableBasePath);

    // test the table type is already mor
    syncOnce(cfg);
    // out of 1000 new records, 500 are inserts, 450 are updates and 50 are deletes.
    // total records should be 1900 now
    assertRecordCount(1900, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00002", tableBasePath, 3);
    counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1900, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());
    TestHelpers.assertAtLeastNCommits(1, tableBasePath);
    // currently there should be 2 deltacommits now
    TestHelpers.assertAtleastNDeltaCommits(2, tableBasePath);

    // clean up
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @Test
  public void testResumeCheckpointAfterChangingMOR2COW() throws Exception {
    String tableBasePath = basePath + "/test_resume_checkpoint_after_changing_mor_to_cow";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    // change table type to MOR
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    syncOnce(cfg);
    assertRecordCount(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);
    TestHelpers.assertAtLeastNCommits(1, tableBasePath);

    // sync once, make one deltacommit and do a full compaction
    cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    cfg.configs.add("hoodie.compaction.strategy=org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy");
    cfg.configs.add("hoodie.compact.inline.max.delta.commits=1");
    syncOnce(cfg);
    // out of 1000 new records, 500 are inserts, 450 are updates and 50 are deletes.
    assertRecordCount(1450, tableBasePath, sqlContext);
    // totalCommits: 1 deltacommit(bulk_insert) + 1 deltacommit(upsert) + 1 commit(compaction)
    // there is no checkpoint in the compacted commit metadata, the latest checkpoint 00001 is in the upsert deltacommit
    TestHelpers.assertCommitMetadata(null, tableBasePath, 3);
    List<Row> counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1450, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());
    TestHelpers.assertAtLeastNCommits(3, tableBasePath);
    // currently there should be 2 deltacommits now
    TestHelpers.assertAtleastNDeltaCommits(2, tableBasePath);

    // change mor to cow
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(storage.getConf().newInstance())
        .setBasePath(cfg.targetBasePath)
        .setLoadActiveTimelineOnLoad(false)
        .build();
    Properties hoodieProps = new Properties();
    hoodieProps.load(fs.open(new Path(cfg.targetBasePath + "/.hoodie/hoodie.properties")));
    log.info("Old props: {}", hoodieProps);
    hoodieProps.put("hoodie.table.type", HoodieTableType.COPY_ON_WRITE.name());
    log.info("New props: {}", hoodieProps);
    StoragePath metaPathDir = new StoragePath(metaClient.getBasePath(), ".hoodie");
    HoodieTableConfig.create(metaClient.getStorage(), metaPathDir, hoodieProps);

    // continue deltastreamer
    cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    syncOnce(cfg);
    // out of 1000 new records, 500 are inserts, 450 are updates and 50 are deletes.
    assertRecordCount(1900, tableBasePath, sqlContext);
    // the checkpoint now should be 00002
    TestHelpers.assertCommitMetadata("00002", tableBasePath, 4);
    counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1900, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());
    TestHelpers.assertAtLeastNCommits(4, tableBasePath);

    // test the table type is already cow
    syncOnce(cfg);
    // out of 1000 new records, 500 are inserts, 450 are updates and 50 are deletes.
    // total records should be 2350 now
    assertRecordCount(2350, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00003", tableBasePath, 5);
    counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(2350, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());
    TestHelpers.assertAtLeastNCommits(5, tableBasePath);

    // clean up
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  static class DummyStreamSync extends StreamSync {

    public DummyStreamSync(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider, TypedProperties props,
                           JavaSparkContext jssc, FileSystem fs, Configuration conf,
                           Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient) throws IOException {
      super(cfg, sparkSession, schemaProvider, props, jssc, fs, conf, onInitializingHoodieWriteClient);
    }
  }

  static class TestReleaseResourcesStreamSync extends DeltaSync {

    // Package-private so the continuous-mode tests, which now live in a sibling class, can read it.
    final Set<String> releaseResourcesCalledSet = new HashSet<>();

    public TestReleaseResourcesStreamSync(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider, TypedProperties props,
                                          JavaSparkContext jssc, FileSystem fs, Configuration conf,
                                          Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient) throws IOException {
      super(cfg, sparkSession, schemaProvider, props, jssc, fs, conf, onInitializingHoodieWriteClient);
    }

    @Override
    protected void releaseResources(String instantTime) {
      super.releaseResources(instantTime);
      releaseResourcesCalledSet.add(instantTime);
    }
  }

  /**
   * UDF to calculate Haversine distance.
   */
  public static class DistanceUDF implements UDF4<Double, Double, Double, Double, Double> {

    /**
     * Returns some random number as distance between the points.
     *
     * @param lat1 Latitude of source
     * @param lat2 Latitude of destination
     * @param lon1 Longitude of source
     * @param lon2 Longitude of destination
     */
    @Override
    public Double call(Double lat1, Double lat2, Double lon1, Double lon2) {
      return RANDOM.nextDouble();
    }
  }

  /**
   * Adds a new field "haversine_distance" to the row.
   */
  public static class TripsWithDistanceTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                              TypedProperties properties) {
      rowDataset.sparkSession().sqlContext().udf().register("distance_udf", new DistanceUDF(), DataTypes.DoubleType);
      return rowDataset.withColumn("haversine_distance", functions.callUDF("distance_udf", functions.col("begin_lat"),
          functions.col("end_lat"), functions.col("begin_lon"), functions.col("end_lat")));
    }
  }

  public static class TestGenerator extends SimpleKeyGenerator {

    public TestGenerator(TypedProperties props) {
      super(props);
    }
  }

  public static class DummyAvroPayload extends OverwriteWithLatestAvroPayload {

    public DummyAvroPayload(GenericRecord gr, Comparable orderingVal) {
      super(gr, orderingVal);
    }
  }

  /**
   * Return empty table.
   */
  @Slf4j
  public static class DropAllTransformer implements Transformer {

    @Override
    public Dataset apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                         TypedProperties properties) {
      log.info("DropAllTransformer called !!");
      return sparkSession.createDataFrame(jsc.emptyRDD(), rowDataset.schema());
    }
  }

  public static class TestIdentityTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                              TypedProperties properties) {
      return rowDataset;
    }
  }

  public static class TestSpecificPartitionTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                              TypedProperties properties) {
      Dataset<Row> toReturn = rowDataset.filter("partition_path == '" + HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "'");
      return toReturn;
    }
  }

  /**
   * Add new field evoluted_optional_union_field with value of the field rider.
   */
  public static class TripsWithEvolvedOptionalFieldTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                              TypedProperties properties) {
      return rowDataset.withColumn("evoluted_optional_union_field", functions.col("rider"));
    }
  }

  /**
   * {@link FilebasedSchemaProvider} to be used in tests where target schema is null.
   */
  public static class TestFileBasedSchemaProviderNullTargetSchema extends FilebasedSchemaProvider {

    public TestFileBasedSchemaProviderNullTargetSchema(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getTargetHoodieSchema() {
      return null;
    }
  }

  public static class NullValueSchemaProvider extends SchemaProvider {

    public NullValueSchemaProvider(TypedProperties props) {
      super(props);
    }

    public NullValueSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getSourceHoodieSchema() {
      return null;
    }
  }

}

