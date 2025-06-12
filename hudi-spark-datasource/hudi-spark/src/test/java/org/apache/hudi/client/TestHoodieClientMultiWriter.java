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

package org.apache.hudi.client;

import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.PreferWriterConflictResolutionStrategy;
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.HoodieSchemaNotFoundException;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieSchemaEvolutionConflictException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.marker.SimpleDirectMarkerBasedDetectionStrategy;
import org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedDetectionStrategy;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.timeline.service.handlers.marker.AsyncTimelineServerBasedDetectionStrategy;

import org.apache.avro.Schema;
import org.apache.curator.test.TestingServer;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_BASE_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECTION_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_LOCK_KEY_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_EVOLVED_1;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_EVOLVED_2;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.util.CommitUtils.buildMetadata;
import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_SCHEMA_CONFLICT_RESOLUTION;
import static org.apache.hudi.config.HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("functional")
public class TestHoodieClientMultiWriter extends HoodieClientTestBase {

  private Properties lockProperties = null;


  /**
   * super is not thread safe!!
   **/
  @Override
  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    return new SparkRDDWriteClient(context, cfg);
  }

  @BeforeEach
  public void setup() throws IOException {
    if (lockProperties == null) {
      lockProperties = new Properties();
      lockProperties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
      lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
      lockProperties.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, "1");
      lockProperties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
      lockProperties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");
      lockProperties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "3");
    }
  }

  public void setUpMORTestTable() throws IOException {
    cleanupResources();
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initHoodieStorage();
    storage.createDirectory(new StoragePath(basePath));
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ,
        HoodieFileFormat.PARQUET);
    initTestDataGenerator();
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  private static final List<Class> LOCK_PROVIDER_CLASSES = Arrays.asList(
      // [HUDI-8887] Based on OS/docker container used, the underlying file system API might not support
      // atomic operations which impairs the functionality of lock provider. Disable the test dimension to
      // avoid false alarm in java CI.
      // FileSystemBasedLockProvider.class,
      InProcessLockProvider.class
  );

  private static final List<ConflictResolutionStrategy> CONFLICT_RESOLUTION_STRATEGY_CLASSES = Arrays.asList(
      new SimpleConcurrentFileWritesConflictResolutionStrategy(),
      new PreferWriterConflictResolutionStrategy());

  private static Iterable<Object[]> providerClassResolutionStrategyAndTableType() {
    List<Object[]> opts = new ArrayList<>();
    for (Object providerClass : LOCK_PROVIDER_CLASSES) {
      for (ConflictResolutionStrategy resolutionStrategy : CONFLICT_RESOLUTION_STRATEGY_CLASSES) {
        opts.add(new Object[] {COPY_ON_WRITE, providerClass, resolutionStrategy});
        opts.add(new Object[] {MERGE_ON_READ, providerClass, resolutionStrategy});
      }
    }
    return opts;
  }

  public static Stream<Arguments> concurrentAlterSchemaTestDimension() {
    Object[][] data =
        new Object[][] {
            // First element set to true means before testing anything, we make a commit
            // with schema TRIP_EXAMPLE_SCHEMA.
            // {<should create initial commit>, <table type>, <txn 1 writer schema>, <txn 2 writer schema>,
            // <should schema conflict>, <expected table schema after resolution>}
            // --------------|-----read write-----|validate & commit|------------------------- txn 1
            // --------------------------------|------read write------|--validate & commit--|- txn 2
            // committed->|------------------------------------------------------------------- initial commit (optional)

            // No schema evolution, no conflict.
            {true, false, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA, false, TRIP_EXAMPLE_SCHEMA},
            {false, false, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA, false, TRIP_EXAMPLE_SCHEMA},
            {false, true, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA, false, TRIP_EXAMPLE_SCHEMA},
            // No concurrent schema evolution, no conflict.
            {true, false, COPY_ON_WRITE, TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, false, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {true, false, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, false, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            // If there is a initial commits defining table schema to TRIP_EXAMPLE_SCHEMA.
            // as long as txn 2 stick to that schema, backwards compatibility handles everything.
            {true, false, COPY_ON_WRITE, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA, false, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {true, false, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA, false, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            // In case no initial commits, table schema is not really predefined.
            // It means are effectively having 2 concurrent txn trying to define table schema
            // differently in this case.
            {false, true, COPY_ON_WRITE, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA, true, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {false, false, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA, true, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {false, true, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA, true, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            // Concurrent schema evolution into the same schema does not conflict.
            {true, false, COPY_ON_WRITE, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, false, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {true, false, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, false, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            // Concurrent schema evolution into different schemas conflicts.
            // from clustering operation, instead of TRIP_EXAMPLE_SCHEMA_EVOLVED_1 (from commit 2)
            {true, false, COPY_ON_WRITE, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA_EVOLVED_2, true, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {true, false, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA_EVOLVED_2, true, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {false, false, COPY_ON_WRITE, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA_EVOLVED_2, true, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {false, false, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA_EVOLVED_2, true, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {false, true, COPY_ON_WRITE, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA_EVOLVED_2, true, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
            {false, true, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, TRIP_EXAMPLE_SCHEMA_EVOLVED_1, false, TRIP_EXAMPLE_SCHEMA_EVOLVED_1},
        };
    return Stream.of(data).map(Arguments::of);
  }

  /**
   * Injects a new instant with customizable schema in commit metadata
   * @param timestamp Instant timestamp
   * @param schemaAttrValue Schema value to set in commit metadata (can be null)
   */
  public void injectInstantWithSchema(
      String timestamp,
      String action,
      String schemaAttrValue) throws Exception {
    HoodieTestTable instantGenerator = HoodieTestTable.of(metaClient);
    instantGenerator.addCommit(timestamp, Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.INSERT,
        schemaAttrValue,
        action)));
  }

  @ParameterizedTest
  @MethodSource("concurrentAlterSchemaTestDimension")
  void testHoodieClientWithSchemaConflictResolution(
      boolean createInitialCommit,
      boolean createEmptyInitialCommit,
      HoodieTableType tableType,
      String writerSchema1,
      String writerSchema2,
      boolean shouldConflict,
      String expectedTableSchemaAfterResolution) throws Exception {
    if (tableType.equals(MERGE_ON_READ)) {
      setUpMORTestTable();
    }

    Properties properties = new Properties();
    properties.setProperty(MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key(), String.valueOf(0));
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    properties.setProperty(ENABLE_SCHEMA_CONFLICT_RESOLUTION.key(), "true");

    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder(TRIP_EXAMPLE_SCHEMA)
        .withHeartbeatIntervalInMs(60 * 1000)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY)
            .withSecondaryStorageType(FileSystemViewStorageType.MEMORY).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withProperties(properties);
    HoodieWriteConfig writeConfig = writeConfigBuilder.build();
    final SparkRDDWriteClient client1 = getHoodieWriteClient(writeConfig);

    int totalCommits = 0;
    final String nextCommitTime11 = "0011";
    final String nextCommitTime12 = "0012";
    if (createInitialCommit) {
      // Create the first commit ingesting some data.
      createCommitWithInserts(writeConfig, client1, "000", nextCommitTime11, 100);
      createCommitWithUpserts(writeConfig, client1, nextCommitTime11, Option.empty(), nextCommitTime12, 100);
      totalCommits += 2;
    }

    if (createEmptyInitialCommit) {
      // Create an empty commit which does not contain a valid schema, schema conflict resolution should still be able
      // to handle it properly. This can happen in delta streamer where no data is ingested but empty commit is created
      // to save the checkpoint.
      HoodieWriteConfig writeConfig22 = HoodieWriteConfig.newBuilder().withProperties(writeConfig.getProps()).build();
      writeConfig22.setSchema("\"null\"");
      try (final SparkRDDWriteClient client22 = getHoodieWriteClient(writeConfig22)) {
        JavaRDD<HoodieRecord> emptyRDD = jsc.emptyRDD();
        // Perform upsert with empty RDD
        WriteClientTestUtils.startCommitWithTime(client22, "0013");
        JavaRDD<WriteStatus> writeStatusRDD = client22.upsert(emptyRDD, "0013");
        client22.commit("0013", writeStatusRDD);
      }
      totalCommits += 1;

      // Validate table schema in the end.
      TableSchemaResolver r = new TableSchemaResolver(metaClient);
      // Assert no table schema is defined.
      assertThrows(HoodieSchemaNotFoundException.class, () -> r.getTableAvroSchema(false));
    }

    // Start txn 002 altering table schema
    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder().withProperties(writeConfig.getProps()).build();
    writeConfig2.setSchema(writerSchema1);
    final SparkRDDWriteClient client2 = getHoodieWriteClient(writeConfig2);
    final String nextCommitTime21 = startSchemaEvolutionTransaction(metaClient, client2, tableType);

    // Start concurrent txn 003 alter table schema
    HoodieWriteConfig writeConfig3 = HoodieWriteConfig.newBuilder().withProperties(writeConfig.getProps()).build();
    writeConfig3.setSchema(writerSchema2);
    final SparkRDDWriteClient client3 = getHoodieWriteClient(writeConfig3);
    final String nextCommitTime31 = startSchemaEvolutionTransaction(metaClient, client3, tableType);

    Properties props = new TypedProperties();
    HoodieWriteConfig tableServiceWriteCfg = tableType.equals(MERGE_ON_READ)
        ? writeConfigBuilder.withProperties(props)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build()).build()
        : writeConfigBuilder.withProperties(props)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClusteringNumCommits(1)
            .build())
        .build();
    SparkRDDWriteClient tableServiceClient = getHoodieWriteClient(tableServiceWriteCfg);
    final String tableServiceCommit32 = "0032";
    // schedule clustering (COW) or compaction (MOR)
    Option<String> tableServiceInstant = Option.empty();
    if (createInitialCommit) {
      tableServiceInstant = tableServiceClient.scheduleTableService(Option.of(tableServiceCommit32),
        Option.empty(), tableType.equals(MERGE_ON_READ) ? TableServiceType.COMPACT : TableServiceType.CLUSTER);
      if (!writerSchema1.equals(writerSchema2)) {
        assertTrue(tableServiceInstant.isPresent());
      }
    }

    // Commit txn 0021.
    client2.commit(nextCommitTime21, jsc.emptyRDD());
    totalCommits += 1;

    // Run clustering (COW) or compaction (MOR)
    if (tableServiceInstant.isPresent()) {
      // Execute pending clustering operation
      if (tableType.equals(MERGE_ON_READ)) {
        tableServiceClient.compact(tableServiceCommit32, true);
      } else {
        tableServiceClient.cluster(tableServiceCommit32, true);
      }
      totalCommits += 1;
    }

    // Inject commit instant that contains no valid schema field in it.
    String action = COPY_ON_WRITE == tableType ? COMMIT_ACTION : HoodieTimeline.DELTA_COMMIT_ACTION;
    injectInstantWithSchema("0033", action, null);
    injectInstantWithSchema("0034", action, "");
    totalCommits += 2;

    Exception e = null;
    try {
      client3.commit(nextCommitTime31, jsc.emptyRDD());
      totalCommits += 1;
    } catch (Exception ex) {
      e = ex;
    }
    if (e != null) {
      // If client 3 fails to commit, it should be due to schema conflict.
      assertTrue(shouldConflict);
      assertTrue(e instanceof HoodieSchemaEvolutionConflictException);
      assertTrue(e.getMessage().contains("Detected incompatible concurrent schema evolution."));
    } else {
      assertFalse(shouldConflict);
    }

    List<String> completedInstant = metaClient.reloadActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstants().stream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toList());

    // The initial txn always succeeds as long as it is triggered.
    // 0021 always succeeds, 0031 depends.
    assertEquals(totalCommits, completedInstant.size());
    if (createInitialCommit) {
      assertTrue(completedInstant.contains(nextCommitTime11));
    }
    assertTrue(completedInstant.contains(nextCommitTime21));
    if (!shouldConflict) {
      assertTrue(completedInstant.contains(nextCommitTime31));
    }

    // Validate table schema in the end.
    TableSchemaResolver r = new TableSchemaResolver(metaClient);
    Schema s = r.getTableAvroSchema(false);
    assertEquals(s, new Schema.Parser().parse(expectedTableSchemaAfterResolution));

    FileIOUtils.deleteDirectory(new File(basePath));
    client1.close();
    client2.close();
    client3.close();
    tableServiceClient.close();
  }

  @ParameterizedTest
  @MethodSource("configParamsDirectBased")
  public void testHoodieClientBasicMultiWriterWithEarlyConflictDetectionDirect(String tableType, String earlyConflictDetectionStrategy) throws Exception {
    testHoodieClientBasicMultiWriterWithEarlyConflictDetection(tableType, MarkerType.DIRECT.name(), earlyConflictDetectionStrategy);
  }

  @ParameterizedTest
  @MethodSource("configParamsTimelineServerBased")
  public void testHoodieClientBasicMultiWriterWithEarlyConflictDetectionTimelineServerBased(String tableType, String earlyConflictDetectionStrategy) throws Exception {
    testHoodieClientBasicMultiWriterWithEarlyConflictDetection(tableType, MarkerType.TIMELINE_SERVER_BASED.name(), earlyConflictDetectionStrategy);
  }

  /**
   * Test multi-writers with early conflict detect enable, including
   * 1. MOR + Direct marker
   * 2. COW + Direct marker
   * 3. MOR + Timeline server based marker
   * 4. COW + Timeline server based marker
   * <p>
   * |---------------------- 003 heartBeat expired -------------------|
   * <p>
   * ---|---------|--------------------|--------------------------------------|-------------------------|-------------------------> time
   * init 001
   * 002 start writing
   * 003 start which has conflict with 002
   * and failed soon
   * 002 commit successfully       004 write successfully
   *
   * @param tableType
   * @param markerType
   * @throws Exception
   */
  private void testHoodieClientBasicMultiWriterWithEarlyConflictDetection(String tableType, String markerType, String earlyConflictDetectionStrategy) throws Exception {
    if (tableType.equalsIgnoreCase(MERGE_ON_READ.name())) {
      setUpMORTestTable();
    }

    int heartBeatIntervalForCommit4 = 10 * 1000;

    HoodieWriteConfig writeConfig;
    TestingServer server = null;
    if (earlyConflictDetectionStrategy.equalsIgnoreCase(SimpleTransactionDirectMarkerBasedDetectionStrategy.class.getName())) {
      // need to setup zk related env there. Bcz SimpleTransactionDirectMarkerBasedDetectionStrategy is only support zk lock for now.
      server = new TestingServer();
      Properties properties = new Properties();
      properties.setProperty(ZK_BASE_PATH_PROP_KEY, basePath);
      properties.setProperty(ZK_CONNECT_URL_PROP_KEY, server.getConnectString());
      properties.setProperty(ZK_BASE_PATH_PROP_KEY, server.getTempDirectory().getAbsolutePath());
      properties.setProperty(ZK_SESSION_TIMEOUT_MS_PROP_KEY, "10000");
      properties.setProperty(ZK_CONNECTION_TIMEOUT_MS_PROP_KEY, "10000");
      properties.setProperty(ZK_LOCK_KEY_PROP_KEY, "key");
      properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");

      writeConfig = buildWriteConfigForEarlyConflictDetect(markerType, properties, ZookeeperBasedLockProvider.class, earlyConflictDetectionStrategy);
    } else {
      Properties properties = new Properties();
      properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
      properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
      writeConfig = buildWriteConfigForEarlyConflictDetect(markerType, properties, InProcessLockProvider.class, earlyConflictDetectionStrategy);
    }

    final SparkRDDWriteClient client1 = getHoodieWriteClient(writeConfig);

    // Create the first commit
    final String nextCommitTime1 = "001";
    createCommitWithInserts(writeConfig, client1, "000", nextCommitTime1, 200);

    final SparkRDDWriteClient client2 = getHoodieWriteClient(writeConfig);
    final SparkRDDWriteClient client3 = getHoodieWriteClient(writeConfig);

    final String nextCommitTime2 = "002";

    // start to write commit 002
    final JavaRDD<WriteStatus> writeStatusList2 = startCommitForUpdate(writeConfig, client2, nextCommitTime2, 100);

    // start to write commit 003
    // this commit 003 will fail quickly because early conflict detection before create marker.
    final String nextCommitTime3 = "003";
    assertThrows(SparkException.class, () -> {
      final JavaRDD<WriteStatus> writeStatusList3 =
          startCommitForUpdate(writeConfig, client3, nextCommitTime3, 100);
      client3.commit(nextCommitTime3, writeStatusList3);
    }, "Early conflict detected but cannot resolve conflicts for overlapping writes");

    // start to commit 002 and success
    assertDoesNotThrow(() -> {
      client2.commit(nextCommitTime2, writeStatusList2);
    });

    HoodieWriteConfig config4 =
        HoodieWriteConfig.newBuilder().withProperties(writeConfig.getProps())
            .withHeartbeatIntervalInMs(heartBeatIntervalForCommit4).build();
    final SparkRDDWriteClient client4 = getHoodieWriteClient(config4);

    StoragePath heartbeatFilePath = new StoragePath(
        HoodieTableMetaClient.getHeartbeatFolderPath(basePath) + StoragePath.SEPARATOR + nextCommitTime3);
    storage.create(heartbeatFilePath, true);

    // Wait for heart beat expired for failed commitTime3 "003"
    // Otherwise commit4 still can see conflict between failed write 003.
    Thread.sleep(heartBeatIntervalForCommit4 * 2);

    final String nextCommitTime4 = "004";
    assertDoesNotThrow(() -> {
      final JavaRDD<WriteStatus> writeStatusList4 =
          startCommitForUpdate(writeConfig, client4, nextCommitTime4, 100);
      client4.commit(nextCommitTime4, writeStatusList4);
    });

    List<String> completedInstant = metaClient.reloadActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstants().stream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toList());

    assertEquals(3, completedInstant.size());
    assertTrue(completedInstant.contains(nextCommitTime1));
    assertTrue(completedInstant.contains(nextCommitTime2));
    assertTrue(completedInstant.contains(nextCommitTime4));

    FileIOUtils.deleteDirectory(new File(basePath));
    if (server != null) {
      server.close();
    }
    client1.close();
    client2.close();
    client3.close();
    client4.close();
  }

  @Test
  public void testHoodieClientBasicMultiWriterCOW_InProcessLP_SimpleCRS() throws Exception {
    testHoodieClientBasicMultiWriter(HoodieTableType.COPY_ON_WRITE, InProcessLockProvider.class, new SimpleConcurrentFileWritesConflictResolutionStrategy());
  }

  @Test
  public void testHoodieClientBasicMultiWriterCOW_FSBasedLP_SimpleCRS() throws Exception {
    testHoodieClientBasicMultiWriter(HoodieTableType.COPY_ON_WRITE, FileSystemBasedLockProvider.class, new SimpleConcurrentFileWritesConflictResolutionStrategy());
  }

  @Test
  public void testHoodieClientBasicMultiWriterCOW_FSBasedLP_PreferWriterCRS() throws Exception {
    testHoodieClientBasicMultiWriter(HoodieTableType.COPY_ON_WRITE, FileSystemBasedLockProvider.class, new PreferWriterConflictResolutionStrategy());
  }

  @Test
  public void testHoodieClientBasicMultiWriterCOW_InProcessLP_PreferWriterCRS() throws Exception {
    testHoodieClientBasicMultiWriter(HoodieTableType.COPY_ON_WRITE, InProcessLockProvider.class, new PreferWriterConflictResolutionStrategy());
  }

  @Test
  public void testHoodieClientBasicMultiWriterMOR_InProcessLP_SimpleCRS() throws Exception {
    testHoodieClientBasicMultiWriter(HoodieTableType.MERGE_ON_READ, InProcessLockProvider.class, new SimpleConcurrentFileWritesConflictResolutionStrategy());
  }

  @Test
  public void testHoodieClientBasicMultiWriterMOR_FSBasedLP_SimpleCRS() throws Exception {
    testHoodieClientBasicMultiWriter(HoodieTableType.MERGE_ON_READ, FileSystemBasedLockProvider.class, new SimpleConcurrentFileWritesConflictResolutionStrategy());
  }

  @Test
  public void testHoodieClientBasicMultiWriterMOR_FSBasedLP_PreferWriterCRS() throws Exception {
    testHoodieClientBasicMultiWriter(HoodieTableType.MERGE_ON_READ, FileSystemBasedLockProvider.class, new PreferWriterConflictResolutionStrategy());
  }

  @Test
  public void testHoodieClientBasicMultiWriterMOR_InProcessLP_PreferWriterCRS() throws Exception {
    testHoodieClientBasicMultiWriter(HoodieTableType.MERGE_ON_READ, InProcessLockProvider.class, new PreferWriterConflictResolutionStrategy());
  }

  private void testHoodieClientBasicMultiWriter(HoodieTableType tableType, Class providerClass,
                                               ConflictResolutionStrategy resolutionStrategy) throws Exception {
    if (tableType == MERGE_ON_READ) {
      setUpMORTestTable();
    }
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");

    HoodieWriteConfig writeConfig = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .withAutoArchive(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(providerClass)
            .withConflictResolutionStrategy(resolutionStrategy)
            .build()).withProperties(lockProperties).build();

    // Create the first commit
    createCommitWithInserts(writeConfig, getHoodieWriteClient(writeConfig), "000", "001", 200);

    final int threadCount = 2;
    final ExecutorService executors = Executors.newFixedThreadPool(2);
    final SparkRDDWriteClient client1 = getHoodieWriteClient(writeConfig);
    final SparkRDDWriteClient client2 = getHoodieWriteClient(writeConfig);

    final CyclicBarrier cyclicBarrier = new CyclicBarrier(threadCount);
    final AtomicBoolean writer1Completed = new AtomicBoolean(false);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    Future future1 = executors.submit(() -> {
      try {
        final String nextCommitTime = client1.createNewInstantTime();
        final JavaRDD<WriteStatus> writeStatusList = startCommitForUpdate(writeConfig, client1, nextCommitTime, 100);

        // Wait for the 2nd writer to start the commit
        cyclicBarrier.await(60, TimeUnit.SECONDS);

        // Commit the update before the 2nd writer
        assertDoesNotThrow(() -> {
          client1.commit(nextCommitTime, writeStatusList);
        });

        // Signal the 2nd writer to go ahead for his commit
        cyclicBarrier.await(60, TimeUnit.SECONDS);
        writer1Completed.set(true);
      } catch (Exception e) {
        writer1Completed.set(false);
      }
    });

    Future future2 = executors.submit(() -> {
      try {
        final String nextCommitTime = client2.createNewInstantTime();

        // Wait for the 1st writer to make progress with the commit
        cyclicBarrier.await(60, TimeUnit.SECONDS);
        final JavaRDD<WriteStatus> writeStatusList = startCommitForUpdate(writeConfig, client2, nextCommitTime, 100);

        // Wait for the 1st writer to complete the commit
        cyclicBarrier.await(60, TimeUnit.SECONDS);
        assertThrows(HoodieWriteConflictException.class, () -> {
          client2.commit(nextCommitTime, writeStatusList);
        });
        writer2Completed.set(true);
      } catch (Exception e) {
        writer2Completed.set(false);
      }
    });

    future1.get();
    future2.get();

    // both should have been completed successfully. I mean, we already assert for conflict for writer2 at L155.
    assertTrue(writer1Completed.get() && writer2Completed.get());
    client1.close();
    client2.close();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testMultiWriterWithInsertsToDistinctPartitions(HoodieTableType tableType) throws Exception {
    if (tableType == MERGE_ON_READ) {
      setUpMORTestTable();
    }

    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "3000");
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "20");

    HoodieWriteConfig cfg = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(2)
            .build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build())
        .withEmbeddedTimelineServerEnabled(false)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withProperties(lockProperties)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY)
            .build())
        .build();

    // Create the first commit
    SparkRDDWriteClient<?> client = getHoodieWriteClient(cfg);
    createCommitWithInsertsForPartition(cfg, client, "000", "001", 100, "2016/03/01");
    client.close();
    int numConcurrentWriters = 5;
    ExecutorService executors = Executors.newFixedThreadPool(numConcurrentWriters);

    List<Future<?>> futures = new ArrayList<>(numConcurrentWriters);
    for (int loop = 0; loop < numConcurrentWriters; loop++) {
      String newCommitTime = "00" + (loop + 2);
      String partition = "2016/03/0" + (loop + 2);
      futures.add(executors.submit(() -> {
        try {
          SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg);
          createCommitWithInsertsForPartition(cfg, writeClient, "001", newCommitTime, 100, partition);
          writeClient.close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }

    futures.forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Count down the latch and await for all the needed threads to join.
   *
   * @param latch          - Count down latch
   * @param waitTimeMillis - Max wait time in millis for waiting
   */
  private void latchCountDownAndWait(CountDownLatch latch, long waitTimeMillis) {
    latch.countDown();
    try {
      latch.await(waitTimeMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      //
    }
  }

  @ParameterizedTest
  @MethodSource("providerClassResolutionStrategyAndTableType")
  public void testMultiWriterWithAsyncTableServicesWithConflict(HoodieTableType tableType, Class<? extends LockProvider<?>> providerClass,
                                                                ConflictResolutionStrategy resolutionStrategy) throws Exception {
    // create inserts X 1
    if (tableType == MERGE_ON_READ) {
      setUpMORTestTable();
    }

    // Use RDD API to perform clustering (TODO: Fix row-writer API)
    Properties properties = new Properties();
    properties.put("hoodie.datasource.write.row.writer.enable", String.valueOf(false));

    // Disabling embedded timeline server, it doesn't work with multiwriter
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAutoClean(false)
            .withAsyncClean(true)
            .retainCommits(0)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .withEmbeddedTimelineServerEnabled(false)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(
            FileSystemViewStorageType.MEMORY).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(providerClass)
            .withConflictResolutionStrategy(resolutionStrategy)
            .build()).withProperties(lockProperties)
        .withProperties(properties);

    Set<String> validInstants = new HashSet<>();

    // Create the first commit with inserts
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    String firstCommitTime = client.createNewInstantTime();
    createCommitWithInserts(cfg, client, "000", firstCommitTime, 200);
    validInstants.add(firstCommitTime);

    // Create 2 commits with upserts
    String secondCommitTime = client.createNewInstantTime();
    createCommitWithUpserts(cfg, client, firstCommitTime, Option.of("000"), secondCommitTime, 100);
    String thirdCommitTime = client.createNewInstantTime();
    createCommitWithUpserts(cfg, client, secondCommitTime, Option.of("000"), thirdCommitTime, 100);
    validInstants.add(secondCommitTime);
    validInstants.add(thirdCommitTime);

    // Three clients running actions in parallel
    final int threadCount = 3;
    final CountDownLatch scheduleCountDownLatch = new CountDownLatch(threadCount);
    final ExecutorService executors = Executors.newFixedThreadPool(threadCount);

    // Write config with clustering enabled
    final HoodieWriteConfig cfg2 = writeConfigBuilder
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClustering(true)
            .withInlineClusteringNumCommits(1)
            .build())
        .build();
    final SparkRDDWriteClient<?> client1 = getHoodieWriteClient(cfg2);
    final SparkRDDWriteClient<?> client2 = getHoodieWriteClient(cfg);
    final SparkRDDWriteClient<?> client3 = getHoodieWriteClient(cfg);
    final String upsertCommitTime = client1.createNewInstantTime(); // upsert commit time has to be lesser than compaction instant time.
    // and w/ MOR table, during conflict resolution, we will definitely hit conflict resolution exception.
    // if the delta commit's instant time is not guaranteed to be < compaction instant time, then delta commit will succeed w/o issues.

    // Test with concurrent operations could be flaky, to reduce possibility of wrong ordering some queue is added
    // For InProcessLockProvider we could wait less
    final int waitAndRunFirst = providerClass.isAssignableFrom(InProcessLockProvider.class) ? 2000 : 20000;
    final int waitAndRunSecond = providerClass.isAssignableFrom(InProcessLockProvider.class) ? 3000 : 30000;

    // Create upserts, schedule cleaning, schedule compaction in parallel
    Future future1 = executors.submit(() -> {
      final int numRecords = 100;
      final String commitTimeBetweenPrevAndNew = secondCommitTime;

      // We want the upsert to go through only after the compaction
      // and cleaning schedule completion. So, waiting on latch here.
      latchCountDownAndWait(scheduleCountDownLatch, waitAndRunSecond);
      // Writes should pass since scheduled compaction does not conflict with upsert for v8 and above
      assertDoesNotThrow(() -> createCommitWithUpserts(cfg, client1, thirdCommitTime, Option.of(commitTimeBetweenPrevAndNew), upsertCommitTime, numRecords));
      validInstants.add(upsertCommitTime);
    });

    Future future2 = executors.submit(() -> {
      if (tableType == MERGE_ON_READ) {
        assertDoesNotThrow(() -> {
          String compactionTimeStamp = client2.scheduleTableService(Option.empty(), TableServiceType.COMPACT).get();
          ValidationUtils.checkArgument(InstantComparison.compareTimestamps(compactionTimeStamp, InstantComparison.GREATER_THAN, upsertCommitTime));
        });
      }
      latchCountDownAndWait(scheduleCountDownLatch, waitAndRunFirst);
    });

    Future future3 = executors.submit(() -> {
      assertDoesNotThrow(() -> {
        latchCountDownAndWait(scheduleCountDownLatch, waitAndRunFirst);
        client3.scheduleTableService(Option.empty(), TableServiceType.CLEAN);
      });
    });
    future1.get();
    future2.get();
    future3.get();

    String pendingCompactionTime = (tableType == MERGE_ON_READ)
        ? metaClient.reloadActiveTimeline().filterPendingCompactionTimeline()
        .firstInstant().get().requestedTime()
        : "";
    Option<HoodieInstant> pendingCleanInstantOp = metaClient.reloadActiveTimeline().getCleanerTimeline().filterInflightsAndRequested()
        .firstInstant();
    String pendingCleanTime = pendingCleanInstantOp.isPresent()
        ? pendingCleanInstantOp.get().requestedTime()
        : client.createNewInstantTime();

    CountDownLatch runCountDownLatch = new CountDownLatch(threadCount);
    // Create inserts, run cleaning, run compaction in parallel
    future1 = executors.submit(() -> {
      final String newCommitTime = client1.createNewInstantTime();
      final int numRecords = 100;
      latchCountDownAndWait(runCountDownLatch, waitAndRunSecond);
      assertDoesNotThrow(() -> {
        createCommitWithInserts(cfg, client1, thirdCommitTime, newCommitTime, numRecords);
        validInstants.add(newCommitTime);
      });
    });

    future2 = executors.submit(() -> {
      latchCountDownAndWait(runCountDownLatch, waitAndRunFirst);
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        assertDoesNotThrow(() -> {
          HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = client2.compact(pendingCompactionTime);
          client2.commitCompaction(pendingCompactionTime, compactionMetadata, Option.empty());
          assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(pendingCompactionTime));
          validInstants.add(pendingCompactionTime);
        });
      }
    });

    future3 = executors.submit(() -> {
      latchCountDownAndWait(runCountDownLatch, waitAndRunFirst);
      assertDoesNotThrow(() -> {
        client3.clean(false);
        validInstants.add(pendingCleanTime);
      });
    });
    future1.get();
    future2.get();
    future3.get();

    validInstants.addAll(
        metaClient.reloadActiveTimeline().getCompletedReplaceTimeline()
            .filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toSet()));
    Set<String> completedInstants = metaClient.reloadActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::requestedTime)
        .collect(Collectors.toSet());
    assertTrue(validInstants.containsAll(completedInstants));

    client.close();
    client1.close();
    client2.close();
    client3.close();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"MERGE_ON_READ", "COPY_ON_WRITE"})
  public void testMultiWriterWithAsyncLazyCleanRollback(HoodieTableType tableType) throws Exception {
    // create inserts X 1
    if (tableType == MERGE_ON_READ) {
      setUpMORTestTable();
    }
    // Disabling embedded timeline server, it doesn't work with multiwriter
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAutoClean(false)
            .withAsyncClean(true)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .withEmbeddedTimelineServerEnabled(false)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(
            FileSystemViewStorageType.MEMORY).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Set the config so that heartbeat will expire in 1 second without update
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .build()).withProperties(lockProperties);
    Set<String> validInstants = new HashSet<>();
    // Create the first commit with inserts
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    createCommitWithInserts(cfg, client, "000", "001", 200);
    validInstants.add("001");

    // Three clients running actions in parallel
    final int threadCount = 3;
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    final SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
    final SparkRDDWriteClient client2 = getHoodieWriteClient(cfg);
    final String commitTime2 = "002";
    final String commitTime3 = "003";
    AtomicReference<Object> writeStatus1 = new AtomicReference<>(null);
    AtomicReference<Object> writeStatus2 = new AtomicReference<>(null);

    Future future1 = executor.submit(() -> {
      final int numRecords = 100;
      assertDoesNotThrow(() -> {
        writeStatus1.set(createCommitWithInserts(cfg, client1, "001", commitTime2, numRecords, false));
      });
    });
    Future future2 = executor.submit(() -> {
      final int numRecords = 100;
      assertDoesNotThrow(() -> {
        writeStatus2.set(createCommitWithInserts(cfg, client2, "001", commitTime3, numRecords, false));
        client2.getHeartbeatClient().stop(commitTime3);
      });
    });

    future1.get();
    future2.get();

    final CountDownLatch commitCountDownLatch = new CountDownLatch(1);
    HoodieTableMetaClient tableMetaClient = client.getTableServiceClient().createMetaClient(true);

    // Commit the instants and get instants to rollback in parallel
    future1 = executor.submit(() -> {
      client1.commit(commitTime2, writeStatus1.get());
      commitCountDownLatch.countDown();
    });

    Future future3 = executor.submit(() -> {
      try {
        commitCountDownLatch.await(30000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        //
      }
      List<String> instantsToRollback =
          client.getTableServiceClient().getInstantsToRollback(tableMetaClient, HoodieFailedWritesCleaningPolicy.LAZY, Option.empty());
      // Only commit3 will be rollback, although commit2 is in the inflight timeline and has no heartbeat file
      assertEquals(1, instantsToRollback.size());
      assertEquals(commitTime3, instantsToRollback.get(0));
    });

    future1.get();
    future3.get();
    client.close();
    client1.close();
    client2.close();
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class, names = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testHoodieClientMultiWriterWithClustering(HoodieTableType tableType) throws Exception {
    if (tableType == MERGE_ON_READ) {
      setUpMORTestTable();
    }
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new SimpleConcurrentFileWritesConflictResolutionStrategy())
            .build()).withProperties(properties);
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    HoodieWriteConfig cfg2 = writeConfigBuilder.build();
    HoodieWriteConfig cfg3 = writeConfigBuilder
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClustering(true).withInlineClusteringNumCommits(1).build())
        .build();

    // Create the first commit
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      createCommitWithInserts(cfg, client, "000", "001", 200);
    }
    // Start another inflight commit
    String newCommitTime = "003";
    int numRecords = 100;
    SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
    String commitTimeBetweenPrevAndNew = "002";
    JavaRDD<WriteStatus> result1 = updateBatch(cfg, client1, newCommitTime, "001",
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2, true, INSTANT_GENERATOR, true);
    // Start and finish another commit while the previous writer for commit 003 is running
    newCommitTime = "004";
    SparkRDDWriteClient client2 = getHoodieWriteClient(cfg);
    JavaRDD<WriteStatus> result2 = updateBatch(cfg2, client2, newCommitTime, "001",
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2, INSTANT_GENERATOR);
    // Schedule and run clustering while previous writer for commit 003 is running
    SparkRDDWriteClient client3 = getHoodieWriteClient(cfg3);
    // schedule clustering
    Option<String> clusterInstant = client3.scheduleTableService(Option.empty(), TableServiceType.CLUSTER);
    assertTrue(clusterInstant.isPresent());
    // Attempt to commit the inflight commit 003
    try {
      client1.commit("003", result1);
      fail("Should have thrown a concurrent conflict exception");
    } catch (Exception e) {
      // Expected
    }
    client1.close();
    client2.close();
    client3.close();
  }

  @Test
  public void testHoodieClientMultiWriterAutoCommitForConflict() throws Exception {
    lockProperties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "100");
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .build()).withProperties(lockProperties);
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    HoodieWriteConfig cfg2 = writeConfigBuilder.build();

    // Create the first commit
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      createCommitWithInserts(cfg, client, "000", "001", 5000);
    }
    // Start another inflight commit
    String newCommitTime1 = "003";
    String newCommitTime2 = "004";
    SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
    SparkRDDWriteClient client2 = getHoodieWriteClient(cfg2);

    List<HoodieRecord> updates1 = dataGen.generateUpdates(newCommitTime1, 5000);
    List<HoodieRecord> updates2 = dataGen.generateUpdates(newCommitTime2, 5000);

    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(updates1, 4);
    JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(updates2, 4);

    runConcurrentAndAssert(writeRecords1, writeRecords2, client1, client2, SparkRDDWriteClient::upsert, true);
    client1.close();
    client2.close();
  }

  private void runConcurrentAndAssert(JavaRDD<HoodieRecord> writeRecords1, JavaRDD<HoodieRecord> writeRecords2,
                                      SparkRDDWriteClient client1, SparkRDDWriteClient client2,
                                      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                      boolean assertForConflict) throws ExecutionException, InterruptedException {

    CountDownLatch runCountDownLatch = new CountDownLatch(2);
    final ExecutorService executors = Executors.newFixedThreadPool(2);
    String newCommitTime1 = "003";
    String newCommitTime2 = "004";

    AtomicBoolean client1Succeeded = new AtomicBoolean(true);
    AtomicBoolean client2Succeeded = new AtomicBoolean(true);

    Future future1 = executors.submit(() -> {
          try {
            ingestBatch(writeFn, client1, newCommitTime1, writeRecords1, runCountDownLatch);
          } catch (IOException e) {
            LOG.error("IOException thrown " + e.getMessage());
          } catch (InterruptedException e) {
            LOG.error("Interrupted Exception thrown " + e.getMessage());
          } catch (Exception e) {
            client1Succeeded.set(false);
          }
        }
    );

    Future future2 = executors.submit(() -> {
          try {
            ingestBatch(writeFn, client2, newCommitTime2, writeRecords2, runCountDownLatch);
          } catch (IOException e) {
            LOG.error("IOException thrown " + e.getMessage());
          } catch (InterruptedException e) {
            LOG.error("Interrupted Exception thrown " + e.getMessage());
          } catch (Exception e) {
            client2Succeeded.set(false);
          }
        }
    );

    future1.get();
    future2.get();
    if (assertForConflict) {
      assertFalse(client1Succeeded.get() && client2Succeeded.get());
      assertTrue(client1Succeeded.get() || client2Succeeded.get());
    } else {
      assertTrue(client2Succeeded.get() && client1Succeeded.get());
    }
  }

  @Test
  public void testHoodieClientMultiWriterAutoCommitNonConflict() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "100");
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .build()).withProperties(properties);
    HoodieWriteConfig cfg = writeConfigBuilder.build();
    HoodieWriteConfig cfg2 = writeConfigBuilder.build();

    // Create the first commit
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      createCommitWithInserts(cfg, client, "000", "001", 200);
    }
    // Start another inflight commit
    String newCommitTime1 = "003";
    String newCommitTime2 = "004";
    SparkRDDWriteClient client1 = getHoodieWriteClient(cfg);
    SparkRDDWriteClient client2 = getHoodieWriteClient(cfg2);

    List<HoodieRecord> updates1 = dataGen.generateInserts(newCommitTime1, 200);
    List<HoodieRecord> updates2 = dataGen.generateInserts(newCommitTime2, 200);

    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(updates1, 1);
    JavaRDD<HoodieRecord> writeRecords2 = jsc.parallelize(updates2, 1);

    runConcurrentAndAssert(writeRecords1, writeRecords2, client1, client2, SparkRDDWriteClient::bulkInsert, false);
    client1.close();
    client2.close();
  }

  /**
   * Test case for multi-writer scenario with index updates and aggressive cleaning.
   */
  @Test
  public void testMultiWriterWithIndexingAndAggressiveCleaning() throws Exception {
    // setting up MOR table so that we can have a log file in the file slice
    setUpMORTestTable();
    // common write configs for both writers
    HoodieWriteConfig.Builder writeConfigBuilder = getConfigBuilder()
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMetadataIndexColumnStats(true)
            .withEnableRecordIndex(true).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .withAutoArchive(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new SimpleConcurrentFileWritesConflictResolutionStrategy())
            .build());
    HoodieWriteConfig writeConfig1 = writeConfigBuilder.build();

    // clean every commit for writer2
    HoodieWriteConfig writeConfig2 = writeConfigBuilder
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(1)
            .withAutoClean(false).build())
        .build();

    // Simulate the first commit with Writer 1
    final SparkRDDWriteClient client1 = getHoodieWriteClient(writeConfig1);
    final SparkRDDWriteClient client2 = getHoodieWriteClient(writeConfig2);
    createCommitWithInserts(writeConfig1, getHoodieWriteClient(writeConfig1), client1.createNewInstantTime(), client1.createNewInstantTime(), 200);

    // multi-writer setup
    final int threadCount = 2;
    final ExecutorService executors = Executors.newFixedThreadPool(threadCount);
    final CyclicBarrier cyclicBarrier = new CyclicBarrier(threadCount);
    final AtomicBoolean writer1Completed = new AtomicBoolean(false);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Writer 1 - Simulating the index update process
    Future future1 = executors.submit(() -> {
      try {
        final String nextCommitTime = client1.createNewInstantTime();
        final JavaRDD<WriteStatus> writeStatusList = startCommitForUpdate(writeConfig1, client1, nextCommitTime, 100);

        // Wait for Writer 2 to start cleaning
        cyclicBarrier.await(60, TimeUnit.SECONDS);

        // Commit the update including index update and assert no exceptions
        assertDoesNotThrow(() -> {
          client1.commit(nextCommitTime, writeStatusList);
        });

        // Signal Writer 2 to continue
        cyclicBarrier.await(60, TimeUnit.SECONDS);
        writer1Completed.set(true);
      } catch (Exception e) {
        writer1Completed.set(false);
      }
    });

    // Writer 2 - Simulating aggressive cleaning
    Future future2 = executors.submit(() -> {
      try {
        // Wait for Writer 1 to make progress
        cyclicBarrier.await(60, TimeUnit.SECONDS);

        // Simulate aggressive cleaning
        metaClient.reloadActiveTimeline();
        HoodieTable table = HoodieSparkTable.create(writeConfig2, context, metaClient);
        table.clean(context, client2.createNewInstantTime()); // clean old file slices

        // Signal Writer 1 to complete its update
        cyclicBarrier.await(60, TimeUnit.SECONDS);
        writer2Completed.set(true);
      } catch (Exception e) {
        writer2Completed.set(false);
      }
    });

    // Wait for both writers to complete
    future1.get();
    future2.get();

    // Assertions to ensure both writers completed their operations
    assertTrue(writer1Completed.get() && writer2Completed.get());

    // Cleanup
    client1.close();
    client2.close();
  }

  private void ingestBatch(Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                           SparkRDDWriteClient writeClient, String commitTime, JavaRDD<HoodieRecord> records,
                           CountDownLatch countDownLatch) throws IOException, InterruptedException {
    WriteClientTestUtils.startCommitWithTime(writeClient, commitTime);
    countDownLatch.countDown();
    countDownLatch.await();
    List<WriteStatus> statuses = writeFn.apply(writeClient, records, commitTime).collect();
    writeClient.commit(commitTime, jsc.parallelize(statuses));
  }

  private void createCommitWithInsertsForPartition(HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                                   String prevCommitTime, String newCommitTime, int numRecords,
                                                   String partition) throws Exception {
    JavaRDD<WriteStatus> result = insertBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, SparkRDDWriteClient::insert,
        false, false, numRecords, numRecords, 1, Option.of(partition), INSTANT_GENERATOR);
  }

  private JavaRDD<WriteStatus> createCommitWithInserts(HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                                       String prevCommitTime, String newCommitTime, int numRecords) throws Exception {
    return createCommitWithInserts(cfg, client, prevCommitTime, newCommitTime, numRecords, true);
  }

  private JavaRDD<WriteStatus> createCommitWithInserts(HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                                       String prevCommitTime, String newCommitTime, int numRecords,
                                                       boolean doCommit) throws Exception {
    // Finish first base commit
    List<WriteStatus> result = insertFirstBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, SparkRDDWriteClient::bulkInsert,
        false, false, numRecords, true, INSTANT_GENERATOR, true).collect();
    if (doCommit) {
      assertTrue(client.commit(newCommitTime, jsc.parallelize(result)), "Commit should succeed");
    }
    return jsc.parallelize(result);
  }

  private static String startSchemaEvolutionTransaction(HoodieTableMetaClient metaClient, SparkRDDWriteClient client, HoodieTableType tableType) throws IOException {
    String commitActionType = CommitUtils.getCommitActionType(WriteOperationType.UPSERT, tableType);
    String instant = client.startCommit(commitActionType);
    client.preWrite(instant, WriteOperationType.UPSERT, client.createMetaClient(true));
    HoodieInstant requested = metaClient.createNewInstant(HoodieInstant.State.REQUESTED, commitActionType, instant);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.setOperationType(WriteOperationType.UPSERT);
    client.createMetaClient(true).getActiveTimeline().transitionRequestedToInflight(requested, Option.of(metadata));
    return instant;
  }

  private void createCommitWithUpserts(HoodieWriteConfig cfg, SparkRDDWriteClient client, String prevCommit,
                                       Option<String> commitTimeBetweenPrevAndNew, String newCommitTime, int numRecords)
      throws Exception {
    List<String> commitsBetweenPrevAndNew = commitTimeBetweenPrevAndNew.isEmpty() ? Collections.emptyList() : Collections.singletonList(commitTimeBetweenPrevAndNew.get());
    JavaRDD<WriteStatus> result = updateBatch(cfg, client, newCommitTime, prevCommit,
        Option.of(commitsBetweenPrevAndNew), "000", numRecords, SparkRDDWriteClient::upsert, false, false,
        numRecords, 200, 2, INSTANT_GENERATOR);
  }

  /**
   * Start the commit for an update operation with given number of records
   *
   * @param writeConfig   - Write config
   * @param writeClient   - Write client for starting the commit
   * @param newCommitTime - Commit time for the update
   * @param numRecords    - Number of records to update
   * @return RDD of write status from the update
   * @throws Exception
   */
  private JavaRDD<WriteStatus> startCommitForUpdate(HoodieWriteConfig writeConfig, SparkRDDWriteClient writeClient,
                                                    String newCommitTime, int numRecords) throws Exception {
    // Start the new commit
    WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

    // Prepare update records
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(false, writeConfig, dataGen::generateUniqueUpdates);
    final List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecords);
    final JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    // Write updates
    Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn = SparkRDDWriteClient::upsert;
    JavaRDD<WriteStatus> result = writeFn.apply(writeClient, writeRecords, newCommitTime);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);
    return result;
  }

  public static Stream<Arguments> configParamsTimelineServerBased() {
    Object[][] data =
        new Object[][] {
            {"COPY_ON_WRITE", AsyncTimelineServerBasedDetectionStrategy.class.getName()},
            {"MERGE_ON_READ", AsyncTimelineServerBasedDetectionStrategy.class.getName()}
        };
    return Stream.of(data).map(Arguments::of);
  }

  public static Stream<Arguments> configParamsDirectBased() {
    Object[][] data =
        new Object[][] {
            {"MERGE_ON_READ", SimpleDirectMarkerBasedDetectionStrategy.class.getName()},
            {"COPY_ON_WRITE", SimpleDirectMarkerBasedDetectionStrategy.class.getName()},
            {"COPY_ON_WRITE", SimpleTransactionDirectMarkerBasedDetectionStrategy.class.getName()}
        };
    return Stream.of(data).map(Arguments::of);
  }

  private HoodieWriteConfig buildWriteConfigForEarlyConflictDetect(String markerType, Properties properties,
                                                                   Class lockProvider, String earlyConflictDetectionStrategy) {
    if (markerType.equalsIgnoreCase(MarkerType.DIRECT.name())) {
      return getConfigBuilder()
          .withHeartbeatIntervalInMs(60 * 1000)
          .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
              .withStorageType(FileSystemViewStorageType.MEMORY)
              .withSecondaryStorageType(FileSystemViewStorageType.MEMORY).build())
          .withCleanConfig(HoodieCleanConfig.newBuilder()
              .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
              .withAutoClean(false).build())
          .withArchivalConfig(HoodieArchivalConfig.newBuilder()
              .withAutoArchive(false).build())
          .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
          .withMarkersType(MarkerType.DIRECT.name())
          .withEarlyConflictDetectionEnable(true)
          .withEarlyConflictDetectionStrategy(earlyConflictDetectionStrategy)
          .withAsyncConflictDetectorInitialDelayMs(0)
          .withAsyncConflictDetectorPeriodMs(100)
          .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(lockProvider).build())
          .withProperties(properties).build();
    } else {
      return getConfigBuilder()
          .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(20 * 1024).build())
          .withHeartbeatIntervalInMs(60 * 1000)
          .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
              .withStorageType(FileSystemViewStorageType.MEMORY)
              .withSecondaryStorageType(FileSystemViewStorageType.MEMORY).build())
          .withCleanConfig(HoodieCleanConfig.newBuilder()
              .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
              .withAutoClean(false).build())
          .withArchivalConfig(HoodieArchivalConfig.newBuilder()
              .withAutoArchive(false).build())
          .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
          .withMarkersType(MarkerType.TIMELINE_SERVER_BASED.name())
          // Set the batch processing interval for marker requests to be larger than
          // the running interval of the async conflict detector so that the conflict can
          // be detected before the marker requests are processed at the timeline server
          // in the test.
          .withMarkersTimelineServerBasedBatchIntervalMs(1000)
          .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(lockProvider).build())
          .withEarlyConflictDetectionEnable(true)
          .withEarlyConflictDetectionStrategy(earlyConflictDetectionStrategy)
          .withAsyncConflictDetectorInitialDelayMs(0)
          .withAsyncConflictDetectorPeriodMs(100)
          .withProperties(properties).build();
    }
  }
}
