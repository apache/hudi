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

package org.apache.hudi.client;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.apache.hudi.metrics.HoodieMetrics.DURATION_STR;
import static org.apache.hudi.metrics.HoodieMetrics.FAILURE_COUNTER;
import static org.apache.hudi.metrics.HoodieMetrics.POST_COMMIT_STR;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSparkRDDWriteClient extends SparkClientFunctionalTestHarness {

  static Stream<Arguments> testWriteClientReleaseResourcesShouldOnlyUnpersistRelevantRdds() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, true, true),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, true, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true, true),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true, false),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false, true),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false, true),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false, false)
    );
  }

  static Stream<Arguments> testSpeculativeExecutionGuardrail() {
    return Stream.of(
        // blockOnSpeculativeExecution, speculationEnabled
        Arguments.of(true, true),    // Guardrail enabled (default), speculation enabled -> should throw
        Arguments.of(true, false),   // Guardrail enabled (default), speculation disabled -> should not throw
        Arguments.of(false, true),   // Guardrail disabled, speculation enabled -> should not throw
        Arguments.of(false, false)   // Guardrail disabled, speculation disabled -> should not throw
    );
  }

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  public void testWriteClientAndTableServiceClientWithTimelineServer(
      boolean enableEmbeddedTimelineServer, boolean passInTimelineServer) throws IOException {
    HoodieTableMetaClient metaClient =
        getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withEmbeddedTimelineServerEnabled(enableEmbeddedTimelineServer)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(incrementTimelineServicePortToUse()).build())
        .build();

    SparkRDDWriteClient writeClient;
    if (passInTimelineServer) {
      EmbeddedTimelineService timelineService = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(context(), null, writeConfig);
      writeConfig.setViewStorageConfig(timelineService.getRemoteFileSystemViewConfig(writeConfig));
      writeClient = new SparkRDDWriteClient(context(), writeConfig, Option.of(timelineService));
      // Both the write client and the table service client should use the same passed-in
      // timeline server instance.
      assertEquals(timelineService, writeClient.getTimelineServer().get());
      assertEquals(timelineService, writeClient.getTableServiceClient().getTimelineServer().get());
      // Write config should not be changed
      assertEquals(writeConfig, writeClient.getConfig());
      timelineService.stopForBasePath(writeConfig.getBasePath());
    } else {
      writeClient = new SparkRDDWriteClient(context(), writeConfig);
      // Only one timeline server should be instantiated, and the same timeline server
      // should be used by both the write client and the table service client.
      assertEquals(
          writeClient.getTimelineServer(),
          writeClient.getTableServiceClient().getTimelineServer());
      if (!enableEmbeddedTimelineServer) {
        assertFalse(writeClient.getTimelineServer().isPresent());
      }
    }
    writeClient.close();
  }

  @ParameterizedTest
  @MethodSource
  void testWriteClientReleaseResourcesShouldOnlyUnpersistRelevantRdds(
      HoodieTableType tableType, boolean shouldReleaseResource, boolean metadataTableEnable) throws IOException {
    final HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), tableType, new Properties());
    final HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withReleaseResourceEnabled(shouldReleaseResource)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(metadataTableEnable).build())
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED);

    String instant0 = getCommitTimeAtUTC(0);
    List<GenericRecord> extraRecords0 = dataGen.generateGenericRecords(10);
    HoodieJavaRDD<GenericRecord> persistedRdd0 = HoodieJavaRDD.of(jsc().parallelize(extraRecords0, 2));
    persistedRdd0.persist("MEMORY_AND_DISK", context(), HoodieDataCacheKey.of(writeConfig.getBasePath(), instant0));

    String instant1 = getCommitTimeAtUTC(1);
    List<GenericRecord> extraRecords1 = dataGen.generateGenericRecords(10);
    HoodieJavaRDD<GenericRecord> persistedRdd1 = HoodieJavaRDD.of(jsc().parallelize(extraRecords1, 2));
    persistedRdd1.persist("MEMORY_AND_DISK", context(), HoodieDataCacheKey.of(writeConfig.getBasePath(), instant1));

    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);
    List<HoodieRecord> records = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 2);
    WriteClientTestUtils.startCommitWithTime(writeClient, instant1);
    List<WriteStatus> writeStatuses = writeClient.insert(writeRecords, instant1).collect();
    assertNoWriteErrors(writeStatuses);
    String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(writeConfig.getBasePath());
    List<Integer> metadataTableCacheIds0 = context().getCachedDataIds(HoodieDataCacheKey.of(metadataTableBasePath, instant0));
    List<Integer> metadataTableCacheIds1 = context().getCachedDataIds(HoodieDataCacheKey.of(metadataTableBasePath, instant1));
    writeClient.commitStats(instant1, writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());
    writeClient.close();

    if (shouldReleaseResource) {
      assertEquals(Collections.singletonList(persistedRdd0.getId()),
          context().getCachedDataIds(HoodieDataCacheKey.of(writeConfig.getBasePath(), instant0)),
          "RDDs cached for " + instant0 + " should be retained.");
      assertEquals(Collections.emptyList(),
          context().getCachedDataIds(HoodieDataCacheKey.of(writeConfig.getBasePath(), instant1)),
          "RDDs cached for " + instant1 + " should be cleared.");
      assertTrue(jsc().getPersistentRDDs().containsKey(persistedRdd0.getId()),
          "RDDs cached for " + instant0 + " should be retained.");
      assertFalse(jsc().getPersistentRDDs().containsKey(persistedRdd1.getId()),
          "RDDs cached for " + instant1 + " should be cleared.");
      assertFalse(jsc().getPersistentRDDs().containsKey(writeRecords.id()),
          "RDDs cached for " + instant1 + " should be cleared.");
      if (metadataTableEnable) {
        assertEquals(metadataTableCacheIds0.stream().sorted().collect(Collectors.toList()),
            context().getCachedDataIds(HoodieDataCacheKey.of(metadataTableBasePath, instant0)).stream().sorted().collect(Collectors.toList()),
            "RDDs cached for metadataTable " + instant0 + " should be retained.");
        assertEquals(Collections.emptyList(),
            context().getCachedDataIds(HoodieDataCacheKey.of(metadataTableBasePath, instant1)),
            "RDDs cached for metadataTable " + instant1 + " should be cleared.");
        metadataTableCacheIds0.forEach(cacheId -> assertTrue(jsc().getPersistentRDDs().containsKey(cacheId),
            "RDDs cached for metadataTable cacheId " + cacheId + " should be retained."));
        metadataTableCacheIds1.forEach(cacheId -> assertFalse(jsc().getPersistentRDDs().containsKey(cacheId),
            "RDDs cached for metadataTable cacheId " + cacheId + " should be cleared."));
      }
    } else {
      assertEquals(Collections.singletonList(persistedRdd0.getId()),
          context().getCachedDataIds(HoodieDataCacheKey.of(writeConfig.getBasePath(), instant0)),
          "RDDs cached for " + instant0 + " should be retained.");
      assertEquals(3,
          context().getCachedDataIds(HoodieDataCacheKey.of(writeConfig.getBasePath(), instant1)).size(),
          "RDDs cached for " + instant1 + " should be retained.");
      assertTrue(jsc().getPersistentRDDs().containsKey(persistedRdd0.getId()),
          "RDDs cached for " + instant0 + " should be retained.");
      assertTrue(jsc().getPersistentRDDs().containsKey(persistedRdd1.getId()),
          "RDDs cached for " + instant1 + " should be retained.");
      assertTrue(jsc().getPersistentRDDs().containsKey(writeRecords.id()),
          "RDDs cached for " + instant1 + " should be retained.");
      if (metadataTableEnable) {
        metadataTableCacheIds0.forEach(cacheId -> assertTrue(jsc().getPersistentRDDs().containsKey(cacheId),
            "RDDs cached for metadataTable cacheId " + cacheId + " should be retained."));
        metadataTableCacheIds1.forEach(cacheId -> assertTrue(jsc().getPersistentRDDs().containsKey(cacheId),
            "RDDs cached for metadataTable cacheId " + cacheId + " should be retained."));
      }
    }
  }

  @Test
  public void testCompletionTimeGreaterThanRequestedTime() throws IOException {
    String basePath = URI.create(basePath()).getPath();
    testAndAssertCompletionIsEarlierThanRequested(basePath, new Properties());

    // retry w/ explicitly setting timezone to UTC
    basePath = basePath + "_UTC";
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.TIMELINE_TIMEZONE.key(), "UTC");
    testAndAssertCompletionIsEarlierThanRequested(basePath, props);
  }

  private void testAndAssertCompletionIsEarlierThanRequested(String basePath, Properties properties) throws IOException {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), basePath, properties);

    HoodieWriteConfig cfg = getConfigBuilder(true).build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      for (int i = 0; i < 10; i++) {
        String requestedTime = client.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(requestedTime, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);
        client.upsert(writeRecords, requestedTime);
      }
    }
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(context().getStorageConf()).build();
    metaClient.reloadActiveTimeline();
    metaClient.getActiveTimeline().filterCompletedInstants().getInstants().forEach(hoodieInstant -> {
      assertTrue(InstantComparison.compareTimestamps(hoodieInstant.requestedTime(), InstantComparison.LESSER_THAN, hoodieInstant.getCompletionTime()));
    });
  }

  /**
   * Test that initializeMetadataTable invokes reloadTableConfig when hasPartitionsStateChanged returns true.
   */
  @Test
  public void testInitializeMetadataTableReloadsConfigWhenPartitionsStateChanged() throws Exception {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());

    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMetadataIndexBloomFilter(true)
            .withMetadataIndexColumnStats(true)
            .build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config)) {
      // Use reflection to access the private initializeMetadataTable method
      java.lang.reflect.Method initializeMetadataTableMethod =
          SparkRDDWriteClient.class.getDeclaredMethod("initializeMetadataTable", Option.class, HoodieTableMetaClient.class);
      initializeMetadataTableMethod.setAccessible(true);

      // Get initial metadata partition count
      metaClient = HoodieTableMetaClient.reload(metaClient);
      int initialPartitionCount = metaClient.getTableConfig().getMetadataPartitions().size();

      // Create a spy on the metaClient to track reloadTableConfig calls
      HoodieTableMetaClient spyMetaClient = org.mockito.Mockito.spy(metaClient);

      // Invoke initializeMetadataTable
      initializeMetadataTableMethod.invoke(client, Option.of("001"), spyMetaClient);

      // Reload metaClient to check if partitions were actually bootstrapped
      HoodieTableMetaClient reloadedMetaClient = HoodieTableMetaClient.reload(metaClient);
      int finalPartitionCount = reloadedMetaClient.getTableConfig().getMetadataPartitions().size();

      // If partitions were bootstrapped (count increased), verify reloadTableConfig was called
      if (finalPartitionCount > initialPartitionCount) {
        org.mockito.Mockito.verify(spyMetaClient, org.mockito.Mockito.atLeastOnce()).reloadTableConfig();
      }
    }
  }

  @ParameterizedTest
  @MethodSource("testSpeculativeExecutionGuardrail")
  public void testSpeculativeExecutionGuardrail(boolean blockOnSpeculativeExecution, boolean speculationEnabled) throws IOException {
    // Set spark.speculation based on the test parameter
    jsc().sc().conf().set("spark.speculation", String.valueOf(speculationEnabled));

    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withPath(metaClient.getBasePath().toString())
        .withBlockWritesOnSpeculativeExecution(blockOnSpeculativeExecution)
        .build();

    if (blockOnSpeculativeExecution && speculationEnabled) {
      // When speculative execution is enabled and the guardrail is enabled,
      // creating a SparkRDDWriteClient should throw an exception
      HoodieException exception = assertThrows(HoodieException.class, () -> {
        new SparkRDDWriteClient(context(), writeConfig);
      });

      // Verify the exception message
      assertTrue(exception.getMessage().contains("Spark speculative execution is enabled"));
      assertTrue(exception.getMessage().contains("can lead to duplicate writes and data corruption"));
    } else {
      // In all other cases, creating a SparkRDDWriteClient should not throw an exception
      SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context(), writeConfig);
      writeClient.close();
    }

    // Reset speculation config after test
    jsc().sc().conf().set("spark.speculation", "false");
  }

  @Test
  public void testPostCommitFailureHandlingWithMetrics() throws IOException {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());

    // Create a custom write client that throws exception during post commit operations
    class PostCommitFailingWriteClient extends SparkRDDWriteClient {
      private boolean shouldFailPostCommit = false;

      public PostCommitFailingWriteClient(HoodieSparkEngineContext context, HoodieWriteConfig writeConfig) {
        super(context, writeConfig);
      }

      public void setShouldFailPostCommit(boolean shouldFail) {
        this.shouldFailPostCommit = shouldFail;
      }

      @Override
      protected void mayBeCleanAndArchive(HoodieTable table) {
        if (shouldFailPostCommit) {
          throw new RuntimeException("Simulated post commit failure for testing");
        }
        super.mayBeCleanAndArchive(table);
      }
    }

    // Test with post commit failures ignored
    HoodieWriteConfig configWithIgnore = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withIgnorePostCommitFailure(true)
        .withMetricsConfig(org.apache.hudi.config.metrics.HoodieMetricsConfig.newBuilder()
            .on(true)
            .withReporterType("INMEMORY")
            .build())
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    String instant1 = getCommitTimeAtUTC(1);

    PostCommitFailingWriteClient clientWithIgnore = new PostCommitFailingWriteClient(context(), configWithIgnore);
    clientWithIgnore.setShouldFailPostCommit(true);

    // Generate and write records - this should succeed even though postCommit fails
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(clientWithIgnore, instant1);
    List<WriteStatus> writeStatuses1 = clientWithIgnore.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    // The commit should succeed despite postCommit failure because we have ignore flag enabled
    clientWithIgnore.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Verify metrics were updated for failure
    Metrics metrics = Metrics.getInstance(configWithIgnore.getMetricsConfig(), hoodieStorage());
    MetricRegistry registry = metrics.getRegistry();

    // Build metric names correctly using HoodieMetrics.getMetricsName pattern
    String failureMetricName = clientWithIgnore.getMetrics().getMetricsName(POST_COMMIT_STR, FAILURE_COUNTER);
    String durationMetricName = clientWithIgnore.getMetrics().getMetricsName(POST_COMMIT_STR, DURATION_STR);

    Gauge<Long> failureGauge = (Gauge<Long>) registry.getGauges().get(failureMetricName);
    Gauge<Long> durationGauge = (Gauge<Long>) registry.getGauges().get(durationMetricName);

    assertNotNull(failureGauge, "Failure metric should be registered");
    assertEquals(1L, failureGauge.getValue(), "Failure count should be 1");
    assertNotNull(durationGauge, "Duration metric should be registered");
    assertTrue(durationGauge.getValue() >= 0, "Duration should be non-negative");

    clientWithIgnore.close();

    // Test with post commit failures NOT ignored (should throw exception)
    HoodieWriteConfig configWithoutIgnore = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withIgnorePostCommitFailure(false)
        .withMetricsConfig(org.apache.hudi.config.metrics.HoodieMetricsConfig.newBuilder()
            .on(true)
            .withReporterType("INMEMORY")
            .build())
        .build();

    String instant2 = getCommitTimeAtUTC(2);
    PostCommitFailingWriteClient clientWithoutIgnore = new PostCommitFailingWriteClient(context(), configWithoutIgnore);
    clientWithoutIgnore.setShouldFailPostCommit(true);

    List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(clientWithoutIgnore, instant2);
    List<WriteStatus> writeStatuses2 = clientWithoutIgnore.insert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);

    // This should throw RuntimeException because ignore flag is false
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      clientWithoutIgnore.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType());
    });

    assertTrue(exception.getMessage().contains("Simulated post commit failure for testing"),
        "Exception message should contain the simulated failure message");

    // Verify metrics were still updated even when exception was thrown
    Metrics metrics2 = Metrics.getInstance(configWithoutIgnore.getMetricsConfig(), hoodieStorage());
    MetricRegistry registry2 = metrics2.getRegistry();

    String failureMetricName2 = clientWithoutIgnore.getMetrics().getMetricsName(POST_COMMIT_STR, FAILURE_COUNTER);
    String durationMetricName2 = clientWithoutIgnore.getMetrics().getMetricsName(POST_COMMIT_STR, DURATION_STR);

    Gauge<Long> failureGauge2 = (Gauge<Long>) registry2.getGauges().get(failureMetricName2);
    Gauge<Long> durationGauge2 = (Gauge<Long>) registry2.getGauges().get(durationMetricName2);

    assertNotNull(failureGauge2, "Failure metric should be registered for second test");
    assertEquals(1L, failureGauge2.getValue(), "Failure count should be 1");
    assertNotNull(durationGauge2, "Duration metric should be registered for second test");
    assertTrue(durationGauge2.getValue() >= 0, "Duration should be non-negative");

    clientWithoutIgnore.close();
  }
}
