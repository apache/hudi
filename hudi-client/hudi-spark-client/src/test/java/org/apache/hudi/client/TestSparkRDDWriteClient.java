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

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
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
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSparkRDDWriteClient extends SparkClientFunctionalTestHarness {

  static Stream<Arguments> testWriteClientReleaseResourcesShouldOnlyUnpersistRelevantRdds() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, true),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false)
    );
  }

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  public void testWriteClientAndTableServiceClientWithTimelineServer(
      boolean enableEmbeddedTimelineServer, boolean passInTimelineServer) throws IOException {
    HoodieTableMetaClient metaClient =
        getHoodieMetaClient(hadoopConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withPath(metaClient.getBasePathV2().toString())
        .withEmbeddedTimelineServerEnabled(enableEmbeddedTimelineServer)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(incrementTimelineServicePortToUse()).build())
        .build();

    SparkRDDWriteClient writeClient;
    if (passInTimelineServer) {
      EmbeddedTimelineService timelineService =
          new EmbeddedTimelineService(context(), null, writeConfig);
      timelineService.startServer();
      writeConfig.setViewStorageConfig(timelineService.getRemoteFileSystemViewConfig());
      writeClient = new SparkRDDWriteClient(context(), writeConfig, Option.of(timelineService));
      // Both the write client and the table service client should use the same passed-in
      // timeline server instance.
      assertEquals(timelineService, writeClient.getTimelineServer().get());
      assertEquals(timelineService, writeClient.getTableServiceClient().getTimelineServer().get());
      // Write config should not be changed
      assertEquals(writeConfig, writeClient.getConfig());
      timelineService.stop();
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
  void testWriteClientReleaseResourcesShouldOnlyUnpersistRelevantRdds(HoodieTableType tableType, boolean shouldReleaseResource) throws IOException {
    final HoodieTableMetaClient metaClient = getHoodieMetaClient(hadoopConf(), URI.create(basePath()).getPath(), tableType, new Properties());
    final HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withPath(metaClient.getBasePathV2().toString())
        .withAutoCommit(false)
        .withReleaseResourceEnabled(shouldReleaseResource)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
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
    writeClient.startCommitWithTime(instant1);
    List<WriteStatus> writeStatuses = writeClient.insert(writeRecords, instant1).collect();
    assertNoWriteErrors(writeStatuses);
    writeClient.commitStats(instant1, HoodieListData.eager(writeStatuses), writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
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
    }
  }
}
