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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;


import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for rolling metadata functionality.
 */
class TestRollingMetadata extends SparkClientFunctionalTestHarness {

  /**
   * Test that rolling metadata keys are carried forward to subsequent commits.
   */
  @Test
  public void testRollingMetadataCarriedForward() throws IOException {
    // Given: A table with rolling metadata keys configured
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("checkpoint.offset,checkpoint.partition")
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // When: First commit with rolling metadata keys
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    // Add rolling metadata to first commit
    Map<String, String> extraMetadata1 = new HashMap<>();
    extraMetadata1.put("checkpoint.offset", "1000");
    extraMetadata1.put("checkpoint.partition", "partition-0");
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata1), metaClient.getCommitActionType());

    // Then: Verify first commit has the rolling metadata
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit1 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata1 = TimelineUtils.getCommitMetadata(commit1, metaClient.getActiveTimeline());
    assertEquals("1000", metadata1.getMetadata("checkpoint.offset"));
    assertEquals("partition-0", metadata1.getMetadata("checkpoint.partition"));

    // When: Second commit updates one rolling metadata key
    String instant2 = client.createNewInstantTime(false);
    List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant2);
    List<WriteStatus> writeStatuses2 = client.insert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);

    // Only update checkpoint.offset, not checkpoint.partition
    Map<String, String> extraMetadata2 = new HashMap<>();
    extraMetadata2.put("checkpoint.offset", "2000");
    client.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata2), metaClient.getCommitActionType());

    // Then: Verify second commit has both rolling metadata keys (one carried forward, one updated)
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit2 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata2 = TimelineUtils.getCommitMetadata(commit2, metaClient.getActiveTimeline());
    assertEquals("2000", metadata2.getMetadata("checkpoint.offset")); // Updated value
    assertEquals("partition-0", metadata2.getMetadata("checkpoint.partition")); // Carried forward from commit1

    // When: Third commit with no rolling metadata keys
    String instant3 = client.createNewInstantTime(false);
    List<HoodieRecord> records3 = dataGen.generateInserts(instant3, 10);
    JavaRDD<HoodieRecord> writeRecords3 = jsc().parallelize(records3, 2);

    client.close();
    client = getHoodieWriteClient(config);

    WriteClientTestUtils.startCommitWithTime(client, instant3);
    List<WriteStatus> writeStatuses3 = client.insert(writeRecords3, instant3).collect();
    assertNoWriteErrors(writeStatuses3);
    client.commitStats(instant3, writeStatuses3.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Verify third commit has all rolling metadata keys carried forward
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit3 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata3 = TimelineUtils.getCommitMetadata(commit3, metaClient.getActiveTimeline());
    assertEquals("2000", metadata3.getMetadata("checkpoint.offset")); // Carried forward from commit2
    assertEquals("partition-0", metadata3.getMetadata("checkpoint.partition")); // Carried forward from commit1

    client.close();
  }

  /**
   * Test that rolling metadata works with no configured keys (default behavior).
   */
  @Test
  public void testRollingMetadataWithNoConfiguredKeys() throws IOException {
    // Given: A table with no rolling metadata keys configured
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // When: Commit with extra metadata
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    Map<String, String> extraMetadata1 = new HashMap<>();
    extraMetadata1.put("some.key", "value1");
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata1), metaClient.getCommitActionType());

    // Then: Verify metadata exists in first commit
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit1 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata1 = TimelineUtils.getCommitMetadata(commit1, metaClient.getActiveTimeline());
    assertEquals("value1", metadata1.getMetadata("some.key"));

    client.close();
    client = getHoodieWriteClient(config);
    // When: Second commit without the key
    String instant2 = client.createNewInstantTime(false);
    List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant2);
    List<WriteStatus> writeStatuses2 = client.insert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);
    client.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Verify key is NOT carried forward (no rolling metadata configured)
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit2 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata2 = TimelineUtils.getCommitMetadata(commit2, metaClient.getActiveTimeline());
    assertNull(metadata2.getMetadata("some.key"));

    client.close();
  }

  /**
   * Test that timeline walkback finds rolling metadata keys from older commits.
   */
  @Test
  public void testRollingMetadataWithTimelineWalkback() throws IOException {
    // Given: A table with rolling metadata keys and walkback configured
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("key1,key2,key3")
        .withRollingMetadataTimelineLookbackCommits(5)
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // When: First commit with key1 and key2
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    Map<String, String> extraMetadata1 = new HashMap<>();
    extraMetadata1.put("key1", "value1");
    extraMetadata1.put("key2", "value2");
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata1), metaClient.getCommitActionType());

    // When: Second commit with key3
    String instant2 = client.createNewInstantTime(false);
    List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant2);
    List<WriteStatus> writeStatuses2 = client.insert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);

    Map<String, String> extraMetadata2 = new HashMap<>();
    extraMetadata2.put("key3", "value3");
    client.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata2), metaClient.getCommitActionType());

    // Then: Verify second commit has all three keys (key3 from current, key1 and key2 from walkback)
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit2 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata2 = TimelineUtils.getCommitMetadata(commit2, metaClient.getActiveTimeline());
    assertEquals("value1", metadata2.getMetadata("key1")); // From commit1 via walkback
    assertEquals("value2", metadata2.getMetadata("key2")); // From commit1 via walkback
    assertEquals("value3", metadata2.getMetadata("key3")); // From current commit

    // When: Third commit with no keys (should walk back to find all three)
    String instant3 = client.createNewInstantTime(false);
    List<HoodieRecord> records3 = dataGen.generateInserts(instant3, 10);
    JavaRDD<HoodieRecord> writeRecords3 = jsc().parallelize(records3, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant3);
    List<WriteStatus> writeStatuses3 = client.insert(writeRecords3, instant3).collect();
    assertNoWriteErrors(writeStatuses3);
    client.commitStats(instant3, writeStatuses3.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Verify third commit has all three keys from walkback
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit3 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata3 = TimelineUtils.getCommitMetadata(commit3, metaClient.getActiveTimeline());
    assertEquals("value1", metadata3.getMetadata("key1")); // From commit1 via walkback
    assertEquals("value2", metadata3.getMetadata("key2")); // From commit1 via walkback
    assertEquals("value3", metadata3.getMetadata("key3")); // From commit2 via walkback

    client.close();
  }

  /**
   * Test that rolling metadata respects the walkback limit.
   */
  @Test
  public void testRollingMetadataWalkbackLimit() throws IOException {
    // Given: A table with rolling metadata keys and small walkback limit
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("checkpoint.offset")
        .withRollingMetadataTimelineLookbackCommits(2) // Only look back 2 commits
        .build();

    HoodieWriteConfig configNoRollingMetadata = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    String instant = client.createNewInstantTime(false);
    List<HoodieRecord> records = dataGen.generateInserts(instant, 10);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> writeStatuses = client.insert(writeRecords, instant).collect();
    assertNoWriteErrors(writeStatuses);
    // Only first commit has the rolling metadata key
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put("checkpoint.offset", "original-value");
    client.commitStats(instant, writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata), metaClient.getCommitActionType());

    // add 4 more commits w/o any rolling metadata.
    client.close();
    client = getHoodieWriteClient(configNoRollingMetadata);
    for (int i = 2; i <= 3; i++) {
      instant = client.createNewInstantTime(false);
      records = dataGen.generateInserts(instant, 10);
      writeRecords = jsc().parallelize(records, 2);

      WriteClientTestUtils.startCommitWithTime(client, instant);
      writeStatuses = client.insert(writeRecords, instant).collect();
      assertNoWriteErrors(writeStatuses);
      client.commitStats(instant, writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType());
    }

    client.close();
    client = getHoodieWriteClient(config);
    for (int i = 4; i <= 5; i++) {
      instant = client.createNewInstantTime(false);
      records = dataGen.generateInserts(instant, 10);
      writeRecords = jsc().parallelize(records, 2);

      WriteClientTestUtils.startCommitWithTime(client, instant);
      writeStatuses = client.insert(writeRecords, instant).collect();
      assertNoWriteErrors(writeStatuses);
      client.commitStats(instant, writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType());
    }

    // Then: Verify that commits 2 and 3 do NOT have rolling metadata (written with configNoRollingMetadata)
    metaClient = HoodieTableMetaClient.reload(metaClient);
    List<HoodieInstant> commits = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
        .collect(Collectors.toList());

    HoodieCommitMetadata metadata2 = TimelineUtils.getCommitMetadata(commits.get(1), metaClient.getActiveTimeline());
    assertFalse(metadata2.getExtraMetadata().containsKey("checkpoint.offset"));

    HoodieCommitMetadata metadata3 = TimelineUtils.getCommitMetadata(commits.get(2), metaClient.getActiveTimeline());
    assertFalse(metadata3.getExtraMetadata().containsKey("checkpoint.offset"));

    // But commit 4 and 5 should NOT have it (beyond walkback limit of 2) even though rolling keys are configured.
    HoodieCommitMetadata metadata4 = TimelineUtils.getCommitMetadata(commits.get(3), metaClient.getActiveTimeline());
    assertNull(metadata4.getMetadata("checkpoint.offset"));

    HoodieCommitMetadata metadata5 = TimelineUtils.getCommitMetadata(commits.get(4), metaClient.getActiveTimeline());
    assertNull(metadata5.getMetadata("checkpoint.offset"));

    client.close();
  }

  /**
   * Test that rolling metadata works with different table types (CoW and MoR).
   */
  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  public void testRollingMetadataWithDifferentTableTypes(String tableTypeStr) throws IOException {
    // Given: A table of specified type with rolling metadata keys
    HoodieTableType tableType = HoodieTableType.valueOf(tableTypeStr);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), tableType, new Properties());
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("test.key")
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // When: First commit with rolling metadata
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    Map<String, String> extraMetadata1 = new HashMap<>();
    extraMetadata1.put("test.key", "test-value");
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata1), metaClient.getCommitActionType());

    // When: Second commit without the key
    String instant2 = client.createNewInstantTime(false);
    List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant2);
    List<WriteStatus> writeStatuses2 = client.insert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);
    client.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Verify rolling metadata works for both table types
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit2 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata2 = TimelineUtils.getCommitMetadata(commit2, metaClient.getActiveTimeline());
    assertEquals("test-value", metadata2.getMetadata("test.key"));

    client.close();
  }

  /**
   * Test that current commit values override old values for rolling metadata.
   */
  @Test
  public void testRollingMetadataOverrideSemantics() throws IOException {
    // Given: A table with rolling metadata keys configured
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("checkpoint.offset")
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // When: First commit with initial value
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    Map<String, String> extraMetadata1 = new HashMap<>();
    extraMetadata1.put("checkpoint.offset", "100");
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata1), metaClient.getCommitActionType());

    client.close();
    client = getHoodieWriteClient(config);
    // When: Second commit with updated value
    String instant2 = client.createNewInstantTime(false);
    List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant2);
    List<WriteStatus> writeStatuses2 = client.insert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);

    Map<String, String> extraMetadata2 = new HashMap<>();
    extraMetadata2.put("checkpoint.offset", "200");
    client.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata2), metaClient.getCommitActionType());

    // Then: Verify second commit has the updated value (not the old one)
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit2 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata2 = TimelineUtils.getCommitMetadata(commit2, metaClient.getActiveTimeline());
    assertEquals("200", metadata2.getMetadata("checkpoint.offset"));

    client.close();
    client = getHoodieWriteClient(config);
    // When: Third commit without the key
    String instant3 = client.createNewInstantTime(false);
    List<HoodieRecord> records3 = dataGen.generateInserts(instant3, 10);
    JavaRDD<HoodieRecord> writeRecords3 = jsc().parallelize(records3, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant3);
    List<WriteStatus> writeStatuses3 = client.insert(writeRecords3, instant3).collect();
    assertNoWriteErrors(writeStatuses3);
    client.commitStats(instant3, writeStatuses3.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Verify third commit has the most recent value from commit2
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit3 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata3 = TimelineUtils.getCommitMetadata(commit3, metaClient.getActiveTimeline());
    assertEquals("200", metadata3.getMetadata("checkpoint.offset"));

    client.close();
  }

  /**
   * Test that rolling metadata is NOT applied to the first commit in an empty table.
   */
  @Test
  public void testRollingMetadataFirstCommit() throws IOException {
    // Given: A new empty table with rolling metadata keys configured
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("checkpoint.offset")
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // When: First commit without rolling metadata
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Verify first commit does NOT have rolling metadata (no previous commits to pull from)
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    assertEquals(1, timeline.countInstants());

    HoodieInstant commit1 = timeline.lastInstant().get();
    HoodieCommitMetadata metadata1 = TimelineUtils.getCommitMetadata(commit1, metaClient.getActiveTimeline());
    assertNull(metadata1.getMetadata("checkpoint.offset"));

    client.close();
  }

  /**
   * Test rolling metadata with empty string and whitespace in keys.
   */
  @Test
  public void testRollingMetadataWithEmptyAndWhitespaceKeys() throws IOException {
    // Given: A table with rolling metadata keys that include empty strings and whitespace
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("key1,  , key2,  ,key3")  // Has empty and whitespace entries
        .build();

    assertEquals(3, config.getRollingMetadataKeys().size());
    assertTrue(config.getRollingMetadataKeys().contains("key1"));
    assertTrue(config.getRollingMetadataKeys().contains("key2"));
    assertTrue(config.getRollingMetadataKeys().contains("key3"));
    assertFalse(config.getRollingMetadataKeys().contains(""));
    assertFalse(config.getRollingMetadataKeys().contains(" "));

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // When: Commit with rolling metadata
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    Map<String, String> extraMetadata1 = new HashMap<>();
    extraMetadata1.put("key1", "value1");
    extraMetadata1.put("key3", "value3");
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata1), metaClient.getCommitActionType());

    // When: Second commit
    String instant2 = client.createNewInstantTime(false);
    List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant2);
    List<WriteStatus> writeStatuses2 = client.insert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);
    client.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Then: Verify rolling metadata works correctly (empty keys were filtered out)
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit2 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata2 = TimelineUtils.getCommitMetadata(commit2, metaClient.getActiveTimeline());
    assertEquals("value1", metadata2.getMetadata("key1"));
    assertEquals("value3", metadata2.getMetadata("key3"));
    assertNull(metadata2.getMetadata("key2")); // Was not in first commit

    client.close();
  }

  /**
   * Test that an empty-string value for a rolling metadata key is treated as missing,
   * so the walkback still finds the most recent non-empty value.
   */
  @Test
  public void testRollingMetadataEmptyStringTreatedAsMissing() throws IOException {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), new Properties());
    HoodieWriteConfig config = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withRollingMetadataKeys("checkpoint.offset")
        .build();

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First commit with valid rolling metadata value
    String instant1 = client.createNewInstantTime(false);
    List<HoodieRecord> records1 = dataGen.generateInserts(instant1, 10);
    JavaRDD<HoodieRecord> writeRecords1 = jsc().parallelize(records1, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant1);
    List<WriteStatus> writeStatuses1 = client.insert(writeRecords1, instant1).collect();
    assertNoWriteErrors(writeStatuses1);

    Map<String, String> extraMetadata1 = new HashMap<>();
    extraMetadata1.put("checkpoint.offset", "1000");
    client.commitStats(instant1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata1), metaClient.getCommitActionType());

    // Second commit explicitly sets the rolling key to empty string
    String instant2 = client.createNewInstantTime(false);
    List<HoodieRecord> records2 = dataGen.generateInserts(instant2, 10);
    JavaRDD<HoodieRecord> writeRecords2 = jsc().parallelize(records2, 2);

    WriteClientTestUtils.startCommitWithTime(client, instant2);
    List<WriteStatus> writeStatuses2 = client.insert(writeRecords2, instant2).collect();
    assertNoWriteErrors(writeStatuses2);

    Map<String, String> extraMetadata2 = new HashMap<>();
    extraMetadata2.put("checkpoint.offset", "");
    client.commitStats(instant2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.of(extraMetadata2), metaClient.getCommitActionType());

    // The empty string should be treated as missing, so walkback finds "1000" from commit1
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant commit2 = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata metadata2 = TimelineUtils.getCommitMetadata(commit2, metaClient.getActiveTimeline());
    assertEquals("1000", metadata2.getMetadata("checkpoint.offset"),
        "Empty string value should be treated as missing; walkback should find the previous non-empty value");

    client.close();
  }
}
