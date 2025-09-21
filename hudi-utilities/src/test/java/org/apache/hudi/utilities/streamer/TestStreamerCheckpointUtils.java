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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.exception.HoodieStreamerException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.utilities.streamer.HoodieStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.streamer.StreamSync.CHECKPOINT_IGNORE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestStreamerCheckpointUtils extends SparkClientFunctionalTestHarness {
  private TypedProperties props;
  private HoodieStreamer.Config streamerConfig;
  protected HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    metaClient = HoodieTestUtils.init(basePath(), HoodieTableType.COPY_ON_WRITE);
    props = new TypedProperties();
    streamerConfig = new HoodieStreamer.Config();
    streamerConfig.tableType = HoodieTableType.COPY_ON_WRITE.name();
  }

  @Test
  public void testEmptyTimelineCase() throws IOException {
    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.isEmpty());
  }

  @Test
  public void testIgnoreCheckpointCaseEmptyIgnoreKey() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(CHECKPOINT_KEY, "ckp_key");
    extraMetadata.put(CHECKPOINT_IGNORE_KEY, "");
    createCommit(commitTime, extraMetadata);

    streamerConfig.ignoreCheckpoint = "ignore_checkpoint_1";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.isEmpty());
  }

  @Test
  public void testIgnoreCheckpointCaseIgnoreKeyMismatch() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(CHECKPOINT_KEY, "ckp_key");
    extraMetadata.put(CHECKPOINT_IGNORE_KEY, "ignore_checkpoint_2");
    createCommit(commitTime, extraMetadata);

    streamerConfig.ignoreCheckpoint = "ignore_checkpoint_1";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.isEmpty());
  }

  @Test
  public void testThrowExceptionCase() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(CHECKPOINT_KEY, "");
    extraMetadata.put(HoodieStreamer.CHECKPOINT_RESET_KEY, "old-reset-key");
    createCommit(commitTime, extraMetadata);

    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    HoodieStreamerException exception = assertThrows(HoodieStreamerException.class, () -> {
      StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
          metaClient.getActiveTimeline(), streamerConfig, props);
    });
    assertTrue(exception.getMessage().contains("Unable to find previous checkpoint"));
  }

  @Test
  public void testNewCheckpointV2WithResetKeyCase() throws IOException {
    String commitTime = "0000000000";
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(CHECKPOINT_KEY, "");
    extraMetadata.put(HoodieStreamer.CHECKPOINT_RESET_KEY, "old-reset-key");
    createCommit(commitTime, extraMetadata);

    streamerConfig.checkpoint = "earliest";
    streamerConfig.sourceClassName = "org.apache.hudi.utilities.sources.KafkaSource";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.get() instanceof StreamerCheckpointV1);
    assertEquals("earliest", checkpoint.get().getCheckpointKey());
  }

  @Test
  public void testNewCheckpointV1WithResetKeyCase() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(HoodieStreamer.CHECKPOINT_RESET_KEY, "old-reset-key");
    createCommit(commitTime, extraMetadata);

    streamerConfig.checkpoint = "earliest";
    streamerConfig.sourceClassName = "org.apache.hudi.utilities.sources.KafkaSource";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "1");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.get() instanceof StreamerCheckpointV1);
    assertEquals("earliest", checkpoint.get().getCheckpointKey());
  }

  @Test
  public void testReuseCheckpointCase() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(CHECKPOINT_KEY, "earliest-0-100");
    extraMetadata.put(CHECKPOINT_IGNORE_KEY, "");
    extraMetadata.put(HoodieStreamer.CHECKPOINT_RESET_KEY, "");
    createCommit(commitTime, extraMetadata);

    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertEquals("earliest-0-100", checkpoint.get().getCheckpointKey());
  }

  public void testNewCheckpointV2NoMetadataCase() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    createCommit(commitTime, extraMetadata);

    streamerConfig.checkpoint = "earliest";
    streamerConfig.sourceClassName = "org.apache.hudi.utilities.sources.KafkaSource";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.get() instanceof StreamerCheckpointV2);
    assertEquals("earliest", checkpoint.get().getCheckpointKey());
  }

  @Test
  public void testNewCheckpointV1NoMetadataCase() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    createCommit(commitTime, extraMetadata);

    streamerConfig.checkpoint = "earliest";
    streamerConfig.sourceClassName = "org.apache.hudi.utilities.sources.KafkaSource";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "1");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.get() instanceof StreamerCheckpointV1);
    assertEquals("earliest", checkpoint.get().getCheckpointKey());
  }

  private void createCommit(String commitTime, Map<String, String> extraMetadata) throws IOException {
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.INFLIGHT,
        HoodieTimeline.COMMIT_ACTION, commitTime, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    timeline.createNewInstant(instant);
    timeline.saveAsComplete(instant, HoodieCommonTestHarness.getCommitMetadata(metaClient, basePath(), "partition1", commitTime, 2, extraMetadata),
        WriteClientTestUtils.createNewInstantTime());
    metaClient.reloadActiveTimeline();
  }

  @Test
  public void testIgnoreCheckpointNullKeyCase() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    // Set empty ignore key
    extraMetadata.put(CHECKPOINT_KEY, "some-checkpoint");
    extraMetadata.put(CHECKPOINT_IGNORE_KEY, "");
    createCommit(commitTime, extraMetadata);

    streamerConfig.ignoreCheckpoint = "ignore_checkpoint_1";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.isEmpty());
  }

  @Test
  public void testNewCheckpointWithEmptyResetKey() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(HoodieStreamer.CHECKPOINT_KEY, "old-checkpoint");
    extraMetadata.put(HoodieStreamer.CHECKPOINT_RESET_KEY, ""); // Empty reset key
    createCommit(commitTime, extraMetadata);

    streamerConfig.checkpoint = "new-checkpoint";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.get() instanceof StreamerCheckpointV1);
    assertEquals("new-checkpoint", checkpoint.get().getCheckpointKey());
  }

  @Test
  public void testNewCheckpointWithDifferentResetKey() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(HoodieStreamer.CHECKPOINT_KEY, "old-checkpoint");
    extraMetadata.put(HoodieStreamer.CHECKPOINT_RESET_KEY, "different-reset-key");
    createCommit(commitTime, extraMetadata);

    streamerConfig.checkpoint = "new-checkpoint";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);
    assertTrue(checkpoint.get() instanceof StreamerCheckpointV1);
    assertEquals("new-checkpoint", checkpoint.get().getCheckpointKey());
  }

  @Test
  public void testMergeOnReadWithDeltaCommits() throws IOException {
    // Setup MOR table
    metaClient = HoodieTestUtils.init(basePath(), HoodieTableType.MERGE_ON_READ);
    streamerConfig.tableType = HoodieTableType.MERGE_ON_READ.name();

    // Create a commit and deltacommit
    String commitTime = "20240120000000";
    String deltaCommitTime = "20240120000001";

    // Create commit
    Map<String, String> commitMetadata = new HashMap<>();
    commitMetadata.put(HoodieStreamer.CHECKPOINT_KEY, "commit-cp");
    createCommit(commitTime, commitMetadata);

    // Create deltacommit
    Map<String, String> deltaCommitMetadata = new HashMap<>();
    deltaCommitMetadata.put(HoodieStreamer.CHECKPOINT_KEY, "deltacommit-cp");
    createDeltaCommit(deltaCommitTime, deltaCommitMetadata);

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);

    // Should use deltacommit checkpoint
    assertEquals("deltacommit-cp", checkpoint.get().getCheckpointKey());
  }

  @Test
  public void testMergeOnReadWithoutDeltaCommits() throws IOException {
    // Setup MOR table
    metaClient = HoodieTestUtils.init(basePath(), HoodieTableType.MERGE_ON_READ);
    streamerConfig.tableType = HoodieTableType.MERGE_ON_READ.name();

    // Create only commit
    String commitTime = "20240120000000";
    Map<String, String> commitMetadata = new HashMap<>();
    commitMetadata.put(HoodieStreamer.CHECKPOINT_KEY, "commit-cp");
    createCommit(commitTime, commitMetadata);

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointBetweenConfigAndPrevCommit(
        metaClient.getActiveTimeline(), streamerConfig, props);

    // Should use commit checkpoint
    assertEquals("commit-cp", checkpoint.get().getCheckpointKey());
  }

  private void createDeltaCommit(String deltaCommitTime, Map<String, String> extraMetadata) throws IOException {
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.INFLIGHT,
        HoodieTimeline.DELTA_COMMIT_ACTION, deltaCommitTime, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    timeline.createNewInstant(instant);
    timeline.saveAsComplete(instant, HoodieCommonTestHarness.getCommitMetadata(metaClient, basePath(), "partition1", deltaCommitTime, 2, extraMetadata),
        WriteClientTestUtils.createNewInstantTime());
    metaClient.reloadActiveTimeline();
  }

  @Test
  public void testCreateNewCheckpointV2WithNullTimeline() throws IOException {
    streamerConfig.checkpoint = "test-cp";
    streamerConfig.sourceClassName = "org.apache.hudi.utilities.sources.KafkaSource";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "2");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointToResumeFrom(
        Option.empty(), streamerConfig, props, metaClient);
    assertTrue(checkpoint.get() instanceof StreamerCheckpointV1);
    assertEquals("test-cp", checkpoint.get().getCheckpointKey());
  }

  @Test
  public void testCreateNewCheckpointV1WithNullTimeline() throws IOException {
    streamerConfig.checkpoint = "test-cp";
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "1");

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointToResumeFrom(
        Option.empty(), streamerConfig, props, metaClient);
    assertTrue(checkpoint.get() instanceof StreamerCheckpointV1);
    assertEquals("test-cp", checkpoint.get().getCheckpointKey());
  }

  @Test
  public void testEmptyTimelineAndNullCheckpoint() throws IOException {
    streamerConfig.checkpoint = null;
    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointToResumeFrom(
        Option.empty(), streamerConfig, props, metaClient);
    assertTrue(checkpoint.isEmpty());
  }

  @Test
  public void testTimelineWithCheckpointOverridesConfigCheckpoint() throws IOException {
    String commitTime = "20240120000000";
    Map<String, String> metadata = new HashMap<>();
    metadata.put(HoodieStreamer.CHECKPOINT_KEY, "commit-cp");
    createCommit(commitTime, metadata);

    streamerConfig.checkpoint = "config-cp";

    Option<Checkpoint> checkpoint = StreamerCheckpointUtils.resolveCheckpointToResumeFrom(
        Option.of(metaClient.getActiveTimeline()), streamerConfig, props, metaClient);
    assertEquals("config-cp", checkpoint.get().getCheckpointKey());
  }

  @Test
  public void testAssertNoCheckpointOverrideDuringUpgradeSuccess() throws IOException {
    // Create metaclient with older version
    metaClient =
        HoodieTableMetaClient.newTableBuilder()
            .setDatabaseName("dataset")
            .setTableName("testTable")
            .setTimelineLayoutVersion(TimelineLayoutVersion.VERSION_1)
            .setTableVersion(HoodieTableVersion.SIX)
            .setTableType(HoodieTableType.MERGE_ON_READ)
            .initTable(getDefaultStorageConf(), basePath());
    // No checkpoint override configs set
    streamerConfig.checkpoint = null;
    streamerConfig.ignoreCheckpoint = null;
    
    // Even with auto-upgrade enabled and version mismatch, should pass when no checkpoint override
    props.setProperty(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "true");
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8");

    // Should not throw exception
    StreamerCheckpointUtils.assertNoCheckpointOverrideDuringUpgradeForHoodieIncSource(metaClient, streamerConfig, props);
  }

  @Test
  public void testAssertNoCheckpointOverrideDuringUpgradeFailure() throws IOException {
    metaClient =
        HoodieTableMetaClient.newTableBuilder()
            .setDatabaseName("dataset")
            .setTableName("testTable")
            .setTimelineLayoutVersion(TimelineLayoutVersion.VERSION_1)
            .setTableVersion(HoodieTableVersion.SIX)
            .setTableType(HoodieTableType.MERGE_ON_READ)
            .initTable(getDefaultStorageConf(), basePath());
    
    // Set checkpoint override
    streamerConfig.checkpoint = "test-cp";
    streamerConfig.targetBasePath = "dummyVal";
    streamerConfig.sourceClassName = "org.apache.hudi.utilities.sources.S3EventsHoodieIncrSource";
    // Enable auto-upgrade and set newer write version
    props.setProperty(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "true");
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8");
    // Should throw exception due to checkpoint override during upgrade
    assertThrows(HoodieUpgradeDowngradeException.class, () -> {
      StreamerCheckpointUtils.assertNoCheckpointOverrideDuringUpgradeForHoodieIncSource(metaClient, streamerConfig, props);
    });

    // If not a hoodie incremental source, checkpoint override is allowed during upgrade.
    streamerConfig.sourceClassName = "org.apache.hudi.utilities.sources.NotAHoodieIncrSource";
    streamerConfig.checkpoint = "test-cp";
    StreamerCheckpointUtils.assertNoCheckpointOverrideDuringUpgradeForHoodieIncSource(metaClient, streamerConfig, props);
  }

  @Test
  public void testAssertNoCheckpointOverrideDuringUpgradeWithIgnoreCheckpoint() throws IOException {
    // Create metaclient with older version
    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setDatabaseName("dataset")
        .setTableName("testTable")
        .setTimelineLayoutVersion(TimelineLayoutVersion.VERSION_1)
        .setTableVersion(HoodieTableVersion.SIX)
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .initTable(getDefaultStorageConf(), basePath());
    // Set ignore checkpoint override
    streamerConfig.ignoreCheckpoint = "ignore-cp";
    streamerConfig.targetBasePath = "dummyVal";
    streamerConfig.sourceClassName = "org.apache.hudi.utilities.sources.GcsEventsHoodieIncrSource";

    // Enable auto-upgrade and set newer write version
    props.setProperty(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "true");
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8");

    // Should throw exception due to ignore checkpoint override during upgrade
    assertThrows(HoodieUpgradeDowngradeException.class, () -> {
      StreamerCheckpointUtils.assertNoCheckpointOverrideDuringUpgradeForHoodieIncSource(metaClient, streamerConfig, props);
    });
  }

  @Test
  public void testAssertNoCheckpointOverrideDuringUpgradeWithAutoUpgradeDisabledVersion6() throws IOException {
    // Test case 1: Version 6 table with version 6 write config
    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setDatabaseName("dataset")
        .setTableName("testTable")
        .setTimelineLayoutVersion(TimelineLayoutVersion.VERSION_1)
        .setTableVersion(HoodieTableVersion.SIX)
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .initTable(getDefaultStorageConf(), basePath());
      
    streamerConfig.checkpoint = "test-cp";
    streamerConfig.targetBasePath = "dummyVal";

    // Disable auto-upgrade and set matching version
    props.setProperty(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "6");

    // Should pass since versions match
    StreamerCheckpointUtils.assertNoCheckpointOverrideDuringUpgradeForHoodieIncSource(metaClient, streamerConfig, props);
  }

  @Test
  public void testAssertNoCheckpointOverrideDuringUpgradeWithAutoUpgradeDisabledVersion8() throws IOException {
    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setDatabaseName("dataset")
        .setTableName("testTable")
        .setTimelineLayoutVersion(TimelineLayoutVersion.VERSION_1)
        .setTableVersion(HoodieTableVersion.EIGHT)
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .initTable(getDefaultStorageConf(), basePath());

    streamerConfig.checkpoint = "test-cp";
    streamerConfig.targetBasePath = "dummyVal";

    // Disable auto-upgrade and set matching version 8
    props.setProperty(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8");

    // Should pass since versions match
    StreamerCheckpointUtils.assertNoCheckpointOverrideDuringUpgradeForHoodieIncSource(metaClient, streamerConfig, props);
  }
}
