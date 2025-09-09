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

import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.checkpoint.CheckpointUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase;
import org.apache.hudi.utilities.sources.GcsEventsHoodieIncrSource;
import org.apache.hudi.utilities.sources.MockS3EventsHoodieIncrSource;
import org.apache.hudi.utilities.sources.S3EventsHoodieIncrSource;
import org.apache.hudi.utilities.sources.S3EventsHoodieIncrSourceHarness;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static javolution.testing.TestContext.assertTrue;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_RESET_KEY_V1;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.CHECKPOINT_FORCE_SKIP;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_IGNORE_KEY_IS_NULL;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_KEY_EQ_VAL;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_RESET_KEY_IS_NULL;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_EMPTY_CKP_KEY;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_INPUT_CKP;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_NON_EMPTY_CKP_ALL_MEMBERS;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_EMPTY_ROW_SET_NONE_NULL_CKP_V2_KEY;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_EMPTY_ROW_SET_NULL_CKP_KEY;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_FETCH_NEXT_BATCH;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.RETURN_CHECKPOINT_KEY;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.apache.hudi.utilities.streamer.StreamSync.CHECKPOINT_IGNORE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class TestHoodieIncrSourceE2E extends S3EventsHoodieIncrSourceHarness {

  private String toggleVersion(String version) {
    return "8".equals(version) ? "6" : "8";
  }

  private HoodieDeltaStreamer.Config createConfig(String basePath, String sourceCheckpoint) {
    return createConfig(basePath, sourceCheckpoint, MockS3EventsHoodieIncrSource.class.getName());
  }

  private HoodieDeltaStreamer.Config createConfig(String basePath, String sourceCheckpoint, String sourceClass) {
    HoodieDeltaStreamer.Config cfg = HoodieDeltaStreamerTestBase.TestHelpers.makeConfig(
        basePath,
        WriteOperationType.INSERT,
        sourceClass,
        Collections.emptyList(),
        sourceCheckpoint != null ? DFSPropertiesConfiguration.DEFAULT_PATH.toString() : null,
        false,
        false,
        100000,
        true,
        HoodieAvroPayload.class.getName(),
        null,
        "timestamp",
        sourceCheckpoint);
    cfg.propsFilePath = DFSPropertiesConfiguration.DEFAULT_PATH.toString();
    return cfg;
  }

  private TypedProperties setupBaseProperties(String tableVersion) {
    TypedProperties props = setProps(READ_UPTO_LATEST_COMMIT);
    props.put(WRITE_TABLE_VERSION.key(), tableVersion);
    return props;
  }

  public void verifyLastInstantCommitMetadata(Map<String, String> expectedMetadata) {
    metaClient.reloadActiveTimeline();
    Option<HoodieCommitMetadata> metadata = HoodieClientTestUtils.getCommitMetadataForInstant(
        metaClient, metaClient.getActiveTimeline().lastInstant().get());
    assertFalse(metadata.isEmpty());
    assertEquals(metadata.get().getExtraMetadata(), expectedMetadata);
  }


  /**
   * Tests the end-to-end sync behavior with multiple sync iterations.
   *
   * Test flow:
   * 1. First sync:
   *    - Starts with no checkpoint
   *    - Ingests data until checkpoint "10"
   *    - Verifies commit metadata contains only checkpoint="10"
   *
   * 2. Second sync:
   *    - Uses different table version
   *    - Continues from checkpoint "10" to "20"
   *    - Verifies commit metadata contains checkpoint="20"
   *
   * 3. Third sync:
   *    - Upgrades to table version 8
   *    - Continues from checkpoint "20" to "30"
   *    - Verifies commit metadata still uses checkpoint V1 format
   *    - Verifies final checkpoint="30"
   */
  /*@ParameterizedTest
  /@CsvSource({
      "6, org.apache.hudi.utilities.sources.MockGeneralHoodieIncrSource",
      "8, org.apache.hudi.utilities.sources.MockGeneralHoodieIncrSource"
  })*/
  @Disabled("HUDI-8952")
  public void testSyncE2EWrongCheckpointVersionErrorOut(String tableVersion, String sourceClass) throws Exception {
    // First start with no previous checkpoint and ingest till ckp 1 with table version.
    // Disable auto upgrade and MDT as we want to keep things as it is.
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), tableVersion);
    TypedProperties props = setupBaseProperties(tableVersion);
    // Round 1: ingest start from beginning to checkpoint 10. Return wrong version of checkpoint.
    props.put(OP_FETCH_NEXT_BATCH,
        tableVersion.equals("6") ? OP_EMPTY_ROW_SET_NONE_NULL_CKP_V2_KEY : OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "10");
    // Validating the source input ckp is empty when doing the sync.
    props.put(VAL_INPUT_CKP, VAL_EMPTY_CKP_KEY);
    props.put("hoodie.metadata.enable", "false");
    props.put("hoodie.write.auto.upgrade", "false");
    props.put("hoodie.write.table.version", tableVersion);

    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(createConfig(basePath(), null, sourceClass), jsc, Option.of(props));
    Exception ex = assertThrows(java.lang.IllegalStateException.class, ds::sync);
    // Contain error messages.
    if (tableVersion.equals("8")) {
      assertTrue(ex.getMessage().contains("Data source should return checkpoint version V2."));
    } else {
      assertTrue(ex.getMessage().contains("Data source should return checkpoint version V1."));
    }
  }

  /**
   * Tests the end-to-end sync behavior with multiple sync iterations.
   *
   * Test flow:
   * 1. First sync:
   *    - Starts with no checkpoint
   *    - Ingests data until checkpoint "10"
   *    - Verifies commit metadata contains only checkpoint="10"
   *
   * 2. Second sync:
   *    - Uses different table version
   *    - Continues from checkpoint "10" to "20"
   *    - Verifies commit metadata contains checkpoint="20"
   *
   * 3. Third sync:
   *    - Upgrades to table version 8
   *    - Continues from checkpoint "20" to "30"
   *    - Verifies commit metadata still uses checkpoint V1 format
   *    - Verifies final checkpoint="30"
   */
  @ParameterizedTest
  @CsvSource({
      "6, org.apache.hudi.utilities.sources.MockS3EventsHoodieIncrSource",
      "8, org.apache.hudi.utilities.sources.MockS3EventsHoodieIncrSource",
      "6, org.apache.hudi.utilities.sources.MockGcsEventsHoodieIncrSource",
      "8, org.apache.hudi.utilities.sources.MockGcsEventsHoodieIncrSource"
  })
  public void testSyncE2ENoPrevCkpThenSyncMultipleTimes(String tableVersion, String sourceClass) throws Exception {
    // First start with no previous checkpoint and ingest till ckp 1 with table version.
    // Disable auto upgrade and MDT as we want to keep things as it is.
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), tableVersion);
    TypedProperties props = setupBaseProperties(tableVersion);
    // Round 1: ingest start from beginning to checkpoint 10. No checkpoint override.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "10");
    // Validating the source input ckp is empty when doing the sync.
    props.put(VAL_INPUT_CKP, VAL_EMPTY_CKP_KEY);
    props.put("hoodie.metadata.enable", "false");
    props.put("hoodie.write.auto.upgrade", "false");

    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(createConfig(basePath(), null, sourceClass), jsc, Option.of(props));
    ds.sync();

    Map<String, String> expectedMetadata = new HashMap<>();
    // Confirm the resulting checkpoint is 10 and no other metadata.
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "10");
    verifyLastInstantCommitMetadata(expectedMetadata);

    // Then resume from ckp 1 and ingest till ckp 2 with table version Y.
    // Disable auto upgrade and MDT as we want to keep things as it is.
    props = setupBaseProperties(toggleVersion(tableVersion));
    props.put("hoodie.metadata.enable", "false");
    // Dummy behavior injection to return ckp 2.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "20");
    props.put("hoodie.write.auto.upgrade", "false");
    // Validate the given checkpoint is ckp 1 when doing the sync.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(VAL_CKP_KEY_EQ_VAL, "10");
    props.put(VAL_CKP_RESET_KEY_IS_NULL, "IGNORED");
    props.put(VAL_CKP_IGNORE_KEY_IS_NULL, "IGNORED");

    ds = new HoodieDeltaStreamer(createConfig(basePath(), null, sourceClass), jsc, Option.of(props));
    ds.sync();

    // We do not allow table version 8 and ingest with version 6 delta streamer. But not table with version 6
    // and delta streamer with version 8.
    expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "20");
    verifyLastInstantCommitMetadata(expectedMetadata);

    // In the third round, disable MDT and auto upgrade, use table write version 8. The result is
    // no upgrade will happen.
    props = setupBaseProperties("8");
    props.put("hoodie.metadata.enable", "false");
    // Dummy behavior injection to return ckp 1.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "30");
    props.put("hoodie.write.auto.upgrade", "false");
    // Validate the given checkpoint is ckp 2 when doing the sync.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(VAL_CKP_KEY_EQ_VAL, "20");
    props.put(VAL_CKP_RESET_KEY_IS_NULL, "IGNORED");
    props.put(VAL_CKP_IGNORE_KEY_IS_NULL, "IGNORED");

    ds = new HoodieDeltaStreamer(createConfig(basePath(), null, sourceClass), jsc, Option.of(props));
    ds.sync();

    // Assert no upgrade happens.
    if (tableVersion.equals("6")) {
      metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), "6");
      assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
      assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_1, metaClient.getTableConfig().getTimelineLayoutVersion().get());
    }

    // After that, we still use checkpoint V1 since this is s3/Gcs incremental source.
    expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "30");
    verifyLastInstantCommitMetadata(expectedMetadata);

    // In the forth round, enable auto upgrade, use table write version 8, the upgrade is successful.
    props = setupBaseProperties("8");
    props.put("hoodie.metadata.enable", "false");
    // Dummy behavior injection to return ckp 1.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "40");
    props.put("hoodie.write.auto.upgrade", "true");
    // Validate the given checkpoint is ckp 2 when doing the sync.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(VAL_CKP_KEY_EQ_VAL, "30");
    props.put(VAL_CKP_RESET_KEY_IS_NULL, "IGNORED");
    props.put(VAL_CKP_IGNORE_KEY_IS_NULL, "IGNORED");

    ds = new HoodieDeltaStreamer(createConfig(basePath(), null, sourceClass), jsc, Option.of(props));
    ds.sync();

    // After upgrading, we still use checkpoint V1 since this is s3/Gcs incremental source.
    // Assert the table is upgraded.
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), "8");
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig().getTableVersion());
    assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_2, metaClient.getTableConfig().getTimelineLayoutVersion().get());
    expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "40");
    verifyLastInstantCommitMetadata(expectedMetadata);
  }

  /**
   * Tests sync behavior with checkpoint override configurations.
   *
   * Test flow:
   * 1. Initial sync with override:
   *    - Starts with no previous commits but config forces start at "10"
   *    - Ingests until checkpoint "30"
   *    - Verifies metadata contains checkpoint="30" and reset_key="10"
   *
   * 2. Second sync with same override:
   *    - Continues from "30" to "40"
   *    - Verifies reset_key="10" is maintained
   *
   * 3. Third sync without override:
   *    - Continues from "40" to "50"
   *    - Verifies reset_key is removed from metadata
   *
   * 4. Fourth sync with ignore checkpoint:
   *    - Sets ignore_checkpoint="50"
   *    - Ingests to "60"
   *    - Verifies metadata contains ignore_key="50"
   *
   * 5. Final syncs:
   *    - Tests that ignore_key persists with same config
   *    - Verifies ignore_key is removed when config is lifted
   */
  @ParameterizedTest
  @ValueSource(strings = {"6", "8"})
  public void testSyncE2ENoPrevCkpWithCkpOverride(String tableVersion) throws Exception {
    // The streamer started clean with no previous commit metadata, now streamer config forcefully
    // set it to start at ckp "10" and in that iteration the streamer stops at ckp "30
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), tableVersion);
    TypedProperties props = setupBaseProperties(tableVersion);
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(RETURN_CHECKPOINT_KEY, "30"); // The data source said it stops at "30".
    props.put(VAL_CKP_KEY_EQ_VAL, "10"); // Ensure the data source is notified we should start at "10"

    HoodieDeltaStreamer.Config cfg = createConfig(basePath(), "10");
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    ds.sync();

    Map<String, String> expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "30");
    expectedMetadata.put(STREAMER_CHECKPOINT_RESET_KEY_V1, "10");
    verifyLastInstantCommitMetadata(expectedMetadata);

    // Set dummy data source behavior
    props.put(RETURN_CHECKPOINT_KEY, "40"); // The data source said it stops at "40".
    props.put(VAL_CKP_KEY_EQ_VAL, "30"); // Ensure the data source is notified we should start at "10".
    // With the same config that contains the checkpoint override, it does not affect ds to proceed normally.
    ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    ds.sync();

    expectedMetadata.clear();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "40");
    expectedMetadata.put(STREAMER_CHECKPOINT_RESET_KEY_V1, "10");
    verifyLastInstantCommitMetadata(expectedMetadata);

    // Later if we lift the override config from props, the reset key info is also gone in the resulting
    // commit metadata.
    props.put(VAL_CKP_KEY_EQ_VAL, "40"); // Ensure the data source is notified we should start at "40".
    props.put(RETURN_CHECKPOINT_KEY, "50");
    ds = new HoodieDeltaStreamer(createConfig(basePath(), null), jsc, Option.of(props));
    ds.sync();

    expectedMetadata.clear();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "50"); // no more reset key.
    verifyLastInstantCommitMetadata(expectedMetadata);

    // Check ignore key config validation
    // Later if we lift the override config from props, the reset key info is also gone in the resulting
    // commit metadata.
    props.put(VAL_INPUT_CKP, VAL_EMPTY_CKP_KEY); // Ensure the data source is notified we should start at "40".
    props.put(RETURN_CHECKPOINT_KEY, "60");
    cfg = createConfig(basePath(), null);
    cfg.ignoreCheckpoint = "50"; // Set the ckp to be the same as where we stopped at in the last iteration.
    ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    ds.sync();

    expectedMetadata.clear();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "60");
    expectedMetadata.put(CHECKPOINT_IGNORE_KEY, "50");
    verifyLastInstantCommitMetadata(expectedMetadata);

    // In the next iteration, using the same config with ignore key setting does not lead to checkpoint reset.
    props = setupBaseProperties(tableVersion);
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "70"); // Stop at 70 after ingestion.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(VAL_CKP_KEY_EQ_VAL, "60"); // Ensure the data source is notified we should start at "60".
    ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    ds.sync();

    expectedMetadata.clear();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "70");
    expectedMetadata.put(CHECKPOINT_IGNORE_KEY, "50");
    verifyLastInstantCommitMetadata(expectedMetadata);

    // Once we lift the ignore key in the given streamer config, the resulting commit metadata also won't
    // contain that info.
    cfg.ignoreCheckpoint = null;
    props = setupBaseProperties(tableVersion);
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "80"); // Stop at 70 after ingestion.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(VAL_CKP_KEY_EQ_VAL, "70"); // Ensure the data source is notified we should start at "60".
    ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    ds.sync();

    expectedMetadata.clear();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "80");
    verifyLastInstantCommitMetadata(expectedMetadata);
  }

  /**
   * Tests sync behavior when source returns null checkpoint and no previous checkpoint exists.
   *
   * Expected behavior:
   * - Sync completes successfully
   * - Commit metadata contains only schema="" with no checkpoint information
   * - Requires allowCommitOnNoCheckpointChange=true to permit commit
   */
  @ParameterizedTest
  @ValueSource(strings = {"6", "8"})
  public void testSyncE2ENoNextCkpNoPrevCkp(String tableVersion) throws Exception {
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), tableVersion);
    TypedProperties props = setupBaseProperties(tableVersion);
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NULL_CKP_KEY);
    props.put(VAL_INPUT_CKP, VAL_EMPTY_CKP_KEY);

    HoodieDeltaStreamer.Config cfg = createConfig(basePath(), null);
    cfg.allowCommitOnNoCheckpointChange = true;
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    ds.sync();

    Map<String, String> expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    verifyLastInstantCommitMetadata(expectedMetadata);
  }

  /**
   * Tests sync behavior when source returns null checkpoint but has previous checkpoint.
   *
   * Expected behavior:
   * - Sync completes with previous checkpoint "previousCkp"
   * - Commit metadata contains:
   *   - schema=""
   *   - checkpoint_reset_key="previousCkp"
   * - Requires allowCommitOnNoCheckpointChange=true to permit commit
   */
  @ParameterizedTest
  @ValueSource(strings = {"6", "8"})
  public void testSyncE2ENoNextCkpHasPrevCkp(String tableVersion) throws Exception {
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), tableVersion);
    String previousCkp = "previousCkp";
    TypedProperties props = setupBaseProperties(tableVersion);
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(VAL_CKP_KEY_EQ_VAL, previousCkp);
    props.put(VAL_CKP_RESET_KEY_IS_NULL, "");
    props.put(VAL_CKP_IGNORE_KEY_IS_NULL, "");
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NULL_CKP_KEY);

    HoodieDeltaStreamer.Config cfg = createConfig(basePath(), previousCkp);
    cfg.allowCommitOnNoCheckpointChange = true;
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    ds.sync();

    Map<String, String> expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_RESET_KEY_V1, previousCkp);
    verifyLastInstantCommitMetadata(expectedMetadata);
  }

  /**
   * Tests sync behavior with force skip checkpoint enabled.
   *
   * Expected behavior:
   * - Sync completes without processing any checkpoints
   * - Commit metadata contains only schema=""
   * - No checkpoint information is stored regardless of source checkpoint
   */
  @ParameterizedTest
  @ValueSource(strings = {"6", "8"})
  public void testSyncE2EForceSkip(String tableVersion) throws Exception {
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), tableVersion);
    TypedProperties props = setupBaseProperties(tableVersion);
    props.put(CHECKPOINT_FORCE_SKIP.key(), "true");
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(VAL_INPUT_CKP, VAL_EMPTY_CKP_KEY);

    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(createConfig(basePath(), null), jsc, Option.of(props));
    ds.sync();

    Map<String, String> expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    verifyLastInstantCommitMetadata(expectedMetadata);
  }

  /**
   * Tests checkpoint version selection for S3 and GCS sources.
   *
   * Expected behavior:
   * - Both S3EventsHoodieIncrSource and GcsEventsHoodieIncrSource must use checkpoint V1
   * - This remains true for both table version 6 and 8
   * - shouldTargetCheckpointV2() returns false in all these cases
   *
   * This ensures these sources maintain backward compatibility with checkpoint V1 format
   */
  @Test
  public void testTargetCheckpointV2ForS3Gcs() {
    // To ensure we properly track sources that must use checkpoint V1.
    assertFalse(CheckpointUtils.shouldTargetCheckpointV2(8, S3EventsHoodieIncrSource.class.getName()));
    assertFalse(CheckpointUtils.shouldTargetCheckpointV2(6, S3EventsHoodieIncrSource.class.getName()));
    assertFalse(CheckpointUtils.shouldTargetCheckpointV2(8, GcsEventsHoodieIncrSource.class.getName()));
    assertFalse(CheckpointUtils.shouldTargetCheckpointV2(6, GcsEventsHoodieIncrSource.class.getName()));
  }
}