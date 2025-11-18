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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase;
import org.apache.hudi.utilities.sources.MockGeneralHoodieIncrSource;
import org.apache.hudi.utilities.sources.S3EventsHoodieIncrSourceHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.table.checkpoint.HoodieIncrSourceCheckpointValUtils.REQUEST_TIME_PREFIX;
import static org.apache.hudi.common.table.checkpoint.HoodieIncrSourceCheckpointValUtils.RESET_CHECKPOINT_V2_SEPARATOR;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_RESET_KEY_V2;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_IGNORE_KEY_IS_NULL;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_INSTANCE_OF;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_KEY_EQ_VAL;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_RESET_KEY_EQUALS;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_RESET_KEY_IS_NULL;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_EMPTY_CKP_KEY;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_INPUT_CKP;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_NON_EMPTY_CKP_ALL_MEMBERS;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_NO_INGESTION_HAPPENS;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_EMPTY_ROW_SET_NONE_NULL_CKP_V2_KEY;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_FETCH_NEXT_BATCH;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.RETURN_CHECKPOINT_KEY;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestHoodieIncrSourceE2EAutoUpgrade extends S3EventsHoodieIncrSourceHarness {

  private String schemaStr;

  @BeforeEach
  void setup() throws IOException {
    super.setUp();
    schemaStr = SchemaTestUtil.getSimpleSchema().toString();
  }

  @Override
  public HoodieTableMetaClient getHoodieMetaClient(StorageConfiguration<?> storageConf, String basePath, Properties props) throws IOException {
    return HoodieTableMetaClient.newTableBuilder()
        .setTableName(RAW_TRIPS_TEST_NAME)
        .setTableType(COPY_ON_WRITE)
        .setPayloadClass(HoodieAvroPayload.class)
        .setTableVersion(ConfigUtils.getIntWithAltKeys(props, WRITE_TABLE_VERSION))
        .setTableCreateSchema(schemaStr)
        .fromProperties(props)
        .initTable(storageConf.newInstance(), basePath);
  }

  private String toggleVersion(String version) {
    return "8".equals(version) ? "6" : "8";
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
    assertEquals(expectedMetadata, metadata.get().getExtraMetadata());
  }

  /**
   * Tests the end-to-end sync behavior with multiple sync iterations.
   */
  @Test
  public void testSyncE2ENoPrevCkpThenSyncMultipleTimes() throws Exception {
    String sourceClass = MockGeneralHoodieIncrSource.class.getName();
    // ========== First start with no previous checkpoint and ingest till ckp 10 with table version. ============
    // table version 6, write version 6 without checkpoint override.
    // start from checkpoint v1 null stop at checkpoint v1 10.

    // Disable auto upgrade and MDT as we want to keep things as it is.
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), "6");
    TypedProperties props = setupBaseProperties("6");
    // Round 1: ingest start from beginning to checkpoint V1 10. No checkpoint override.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "10");
    // Validating the source input ckp is empty when doing the sync.
    props.put(VAL_INPUT_CKP, VAL_EMPTY_CKP_KEY);
    props.put("hoodie.metadata.enable", "false");
    props.put("hoodie.write.auto.upgrade", "false");

    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(createConfig(basePath(), null, sourceClass), jsc, Option.of(props));
    ds.sync();
    metaClient.reloadActiveTimeline();

    Map<String, String> expectedMetadata = new HashMap<>();
    // Confirm the resulting checkpoint is 10 and no other metadata.
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, "10");
    verifyLastInstantCommitMetadata(expectedMetadata);
    assertEquals(HoodieTableVersion.SIX.versionCode(), metaClient.getTableConfig().getTableVersion().versionCode());
    assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_1.getVersion(), metaClient.getTableConfig().getTimelineLayoutVersion().get().getVersion());

    // Save the last instant for later validation.
    HoodieInstant instantAfterFirstRound = metaClient.getActiveTimeline().lastInstant().get();

    //  ========== Verify we don't allow checkpoint config when upgrading. ==========
    // table version 6, write version 6 without checkpoint override.
    // start from checkpoint v1 null, no successful ingestion.

    // Then resume from ckp 1 and ingest till ckp V2 20 with table version 8. Expect exception thrown and nothing is ingested.
    props = setupBaseProperties("8");
    // In the ingestion iteration that does upgrade, we should allow MDT on at the same time.
    props.put("hoodie.metadata.enable", "true");
    // Dummy behavior injection to return ckp 2.
    props.put("hoodie.write.auto.upgrade", "true");
    // Validate no ingestion happens.
    props.put(VAL_INPUT_CKP, VAL_NO_INGESTION_HAPPENS);

    HoodieDeltaStreamer.Config cfg = createConfig(basePath(), null, sourceClass);
    cfg.checkpoint = "overrideWhenAutoUpgradingWouldFail";
    ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    Exception ex = assertThrows(HoodieUpgradeDowngradeException.class, ds::sync);
    assertTrue(ex.getMessage().contains("When upgrade/downgrade is happening, please avoid setting --checkpoint option and --ignore-checkpoint for your delta streamers."));
    // No changes to the timeline / table config.
    metaClient.reloadActiveTimeline();
    assertEquals(metaClient.getActiveTimeline().lastInstant().get(), instantAfterFirstRound);
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
    assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_1, metaClient.getTableConfig().getTimelineLayoutVersion().get());

    //  ========== If no checkpoint override, upgrade would go through ==========
    // table version 6, write version 8 without checkpoint override.
    // start from checkpoint v1 10 stop at checkpoint v2 20.
    props = setupBaseProperties("8");
    props.put("hoodie.metadata.enable", "false"); // follow up with MDT enabled
    // In this iteration, we expect to resume from the previous checkpoint "10". The data source would get a v1 checkpoint as
    // for all hoodie incremental source the checkpoint translation step is a no-op.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(VAL_CKP_INSTANCE_OF, StreamerCheckpointV1.class.getName());
    props.put(VAL_CKP_KEY_EQ_VAL, "10");
    props.put(VAL_CKP_RESET_KEY_IS_NULL, "IGNORED");
    props.put(VAL_CKP_IGNORE_KEY_IS_NULL, "IGNORED");
    // Dummy behavior injection to return ckp v2 20.
    // Expect to get previous checkpoint v1 but it is directly translated to v2
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V2_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "20");
    props.put("hoodie.write.auto.upgrade", "true");

    ds = new HoodieDeltaStreamer(createConfig(basePath(), null, sourceClass), jsc, Option.of(props));
    ds.sync();
    // After the sync the table is upgraded to version 8.
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), "8");
    // Confirm the resulting checkpoint is 10 and no other metadata.
    expectedMetadata.clear();
    expectedMetadata.put("schema", schemaStr);
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V2, "20");
    verifyLastInstantCommitMetadata(expectedMetadata);
    // Table is upgraded
    metaClient.reloadActiveTimeline();
    assertNotEquals(metaClient.getActiveTimeline().lastInstant().get(), instantAfterFirstRound);
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig().getTableVersion());
    assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_2, metaClient.getTableConfig().getTimelineLayoutVersion().get());

    //  ========== Checkpoint override with new format <eventOrderingMode>:<checkpoint> ==========
    props = setupBaseProperties("8");
    props.put("hoodie.metadata.enable", "true");
    // If user wants to do request time based checkpoint override.
    cfg = createConfig(basePath(), REQUEST_TIME_PREFIX + RESET_CHECKPOINT_V2_SEPARATOR + "30", sourceClass);
    // In this iteration, we override the checkpoint instead of resuming from ckp 20. Covers 2 modes: Event time based and
    // commit time based.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    // Data source will know this checkpoint is configured by the override, and it is translated to
    // checkpoint v1 in translate checkpoint step.
    props.put(VAL_CKP_INSTANCE_OF, StreamerCheckpointV1.class.getName());
    // The data source will only see this is a v1 checkpoint (request time based) with offset 30 to resume.
    props.put(VAL_CKP_KEY_EQ_VAL, "30");
    props.put(VAL_CKP_RESET_KEY_IS_NULL, "IGNORED");
    props.put(VAL_CKP_IGNORE_KEY_IS_NULL, "IGNORED");
    // Dummy behavior is consuming the given v1 checkpoint and always return a v2 checkpoint. We only support resetting checkpoint
    // based on request time, but after that it would be translated to completion time and everything after that is completion
    // time based. We are not following request time ordering anymore in version 8.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V2_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "40");
    props.put("hoodie.write.auto.upgrade", "true");

    ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    ds.sync();
    // After the sync the table is upgraded to version 8.
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), "8");
    // Confirm the resulting commit metadata contains the reset ckp and 30
    expectedMetadata.clear();
    expectedMetadata.put("schema", schemaStr);
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V2, "40"); // V2 means completion time based.
    expectedMetadata.put(STREAMER_CHECKPOINT_RESET_KEY_V2, REQUEST_TIME_PREFIX + RESET_CHECKPOINT_V2_SEPARATOR + "30");
    verifyLastInstantCommitMetadata(expectedMetadata);
    // Table is upgraded
    metaClient.reloadActiveTimeline();
    assertNotEquals(metaClient.getActiveTimeline().lastInstant().get(), instantAfterFirstRound);
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig().getTableVersion());
    assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_2, metaClient.getTableConfig().getTimelineLayoutVersion().get());

    // ============ Reuse the same writ config with <eventOrderingMode>:<checkpoint> ==========
    // table version 8, write version 8 without request time based checkpoint override.
    // start from checkpoint v2 40 stop at checkpoint v2 50.

    // after resetting the checkpoint, reusing the same configure should not lead to checkpoint reset.
    props = setupBaseProperties("8");
    props.put("hoodie.metadata.enable", "true");
    // If user wants to do request time based checkpoint override.
    // In this iteration, we override the checkpoint instead of resuming from ckp 20. Covers 2 modes: Event time based and
    // commit time based.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    // As normal iteration does, it should use v8 ckp for version 8.
    props.put(VAL_CKP_INSTANCE_OF, StreamerCheckpointV2.class.getName());
    // The data source now starts at what the previous iteration stops, no override.
    props.put(VAL_CKP_KEY_EQ_VAL, "40");
    props.put(VAL_CKP_RESET_KEY_EQUALS, REQUEST_TIME_PREFIX + RESET_CHECKPOINT_V2_SEPARATOR + "30");
    props.put(VAL_CKP_IGNORE_KEY_IS_NULL, "IGNORED");
    // Dummy behavior is consuming the given v1 checkpoint and always return a v2 checkpoint. We only support resetting checkpoint
    // based on request time, but after that it would be translated to completion time and everything after that is completion
    // time based. We are not following request time ordering anymore in version 8.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V2_KEY);
    props.put(RETURN_CHECKPOINT_KEY, "50");
    props.put("hoodie.write.auto.upgrade", "true");

    ds = new HoodieDeltaStreamer(cfg, jsc, Option.of(props));
    ds.sync();
    // After the sync the table is upgraded to version 8.
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), "8");
    // Confirm the checkpoint commit metadata.
    expectedMetadata.clear();
    expectedMetadata.put("schema", schemaStr);
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V2, "50");
    expectedMetadata.put(STREAMER_CHECKPOINT_RESET_KEY_V2, REQUEST_TIME_PREFIX + RESET_CHECKPOINT_V2_SEPARATOR + "30");
    verifyLastInstantCommitMetadata(expectedMetadata);
    // Table is still version 8
    metaClient.reloadActiveTimeline();
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig().getTableVersion());
    assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_2, metaClient.getTableConfig().getTimelineLayoutVersion().get());
  }
}