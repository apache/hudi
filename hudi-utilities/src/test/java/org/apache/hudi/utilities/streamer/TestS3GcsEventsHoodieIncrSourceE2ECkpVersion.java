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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.checkpoint.CheckpointUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase;
import org.apache.hudi.utilities.sources.GcsEventsHoodieIncrSource;
import org.apache.hudi.utilities.sources.S3EventsHoodieIncrSource;
import org.apache.hudi.utilities.sources.S3EventsHoodieIncrSourceHarness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_RESET_KEY_V1;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.CHECKPOINT_FORCE_SKIP;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_IGNORE_KEY_IS_NULL;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_KEY_EQ_VAL;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_CKP_RESET_KEY_IS_NULL;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_EMPTY_CKP_KEY;
import static org.apache.hudi.utilities.sources.CheckpointValidator.VAL_NON_EMPTY_CKP_ALL_MEMBERS;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.CUSTOM_CHECKPOINT1;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.CUSTOM_CHECKPOINT2;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_EMPTY_ROW_SET_NONE_NULL_CKP1_KEY;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_EMPTY_ROW_SET_NONE_NULL_CKP2_KEY;
import static org.apache.hudi.utilities.sources.DummyOperationExecutor.OP_EMPTY_ROW_SET_NULL_CKP_KEY;
import static org.apache.hudi.utilities.sources.MockS3EventsHoodieIncrSource.OP_FETCH_NEXT_BATCH;
import static org.apache.hudi.utilities.sources.MockS3EventsHoodieIncrSource.VAL_INPUT_CKP;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(MockitoExtension.class)
public class TestS3GcsEventsHoodieIncrSourceE2ECkpVersion extends S3EventsHoodieIncrSourceHarness {

  private String toggleVersion(String version) {
    return "8".equals(version) ? "6" : "8";
  }

  private HoodieDeltaStreamer.Config createConfig(String basePath, String sourceCheckpoint) {
    HoodieDeltaStreamer.Config cfg = HoodieDeltaStreamerTestBase.TestHelpers.makeConfig(
        basePath,
        WriteOperationType.INSERT,
        "org.apache.hudi.utilities.sources.MockS3EventsHoodieIncrSource",
        Collections.emptyList(),
        sourceCheckpoint != null ? DFSPropertiesConfiguration.DEFAULT_PATH.toString() : null,
        false,
        false,
        100000,
        false,
        null,
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

  @ParameterizedTest
  @ValueSource(strings = {"6", "8"})
  public void testSyncE2ENoPrevCkpThenSyncTwice(String tableVersion) throws Exception {
    // First start with no previous checkpoint and ingest till ckp 1 with table version.
    // Disable auto upgrade and MDT as we want to keep things as it is.
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), tableVersion);
    TypedProperties props = setupBaseProperties(tableVersion);
    // Dummy behavior injection to return ckp 1.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP1_KEY);
    // Validating the source input ckp is empty when doing the sync.
    props.put(VAL_INPUT_CKP, VAL_EMPTY_CKP_KEY);
    props.put("hoodie.metadata.enable", "false");
    props.put("hoodie.write.auto.upgrade", "false");

    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(createConfig(basePath(), null), jsc, Option.of(props));
    ds.sync();

    Map<String, String> expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, CUSTOM_CHECKPOINT1);
    verifyLastInstantCommitMetadata(expectedMetadata);

    // Then resume from ckp 1 and ingest till ckp 2 with table version Y.
    // Disable auto upgrade and MDT as we want to keep things as it is.
    props = setupBaseProperties(toggleVersion(tableVersion));
    props.put("hoodie.metadata.enable", "false");
    // Dummy behavior injection to return ckp 2.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP2_KEY);
    props.put("hoodie.write.auto.upgrade", "false");
    // Validate the given checkpoint is ckp 1 when doing the sync.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(VAL_CKP_KEY_EQ_VAL, CUSTOM_CHECKPOINT1);
    props.put(VAL_CKP_RESET_KEY_IS_NULL, "IGNORED");
    props.put(VAL_CKP_IGNORE_KEY_IS_NULL, "IGNORED");

    ds = new HoodieDeltaStreamer(createConfig(basePath(), CUSTOM_CHECKPOINT1), jsc, Option.of(props));
    ds.sync();

    // We do not allow table version 8 and ingest with version 6 delta streamer. But not table with version 6
    // and delta streamer with version 8.
    expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, CUSTOM_CHECKPOINT2);
    expectedMetadata.put(STREAMER_CHECKPOINT_RESET_KEY_V1, CUSTOM_CHECKPOINT1);
    verifyLastInstantCommitMetadata(expectedMetadata);

    // In the third round, enable MDT and auto upgrade, use table version 8
    props = setupBaseProperties("8");
    props.put("hoodie.metadata.enable", "false");
    // Dummy behavior injection to return ckp 2.
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP1_KEY);
    props.put("hoodie.write.auto.upgrade", "false");
    // Validate the given checkpoint is ckp 2 when doing the sync.
    props.put(VAL_INPUT_CKP, VAL_NON_EMPTY_CKP_ALL_MEMBERS);
    props.put(VAL_CKP_KEY_EQ_VAL, CUSTOM_CHECKPOINT2);
    props.put(VAL_CKP_RESET_KEY_IS_NULL, "IGNORED");
    props.put(VAL_CKP_IGNORE_KEY_IS_NULL, "IGNORED");

    ds = new HoodieDeltaStreamer(createConfig(basePath(), CUSTOM_CHECKPOINT2), jsc, Option.of(props));
    ds.sync();

    // After upgrading, we still use checkpoint V1 since this is s3/Gcs incremental source.
    expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    expectedMetadata.put(STREAMER_CHECKPOINT_KEY_V1, CUSTOM_CHECKPOINT1);
    expectedMetadata.put(STREAMER_CHECKPOINT_RESET_KEY_V1, CUSTOM_CHECKPOINT2);
    verifyLastInstantCommitMetadata(expectedMetadata);
  }

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

  @ParameterizedTest
  @ValueSource(strings = {"6", "8"})
  public void testSyncE2EForceSkip(String tableVersion) throws Exception {
    metaClient = getHoodieMetaClientWithTableVersion(storageConf(), basePath(), tableVersion);
    TypedProperties props = setupBaseProperties(tableVersion);
    props.put(CHECKPOINT_FORCE_SKIP.key(), "true");
    props.put(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP1_KEY);
    props.put(VAL_INPUT_CKP, VAL_EMPTY_CKP_KEY);

    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(createConfig(basePath(), null), jsc, Option.of(props));
    ds.sync();

    Map<String, String> expectedMetadata = new HashMap<>();
    expectedMetadata.put("schema", "");
    verifyLastInstantCommitMetadata(expectedMetadata);
  }

  @Test
  public void testTargetCheckpointV2ForS3Gcs() {
    // To ensure we properly track sources that must use checkpoint V1.
    assertFalse(CheckpointUtils.targetCheckpointV2(8, S3EventsHoodieIncrSource.class.getName()));
    assertFalse(CheckpointUtils.targetCheckpointV2(6, S3EventsHoodieIncrSource.class.getName()));
    assertFalse(CheckpointUtils.targetCheckpointV2(8, GcsEventsHoodieIncrSource.class.getName()));
    assertFalse(CheckpointUtils.targetCheckpointV2(6, GcsEventsHoodieIncrSource.class.getName()));
  }
}