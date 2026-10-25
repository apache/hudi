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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase;
import org.apache.hudi.utilities.sources.ParquetDFSSource;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Continues ingestion against a fixture table that was originally written with V1 checkpoint
 * format only and is now being resumed on a Hudi 1.x release with
 * {@code hoodie.write.table.version=6}.
 *
 * <p>Expected: the next commit's {@code extraMetadata} carries the V1 key
 * {@code deltastreamer.checkpoint.key} and NOT the V2 key {@code streamer.checkpoint.key.v2},
 * because non-incremental sources emit V1 regardless of write table version.
 *
 * <p>If a V2 key shows up on the resumed commit, that reproduces the bug reported against
 * {@code hoodie.write.table.version=6}.
 */
public class TestParquetDfsCheckpointFormatOnV6 extends HoodieDeltaStreamerTestBase {

  private static final String FIXTURE_RESOURCE = "checkpoint-v6/parquet-dfs-v1-fixture.zip";

  @Test
  public void resumedV6CommitKeepsV1CheckpointFormat() throws Exception {
    String dirName = "checkpoint-v6-resume-" + System.currentTimeMillis();
    String dataPath = basePath + "/" + dirName;
    Path zipOutput = Paths.get(new URI(dataPath));
    Files.createDirectories(zipOutput);
    HoodieTestUtils.extractZipToDirectory(FIXTURE_RESOURCE, zipOutput, getClass());

    // The fixture table is rooted directly at zipOutput (zip contains .hoodie/, partitions, etc.).
    String tableBasePath = zipOutput.toString();
    assertTrue(Files.exists(zipOutput.resolve(".hoodie/hoodie.properties")),
        "Fixture did not unpack a hudi table at " + tableBasePath);

    // Sanity-check that the fixture's last commit carries a V1 checkpoint and no V2 key.
    HoodieCommitMetadata baselineCommit = readLatestCommitMetadata(tableBasePath);
    assertNotNull(baselineCommit.getMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1),
        "Fixture baseline expected to carry V1 checkpoint key.");
    assertNull(baselineCommit.getMetadata(StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2),
        "Fixture baseline must not have a V2 checkpoint key (was built on master).");

    // Produce a fresh parquet source file under a new root so the streamer has work to do.
    String parquetSourceRoot = basePath + "/" + dirName + "-source";
    prepareParquetDFSFiles(50, parquetSourceRoot, "resume.parquet", false, null, null).close();

    String propsFile = dirName + "-source.properties";
    TypedProperties extraProps = new TypedProperties();
    extraProps.setProperty("hoodie.datasource.write.table.type", HoodieTableType.COPY_ON_WRITE.name());
    prepareParquetDFSSource(false, false, "source.avsc", "target.avsc",
        propsFile, parquetSourceRoot, false, "partition_path", "", extraProps, false, false);

    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        ParquetDFSSource.class.getName(), Collections.emptyList(), propsFile, false, false,
        100_000, false, null, HoodieTableType.COPY_ON_WRITE.name(), "timestamp", null);
    cfg.configs.add(HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "=6");
    new HoodieDeltaStreamer(cfg, jsc).sync();

    // Confirm the on-disk table version stayed at 6 so we are testing the v6-write path, not an
    // accidental v6 -> v9 auto-upgrade (which would make V2 the correct result).
    HoodieTableMetaClient resumedMetaClient = HoodieTestUtils.createMetaClient(storage, tableBasePath);
    assertEquals(6, resumedMetaClient.getTableConfig().getTableVersion().versionCode(),
        "Table version on disk should still be 6 after resume");

    HoodieCommitMetadata resumedCommit = readLatestCommitMetadata(tableBasePath);
    // Under hoodie.write.table.version=6, ParquetDFSSource must keep emitting V1 keys; in the
    // always-V1 design, this holds across every write table version for non-incremental sources.
    assertNotNull(resumedCommit.getMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1),
        "Resumed v6 commit must persist V1 checkpoint key. extraMetadata="
            + resumedCommit.getExtraMetadata());
    assertNull(resumedCommit.getMetadata(StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2),
        "Resumed v6 commit must NOT persist a V2 checkpoint key. extraMetadata="
            + resumedCommit.getExtraMetadata());
  }

  private static HoodieCommitMetadata readLatestCommitMetadata(String tableBasePath) throws IOException {
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storage, tableBasePath);
    HoodieInstant lastInstant = metaClient.getActiveTimeline()
        .getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .orElseThrow(() -> new IllegalStateException("No completed commit found in " + tableBasePath));
    return metaClient.getActiveTimeline().readCommitMetadata(lastInstant);
  }
}
