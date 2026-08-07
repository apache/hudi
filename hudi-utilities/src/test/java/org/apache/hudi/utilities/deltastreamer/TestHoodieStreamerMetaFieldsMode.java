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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage for {@code hoodie.meta.fields.mode} through the HoodieStreamer entrypoint.
 * Each parameterized invocation runs a single ingest cycle in the given {@link MetaFieldsMode} and
 * verifies both the persisted table property and the actual on-disk parquet column population.
 *
 * <p>Rejection paths (unknown token, populate=true+mode, MoR+mode) are exercised in the datasource
 * test {@code TestMetaFieldsMode}; this fixture focuses on the streamer control-flow.
 */
public class TestHoodieStreamerMetaFieldsMode extends HoodieDeltaStreamerTestBase {

  /**
   * Only the selective modes are parameterized here. ALL and NONE add no mode key at all, so they
   * exercise none of the streamer-side plumbing this feature introduced — they are covered by the
   * datasource tests and by {@code TestHoodieTableConfig}'s resolution cases.
   */
  @ParameterizedTest
  @EnumSource(value = MetaFieldsMode.class,
      names = {"COMMIT_TIME_ONLY", "FILE_NAME_ONLY", "COMMIT_TIME_AND_FILE_NAME"})
  public void testStreamerRespectsMetaFieldsMode(MetaFieldsMode mode) throws Exception {
    String tablePath = basePath + "/streamer_meta_fields_mode_" + mode.name();
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    // Force CoW; selective modes are CoW-only until MoR log-write is wired.
    cfg.tableType = "COPY_ON_WRITE";
    // The mode alone — pairing it with populate.meta.fields would now be a stated conflict, since the
    // mode is authoritative and the boolean is only the fallback for resolving an absent one.
    cfg.configs.add(HoodieTableConfig.META_FIELDS_MODE.key() + "=" + mode.name());
    HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(cfg, jsc);
    streamer.getIngestionService().ingestOnce();
    streamer.shutdownGracefully();

    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(context, tablePath);
    assertEquals(mode, metaClient.getTableConfig().getMetaFieldsMode(),
        "streamer must persist mode=" + mode + " on hoodie.properties");
    assertOnDiskMetaColumns(tablePath, mode);
  }

  /**
   * The regression this fixture exists for, and the one cshuo raised in review: a restarted streamer
   * that does not restate the mode must keep writing the table's meta columns.
   *
   * <p>StreamSync builds its write config from {@code props} alone, and the mode is persisted only by
   * {@code initializeEmptyTable}, which runs solely when the base path does not exist. So a second
   * run used to resolve to {@link MetaFieldsMode#NONE} and write base files with a null
   * {@code _hoodie_commit_time} while {@code hoodie.properties} still advertised
   * {@code COMMIT_TIME_ONLY} — incremental queries were then admitted and silently dropped every one
   * of those rows.
   *
   * <p>The rule now lives in {@code BaseHoodieWriteClient#validateAgainstTableProperties} for every
   * engine rather than in StreamSync, so this asserts it end-to-end through the streamer: a run that
   * states neither meta-field property inherits the table's mode.
   */
  @Test
  public void testRestartWithoutRestatingTheModeKeepsWritingCommitTimes() throws Exception {
    String tablePath = basePath + "/streamer_restart_inherits_mode";

    HoodieDeltaStreamer.Config first = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    first.tableType = "COPY_ON_WRITE";
    first.configs.add(HoodieTableConfig.META_FIELDS_MODE.key() + "=" + MetaFieldsMode.COMMIT_TIME_ONLY.name());
    HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(first, jsc);
    streamer.getIngestionService().ingestOnce();
    streamer.shutdownGracefully();

    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY,
        HoodieTestUtils.createMetaClient(context, tablePath).getTableConfig().getMetaFieldsMode());

    // Restart against the existing table stating neither the mode nor the legacy boolean.
    HoodieDeltaStreamer.Config restart = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    restart.tableType = "COPY_ON_WRITE";
    HoodieDeltaStreamer restarted = new HoodieDeltaStreamer(restart, jsc);
    restarted.getIngestionService().ingestOnce();
    restarted.shutdownGracefully();

    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(context, tablePath);
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, metaClient.getTableConfig().getMetaFieldsMode(),
        "the restart must not have changed the table's mode");
    assertTrue(metaClient.getActiveTimeline().filterCompletedInstants().countInstants() >= 2,
        "expected the restart to have produced a second commit");

    // Every row across both commits carries a commit time. A row with a null one is what incremental
    // queries silently drop, so this is the assertion that catches the regression.
    Dataset<Row> raw = sparkSession.read().parquet(tablePath + "/*/*/*/*.parquet");
    assertEquals(0,
        raw.filter(functions.col(HoodieRecord.COMMIT_TIME_METADATA_FIELD).isNull()).count(),
        "no row may have a null _hoodie_commit_time on a COMMIT_TIME_ONLY table");
    assertTrue(raw.select(HoodieRecord.COMMIT_TIME_METADATA_FIELD).distinct().count() >= 2,
        "both commits must be represented, so the second run really did write through this path");
  }

  @Test
  public void testStreamerRejectsMorWithSelectiveMode() throws Exception {
    String tablePath = basePath + "/streamer_mor_selective_rejected";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tablePath, WriteOperationType.BULK_INSERT);
    cfg.tableType = "MERGE_ON_READ";
    cfg.configs.add(HoodieTableConfig.META_FIELDS_MODE.key() + "=" + MetaFieldsMode.COMMIT_TIME_ONLY.name());

    Throwable thrown = assertThrows(Throwable.class, () -> {
      HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(cfg, jsc);
      streamer.getIngestionService().ingestOnce();
      streamer.shutdownGracefully();
    });

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains("COPY_ON_WRITE") || rootMessage.contains("MERGE_ON_READ")
            || rootMessage.contains("MoR") || rootMessage.contains(HoodieTableConfig.META_FIELDS_MODE.key()),
        "Expected MoR-restriction error, got: " + rootMessage);
  }

  private void assertOnDiskMetaColumns(String tablePath, MetaFieldsMode expectedMode) {
    // Default HoodieTestDataGenerator partitions are YYYY/MM/DD (three levels).
    Dataset<Row> raw = sparkSession.read().parquet(tablePath + "/*/*/*/*.parquet");
    Row first = raw.select(
        HoodieRecord.COMMIT_TIME_METADATA_FIELD,
        HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
        HoodieRecord.RECORD_KEY_METADATA_FIELD,
        HoodieRecord.PARTITION_PATH_METADATA_FIELD,
        HoodieRecord.FILENAME_METADATA_FIELD).first();

    if (expectedMode.isCommitTimePopulated()) {
      assertNotNull(first.get(0), "commit_time must be populated in mode " + expectedMode);
    } else {
      assertNull(first.get(0), "commit_time must be null in mode " + expectedMode);
    }
    if (expectedMode.isFileNamePopulated()) {
      assertNotNull(first.get(4), "file_name must be populated in mode " + expectedMode);
    } else {
      assertNull(first.get(4), "file_name must be null in mode " + expectedMode);
    }
    if (expectedMode == MetaFieldsMode.ALL) {
      assertNotNull(first.get(1), "commit_seq_no must be populated in ALL mode");
      assertNotNull(first.get(2), "record_key must be populated in ALL mode");
      assertNotNull(first.get(3), "partition_path must be populated in ALL mode");
    } else {
      assertNull(first.get(1), "commit_seq_no must be null outside ALL mode");
      assertNull(first.get(2), "record_key must be null outside ALL mode");
      assertNull(first.get(3), "partition_path must be null outside ALL mode");
    }
  }

  private static String rootMessageOf(Throwable thrown) {
    Throwable root = thrown;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root.getMessage() == null ? "" : root.getMessage();
  }
}
