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
   * The regression this fixture exists for: a restarted streamer
   * that does not restate the mode must not silently write the wrong meta columns.
   *
   * <p>StreamSync builds its write config from {@code props} alone, and the mode is persisted only by
   * {@code initializeEmptyTable}, which runs solely when the base path does not exist. So a second run
   * used to resolve to {@link MetaFieldsMode#NONE} and write base files with a null
   * {@code _hoodie_commit_time} while {@code hoodie.properties} still advertised
   * {@code COMMIT_TIME_ONLY} — incremental queries were then admitted and silently dropped every one
   * of those rows.
   *
   * <p>A selective table now requires the writer to state the mode, so the restart is <b>rejected</b>
   * rather than inheriting. Inheritance would work here, but it cannot be relied on everywhere: the
   * write path reads the mode in factories and handles that hold no table config at all, so the write
   * config has to be right on its own. Failing loudly at init is what makes that guarantee real —
   * and a rejected run leaves the table exactly as it was, which silent narrowing did not.
   * See {@code testRestartRestatingTheModeSucceeds} for the migration path.
   */
  @Test
  public void testRestartWithoutRestatingTheModeIsRejected() throws Exception {
    String tablePath = basePath + "/streamer_restart_inherits_mode";

    HoodieDeltaStreamer.Config first = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    first.tableType = "COPY_ON_WRITE";
    first.configs.add(HoodieTableConfig.META_FIELDS_MODE.key() + "=" + MetaFieldsMode.COMMIT_TIME_ONLY.name());
    HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(first, jsc);
    streamer.getIngestionService().ingestOnce();
    streamer.shutdownGracefully();

    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY,
        HoodieTestUtils.createMetaClient(context, tablePath).getTableConfig().getMetaFieldsMode());

    long commitsAfterFirstRun = HoodieTestUtils.createMetaClient(context, tablePath)
        .getActiveTimeline().filterCompletedInstants().countInstants();

    // Restart against the existing table stating neither the mode nor the legacy boolean.
    HoodieDeltaStreamer.Config restart = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    restart.tableType = "COPY_ON_WRITE";

    Throwable thrown = assertThrows(Throwable.class, () -> {
      HoodieDeltaStreamer restarted = new HoodieDeltaStreamer(restart, jsc);
      restarted.getIngestionService().ingestOnce();
      restarted.shutdownGracefully();
    });

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains(HoodieTableConfig.META_FIELDS_MODE.key()),
        "expected the writer to be told to state the mode, got: " + rootMessage);

    // The rejected run must have left the table untouched -- no mode change, no extra commit, and no
    // base file carrying a null commit time. That last one is the actual data loss being prevented:
    // such rows are admitted by incremental queries and then silently dropped.
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(context, tablePath);
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, metaClient.getTableConfig().getMetaFieldsMode(),
        "a rejected restart must leave the table's mode untouched");
    assertEquals(commitsAfterFirstRun,
        metaClient.getActiveTimeline().filterCompletedInstants().countInstants(),
        "a rejected restart must not have committed anything");
    Dataset<Row> raw = sparkSession.read().parquet(tablePath + "/*/*/*/*.parquet");
    assertEquals(0,
        raw.filter(functions.col(HoodieRecord.COMMIT_TIME_METADATA_FIELD).isNull()).count(),
        "no row may have a null _hoodie_commit_time on a COMMIT_TIME_ONLY table");
  }

  /**
   * The migration path for the case above: a restart that restates the table's mode proceeds normally.
   *
   * <p>This is what every writer against a selective table must do, and it is the assertion that keeps
   * the requirement from being a dead end -- the mode is stateable, and stating it is enough.
   */
  @Test
  public void testRestartRestatingTheModeSucceeds() throws Exception {
    String tablePath = basePath + "/streamer_restart_restates_mode";

    HoodieDeltaStreamer.Config first = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    first.tableType = "COPY_ON_WRITE";
    first.configs.add(HoodieTableConfig.META_FIELDS_MODE.key() + "=" + MetaFieldsMode.COMMIT_TIME_ONLY.name());
    HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(first, jsc);
    streamer.getIngestionService().ingestOnce();
    streamer.shutdownGracefully();

    // Restart restating the same mode.
    HoodieDeltaStreamer.Config restart = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    restart.tableType = "COPY_ON_WRITE";
    restart.configs.add(HoodieTableConfig.META_FIELDS_MODE.key() + "=" + MetaFieldsMode.COMMIT_TIME_ONLY.name());
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

  /**
   * The variant where the restart states the deprecated boolean rather than nothing at all.
   *
   * <p>Both cases are rejected against a selective table, for the same reason: the writer has not
   * stated the mode the table is on. Stating the boolean instead makes the disagreement explicit
   * rather than merely unstated, but the outcome is the same. Before this rule it narrowed silently,
   * writing base files with a null {@code _hoodie_commit_time} into a table that still advertised the
   * mode.
   */
  @Test
  public void testRestartStatingTheLegacyBooleanIsRejected() throws Exception {
    String tablePath = basePath + "/streamer_restart_legacy_boolean_conflict";

    HoodieDeltaStreamer.Config first = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    first.tableType = "COPY_ON_WRITE";
    first.configs.add(HoodieTableConfig.META_FIELDS_MODE.key() + "=" + MetaFieldsMode.COMMIT_TIME_ONLY.name());
    HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(first, jsc);
    streamer.getIngestionService().ingestOnce();
    streamer.shutdownGracefully();

    HoodieDeltaStreamer.Config restart = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    restart.tableType = "COPY_ON_WRITE";
    restart.configs.add(HoodieTableConfig.POPULATE_META_FIELDS.key() + "=false");

    Throwable thrown = assertThrows(Throwable.class, () -> {
      HoodieDeltaStreamer restarted = new HoodieDeltaStreamer(restart, jsc);
      restarted.getIngestionService().ingestOnce();
      restarted.shutdownGracefully();
    });

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains(HoodieTableConfig.META_FIELDS_MODE.key())
            || rootMessage.contains(HoodieTableConfig.POPULATE_META_FIELDS.key()),
        "expected a meta-fields conflict, got: " + rootMessage);

    // The failed run must not have changed the table, nor written rows with a null commit time.
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(context, tablePath);
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, metaClient.getTableConfig().getMetaFieldsMode(),
        "a rejected restart must leave the table's mode untouched");
    Dataset<Row> raw = sparkSession.read().parquet(tablePath + "/*/*/*/*.parquet");
    assertEquals(0, raw.filter(functions.col(HoodieRecord.COMMIT_TIME_METADATA_FIELD).isNull()).count(),
        "no row may have a null _hoodie_commit_time");
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

  /**
   * Counts across every row rather than inspecting one: a writer that populated a column on some rows
   * and not others would satisfy a single-row check while producing exactly the mixed-mode output the
   * mode being immutable exists to prevent.
   */
  private void assertOnDiskMetaColumns(String tablePath, MetaFieldsMode expectedMode) {
    // Default HoodieTestDataGenerator partitions are YYYY/MM/DD (three levels).
    Dataset<Row> raw = sparkSession.read().parquet(tablePath + "/*/*/*/*.parquet");
    long total = raw.count();
    assertTrue(total > 1, "the fixture must write more than one row for this assertion to mean anything");

    assertMetaColumn(raw, total, HoodieRecord.COMMIT_TIME_METADATA_FIELD,
        expectedMode.isCommitTimePopulated(), expectedMode);
    assertMetaColumn(raw, total, HoodieRecord.FILENAME_METADATA_FIELD,
        expectedMode.isFileNamePopulated(), expectedMode);
    boolean allMode = expectedMode == MetaFieldsMode.ALL;
    assertMetaColumn(raw, total, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, allMode, expectedMode);
    assertMetaColumn(raw, total, HoodieRecord.RECORD_KEY_METADATA_FIELD, allMode, expectedMode);
    assertMetaColumn(raw, total, HoodieRecord.PARTITION_PATH_METADATA_FIELD, allMode, expectedMode);
  }

  private static void assertMetaColumn(Dataset<Row> raw, long total, String column,
                                       boolean expectPopulated, MetaFieldsMode mode) {
    long nonNull = raw.filter(functions.col(column).isNotNull()).count();
    if (expectPopulated) {
      assertEquals(total, nonNull,
          "every row must carry " + column + " in mode " + mode + "; " + (total - nonNull) + " of "
              + total + " were null");
    } else {
      assertEquals(0, nonNull,
          "no row may carry " + column + " in mode " + mode + "; " + nonNull + " of " + total
              + " were populated");
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
