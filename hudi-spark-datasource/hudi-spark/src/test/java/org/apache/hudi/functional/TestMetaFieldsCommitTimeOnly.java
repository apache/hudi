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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spark-datasource tests for the COMMIT_TIME_ONLY meta-fields-population mode.
 *
 * <p>The mode is an additive layer on top of the existing NONE mode
 * ({@code hoodie.populate.meta.fields=false}): it additionally populates
 * {@code _hoodie_commit_time} so incremental queries remain functional. The remaining four meta
 * columns stay null on disk, giving the storage saving while keeping incremental-query semantics.
 *
 * <p>This test fixture covers:
 * <ul>
 *   <li>The persisted table-config flag is set correctly when the writer opts into the mode.</li>
 *   <li>The {@code HoodieTableConfig} accessors correctly report the mode after a write/reread.</li>
 *   <li>The ambiguous combination ({@code populate.meta.fields=true} together with
 *   {@code meta.fields.commit.time.enabled=true}) is rejected at writer init.</li>
 *   <li>The ALL mode (default) is not regressed.</li>
 * </ul>
 *
 * <p>End-to-end snapshot/incremental query semantics that depend on Spark's schema-selection
 * pathway are exercised in follow-up integration tests; the writer-engine-level coverage of the
 * COMMIT_TIME_ONLY branch lives in {@code TestHoodieSparkParquetWriter}-style unit tests.
 */
class TestMetaFieldsCommitTimeOnly extends SparkClientFunctionalTestHarness {

  // ---- helpers ------------------------------------------------------------

  private static StructType simpleSchema() {
    return DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("column1", DataTypes.StringType, true),
        DataTypes.createStructField("column2", DataTypes.StringType, true),
        DataTypes.createStructField("column3", DataTypes.StringType, true)
    }).asNullable();
  }

  private Map<String, String> baseOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "column1");
    opts.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "column2");
    opts.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "column3");
    opts.put(HoodieTableConfig.NAME.key(), "test_commit_time_only");
    opts.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    opts.put(HoodieMetadataConfig.ENABLE.key(), "false");
    return opts;
  }

  private void writeRows(List<Row> records, StructType schema, Map<String, String> options, String path, SaveMode mode) {
    spark().createDataset(records,
            SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema))
        .write()
        .format("hudi")
        .options(options)
        .mode(mode)
        .save(path);
  }

  // ---- tests --------------------------------------------------------------

  /**
   * COMMIT_TIME_ONLY mode: writer succeeds, persists both legacy and new properties on
   * {@code hoodie.properties}, and the read-back table config correctly reports the mode via the
   * three accessors.
   */
  @Test
  void commitTimeOnlyModePersistsPropertiesAndReportsMode() {
    String path = basePath();
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_COMMIT_TIME_ENABLED.key(), "true");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")), simpleSchema(), options, path, SaveMode.Overwrite);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    HoodieTableConfig tc = metaClient.getTableConfig();

    assertEquals("false", tc.getProps().getProperty(HoodieTableConfig.POPULATE_META_FIELDS.key()),
        "populate.meta.fields=false must be persisted");
    assertEquals("true", tc.getProps().getProperty(HoodieTableConfig.META_FIELDS_COMMIT_TIME_ENABLED.key()),
        "meta.fields.commit.time.enabled=true must be persisted");
    assertFalse(tc.populateMetaFields());
    assertTrue(tc.isCommitTimeOnlyMetaFieldsMode(), "table must report COMMIT_TIME_ONLY mode");
    assertTrue(tc.isCommitTimePopulated(), "_hoodie_commit_time is logically populated under COMMIT_TIME_ONLY");
    assertFalse(tc.isRecordKeyPopulated(), "_hoodie_record_key remains unpopulated under COMMIT_TIME_ONLY");
  }

  /**
   * NONE mode (today's behavior): writer succeeds, persists populate.meta.fields=false, and the
   * three accessors all report the NONE state.
   */
  @Test
  void noneModePersistsAndReportsCorrectly() {
    String path = basePath();
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    // commit.time.enabled defaults to false; leave it absent.
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")), simpleSchema(), options, path, SaveMode.Overwrite);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    HoodieTableConfig tc = metaClient.getTableConfig();

    assertFalse(tc.populateMetaFields());
    assertFalse(tc.isCommitTimeOnlyMetaFieldsMode());
    assertFalse(tc.isCommitTimePopulated());
    assertFalse(tc.isRecordKeyPopulated());
  }

  /**
   * Default (ALL) mode: every accessor reports the populated state.
   */
  @Test
  void defaultAllModeReportsPopulated() {
    String path = basePath();
    Map<String, String> options = baseOptions();
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")), simpleSchema(), options, path, SaveMode.Overwrite);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    HoodieTableConfig tc = metaClient.getTableConfig();

    assertTrue(tc.populateMetaFields());
    assertFalse(tc.isCommitTimeOnlyMetaFieldsMode());
    assertTrue(tc.isCommitTimePopulated());
    assertTrue(tc.isRecordKeyPopulated());
  }

  /**
   * Setting both {@code populate.meta.fields=true} AND {@code meta.fields.commit.time.enabled=true}
   * is rejected at writer init. The combination is ambiguous (the new flag has no effect when all
   * meta fields are already populated).
   */
  @Test
  void invalidCombinationIsRejectedAtWriterInit() {
    String path = basePath();
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true");
    options.put(HoodieTableConfig.META_FIELDS_COMMIT_TIME_ENABLED.key(), "true");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    Throwable thrown = assertThrows(Throwable.class, () ->
        writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")),
            simpleSchema(), options, path, SaveMode.Overwrite));

    // Unwrap to root cause.
    Throwable root = thrown;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    String message = root.getMessage() == null ? "" : root.getMessage();
    assertTrue(message.contains(HoodieTableConfig.META_FIELDS_COMMIT_TIME_ENABLED.key())
            || message.contains(HoodieTableConfig.POPULATE_META_FIELDS.key()),
        "Expected validation error to name one of the conflicting properties, got: " + message);
  }
}
