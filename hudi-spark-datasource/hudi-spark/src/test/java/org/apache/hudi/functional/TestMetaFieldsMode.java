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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spark-datasource end-to-end tests for the {@code hoodie.meta.fields.mode} property on CoW tables.
 * Every {@link MetaFieldsMode} value is exercised via a write / re-read round trip; on-disk column
 * population is verified by reading the parquet files back and inspecting the meta-column values.
 */
class TestMetaFieldsMode extends SparkClientFunctionalTestHarness {

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
    opts.put(HoodieTableConfig.NAME.key(), "test_meta_fields_mode");
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

  private HoodieTableConfig writeSampleAndGetTableConfig(Map<String, String> options, String path) {
    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2")),
        simpleSchema(), options, path, SaveMode.Overwrite);
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    return metaClient.getTableConfig();
  }

  /**
   * End-to-end assertion of the on-disk meta columns after a write. Reads the parquet files back
   * (bypassing Hudi's own read path so we see the raw column values) and asserts which meta
   * columns are non-null.
   */
  private void assertMetaColumnPopulation(String path, MetaFieldsMode expectedMode) {
    Dataset<Row> raw = spark().read().parquet(path + "/*/*.parquet");
    Row first = raw.select(
        HoodieRecord.COMMIT_TIME_METADATA_FIELD,
        HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
        HoodieRecord.RECORD_KEY_METADATA_FIELD,
        HoodieRecord.PARTITION_PATH_METADATA_FIELD,
        HoodieRecord.FILENAME_METADATA_FIELD).first();

    if (expectedMode.isCommitTimePopulated()) {
      assertNotNull(first.get(0), "expected _hoodie_commit_time to be populated for mode " + expectedMode);
    } else {
      assertNull(first.get(0), "expected _hoodie_commit_time to be null for mode " + expectedMode);
    }
    if (expectedMode.isFileNamePopulated()) {
      assertNotNull(first.get(4), "expected _hoodie_file_name to be populated for mode " + expectedMode);
    } else {
      assertNull(first.get(4), "expected _hoodie_file_name to be null for mode " + expectedMode);
    }
    // Record key, partition path, and commit seq no are ALL-only.
    if (expectedMode == MetaFieldsMode.ALL) {
      assertNotNull(first.get(2), "record key must be populated in ALL mode");
      assertNotNull(first.get(3), "partition path must be populated in ALL mode");
      assertNotNull(first.get(1), "commit seq no must be populated in ALL mode");
    } else {
      assertNull(first.get(2), "record key must be null outside ALL mode, got: " + first.get(2));
      assertNull(first.get(3), "partition path must be null outside ALL mode, got: " + first.get(3));
      assertNull(first.get(1), "commit seq no must be null outside ALL mode, got: " + first.get(1));
    }
  }

  @Test
  void allModePersistsAndPopulatesAllColumns() {
    Map<String, String> options = baseOptions();
    // ALL is the default; no need to set the mode explicitly.
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertTrue(tc.populateMetaFields());
    assertEquals(MetaFieldsMode.ALL, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.ALL);
  }

  @Test
  void noneModePersistsAndLeavesAllColumnsNull() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertFalse(tc.populateMetaFields());
    assertEquals(MetaFieldsMode.NONE, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.NONE);
  }

  @Test
  void commitTimeOnlyModePopulatesOnlyCommitTime() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY.name(),
        tc.getProps().getProperty(HoodieTableConfig.META_FIELDS_MODE.key()));
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_ONLY);
  }

  @Test
  void fileNameOnlyModePopulatesOnlyFileName() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.FILE_NAME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertEquals(MetaFieldsMode.FILE_NAME_ONLY, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.FILE_NAME_ONLY);
  }

  @Test
  void commitTimeAndFileNameModePopulatesBoth() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertEquals(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME);
  }

  @Test
  void populateTrueWithSelectiveModeIsRejected() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    Throwable thrown = assertThrows(Throwable.class, () ->
        writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")),
            simpleSchema(), options, basePath(), SaveMode.Overwrite));

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains(HoodieTableConfig.META_FIELDS_MODE.key())
            || rootMessage.contains(HoodieTableConfig.POPULATE_META_FIELDS.key()),
        "Expected validation error to name one of the conflicting properties, got: " + rootMessage);
  }

  @Test
  void unknownModeValueIsRejected() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), "SOMETHING_BOGUS");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    Throwable thrown = assertThrows(Throwable.class, () ->
        writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")),
            simpleSchema(), options, basePath(), SaveMode.Overwrite));

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains("SOMETHING_BOGUS"),
        "Expected error to name the rejected value, got: " + rootMessage);
  }

  // -------------------------------------------------------------------------
  // Non-row-writer path coverage. Bulk insert with row.writer.enable=false forces the
  // HoodieAvroParquetWriter path (via HoodieCreateHandle) instead of the internal-row writer path.
  // Both paths must respect the mode identically.
  // -------------------------------------------------------------------------

  @Test
  void nonRowWriterPathAllMode() {
    Map<String, String> options = baseOptions();
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.datasource.write.row.writer.enable", "false");

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());
    assertEquals(MetaFieldsMode.ALL, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.ALL);
  }

  @Test
  void nonRowWriterPathNoneMode() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.datasource.write.row.writer.enable", "false");

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());
    assertEquals(MetaFieldsMode.NONE, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.NONE);
  }

  @Test
  void nonRowWriterPathCommitTimeOnly() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.datasource.write.row.writer.enable", "false");

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_ONLY);
  }

  @Test
  void nonRowWriterPathFileNameOnly() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.FILE_NAME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.datasource.write.row.writer.enable", "false");

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());
    assertEquals(MetaFieldsMode.FILE_NAME_ONLY, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.FILE_NAME_ONLY);
  }

  @Test
  void nonRowWriterPathCommitTimeAndFileName() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.datasource.write.row.writer.enable", "false");

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());
    assertEquals(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME);
  }

  // -------------------------------------------------------------------------
  // Clustering coverage. Inline clustering rewrites files through the create/merge handles which
  // delegate to the same underlying HoodieAvroParquetWriter / HoodieRowCreateHandle we exercise
  // in the write tests. Verifies clustered files preserve the mode's column population semantics.
  // -------------------------------------------------------------------------

  @Test
  void clusteringPreservesCommitTimeOnlyMode() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    // Inline clustering after each write.
    options.put("hoodie.clustering.inline", "true");
    options.put("hoodie.clustering.inline.max.commits", "1");
    options.put("hoodie.clustering.plan.strategy.target.file.max.bytes", "10485760");
    options.put("hoodie.clustering.plan.strategy.small.file.limit", "10485760");

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(basePath()).setConf(storageConf()).build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, metaClient.getTableConfig().getMetaFieldsMode());
    // After clustering, files are rewritten — verify the rewritten files still respect the mode.
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_ONLY);
  }

  @Test
  void clusteringPreservesFileNameOnlyMode() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.FILE_NAME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.clustering.inline", "true");
    options.put("hoodie.clustering.inline.max.commits", "1");
    options.put("hoodie.clustering.plan.strategy.target.file.max.bytes", "10485760");
    options.put("hoodie.clustering.plan.strategy.small.file.limit", "10485760");

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    assertMetaColumnPopulation(basePath(), MetaFieldsMode.FILE_NAME_ONLY);
  }

  @Test
  void clusteringPreservesCommitTimeAndFileNameMode() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.clustering.inline", "true");
    options.put("hoodie.clustering.inline.max.commits", "1");
    options.put("hoodie.clustering.plan.strategy.target.file.max.bytes", "10485760");
    options.put("hoodie.clustering.plan.strategy.small.file.limit", "10485760");

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME);
  }

  @Test
  void clusteringPreservesAllMode() {
    Map<String, String> options = baseOptions();
    // ALL is the default.
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.clustering.inline", "true");
    options.put("hoodie.clustering.inline.max.commits", "1");
    options.put("hoodie.clustering.plan.strategy.target.file.max.bytes", "10485760");
    options.put("hoodie.clustering.plan.strategy.small.file.limit", "10485760");

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    assertMetaColumnPopulation(basePath(), MetaFieldsMode.ALL);
  }

  @Test
  void clusteringPreservesNoneMode() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.clustering.inline", "true");
    options.put("hoodie.clustering.inline.max.commits", "1");
    options.put("hoodie.clustering.plan.strategy.target.file.max.bytes", "10485760");
    options.put("hoodie.clustering.plan.strategy.small.file.limit", "10485760");

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    assertMetaColumnPopulation(basePath(), MetaFieldsMode.NONE);
  }

  @Test
  void morWithSelectiveModeIsRejected() {
    Map<String, String> options = baseOptions();
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    Throwable thrown = assertThrows(Throwable.class, () ->
        writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")),
            simpleSchema(), options, basePath(), SaveMode.Overwrite));

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains("COPY_ON_WRITE") || rootMessage.contains("MoR") || rootMessage.contains("MERGE_ON_READ"),
        "Expected MoR-restriction error, got: " + rootMessage);
  }

  private static String rootMessageOf(Throwable thrown) {
    Throwable root = thrown;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root.getMessage() == null ? "" : root.getMessage();
  }
}
