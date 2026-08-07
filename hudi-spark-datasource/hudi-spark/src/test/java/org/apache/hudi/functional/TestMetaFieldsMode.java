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

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
  void selectiveModeWinsOverLegacyPopulateTrue() {
    // hoodie.meta.fields.mode is the source of truth: an explicit mode is honored regardless of
    // the deprecated boolean, so this combination is no longer ambiguous and is not rejected.
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_ONLY);
    // ...and hoodie.properties must not contradict the mode. A pre-1.3.0 reader ignores the mode
    // property entirely, so leaving populate.meta.fields=true here would make it treat a
    // selectively-written table as ALL.
    assertFalse(tc.populateMetaFields(),
        "legacy populate.meta.fields must be derived from the mode, not carried through verbatim");
  }

  @Test
  void noneModePersistsLegacyBooleanAsFalse() {
    // The unsafe case: an old incremental reader that sees populate.meta.fields=true on a NONE
    // table would run against all-null commit times and silently return zero rows.
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.NONE.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertEquals(MetaFieldsMode.NONE, tc.getMetaFieldsMode());
    assertFalse(tc.populateMetaFields(),
        "NONE must persist populate.meta.fields=false so pre-1.3.0 readers do not treat it as ALL");
  }

  @Test
  void allModePersistsLegacyBooleanAsTrue() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.ALL.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertEquals(MetaFieldsMode.ALL, tc.getMetaFieldsMode());
    assertTrue(tc.populateMetaFields(),
        "ALL must persist populate.meta.fields=true for pre-1.3.0 readers");
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

  // ---------------------------------------------------------------------------------------------
  // Incremental queries. This is the reason COMMIT_TIME_ONLY exists: a table opts out of four meta
  // columns but keeps _hoodie_commit_time so incremental reads still work. The assertions below are
  // on the query result, not on the raw parquet — a populated commit-time column is necessary but
  // not sufficient, since the relation also has to admit the table and filter on the right range.
  // ---------------------------------------------------------------------------------------------

  /**
   * Writes two commits and returns their instant times, oldest first. The first is an Overwrite so
   * the table is created with the given options; the second appends without restating the mode,
   * which is also the inheritance path a real second write takes.
   */
  private List<String> writeTwoCommits(Map<String, String> options, String path) {
    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2")),
        simpleSchema(), options, path, SaveMode.Overwrite);
    writeRows(Arrays.asList(
            RowFactory.create("k3", "p1", "v3"),
            RowFactory.create("k4", "p1", "v4")),
        simpleSchema(), options, path, SaveMode.Append);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    return metaClient.getActiveTimeline().filterCompletedInstants().getInstants()
        .stream()
        .map(HoodieInstant::requestedTime)
        .sorted()
        .collect(Collectors.toList());
  }

  @Test
  void incrementalQueryReturnsOnlyNewRowsUnderCommitTimeOnly() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    List<String> instants = writeTwoCommits(options, basePath());
    assertEquals(2, instants.size(), "expected exactly two completed commits");

    // START_COMMIT is inclusive, so naming the second instant scopes the read to that commit alone.
    Dataset<Row> incremental = spark().read().format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), instants.get(1))
        .load(basePath());

    // Read whole rows rather than a projection: a CoW incremental scan that projects a subset drops
    // every row, on a default ALL table just as much as here. That is a pre-existing defect in the
    // shared read path (reproduced on master at table version 9), not something this mode affects,
    // so these assertions deliberately go through the full row.
    List<Row> rows = incremental.collectAsList();

    List<String> keys = rows.stream()
        .map(r -> r.getAs("column1").toString()).sorted().collect(Collectors.toList());
    assertEquals(Arrays.asList("k3", "k4"), keys,
        "incremental read must return exactly the second commit's rows");

    // The rows carry the second commit's time — this is what the range filter keys off, so a null
    // here would silently drop every row rather than fail. This is the assertion that would have
    // caught a writer that stopped populating _hoodie_commit_time under a selective mode.
    List<String> commitTimes = rows.stream()
        .map(r -> r.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString())
        .distinct().collect(Collectors.toList());
    assertEquals(Collections.singletonList(instants.get(1)), commitTimes);
  }

  @Test
  void incrementalQueryFromTableCreationReturnsEveryRowUnderCommitTimeOnly() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    List<String> instants = writeTwoCommits(options, basePath());

    // Guards against the zero-row regression fixed in 13b39db9fb22, where the read schema omitted
    // the meta columns for selective modes and the incremental relation returned nothing at all.
    Dataset<Row> incremental = spark().read().format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), "000")
        .load(basePath());

    // collectAsList() rather than count(): a column-less CoW incremental scan projects no columns,
    // so the pushed-down commit-time filter matches nothing and count() returns 0. That behaves
    // identically on a default ALL table and predates this patch, so it is not asserted here.
    assertEquals(4, incremental.collectAsList().size(),
        "both commits fall in range, so every row must come back");
  }

  @Test
  void incrementalQueryAlsoWorksUnderCommitTimeAndFileName() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    List<String> instants = writeTwoCommits(options, basePath());

    Dataset<Row> incremental = spark().read().format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), instants.get(1))
        .load(basePath());

    assertEquals(2, incremental.collectAsList().size());
  }

  /**
   * The two modes that do not populate {@code _hoodie_commit_time} must be rejected outright rather
   * than returning an empty result — an incremental query that silently returns nothing is the
   * failure mode this guard exists to prevent.
   */
  @ParameterizedTest
  @EnumSource(value = MetaFieldsMode.class, names = {"FILE_NAME_ONLY", "NONE"})
  void incrementalQueryIsRejectedWhenCommitTimeIsNotPopulated(MetaFieldsMode mode) {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), mode.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    List<String> instants = writeTwoCommits(options, basePath());

    Throwable thrown = assertThrows(Throwable.class, () ->
        spark().read().format("hudi")
            .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
            .option(DataSourceReadOptions.START_COMMIT().key(), instants.get(0))
            .load(basePath())
            .collectAsList());

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains("_hoodie_commit_time"),
        "Expected the commit-time guard from IncrementalRelation, got: " + rootMessage);
    assertTrue(rootMessage.contains(HoodieTableConfig.META_FIELDS_MODE.key()),
        "the message must name the property so users know what to change, got: " + rootMessage);
  }

  private static String rootMessageOf(Throwable thrown) {
    Throwable root = thrown;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root.getMessage() == null ? "" : root.getMessage();
  }
}
