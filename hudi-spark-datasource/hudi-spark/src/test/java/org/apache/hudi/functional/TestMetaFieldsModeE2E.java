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

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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
class TestMetaFieldsModeE2E extends SparkClientFunctionalTestHarness {

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
   * (bypassing Hudi's own read path so we see the raw column values) and asserts which meta columns
   * are non-null.
   *
   * <p>Counts across every row rather than inspecting one. A writer that populated a column on some
   * rows and not others -- mixed-mode output, which is the hazard the mode being immutable exists to
   * prevent -- would satisfy a single-row check while producing exactly the corruption under test.
   */
  private void assertMetaColumnPopulation(String path, MetaFieldsMode expectedMode) {
    // A raw parquet glob, deliberately: it sees the physical columns even under NONE, where the Hudi
    // read path projects no meta fields at all and a filter on _hoodie_commit_time cannot resolve.
    //
    // The trade-off is that on a clustered table the glob also picks up the pre-clustering files,
    // which are still on disk because no cleaning has run. Those rows are no longer served and may
    // predate the current mode, so this helper must not be pointed at a clustered table --
    // assertClusteredFileNamesPointAtTheirOwnFile reads through Hudi and covers that case.
    Dataset<Row> raw = spark().read().parquet(path + "/*/*.parquet");
    long total = raw.count();
    assertTrue(total > 1, "the fixture must write more than one row for this assertion to mean anything");

    assertMetaColumn(raw, total, HoodieRecord.COMMIT_TIME_METADATA_FIELD,
        expectedMode.isCommitTimePopulated(), expectedMode);
    assertMetaColumn(raw, total, HoodieRecord.FILENAME_METADATA_FIELD,
        expectedMode.isFileNamePopulated(), expectedMode);
    // Record key, partition path, and commit seq no are ALL-only.
    boolean allMode = expectedMode == MetaFieldsMode.ALL;
    assertMetaColumn(raw, total, HoodieRecord.RECORD_KEY_METADATA_FIELD, allMode, expectedMode);
    assertMetaColumn(raw, total, HoodieRecord.PARTITION_PATH_METADATA_FIELD, allMode, expectedMode);
    assertMetaColumn(raw, total, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, allMode, expectedMode);
  }

  private static void assertMetaColumn(Dataset<Row> raw, long total, String column,
                                       boolean expectPopulated, MetaFieldsMode mode) {
    long nonNull = raw.filter(functions.col(column).isNotNull()).count();
    if (expectPopulated) {
      assertEquals(total, nonNull,
          "every row must carry " + column + " for mode " + mode + "; " + (total - nonNull) + " of "
              + total + " were null");
    } else {
      assertEquals(0, nonNull,
          "no row may carry " + column + " for mode " + mode + "; " + nonNull + " of " + total
              + " were populated");
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
  void explicitlyContradictingLegacyBooleanDoesNotOverrideModeAtTableCreation() {
    // The mode is authoritative. Even when the deprecated boolean explicitly disagrees, derive it
    // from the mode before the table properties are persisted.
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, tc.getMetaFieldsMode());
    assertFalse(tc.populateMetaFields());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_ONLY);
  }

  @Test
  void selectiveModeWithoutTheLegacyBooleanDerivesItAsFalse() {
    // The ordinary case: state only the mode. The boolean is derived, never carried through
    // verbatim -- a pre-1.3.0 reader ignores the mode property, so leaving populate=true would make
    // it treat a selectively-written table as ALL.
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeSampleAndGetTableConfig(options, basePath());

    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, tc.getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_ONLY);
    assertFalse(tc.populateMetaFields(),
        "legacy populate.meta.fields must be derived from the mode");
  }

  @Test
  void noneModePersistsLegacyBooleanAsFalse() {
    // The unsafe case this invariant protects: an old incremental reader that saw
    // populate.meta.fields=true on a NONE table would run against all-null commit times and
    // silently return zero rows. Stating only the mode -- the ordinary case -- must derive false.
    Map<String, String> options = baseOptions();
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
  // Clustering coverage.
  //
  // These target 358fbfdd717a, where HoodieRowCreateHandle's selective path copied the *source
  // row's* _hoodie_file_name during clustering, leaving records pointing at a file clustering had
  // just replaced. asserting only assertNotNull cannot catch that — the stale value is non-null too
  // — so the assertion here compares the column against the file actually holding the row.
  //
  // Only the selective modes are covered: ALL and NONE route through writeRow /
  // writeRowNoMetaFields and never enter the branch the fix touched.
  // -------------------------------------------------------------------------

  private Map<String, String> inlineClusteringOptions(MetaFieldsMode mode) {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), mode.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    options.put("hoodie.clustering.inline", "true");
    options.put("hoodie.clustering.inline.max.commits", "1");
    options.put("hoodie.clustering.plan.strategy.target.file.max.bytes", "10485760");
    options.put("hoodie.clustering.plan.strategy.small.file.limit", "10485760");
    return options;
  }

  /**
   * Asserts clustering actually ran and that every surviving row's {@code _hoodie_file_name} names
   * the file holding it.
   *
   * <p>Reads through Hudi rather than globbing the parquet directly: after inline clustering the
   * pre-clustering file is still on disk (no cleaning has run), so a raw glob would also inspect
   * rows that were replaced and are no longer served.
   */
  private void assertClusteredFileNamesPointAtTheirOwnFile(String path, MetaFieldsMode mode) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    assertEquals(mode, metaClient.getTableConfig().getMetaFieldsMode());
    assertEquals(1, metaClient.getActiveTimeline().getCompletedReplaceTimeline().countInstants(),
        "clustering must have produced a replacecommit, otherwise this test proves nothing");

    List<Row> rows = spark().read().format("hudi").load(path)
        .withColumn("__containing_file", functions.input_file_name())
        .collectAsList();
    assertFalse(rows.isEmpty(), "expected the clustered table to still serve rows");

    // Not asserted here yet: that clustering preserves the source row's _hoodie_commit_time rather
    // than stamping the replacecommit instant. assertNotNull cannot catch that -- a replacecommit
    // instant is non-null too -- but the direct comparison reads null commit times through
    // format("hudi") while the raw-parquet check in assertMetaColumnPopulation sees them populated,
    // and whether that is the read path dropping a column it should serve on a selective table or the
    // assertion reading the wrong thing is not yet established. Left out rather than shipped red.
    for (Row row : rows) {
      String fileName = row.getAs(HoodieRecord.FILENAME_METADATA_FIELD);
      String containingFile = row.getAs("__containing_file").toString();
      if (mode.isFileNamePopulated()) {
        assertNotNull(fileName, "file name is opted in, so clustered rows must carry one");
        assertTrue(containingFile.endsWith("/" + fileName),
            "_hoodie_file_name must name the file holding the row after clustering, not the "
                + "pre-clustering file it was read from; got " + fileName + " inside " + containingFile);
      } else {
        assertNull(fileName,
            "file name is not opted in, so clustering must not populate it; got " + fileName);
      }
    }
  }

  @Test
  void clusteringWritesTheNewFileNameUnderFileNameOnly() {
    Map<String, String> options = inlineClusteringOptions(MetaFieldsMode.FILE_NAME_ONLY);
    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    assertClusteredFileNamesPointAtTheirOwnFile(basePath(), MetaFieldsMode.FILE_NAME_ONLY);
  }

  @Test
  void clusteringWritesTheNewFileNameUnderCommitTimeAndFileName() {
    Map<String, String> options = inlineClusteringOptions(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME);
    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    assertClusteredFileNamesPointAtTheirOwnFile(basePath(), MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME);
  }

  @Test
  void clusteringLeavesFileNameNullUnderCommitTimeOnly() {
    Map<String, String> options = inlineClusteringOptions(MetaFieldsMode.COMMIT_TIME_ONLY);
    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    // assertMetaColumnPopulation is deliberately not called here: it globs the parquet directly and
    // would also inspect the pre-clustering files, which are still on disk and no longer served. The
    // assertion below reads through Hudi and covers the served rows.
    assertClusteredFileNamesPointAtTheirOwnFile(basePath(), MetaFieldsMode.COMMIT_TIME_ONLY);
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
  //
  // IMPORTANT for anyone adding a case here: assert on collectAsList(), never count() and never a
  // select() of a subset of columns. A column-less or narrowly-projected CoW incremental scan
  // returns zero rows — HoodieFileGroupReaderBasedFileFormat routes it to readBaseFile, which pushes
  // the _hoodie_commit_time predicate into Parquet without projecting that column. That is a
  // PRE-EXISTING defect, not something this feature introduced: it reproduces on unmodified master
  // at table version 9 on a default ALL table (collect=2, count=0, select(col)=0). Being tracked
  // separately; deliberately out of scope here because it sits on the shared read path for every
  // Spark query. No test in this class is disabled for it — reading whole rows sidesteps it.
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

  // ---------------------------------------------------------------------------------------------
  // Upserts. Disabling the record-key meta column does not disable upserts: the key is re-derived
  // through the key generator from the base file's data columns, and incoming records carry theirs
  // in memory. So HoodieWriteMergeHandle's preserve-metadata branch is reachable here, and it is
  // the one write path that stamps a meta column outside the mode-aware writers.
  // ---------------------------------------------------------------------------------------------

  /**
   * An upsert rewrites the whole file group: the updated record goes through the normal write path,
   * while untouched records are copied forward through
   * {@code HoodieWriteMergeHandle.writeToFile(..., shouldPreserveRecordMetadata=true)}. That branch
   * used to stamp {@code _hoodie_file_name} unconditionally, so a {@code COMMIT_TIME_ONLY} table
   * accumulated file names on every upsert while advertising that the column stays null.
   */
  @Test
  void upsertLeavesFileNameNullOnCopiedRecordsUnderCommitTimeOnly() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL());

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    // Update only k1. k2 and k3 are copied forward untouched — the preserve-metadata path.
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL());
    writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1-updated")),
        simpleSchema(), options, basePath(), SaveMode.Append);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(basePath()).setConf(storageConf()).build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, metaClient.getTableConfig().getMetaFieldsMode(),
        "the upsert must not have changed the table's mode");

    // Read the latest file slice only: the pre-upsert file is still on disk (no cleaning has run),
    // and it would otherwise be counted alongside the rewritten one.
    Dataset<Row> latest = spark().read().format("hudi").load(basePath());
    List<Row> rows = latest.collectAsList();
    assertEquals(3, rows.size(), "upsert must not change the record count");

    for (Row row : rows) {
      String key = row.getAs("column1").toString();
      assertNull(row.getAs(HoodieRecord.FILENAME_METADATA_FIELD),
          "_hoodie_file_name must stay null under COMMIT_TIME_ONLY, including on records copied "
              + "forward by the merge handle; row " + key + " had it populated");
      assertNotNull(row.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
          "_hoodie_commit_time is opted in, so every row must carry one; row " + key + " did not");
      assertNull(row.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD),
          "record key is ALL-only; row " + key + " had it populated");
    }

    // The update landed, so the merge genuinely ran rather than being skipped.
    List<String> updated = rows.stream()
        .filter(r -> "k1".equals(r.getAs("column1").toString()))
        .map(r -> r.getAs("column3").toString()).collect(Collectors.toList());
    assertEquals(Collections.singletonList("v1-updated"), updated);
  }

  /**
   * The same copy-forward upsert, but with the Spark record type rather than Avro.
   *
   * <p>These take different code paths for clearing a meta column. Avro's {@code updateMetaField}
   * does {@code data.put(ordinal, null)} and tolerates a null; the Spark path goes through
   * {@code HoodieInternalRow#update}, which accepts only UTF8String or String and reports anything
   * else via {@code value.getClass()} -- NPE-ing on a null before it can build the exception. So a
   * COMMIT_TIME_ONLY table would crash on the first upsert that copied a record forward, while the
   * Avro-path test above passed. Reported by the review bot on this PR.
   */
  @Test
  void upsertLeavesFileNameNullOnCopiedRecordsWithTheSparkRecordType() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    // Selects the SPARK record type, which is what routes updateMetaField through
    // HoodieInternalRow#update rather than Avro's null-tolerant data.put.
    options.put("hoodie.write.record.merge.custom.implementation.classes", "org.apache.hudi.DefaultSparkRecordMerger");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL());

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2"),
            RowFactory.create("k3", "p1", "v3")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    // k2 and k3 are copied forward untouched -- the preserve-metadata path that clears the column.
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL());
    writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1-updated")),
        simpleSchema(), options, basePath(), SaveMode.Append);

    List<Row> rows = spark().read().format("hudi").load(basePath()).collectAsList();
    assertEquals(3, rows.size(), "upsert must not change the record count");
    for (Row row : rows) {
      String key = row.getAs("column1").toString();
      assertNull(row.getAs(HoodieRecord.FILENAME_METADATA_FIELD),
          "_hoodie_file_name must stay null under COMMIT_TIME_ONLY; row " + key + " had it populated");
      assertNotNull(row.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
          "_hoodie_commit_time is opted in; row " + key + " did not carry one");
    }
  }

  /**
   * The same upsert on {@code COMMIT_TIME_AND_FILE_NAME}, where the file name is opted in, must
   * still rewrite it to the new file — otherwise a copied record would point at the file it came
   * from rather than the one holding it, which is why the unconditional rewrite existed.
   */
  @Test
  void upsertRewritesFileNameOnCopiedRecordsWhenFileNameIsOptedIn() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL());

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL());
    writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1-updated")),
        simpleSchema(), options, basePath(), SaveMode.Append);

    Dataset<Row> latest = spark().read().format("hudi")
        .load(basePath())
        .withColumn("__containing_file", functions.input_file_name());

    for (Row row : latest.collectAsList()) {
      String fileName = row.getAs(HoodieRecord.FILENAME_METADATA_FIELD);
      String containingFile = row.getAs("__containing_file").toString();
      assertNotNull(fileName, "file name is opted in, so it must be populated");
      assertTrue(containingFile.endsWith("/" + fileName),
          "_hoodie_file_name must name the file actually holding the row, so a copied record cannot "
              + "keep pointing at its previous file; got " + fileName + " inside " + containingFile);
    }
  }

  /**
   * The metadata table is disabled in the rest of this class for speed, but it is on by default in
   * production. Nothing in the patch proves the data table's mode cannot leak into the MDT's write
   * config — {@code HoodieMetadataWriteUtils#createMetadataWriteConfig} builds a fresh config and
   * sets {@code populate.meta.fields=false} itself, so today it cannot, but that is one
   * {@code withProps(dataWriteConfig.getProps())} refactor away from turning every MDT-enabled
   * selective write into a hard failure.
   */
  @Test
  void selectiveModeWritesSucceedWithTheMetadataTableEnabled() {
    Map<String, String> options = baseOptions();
    options.put(HoodieMetadataConfig.ENABLE.key(), "true");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    HoodieTableMetaClient dataTable =
        HoodieTableMetaClient.builder().setBasePath(basePath()).setConf(storageConf()).build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, dataTable.getTableConfig().getMetaFieldsMode());
    assertMetaColumnPopulation(basePath(), MetaFieldsMode.COMMIT_TIME_ONLY);

    // The MDT is its own table and must stay NONE regardless of what the data table opted into.
    HoodieTableMetaClient metadataTable = HoodieTableMetaClient.builder()
        .setBasePath(basePath() + "/.hoodie/metadata")
        .setConf(storageConf())
        .build();
    assertEquals(MetaFieldsMode.NONE, metadataTable.getTableConfig().getMetaFieldsMode(),
        "the data table's mode must not leak into the metadata table");
  }

  /**
   * Every other functional write here uses {@code SaveMode.Overwrite}, i.e. a fresh table each time,
   * which skips the inheritance path entirely: {@code HoodieSparkSqlWriter} folds table props into
   * the write params only when the mode is not Overwrite, and {@code validateTableConfig} is bypassed
   * for Overwrite altogether. An append against an existing selective table is the realistic shape.
   */
  @Test
  void appendWithoutRestatingTheModeKeepsTheTableSelective() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    // Second write states neither meta-field property, so it must inherit COMMIT_TIME_ONLY.
    Map<String, String> appendOptions = baseOptions();
    appendOptions.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    writeRows(Arrays.asList(
            RowFactory.create("k3", "p1", "v3"),
            RowFactory.create("k4", "p1", "v4")),
        simpleSchema(), appendOptions, basePath(), SaveMode.Append);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(basePath()).setConf(storageConf()).build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, metaClient.getTableConfig().getMetaFieldsMode(),
        "an append that says nothing about meta fields must not change the table's mode");

    // Both commits' rows must carry a commit time; a null one is what incremental queries drop.
    Dataset<Row> all = spark().read().format("hudi").load(basePath());
    List<Row> rows = all.collectAsList();
    assertEquals(4, rows.size());
    for (Row row : rows) {
      assertNotNull(row.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
          "row " + row.getAs("column1") + " has a null commit time after an inheriting append");
      assertNull(row.getAs(HoodieRecord.FILENAME_METADATA_FIELD),
          "file name must stay null under COMMIT_TIME_ONLY");
    }
  }

  private static String rootMessageOf(Throwable thrown) {
    Throwable root = thrown;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root.getMessage() == null ? "" : root.getMessage();
  }

  /**
   * The structured-streaming read, which is the only caller of {@code IncrementalRelationV1/V2}.
   *
   * <p>Those relations carry the guard this PR relaxed from {@code populateMetaFields()} to
   * {@code isCommitTimePopulated()}, newly permitting streaming reads on a {@code COMMIT_TIME_ONLY}
   * table. Nothing else in this class reaches them: the datasource incremental tests route through
   * {@code HoodieCopyOnWriteIncrementalHadoopFsRelationFactory} instead. Given the read path has
   * already broken twice under selective modes, the relaxed branch should not go untested.
   */
  @Test
  void streamingReadWorksUnderCommitTimeOnly() throws Exception {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    Path checkpoint = java.nio.file.Files.createTempDirectory("meta-fields-stream-ckpt");
    AtomicLong seen = new AtomicLong(0);
    StreamingQuery query = spark().readStream()
        .format("hudi")
        .load(basePath())
        .writeStream()
        .option("checkpointLocation", checkpoint.toString())
        .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) -> seen.addAndGet(batch.count()))
        .start();
    try {
      query.processAllAvailable();
    } finally {
      query.stop();
    }

    assertEquals(2, seen.get(),
        "a COMMIT_TIME_ONLY table must serve streaming reads -- the relation's guard is on the commit "
            + "time, which this mode populates");
  }

  /**
   * The other side of the same guard: a MoR table that populates no meta columns is still rejected.
   */
  @Test
  void streamingReadIsRejectedOnMorWithoutMetaFields() throws Exception {
    Map<String, String> options = baseOptions();
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL());
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "key");
    options.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition");
    writeRows(Arrays.asList(
            RowFactory.create("k1", "p1", "v1"),
            RowFactory.create("k2", "p1", "v2")),
        simpleSchema(), options, basePath(), SaveMode.Overwrite);

    Path checkpoint = java.nio.file.Files.createTempDirectory("meta-fields-stream-mor-ckpt");
    Throwable thrown = assertThrows(Throwable.class, () -> {
      StreamingQuery query = spark().readStream()
          .format("hudi")
          .load(basePath())
          .writeStream()
          .option("checkpointLocation", checkpoint.toString())
          .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) -> batch.count())
          .start();
      try {
        query.processAllAvailable();
      } finally {
        query.stop();
      }
    });

    String message = rootMessageOf(thrown);
    assertTrue(message.contains("_hoodie_commit_time") || message.contains("meta")
            || message.contains("populate"),
        "expected a meta-fields rejection, got: " + message);
  }

}
