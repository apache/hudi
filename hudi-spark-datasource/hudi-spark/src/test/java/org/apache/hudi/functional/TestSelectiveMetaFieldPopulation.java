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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spark-datasource tests for the META_FIELDS_EXCLUDE_LIST table property.
 *
 * <p>The supported way to set the exclude list on an existing table is via
 * {@link HoodieTableConfig#update}, mirroring what the {@code hudi-cli table update-configs}
 * command does. Setting it as a writer option would be rejected by
 * {@code HoodieWriterUtils.validateTableConfig} as a config conflict. Subsequent writes
 * must pass the same exclude list as a writer option to satisfy that same conflict check.
 *
 * <p>Covers:
 * <ul>
 *   <li>Incremental query error path: {@code _hoodie_commit_time} excluded ⇒ query fails.</li>
 *   <li>Incremental query happy path: only {@code _hoodie_commit_time} populated, CoW + MoR.</li>
 *   <li>MoR upsert + snapshot + compaction + snapshot with only {@code _hoodie_commit_time}.</li>
 * </ul>
 */
class TestSelectiveMetaFieldPopulation extends SparkClientFunctionalTestHarness {

  private static final String EXCLUDE_ALL_EXCEPT_COMMIT_TIME =
      HoodieRecord.COMMIT_SEQNO_METADATA_FIELD + ","
          + HoodieRecord.RECORD_KEY_METADATA_FIELD + ","
          + HoodieRecord.PARTITION_PATH_METADATA_FIELD + ","
          + HoodieRecord.FILENAME_METADATA_FIELD;

  private static final String EXCLUDE_ALL =
      HoodieRecord.COMMIT_TIME_METADATA_FIELD + ","
          + HoodieRecord.COMMIT_SEQNO_METADATA_FIELD + ","
          + HoodieRecord.RECORD_KEY_METADATA_FIELD + ","
          + HoodieRecord.PARTITION_PATH_METADATA_FIELD + ","
          + HoodieRecord.FILENAME_METADATA_FIELD;

  /**
   * Incremental query must fail when _hoodie_commit_time is in the exclusion list.
   * Exercises CoW V1 / V2 and MoR V1 / V2 incremental relations via table-version parameterization
   * (version 6 -> V1, versions 8 and 9 -> V2).
   */
  @ParameterizedTest
  @CsvSource({
      "6,COPY_ON_WRITE", "8,COPY_ON_WRITE", "9,COPY_ON_WRITE",
      "6,MERGE_ON_READ", "8,MERGE_ON_READ", "9,MERGE_ON_READ"})
  void testIncrementalQueryFailsWhenCommitTimeExcluded(String tableVersion, String tableType) {
    String path = basePath();
    Map<String, String> writeOptions = baseWriteOptions(tableVersion, tableType);
    StructType schema = simpleSchema();

    // Create the table with two commits using all-meta-fields-populated.
    writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")), schema, writeOptions, path);
    writeRows(Collections.singletonList(RowFactory.create("k2", "p1", "v2")), schema, writeOptions, path);

    // Persist the exclude list (covering commit_time) directly via HoodieTableConfig.update -
    // the CLI-equivalent path. Writer-option route would be rejected by validateTableConfig.
    persistMetaFieldsExcludeList(path, EXCLUDE_ALL);

    HoodieException ex = assertThrows(HoodieException.class, () ->
        spark().read().format("hudi")
            .option(DataSourceReadOptions.QUERY_TYPE().key(),
                DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
            .option(DataSourceReadOptions.START_COMMIT().key(), "000")
            .load(path)
            .collect());

    String message = ex.getMessage() == null ? "" : ex.getMessage();
    assertTrue(
        message.contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
            || message.toLowerCase().contains("incremental"),
        "Expected error to mention commit_time exclusion / incremental, was: " + message);
  }

  /**
   * Incremental query happy path on CoW: exclude every meta field except _hoodie_commit_time
   * and verify the query still returns the right rows.
   */
  @ParameterizedTest
  @CsvSource({"6,COPY_ON_WRITE", "8,COPY_ON_WRITE", "9,COPY_ON_WRITE"})
  void testIncrementalQueryServesDataWithOnlyCommitTimePopulated_CoW(String tableVersion,
                                                                     String tableType) {
    runIncrementalHappyPath(tableVersion, tableType);
  }

  /**
   * Incremental query happy path on MoR. Excludes every meta field except
   * _hoodie_commit_time so the merge of base + log files must still resolve correctly.
   */
  @ParameterizedTest
  @CsvSource({"6,MERGE_ON_READ", "8,MERGE_ON_READ", "9,MERGE_ON_READ"})
  void testIncrementalQueryServesDataWithOnlyCommitTimePopulated_MoR(String tableVersion,
                                                                     String tableType) {
    runIncrementalHappyPath(tableVersion, tableType);
  }

  /**
   * MoR write -> snapshot -> compaction -> snapshot, with only _hoodie_commit_time populated.
   * Ensures (a) snapshot reads merging base + log files do not assume the four excluded
   * fields are present and (b) compaction preserves the null-meta-field invariant.
   */
  @ParameterizedTest
  @CsvSource({"8", "9"})
  void testMoRSnapshotAndCompactionWithOnlyCommitTimePopulated(String tableVersion) {
    String path = basePath();
    Map<String, String> writeOptions = baseWriteOptions(tableVersion, "MERGE_ON_READ");
    writeOptions.put(HoodieCompactionConfig.INLINE_COMPACT.key(), "false");
    StructType schema = simpleSchema();

    // Bootstrap the table with one delta commit so it exists. Then flip the exclude list.
    writeRows(Collections.singletonList(RowFactory.create("k0", "p1", "v0")), schema, writeOptions, path);
    persistMetaFieldsExcludeList(path, EXCLUDE_ALL_EXCEPT_COMMIT_TIME);

    // Subsequent writes must echo the same exclude list to satisfy the writer-side conflict check.
    Map<String, String> writeOptionsWithExclude = withExcludeList(writeOptions, EXCLUDE_ALL_EXCEPT_COMMIT_TIME);

    // delta commit 2: insert.
    writeRows(Arrays.asList(
        RowFactory.create("k1", "p1", "v1"),
        RowFactory.create("k2", "p1", "v2")), schema, writeOptionsWithExclude, path);

    // delta commit 3: update k1, insert k3.
    writeRows(Arrays.asList(
        RowFactory.create("k1", "p1", "v1-updated"),
        RowFactory.create("k3", "p1", "v3")), schema, writeOptionsWithExclude, path);

    Dataset<Row> preCompactSnapshot = spark().read().format("hudi").load(path);
    assertEquals(4, preCompactSnapshot.count(),
        "Snapshot read pre-compaction must surface all 4 keys (k0, k1 updated, k2, k3)");
    assertEquals(1, preCompactSnapshot.filter("column1 = 'k1' and column3 = 'v1-updated'").count());
    // Excluded fields must be null for rows written post-flip (k1, k2, k3). k0 was written before
    // the flip so its excluded fields may be populated -- assert at least 3 are null.
    assertTrue(preCompactSnapshot.filter(HoodieRecord.RECORD_KEY_METADATA_FIELD + " is null").count() >= 3,
        "At least 3 rows (post-flip writes) should have null _hoodie_record_key");
    // _hoodie_commit_time must always be populated.
    assertEquals(4, preCompactSnapshot.filter(HoodieRecord.COMMIT_TIME_METADATA_FIELD + " is not null").count());

    // delta commit 4 + inline compaction.
    Map<String, String> compactOptions = new HashMap<>(writeOptionsWithExclude);
    compactOptions.put(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
    compactOptions.put(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
    writeRows(Collections.singletonList(RowFactory.create("k2", "p1", "v2-updated")), schema, compactOptions, path);

    Dataset<Row> postCompactSnapshot = spark().read().format("hudi").load(path);
    assertEquals(4, postCompactSnapshot.count());
    assertEquals(1, postCompactSnapshot.filter("column1 = 'k1' and column3 = 'v1-updated'").count());
    assertEquals(1, postCompactSnapshot.filter("column1 = 'k2' and column3 = 'v2-updated'").count());
    assertEquals(4, postCompactSnapshot.filter(HoodieRecord.COMMIT_TIME_METADATA_FIELD + " is not null").count());

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    assertTrue(metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants()
            .getInstantsAsStream().anyMatch(i -> i.getAction().equals("commit")),
        "Expected a completed compaction (commit) instant on the timeline");
  }

  private void runIncrementalHappyPath(String tableVersion, String tableType) {
    String path = basePath();
    Map<String, String> writeOptions = baseWriteOptions(tableVersion, tableType);
    StructType schema = simpleSchema();

    // Bootstrap with one commit (under default populate=all) so the table exists, then flip.
    writeRows(Collections.singletonList(RowFactory.create("k0", "p1", "v0")), schema, writeOptions, path);
    persistMetaFieldsExcludeList(path, EXCLUDE_ALL_EXCEPT_COMMIT_TIME);

    Map<String, String> writeOptionsWithExclude = withExcludeList(writeOptions, EXCLUDE_ALL_EXCEPT_COMMIT_TIME);

    // Commits 2, 3, 4 are written under the new exclude-list state.
    writeRows(Arrays.asList(
        RowFactory.create("k1", "p1", "v1"),
        RowFactory.create("k2", "p1", "v2")), schema, writeOptionsWithExclude, path);
    writeRows(Collections.singletonList(RowFactory.create("k3", "p1", "v3")), schema, writeOptionsWithExclude, path);
    writeRows(Collections.singletonList(RowFactory.create("k4", "p1", "v4")), schema, writeOptionsWithExclude, path);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    // Use the completed-commit timeline so each commit shows up exactly once. Active-timeline
    // getInstants() returns requested/inflight/completed states separately, which would interleave
    // and break positional indexing below.
    List<Pair<HoodieInstant, String>> sortedInstants = metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants().getInstants()
        .stream()
        .map(instant -> Pair.of(
            instant, tableVersion.equals("6") ? instant.requestedTime() : instant.getCompletionTime()))
        .collect(Collectors.toList());

    // Snapshot sanity: 5 rows total, the four excluded fields are null on the 4 post-flip rows.
    Dataset<Row> snapshot = spark().read().format("hudi").load(path);
    assertEquals(5, snapshot.count());
    assertTrue(snapshot.filter(HoodieRecord.RECORD_KEY_METADATA_FIELD + " is null").count() >= 4);
    assertTrue(snapshot.filter(HoodieRecord.PARTITION_PATH_METADATA_FIELD + " is null").count() >= 4);
    assertTrue(snapshot.filter(HoodieRecord.FILENAME_METADATA_FIELD + " is null").count() >= 4);
    assertTrue(snapshot.filter(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD + " is null").count() >= 4);
    assertEquals(5, snapshot.filter(HoodieRecord.COMMIT_TIME_METADATA_FIELD + " is not null").count());

    // Incremental query covering commits 3 and 4 (post-flip).
    Dataset<Row> incremental = spark().read().format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(),
            DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), sortedInstants.get(1).getRight())
        .option(DataSourceReadOptions.END_COMMIT().key(), sortedInstants.get(3).getRight())
        .load(path);

    // NOTE: prefer collectAsList() over count()/filter().count() here. With selective meta
    // exclusion, the _hoodie_record_key column is all-null on post-flip rows; Spark's
    // Parquet count/predicate-pushdown shortcut can read all-null column statistics and
    // skip materializing rows entirely, returning 0 even though rows exist. collectAsList()
    // forces materialization and avoids the stats shortcut.
    List<Row> incrementalRows = incremental.collectAsList();
    assertEquals(2, incrementalRows.size(),
        "Incremental query should return rows from commits 3 and 4 only");
    List<String> incrementalKeys = incrementalRows.stream()
        .map(r -> r.<String>getAs("column1"))
        .collect(Collectors.toList());
    assertTrue(incrementalKeys.contains("k3"), "Expected k3 in incremental, was: " + incrementalKeys);
    assertTrue(incrementalKeys.contains("k4"), "Expected k4 in incremental, was: " + incrementalKeys);

    Row sample = incrementalRows.get(0);
    assertNotNull(sample.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
    assertNull(sample.getAs(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD));
    assertNull(sample.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD));
    assertNull(sample.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
    assertNull(sample.getAs(HoodieRecord.FILENAME_METADATA_FIELD));
  }

  private void persistMetaFieldsExcludeList(String path, String value) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    Properties update = new Properties();
    update.setProperty(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key(), value);
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), update);

    // Verify the property landed.
    HoodieTableMetaClient reread =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    assertEquals(value,
        reread.getTableConfig().getProps().getProperty(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key()),
        "META_FIELDS_EXCLUDE_LIST should be persisted to hoodie.properties");
  }

  private Map<String, String> withExcludeList(Map<String, String> base, String value) {
    Map<String, String> next = new HashMap<>(base);
    next.put(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key(), value);
    return next;
  }

  private void writeRows(List<Row> records, StructType schema, Map<String, String> options, String path) {
    spark().createDataset(records,
            SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema))
        .write()
        .format("hudi")
        .options(options)
        .mode(SaveMode.Append)
        .save(path);
  }

  private Map<String, String> baseWriteOptions(String tableVersion, String tableType) {
    Map<String, String> opts = new HashMap<>();
    opts.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "column3");
    opts.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "column1");
    opts.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "column2");
    opts.put(HoodieTableConfig.NAME.key(), "test_selective_meta_fields");
    opts.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersion);
    opts.put(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    opts.put(HoodieMetadataConfig.ENABLE.key(), "false");
    opts.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType);
    return opts;
  }

  private StructType simpleSchema() {
    return DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("column1", DataTypes.StringType, true),
        DataTypes.createStructField("column2", DataTypes.StringType, true),
        DataTypes.createStructField("column3", DataTypes.StringType, true)
    }).asNullable();
  }
}
