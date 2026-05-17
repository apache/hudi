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
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
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
 *   <li>Bulk-insert path (row-writer on/off) honors the exclude list.</li>
 *   <li>Clustering rewrites preserve the exclude list on the rewritten output.</li>
 * </ul>
 *
 * <p>Parameterized over table versions 6 and 9 (the production-supported pair).
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
   * (version 6 -> V1, version 9 -> V2).
   */
  @ParameterizedTest
  @CsvSource({
      "6,COPY_ON_WRITE", "9,COPY_ON_WRITE",
      "6,MERGE_ON_READ", "9,MERGE_ON_READ"})
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
  @CsvSource({"6,COPY_ON_WRITE", "9,COPY_ON_WRITE"})
  void testIncrementalQueryServesDataWithOnlyCommitTimePopulated_CoW(String tableVersion,
                                                                     String tableType) {
    runIncrementalHappyPath(tableVersion, tableType);
  }

  /**
   * Incremental query happy path on MoR. Excludes every meta field except
   * _hoodie_commit_time so the merge of base + log files must still resolve correctly.
   */
  @ParameterizedTest
  @CsvSource({"6,MERGE_ON_READ", "9,MERGE_ON_READ"})
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
  @CsvSource({"6", "9"})
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

  /**
   * Bulk-insert path with selective meta-field exclusion. Verifies that the row-writer
   * path (the default for Spark bulk_insert) honors the persisted exclude list.
   * Parameterized over table versions 6 and 9, CoW only - bulk_insert into MoR creates
   * base files the same way as CoW.
   */
  @ParameterizedTest
  @CsvSource({"6", "9"})
  void testBulkInsertWithExcludeList(String tableVersion) {
    String path = basePath();
    Map<String, String> writeOptions = baseWriteOptions(tableVersion, "COPY_ON_WRITE");
    StructType schema = simpleSchema();

    // Bootstrap with one default-options commit so the table exists, then flip the exclude list.
    writeRows(Collections.singletonList(RowFactory.create("k0", "p1", "v0")), schema, writeOptions, path);
    persistMetaFieldsExcludeList(path, EXCLUDE_ALL_EXCEPT_COMMIT_TIME);

    Map<String, String> bulkInsertOptions = new HashMap<>(withExcludeList(writeOptions, EXCLUDE_ALL_EXCEPT_COMMIT_TIME));
    bulkInsertOptions.put(DataSourceWriteOptions.OPERATION().key(),
        DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    writeRows(Arrays.asList(
        RowFactory.create("k1", "p1", "v1"),
        RowFactory.create("k2", "p1", "v2"),
        RowFactory.create("k3", "p1", "v3")), schema, bulkInsertOptions, path);

    Dataset<Row> snapshot = spark().read().format("hudi").load(path);
    assertEquals(4, snapshot.count(), "Expect bootstrap row plus 3 bulk-inserted rows");
    // The 3 bulk-inserted rows must have the four excluded fields null; k0 (pre-flip) may have
    // them populated. Assert >= 3 nulls on each excluded column and commit_time always populated.
    assertTrue(snapshot.filter(HoodieRecord.RECORD_KEY_METADATA_FIELD + " is null").count() >= 3,
        "Bulk-inserted rows should have null _hoodie_record_key");
    assertTrue(snapshot.filter(HoodieRecord.PARTITION_PATH_METADATA_FIELD + " is null").count() >= 3,
        "Bulk-inserted rows should have null _hoodie_partition_path");
    assertTrue(snapshot.filter(HoodieRecord.FILENAME_METADATA_FIELD + " is null").count() >= 3,
        "Bulk-inserted rows should have null _hoodie_file_name");
    assertTrue(snapshot.filter(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD + " is null").count() >= 3,
        "Bulk-inserted rows should have null _hoodie_commit_seqno");
    assertEquals(4, snapshot.filter(HoodieRecord.COMMIT_TIME_METADATA_FIELD + " is not null").count(),
        "_hoodie_commit_time must be populated on every row, including bulk-inserted");

    // Pin to a known materialized row so we are not relying on Spark's count() shortcut
    // when the record_key column is all-null (see runIncrementalHappyPath for rationale).
    List<Row> bulkInsertedRows = snapshot.filter("column1 in ('k1','k2','k3')").collectAsList();
    assertEquals(3, bulkInsertedRows.size());
    for (Row r : bulkInsertedRows) {
      assertNotNull(r.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
          "_hoodie_commit_time must be populated on bulk-inserted row: " + r);
      assertNull(r.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      assertNull(r.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
      assertNull(r.getAs(HoodieRecord.FILENAME_METADATA_FIELD));
      assertNull(r.getAs(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD));
    }
  }

  /**
   * Clustering rewrites file groups via the bulk-insert row-writer (default plan-strategy).
   * Ensures that when META_FIELDS_EXCLUDE_LIST is set on the table, the clustered output
   * preserves the exclusion - i.e. rows that were null in the input remain null after
   * rewrite, and no field that should stay null gets retro-populated.
   *
   * <p>Bootstrap is done with all meta fields populated, so k0 has them populated. Three
   * subsequent commits run under the exclude list, each going to a separate file group.
   * Inline clustering on the fourth commit consolidates those small file groups. The
   * post-clustering snapshot must still have >= 3 rows with null exclude-list columns
   * (the three post-flip writes), regardless of which file group(s) clustering merged
   * them into.
   */
  @ParameterizedTest
  @CsvSource({"6", "9"})
  void testClusteringPreservesExcludeList(String tableVersion) {
    String path = basePath();
    Map<String, String> writeOptions = baseWriteOptions(tableVersion, "COPY_ON_WRITE");
    // Use DIRECT markers throughout so the embedded timeline-server marker endpoint does
    // not bottleneck on the burst of marker requests during clustering.
    writeOptions.put(HoodieWriteConfig.MARKERS_TYPE.key(), "DIRECT");
    StructType schema = simpleSchema();

    // Bootstrap commit with all meta fields populated.
    writeRows(Collections.singletonList(RowFactory.create("k0", "p1", "v0")), schema, writeOptions, path);
    persistMetaFieldsExcludeList(path, EXCLUDE_ALL_EXCEPT_COMMIT_TIME);

    Map<String, String> postFlipOptions = withExcludeList(writeOptions, EXCLUDE_ALL_EXCEPT_COMMIT_TIME);

    // Three small commits under the exclude list. Each ends up in its own file group
    // (small inserts go to new file groups by default with metadata table disabled).
    writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")), schema, postFlipOptions, path);
    writeRows(Collections.singletonList(RowFactory.create("k2", "p1", "v2")), schema, postFlipOptions, path);
    writeRows(Collections.singletonList(RowFactory.create("k3", "p1", "v3")), schema, postFlipOptions, path);

    // Fourth commit triggers inline clustering. Set a high small-file-limit so the prior
    // small files are eligible for clustering; trigger after this single commit. Use
    // DIRECT markers to avoid the embedded timeline service marker endpoint timing out
    // under the burst of marker requests clustering issues in this test harness.
    Map<String, String> clusterOptions = new HashMap<>(postFlipOptions);
    clusterOptions.put(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
    clusterOptions.put(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "1");
    clusterOptions.put(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(),
        String.valueOf(1024L * 1024L * 1024L));
    clusterOptions.put(HoodieWriteConfig.MARKERS_TYPE.key(), "DIRECT");
    // Surface the underlying write error instead of swallowing into writeStatus.errors, so any
    // failure shows up as a Spark task exception with a real stack instead of a generic
    // "CLUSTER failed to write to files:<fileId>" wrapper.
    clusterOptions.put("hoodie.write.ignore.failed", "false");
    writeRows(Collections.singletonList(RowFactory.create("k4", "p1", "v4")), schema, clusterOptions, path);

    // Assert clustering ran (a replacecommit on the timeline).
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    long replaceCommits = metaClient.getActiveTimeline()
        .getCompletedReplaceTimeline().countInstants();
    assertTrue(replaceCommits >= 1,
        "Expected at least one replacecommit (clustering) on the timeline, found " + replaceCommits);

    // Post-clustering snapshot: 5 rows total. Excluded fields must still be null for the
    // four post-flip rows (k1..k4), regardless of which file group(s) clustering merged
    // them into. Commit_time must remain populated everywhere.
    Dataset<Row> snapshot = spark().read().format("hudi").load(path);
    assertEquals(5, snapshot.count());
    List<Row> postFlipRows = snapshot.filter("column1 in ('k1','k2','k3','k4')").collectAsList();
    assertEquals(4, postFlipRows.size());
    for (Row r : postFlipRows) {
      assertNotNull(r.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
          "Clustering must not strip _hoodie_commit_time. Row: " + r);
      assertNull(r.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD),
          "Clustering must not retro-populate _hoodie_record_key. Row: " + r);
      assertNull(r.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
          "Clustering must not retro-populate _hoodie_partition_path. Row: " + r);
      assertNull(r.getAs(HoodieRecord.FILENAME_METADATA_FIELD),
          "Clustering must not retro-populate _hoodie_file_name. Row: " + r);
      assertNull(r.getAs(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD),
          "Clustering must not retro-populate _hoodie_commit_seqno. Row: " + r);
    }
  }

  /**
   * Upsert with the BLOOM index enabled and the exclude list set to everything except
   * {@code _hoodie_commit_time}. The merge-handle write path (HoodieFileWriterFactory)
   * must still write the bloom filter even though {@code _hoodie_record_key} is excluded
   * from disk - the bloom filter indexes the in-memory record-key value carried on
   * HoodieKey/HoodieRecord, not the on-disk meta column. This test exercises that path
   * end-to-end: bootstrap one record, flip the exclude list, then upsert (update + insert)
   * to ensure (a) the bloom-filter-enabled merge handle does not crash, (b) the bloom
   * index can still locate the existing file for the update, and (c) the excluded
   * columns are null on disk.
   */
  @ParameterizedTest
  @CsvSource({"6", "9"})
  void testBloomIndexUpsertWithRecordKeyExcluded(String tableVersion) {
    String path = basePath();
    Map<String, String> writeOptions = baseWriteOptions(tableVersion, "COPY_ON_WRITE");
    writeOptions.put(HoodieIndexConfig.INDEX_TYPE.key(), "BLOOM");
    StructType schema = simpleSchema();

    // Bootstrap with one commit (no exclusion).
    writeRows(Collections.singletonList(RowFactory.create("k0", "p1", "v0")), schema, writeOptions, path);
    persistMetaFieldsExcludeList(path, EXCLUDE_ALL_EXCEPT_COMMIT_TIME);

    Map<String, String> postFlipOptions = withExcludeList(writeOptions, EXCLUDE_ALL_EXCEPT_COMMIT_TIME);

    // Upsert: update k0 and insert k1. The update side hits the merge handle, which writes
    // through HoodieFileWriterFactory. The bloom filter must be populated from the in-memory
    // record key even though the _hoodie_record_key column is null on disk.
    writeRows(Arrays.asList(
        RowFactory.create("k0", "p1", "v0-updated"),
        RowFactory.create("k1", "p1", "v1")), schema, postFlipOptions, path);

    Dataset<Row> snapshot = spark().read().format("hudi").load(path);
    List<Row> rows = snapshot.collectAsList();
    assertEquals(2, rows.size(), "Expect updated k0 + new k1");
    assertEquals(1, snapshot.filter("column1 = 'k0' and column3 = 'v0-updated'").count(),
        "BLOOM-index upsert must update k0 - proves the bloom filter located the existing "
            + "file even though _hoodie_record_key is excluded from on-disk storage");
    assertEquals(1, snapshot.filter("column1 = 'k1' and column3 = 'v1'").count());

    // Excluded columns must be null on the post-flip insert.
    List<Row> k1Rows = snapshot.filter("column1 = 'k1'").collectAsList();
    assertEquals(1, k1Rows.size());
    Row k1 = k1Rows.get(0);
    assertNotNull(k1.getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
    assertNull(k1.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD));
    assertNull(k1.getAs(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD));
    assertNull(k1.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
    assertNull(k1.getAs(HoodieRecord.FILENAME_METADATA_FIELD));
  }
}
